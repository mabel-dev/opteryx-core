# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Pronouns inside a relation NAME — `personal.$me.dataset`.

`$me` stands for the session's own username, so a statement addressing the
caller's personal collection is the same text for everybody who runs it. It is
a PRONOUN, not a variable reference: there is exactly one, it can only ever
mean the caller, and it is resolved here and nowhere else.

The parser already hands it through: `$` is an identifier character in this
dialect (`OpteryxDialect::is_identifier_start`), so `$me` between two dots is
an ordinary identifier part and a name carrying one arrives as a plain
three-part `ObjectName`. This module is the ONE place that resolves it, and it
runs in the AST rewriter so that everything after it — the logical planner's
name joins, the binder's permission gates, the catalog cache, egress,
telemetry — sees only the substituted name. There is no second resolution
point and no downstream code that has to know the feature exists.

WHAT IS NOT "AFTER IT"
----------------------
`analyze_query` and the describe half of `Session.check` read the PRE-rewrite
AST — they have to, because the rewriter substitutes `:name` placeholders and
they report which were written. They are not downstream of this, so they called
it on nothing and reported `personal.$me.x` as the relation a statement names.
A caller matching that against grants matches nothing, which is how a `$me`
relation the reader owns was refused in production as unauthorized rather than
reported as missing.

So both call this FIRST, on the AST they are about to describe, and
`analyze_query` takes the username to resolve against (it has no session). That
is a second CALL SITE, not a second resolution: the pronoun is still read in
exactly one place, and substituting early is invisible to the rewriter's own
pass, which sees a name that is no longer a pronoun and leaves it alone.

WHY A PRONOUN RATHER THAN A VARIABLE
------------------------------------
This started as `@@external_user`, reusing the system-variable namespace, and
that shape brought two problems a single pronoun does not have:

- It invited "which variables?", and the answer had to be an allowlist —
  because a relation that cannot be found reports the name it looked for, so
  an unrestricted substitution would turn `FROM x.@@local_store_root.y` into a
  read channel for RESTRICTED variables.
- `@@` means "read this variable" everywhere else in the language. Inside a
  name it is not a read; nothing is being selected.

`$me` has no allowlist to get wrong, and `$` already means "the engine's own"
in this dialect — `$planets`, `$variables`, `$user`. It also FAILS CLOSED in a
way `@@` did not: `opteryx_access.is_engine_private` denies any resource with
a `$`-prefixed segment, consulted BEFORE any grant, so a name that somehow
reached a permission check unresolved is refused outright rather than merely
failing to match a pattern.

WHY THE POSITIONS ARE ENUMERATED
--------------------------------
Substitution is applied to ObjectName positions ONLY — reached by key, not by
shape. Shape alone cannot do it: sqlparser renders `GROUP BY x` as a list of
Identifier dicts, which is the same shape as an ObjectName. So the walk
matches the keys that hold relation names and excludes `Function.name`, which
shares the `name` key.

A position NOT enumerated here leaves the `$me` in the name, which then fails
as an unknown relation (and, at a permission check, as engine-private) — loud
and wrong-looking rather than silent. But it is a gap, so add the key here
rather than working around it at the far end.

VALUES ARE VALIDATED, NEVER REPAIRED
------------------------------------
The username is spliced in as ONE name part, so it must be one name part. A
value containing a dot would change the name's ARITY — `personal.$me.ds` with
a dotted username resolves to a four-part name addressing a different
workspace — and that must fail, not be guessed at or quietly quoted. Note the
consequence: if usernames ever become email addresses, this refuses rather
than mangling, and the fix is a name-safe identity, not sanitising here.
"""

import re
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from opteryx.exceptions import SqlError
from opteryx.exceptions import compose
from opteryx.exceptions import md_code
from opteryx.exceptions import md_column

# The pronoun, and the session variable it stands for. One entry: `$me` is the
# only thing a reader can say about themselves that a NAME can carry. Adding a
# second is not a matter of appending here — it needs the same argument this
# one has, that the value is always name-shaped and always the caller's own.
PRONOUNS: Dict[str, str] = {"$me": "external_user"}

# The keys whose value is an ObjectName - a dotted relation name as a list of
# Identifier parts. `name` covers Table (FROM, JOIN, DELETE, MERGE, TRUNCATE,
# OPTIMIZE) and the DDL statements (CREATE TABLE, CREATE VIEW, ALTER TABLE);
# `TableName` is INSERT's spelling, `table_name` is ANALYZE's, `parent_name` is
# SHOW COLUMNS FROM's and `obj_name` is SHOW CREATE's.
#
# The rest are the aside parser's (`src/aside/`). There are more than a tidy
# `name` + `table` because several of those statements name SEVERAL relations
# that mean different things, and collapsing them would make the planner guess
# which it had: a schedule trigger names its holder (`table`), the task it
# fires (`task`) and the dataset its runs are windowed over (`window_source`);
# fork maintenance names the fork (`relation`); the egress exemption names a
# workspace AND the task or view copying out of it (`object`).
#
# Several of these keys are shared with sqlparser - `table` is MERGE's
# TableFactor, `relation` is a FROM item - but both hold a DICT there, and the
# ObjectName shape test below (a LIST of Identifier parts) tells them apart.
OBJECT_NAME_KEYS = frozenset(
    {
        "name",
        "TableName",
        "table_name",
        "table",
        "task",
        "window_source",
        "relation",
        "object",
        "parent_name",
        "obj_name",
    }
)

# The key whose value is a LIST of ObjectNames - `DROP TABLE a.b, c.d`.
OBJECT_NAME_LIST_KEYS = frozenset({"names"})

# Keys whose value is a list of BARE `{"value": ...}` parts rather than
# Identifier-wrapped ones, qualified by the parent that may carry them:
#
#   CompoundIdentifier - a qualified column reference, `personal.$me.ds.col`.
#   ShowVariable       - every `SHOW <words>` form the parser does not recognise
#                        as its own statement, flattened to words with the dots
#                        dropped: `SHOW SNAPSHOTS FOR a.b.c` is
#                        ["SNAPSHOTS", "FOR", "a", "b", "c"]. The control words
#                        are matched case-folded against a fixed vocabulary, so
#                        a pronoun word can only ever be part of the name.
#
# `variable` is qualified by parent BECAUSE `Set` uses the same key for the
# variable it is ASSIGNING - `SET @@disable_optimizer = true` - which is a write
# to the variable, not a name to resolve.
VALUE_PART_KEYS = {"CompoundIdentifier": None, "variable": "ShowVariable"}

# A substituted part must be exactly one name part: no dot (which would change
# the name's arity), no space, nothing that needs quoting. `$` and `*` are
# excluded too - the first is the engine-private prefix, the second a glob in
# every permission pattern.
NAME_PART = re.compile(r"^[A-Za-z0-9_\-]+$")


def _pronoun(written: str) -> Optional[str]:
    """The variable `written` stands for, or None if it is not a pronoun.

    Case-insensitive, because a pronoun is a keyword and keywords in this
    dialect are. A QUOTED part is never a pronoun - `` `$me` `` is a name the
    reader deliberately escaped - and that is checked by the caller, which is
    the only place the quoting is visible.
    """
    return PRONOUNS.get(written.lower())


def _resolve(written: str, variables) -> str:
    """Resolve one pronoun to the session's value, or fail saying why."""
    name = _pronoun(written)

    # `variables` is None for a plan built without a session - an API-assembled
    # statement, a test harness. There is no identity to substitute, and
    # guessing one would address another user's data.
    if variables is None or name not in variables:
        raise SqlError(
            compose(
                f"{md_column(written)} cannot be read here, so the relation name it "
                f"appears in cannot be resolved",
            )
        )

    value = variables[name]

    if not value:
        raise SqlError(
            compose(
                f"{md_column(written)} has no value for this session, so the relation "
                f"name it appears in cannot be resolved",
            )
        )

    if not isinstance(value, str) or not NAME_PART.match(value):
        raise SqlError(
            compose(
                f"{md_column(written)} stands for {md_code(value)}, which is not a name "
                f"and cannot be used as part of one",
                f"A relation name part may hold letters, digits, {md_code('_')} and "
                f"{md_code('-')} only",
            )
        )

    return value


def _substitute_object_name(parts: List[Any], variables) -> None:
    """Rewrite the pronoun parts of one ObjectName, in place."""
    for part in parts:
        identifier = part.get("Identifier")
        # A quoted part is a name the reader escaped, never a pronoun.
        if identifier.get("quote_style") is not None:
            continue
        if _pronoun(identifier["value"]) is not None:
            identifier["value"] = _resolve(identifier["value"], variables)


def _is_object_name(value: Any) -> bool:
    """Whether `value` is an ObjectName - a non-empty list of Identifier parts."""
    return (
        isinstance(value, list)
        and len(value) > 0
        and all(isinstance(part, dict) and tuple(part) == ("Identifier",) for part in value)
    )


def _walk(node: Union[Dict, List], variables, parent_key: Optional[str] = None) -> None:
    if isinstance(node, list):
        for item in node:
            _walk(item, variables, parent_key)
        return
    if not isinstance(node, dict):
        return

    for key, value in node.items():
        # `Function` shares the `name` key with the relation-bearing nodes. A
        # function called `$me` is not a pronoun, but it is not a relation
        # either, so it is left exactly as written to fail as an unknown
        # function.
        if key in OBJECT_NAME_KEYS and parent_key != "Function" and _is_object_name(value):
            _substitute_object_name(value, variables)
            continue
        if key in OBJECT_NAME_LIST_KEYS and isinstance(value, list):
            for object_name in value:
                if _is_object_name(object_name):
                    _substitute_object_name(object_name, variables)
            continue
        if key in VALUE_PART_KEYS and isinstance(value, list):
            required_parent = VALUE_PART_KEYS[key]
            if required_parent is None or parent_key == required_parent:
                for part in value:
                    written = part.get("value") if isinstance(part, dict) else None
                    if (
                        isinstance(written, str)
                        and part.get("quote_style") is None
                        and _pronoun(written) is not None
                    ):
                        part["value"] = _resolve(written, variables)
                continue
        _walk(value, variables, key)


def do_substitute_relation_pronouns(asts: List[dict], variables) -> List[dict]:
    """Resolve pronouns inside relation names, in place."""
    _walk(asts, variables)
    return asts
