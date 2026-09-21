# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Session variables inside a relation NAME - `personal.@@external_user.dataset`.

The parser already hands these through: `@@external_user` between two dots is an
ordinary identifier part, so a name carrying one arrives as a plain three-part
ObjectName and nothing downstream knows it is special. This module is the ONE
place that resolves it, and it runs in the AST rewriter so that everything after
it - the logical planner's name joins, the binder's permission gates, the catalog
cache, egress, telemetry - sees only the substituted name. There is no second
resolution point and no downstream code that has to know the feature exists.

WHY AN ALLOWLIST
----------------
`SUBSTITUTABLE` is a fixed set, NOT "any variable the container holds":

- A relation that cannot be found reports the name it looked for, so an
  unrestricted substitution would turn `FROM x.@@local_store_root.y` into a read
  channel for RESTRICTED variables - the exact defect `variables.as_column`
  documents, arriving through a door that never checks visibility.
- Most variables are not name-shaped anyway. An ARRAY or an INT64 spliced into a
  relation name is meaningless, and a variable whose value happens to be
  name-shaped today would silently become part of the addressing surface.

The set is public and identical for every session, so refusing a name that is not
in it discloses nothing about what else exists.

WHY THE POSITIONS ARE ENUMERATED
--------------------------------
Substitution is applied to ObjectName positions ONLY - reached by key, not by
shape. A bare `{"Identifier": {"value": "@@version"}}` is how `SELECT @@version`
is parsed, so a blanket walk would rewrite a variable READ into a column
reference. Shape alone cannot separate the two either: sqlparser renders
`GROUP BY x` as a list of Identifier dicts, which is the same shape as an
ObjectName. So the walk matches the keys that hold relation names and excludes
`Function.name`, which shares the `name` key.

A position NOT enumerated here leaves the `@@` in the name and fails as an
unknown relation, which is loud and wrong-looking rather than silent - but it is
a gap, so add the key here rather than working around it at the far end.

VALUES ARE VALIDATED, NEVER REPAIRED
------------------------------------
A value is spliced in as ONE name part, so it must be one name part. A value
containing a dot would change the name's ARITY - `personal.@@external_user.ds`
with a dotted user resolves to a four-part name addressing a different workspace
- and that must fail, not be guessed at or quietly quoted. Note the consequence:
if `external_user` holds an email address, this feature refuses rather than
mangling it, and the fix is a name-safe identity variable, not sanitising here.
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
from opteryx.exceptions import md_list

# The variables that may be written into a relation name. Both are VARCHAR,
# UNRESTRICTED and INTERNAL-owned (opteryx/variables.py) - the session's own
# identity, which it can already read with `SELECT @@name`.
SUBSTITUTABLE = ("billing_account", "external_user")

# The keys whose value is an ObjectName - a dotted relation name as a list of
# Identifier parts. `name` covers Table (FROM, JOIN, DELETE, MERGE, TRUNCATE,
# OPTIMIZE) and the DDL statements (CREATE TABLE, CREATE VIEW, ALTER TABLE);
# `TableName` is INSERT's spelling, `table_name` is ANALYZE's, `parent_name` is
# SHOW COLUMNS FROM's and `obj_name` is SHOW CREATE's. `table` is the aside
# parser's (`src/aside/`) for a relation a statement acts over - `CREATE TASK
# ... ON <table>`; sqlparser also uses `table`, but for a TableFactor dict, and
# the ObjectName shape test below tells the two apart.
# The rest are the aside parser's. There are more than a tidy `name` + `table`
# because several of these statements name SEVERAL relations that mean
# different things, and collapsing them would make the planner guess which it
# had: a schedule trigger names its holder (`table`), the task it fires
# (`task`) and the dataset its runs are windowed over (`window_source`); fork
# maintenance names the fork (`relation`); the egress exemption names a
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
#   CompoundIdentifier - a qualified column reference, `personal.@@u.ds.col`.
#   ShowVariable       - every `SHOW <words>` form the parser does not recognise
#                        as its own statement, flattened to words with the dots
#                        dropped: `SHOW SNAPSHOTS FOR a.b.c` is
#                        ["SNAPSHOTS", "FOR", "a", "b", "c"]. The control words
#                        are matched case-folded against a fixed vocabulary, so a
#                        `@@name` word can only ever be part of the relation name.
#
# `variable` is qualified by parent BECAUSE `Set` uses the same key for the
# variable it is ASSIGNING - `SET @@disable_optimizer = true` - which is a write
# to the variable, not a name to resolve.
VALUE_PART_KEYS = {"CompoundIdentifier": None, "variable": "ShowVariable"}

# A substituted part must be exactly one name part: no dot (which would change the
# name's arity), no space, nothing that needs quoting.
NAME_PART = re.compile(r"^[A-Za-z0-9_\-]+$")


def _resolve(written: str, variables) -> str:
    """Resolve one `@@name` name part to its value, or fail saying why."""
    name = written[2:]

    if name not in SUBSTITUTABLE:
        raise SqlError(
            compose(
                f"Variable {md_column(written)} cannot be used inside a relation name",
                f"The variables that can be are {md_list('@@' + v for v in SUBSTITUTABLE)}",
            )
        )

    # `variables` is None for a plan built without a session - an API-assembled
    # statement, a test harness. There is no identity to substitute, and guessing
    # one would address another user's data.
    if variables is None or name not in variables:
        raise SqlError(
            compose(
                f"Variable {md_column(written)} cannot be read here, so the relation "
                f"name it appears in cannot be resolved",
            )
        )

    value = variables[name]

    if not value:
        raise SqlError(
            compose(
                f"Variable {md_column(written)} is not set for this session, so the "
                f"relation name it appears in cannot be resolved",
            )
        )

    if not isinstance(value, str) or not NAME_PART.match(value):
        raise SqlError(
            compose(
                f"Variable {md_column(written)} holds {md_code(value)}, which is not a "
                f"name and cannot be used as part of one",
                f"A relation name part may hold letters, digits, {md_code('_')} and "
                f"{md_code('-')} only",
            )
        )

    return value


def _substitute_object_name(parts: List[Any], variables) -> None:
    """Rewrite the `@@name` parts of one ObjectName, in place."""
    for part in parts:
        identifier = part.get("Identifier")
        written = identifier["value"]
        if written.startswith("@@"):
            identifier["value"] = _resolve(written, variables)


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
        # function called `@@foo` is not a variable read, but it is not a relation
        # either, so it is left exactly as written to fail as an unknown function.
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
                    if isinstance(written, str) and written.startswith("@@"):
                        part["value"] = _resolve(written, variables)
                continue
        _walk(value, variables, key)


def do_substitute_relation_variables(asts: List[dict], variables) -> List[dict]:
    """Resolve `@@name` parts inside relation names, in place."""
    _walk(asts, variables)
    return asts
