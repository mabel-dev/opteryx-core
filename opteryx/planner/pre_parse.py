# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Statements recognized before the SQL parser sees them.

sqlparser's Opteryx dialect has no grammar for some of the statements this engine
supports - REFRESH has no statement at all, trigger statements are not in the
dialect's allowlist, and `DROP STATISTICS` mis-parses STATISTICS as a column name.
Each is matched here by regex and turned straight into the one-statement AST the
logical planner expects, so the rest of the pipeline never learns they are special.

This lives in its own module rather than inside the planner because it is the
FRONT of parsing, not part of planning, and it has two callers: `query_planner`,
which goes on to plan the statement, and `opteryx.analyze_query`, which only wants
to know what the statement is and what it touches. When these interceptors lived in
the planner alone, `analyze_query` handed the same SQL to sqlparser unprepared and
reported a syntax error for a statement the engine runs perfectly well - so a
caller that pre-flights a query (the jobs API does, to check permissions before
queueing) rejected every REFRESH, DROP TRIGGER and DROP STATISTICS submitted to it.
"""

import re

# --- value slots and parameter binding
#
# A synthesized statement is built from a regex match, not from the parser, so a
# `:name` in it never became a Placeholder node and could never be bound - it was
# captured as text and used as data. `GRANT ... TO USER :username` created a grant
# for a principal literally called ":username".
#
# The slots below that hold a VALUE (a principal, the object a grant is
# administered on) therefore accept a placeholder, and emit the same
# `{"Placeholder": ":name"}` node sqlparser emits, which the AST rewriter binds
# exactly as it binds one in a SELECT. The slots that hold an IDENTIFIER (a table,
# a trigger, a materialized view) do NOT: a parameterised relation name would let
# runtime data decide what the statement reads or writes, which is why the parser
# does not accept `SELECT * FROM :t` either.
#
# Placeholders come FIRST in each alternation so `:name` is read as a parameter
# rather than as a literal; the literal alternatives no longer admit a colon.
_PLACEHOLDER = r":[A-Za-z_]\w*|\?"
# A principal names a person: quoted (with '' / "" escaping the quote character,
# so an identity containing one is expressible), or bare. A bare principal may
# still contain a colon (`svc:account`) but may not START with one - that is the
# character the placeholder alternative claims, and the reason `:username` used
# to be taken as a literal principal name.
_PRINCIPAL_SLOT = (
    _PLACEHOLDER + r"|'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|[\w.@+-][\w.@:+-]*"
)
# A grant's object is a dotted name, checked for arity when it is planned.
_OBJECT_SLOT = _PLACEHOLDER + r"|[A-Za-z_][\w.$]*"


def _slot_value(raw: str):
    """Turn one captured value slot into what the synthesized AST should carry.

    A placeholder becomes the Placeholder node the AST rewriter binds; a quoted
    literal is unquoted, doubled quote characters collapsing to one; anything else
    is the bare text. The literal charsets exclude `:`, so a slot starting with one
    is unambiguously a placeholder and can never be read back as a literal.
    """
    if raw[0] in ":?":
        return {"Placeholder": raw}
    if raw[0] == "'":
        return raw[1:-1].replace("''", "'")
    if raw[0] == '"':
        return raw[1:-1].replace('""', '"')
    return raw


def resolve_slot_value(value, slot: str) -> str:
    """Read a value slot back at plan time, after the AST rewriter has run.

    The slot holds either the literal the regex captured, or - where it held a
    placeholder - the literal node the rewriter substituted for it. An UNBOUND
    placeholder arrives here still a Placeholder node (the rewriter does no work
    when no parameters were supplied) and fails with the same error any other
    unbound placeholder raises. It is never used as data: that is the whole point
    of routing these slots through the rewriter.

    Parameters:
        value: the slot, as it stands on the statement.
        slot:  what the slot is, for the error message.

    Returns:
        The string the statement should act on.
    """
    from opteryx.exceptions import ParameterError

    if isinstance(value, str):
        return value
    if "Placeholder" in value:
        raise ParameterError(
            "Unresolved parameter in query. Supply a value for every placeholder in the statement."
        )
    literal = value.get("Value")
    if isinstance(literal, dict) and "SingleQuotedString" in literal:
        return literal["SingleQuotedString"]
    raise ParameterError(
        f"The {slot} must be a string; the value supplied for its placeholder is not one."
    )


# DROP STATISTICS ON <table> [FOR COLUMNS <c1>, <c2>, ...]
# No placeholders: the table and the columns are identifiers, not values. A
# parameterised relation or column name would let runtime data decide what the
# statement acts on - the parser rejects `SELECT * FROM :t` for the same reason.
_DROP_STATS_RE = re.compile(
    r"^\s*DROP\s+STATISTICS\s+ON\s+(?P<table>[A-Za-z_][\w.$]*)"
    r"(?:\s+FOR\s+COLUMNS\s+(?P<cols>.+?))?\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_DROP_STATS_LEAD = re.compile(r"^\s*DROP\s+STATISTICS\b", re.IGNORECASE)


def _intercept_drop_statistics(clean_sql: str):
    """Recognize `DROP STATISTICS ON t [FOR COLUMNS …]` before the SQL parser.

    Returns a synthesized single-statement AST list, or None if the statement is
    not a DROP STATISTICS. A statement that begins with DROP STATISTICS but does
    not match the full grammar fails loudly rather than falling through to the
    parser (which would emit a confusing error or, worse, mis-parse it)."""
    if not _DROP_STATS_LEAD.match(clean_sql):
        return None
    match = _DROP_STATS_RE.match(clean_sql)
    if match is None:
        from opteryx.exceptions import UnsupportedSyntaxError

        raise UnsupportedSyntaxError(
            "Expected: DROP STATISTICS ON <table> [FOR COLUMNS <col>, ...]"
        )
    cols_raw = match.group("cols")
    columns = []
    if cols_raw:
        for part in cols_raw.split(","):
            name = part.strip().strip('"').strip("`")
            if name:
                columns.append(name)
    return [{"DropStatistics": {"table_name": match.group("table"), "columns": columns}}]


# DROP TRIGGER [IF EXISTS] <name> ON <table>
# No placeholders: both the trigger and the table are identifiers, not values.
# The table is REQUIRED: trigger names are only unique per dataset, and naming
# the table makes the permission target (WRITE on that table) explicit.
_DROP_TRIGGER_RE = re.compile(
    r"^\s*DROP\s+TRIGGER\s+(?P<if_exists>IF\s+EXISTS\s+)?"
    r"(?P<name>[A-Za-z_][\w$]*)\s+ON\s+(?P<table>[A-Za-z_][\w.$]*)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_DROP_TRIGGER_LEAD = re.compile(r"^\s*DROP\s+TRIGGER\b", re.IGNORECASE)
_CREATE_TRIGGER_LEAD = re.compile(r"^\s*CREATE\s+(OR\s+REPLACE\s+)?TRIGGER\b", re.IGNORECASE)

# CREATE [OR REPLACE] TRIGGER <name> ON <table> EXECUTE <task>
#
# `ON <table>` is a CATALOG event: the trigger fires when that dataset takes a
# user-created commit. Two other event kinds are imaginable and neither is
# accepted, because neither has anything to fire it - see `_TRIGGER_EVENT_LEAD`
# below. A trigger that is stored but never dispatched is worse than one that
# does not exist: the table it maintains silently stops updating, and the
# trigger record says it is fine.
_CREATE_TRIGGER_RE = re.compile(
    r"^\s*CREATE\s+(?P<or_replace>OR\s+REPLACE\s+)?TRIGGER\s+"
    r"(?P<name>[A-Za-z_][\w$]*)\s+ON\s+(?P<table>[A-Za-z_][\w.$]*)\s+"
    r"EXECUTE\s+(?P<task>[A-Za-z_][\w.$]*)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
# The event kinds this engine can express but cannot yet dispatch. Matched so
# they are refused BY NAME rather than as a generic syntax error, and so the
# refusal can say what is missing rather than that the word is unknown.
_TRIGGER_EVENT_LEAD = re.compile(
    r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\s+[A-Za-z_][\w$]*\s+ON\s+"
    r"(?P<kind>SCHEDULE|EVERY|EVENT|SIGNAL)\b",
    re.IGNORECASE,
)

# ALTER TRIGGER <name> ON <table> SUSPEND|RESUME
#
# Suspension is expressed on the TRIGGER, unlike a materialized view's, which is
# expressed on the view: a view owns its triggers and suspending some of them
# would leave it refreshing from a subset of its sources. A task has exactly one
# trigger (the catalog's one-trigger rule - a task's window is one source's
# version sequence), so suspending it is suspending the task's unattended runs
# entirely; there is no subset to be left half-firing.
_ALTER_TRIGGER_LEAD = re.compile(r"^\s*ALTER\s+TRIGGER\b", re.IGNORECASE)
_ALTER_TRIGGER_RE = re.compile(
    r"^\s*ALTER\s+TRIGGER\s+(?P<name>[A-Za-z_][\w$]*)\s+ON\s+"
    r"(?P<table>[A-Za-z_][\w.$]*)\s+(?P<state>SUSPEND|RESUME)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
# ALTER TRIGGER <name> ON <table> OWNER TO <principal>|CURRENT_USER
#
# The identity an UNATTENDED run executes as. It lives on the trigger rather
# than the task because that is what distinguishes unattended from attended: a
# person running `EXECUTE` runs it as themselves and answers for it, so nothing
# needs pinning. A trigger fires with nobody present, so it must say whose
# authority it carries. A task has one trigger, so that is one unattended
# identity per task - the field lives on the trigger because that is where
# "unattended" is decided, not because a task could have several.
_ALTER_TRIGGER_OWNER_RE = re.compile(
    r"^\s*ALTER\s+TRIGGER\s+(?P<name>[A-Za-z_][\w$]*)\s+ON\s+"
    r"(?P<table>[A-Za-z_][\w.$]*)\s+OWNER\s+TO\s+(?P<owner>" + _PRINCIPAL_SLOT + r")\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)

# REFRESH MATERIALIZED VIEW <name>. sqlparser has no REFRESH statement in the
# Opteryx dialect, so it takes the same pre-parse route DROP TRIGGER does. No
# placeholders: the view is an identifier, not a value.
_REFRESH_MV_RE = re.compile(
    r"^\s*REFRESH\s+MATERIALIZED\s+VIEW\s+(?P<name>[A-Za-z_][\w.$]*)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_REFRESH_LEAD = re.compile(r"^\s*REFRESH\b", re.IGNORECASE)


def _intercept_refresh_statements(clean_sql: str):
    """Recognize `REFRESH MATERIALIZED VIEW <name>` before the SQL parser.

    Returns a synthesized single-statement AST list, or None when the statement
    does not begin with REFRESH.

    Anything else beginning with REFRESH is rejected here by name rather than
    left to the parser, which would report it as a generic syntax error several
    layers away from the word that caused it.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    if not _REFRESH_LEAD.match(clean_sql):
        return None
    match = _REFRESH_MV_RE.match(clean_sql)
    if match is None:
        raise UnsupportedSyntaxError(
            "Expected: **REFRESH MATERIALIZED VIEW** <name>. It is the only "
            "**REFRESH** statement, and it takes no options."
        )
    return [{"RefreshMaterializedView": {"name": match.group("name")}}]


# SAVE RESULTS OF <job> AS <dataset>. Copies the results a job already produced
# into a new dataset, so they outlive the job's retention window.
#
# Neither slot takes a placeholder, and the job handle is the reason to be firm
# about it. It names WHICH RESULTS get copied — a parameterised handle would let
# runtime data choose whose results land in the caller's own workspace, which is
# the identifier-slot rule in this module's header doing exactly the job it is
# there for. The target is an identifier for the same reason relations always
# are.
#
# The handle is not identifier-shaped: a job id is `YYYYMMDDHHMMSS-<random>`, so
# it opens with a digit and carries a hyphen. It is matched as its own token
# rather than borrowing _OBJECT_SLOT, which would reject every real one.
_SAVE_RESULTS_RE = re.compile(
    r"^\s*SAVE\s+RESULTS\s+OF\s+(?P<handle>[0-9A-Za-z][0-9A-Za-z_-]*)"
    r"\s+AS\s+(?P<name>[A-Za-z_][\w.$]*)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_SAVE_LEAD = re.compile(r"^\s*SAVE\b", re.IGNORECASE)


def _intercept_save_results(clean_sql: str):
    """Recognize `SAVE RESULTS OF <job> AS <dataset>` before the SQL parser.

    Same route as REFRESH: sqlparser's Opteryx dialect has no SAVE statement at
    all, so without this the front of parsing reports `Expected: an SQL
    statement, found: SAVE` — including through `analyze_query`, which is how
    the jobs API pre-flights a statement before queueing it. A statement the
    platform runs perfectly well would be rejected at submission.

    Recognized here, planned nowhere: the engine has no idea its results are
    written to a bucket, so it cannot be the thing that copies them. This makes
    SAVE a statement the engine PARSES and CLASSIFIES — which is what the jobs
    API needs to authorize it and what the worker needs to identify it — while
    the copy itself belongs to the service that owns the results bucket.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    if not _SAVE_LEAD.match(clean_sql):
        return None
    match = _SAVE_RESULTS_RE.match(clean_sql)
    if match is None:
        raise UnsupportedSyntaxError(
            "Expected: **SAVE RESULTS OF** <job> **AS** <dataset>. It is the "
            "only **SAVE** statement."
        )
    return [
        {
            "SaveResults": {
                "handle": match.group("handle"),
                "name": match.group("name"),
            }
        }
    ]


# ALTER MATERIALIZED VIEW <name> OWNER TO <principal>|CURRENT_USER. The owner is
# a value and takes a placeholder, exactly as GRANT's principal does; the view it
# is set on is an identifier and does not. Same route as
# REFRESH, but narrower: ALTER has other legitimate forms (ALTER TABLE, ALTER
# WORKSPACE), so anything not aimed at a materialized view falls through to the
# parser untouched.
_ALTER_MV_LEAD = re.compile(r"^\s*ALTER\s+MATERIALIZED\s+VIEW\b", re.IGNORECASE)
_ALTER_MV_OWNER_RE = re.compile(
    r"^\s*ALTER\s+MATERIALIZED\s+VIEW\s+(?P<name>[A-Za-z_][\w.$]*)\s+"
    r"OWNER\s+TO\s+(?P<owner>" + _PRINCIPAL_SLOT + r")\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


# SUSPEND/RESUME is expressed on the VIEW, never on its triggers. A trigger is
# the mechanism the system uses to keep a view fresh, not something anyone
# creates or reasons about - and a view with four sources has four of them, so
# suspending triggers individually could leave it refreshing from a subset of its
# sources and producing silently partial data.
_ALTER_MV_SUSPEND_RE = re.compile(
    r"^\s*ALTER\s+MATERIALIZED\s+VIEW\s+(?P<name>[A-Za-z_][\w.$]*)\s+"
    r"(?P<action>SUSPEND|RESUME)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _intercept_alter_materialized_view(clean_sql: str):
    """Recognize the ALTER forms a materialized view accepts.

        ALTER MATERIALIZED VIEW <name> OWNER TO <principal>
        ALTER MATERIALIZED VIEW <name> SUSPEND | RESUME

    Returns a synthesized single-statement AST list, or None when the statement
    is not aimed at a materialized view - every other ALTER goes to the parser.

    A statement that IS aimed at one but matches neither form is rejected here
    by name. Those two are the whole surface: everything else about a view
    follows from its defining SELECT and changes by redefining that.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    if not _ALTER_MV_LEAD.match(clean_sql):
        return None

    match = _ALTER_MV_OWNER_RE.match(clean_sql)
    if match is not None:
        raw_owner = match.group("owner")
        owner = _slot_value(raw_owner)
        # Bare CURRENT_USER means "me", resolved to the session identity when the
        # statement runs. Quoting it asks for a principal literally named
        # CURRENT_USER, which is the usual SQL distinction and worth keeping: it
        # is the only way to name such a principal if one ever exists. A parameter
        # is a value like a quoted string, so a bound "CURRENT_USER" names that
        # principal too - what a placeholder carries is never a keyword.
        current_user = raw_owner.upper() == "CURRENT_USER"
        return [
            {
                "AlterMaterializedViewOwner": {
                    "name": match.group("name"),
                    "owner": None if current_user else owner,
                    "current_user": current_user,
                }
            }
        ]

    match = _ALTER_MV_SUSPEND_RE.match(clean_sql)
    if match is not None:
        return [
            {
                "AlterMaterializedViewSuspended": {
                    "name": match.group("name"),
                    "suspended": match.group("action").upper() == "SUSPEND",
                }
            }
        ]

    raise UnsupportedSyntaxError(
        "Expected: **ALTER MATERIALIZED VIEW** <name> **OWNER TO** <principal>, or "
        "**ALTER MATERIALIZED VIEW** <name> **SUSPEND**|**RESUME**. Everything else "
        "about a view follows from its defining SELECT, so change it with "
        "**CREATE OR REPLACE MATERIALIZED VIEW**."
    )


def _intercept_trigger_statements(clean_sql: str):
    """Recognize `DROP TRIGGER [IF EXISTS] <name> ON <table>` before the SQL
    parser (OpteryxDialect is not in sqlparser's allowlist for trigger
    statements, so they would otherwise fail to parse with an unhelpful error).

    Returns a synthesized single-statement AST list, or None if the statement
    is not a trigger statement. `CREATE TRIGGER` is rejected here by name -
    triggers exist only as the automatic artifact of CREATE MATERIALIZED VIEW.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    if _CREATE_TRIGGER_LEAD.match(clean_sql):
        event = _TRIGGER_EVENT_LEAD.match(clean_sql)
        if event is not None:
            # Expressible, and deliberately not accepted: nothing dispatches
            # either kind. A catalog event fires from the commit path; a clock
            # event needs something that polls for due triggers and a user event
            # needs a surface to signal one, and neither exists. Storing such a
            # trigger would leave a table quietly never updating while its
            # trigger record claimed otherwise.
            raise UnsupportedSyntaxError(
                f"**ON {event.group('kind').upper()}** triggers are not dispatched yet. A "
                "trigger fires on a catalog event - a commit to a dataset - written "
                "**ON** <table>. Clock and signal events need a dispatcher that does "
                "not exist, and a trigger nothing fires is worse than none."
            )
        match = _CREATE_TRIGGER_RE.match(clean_sql)
        if match is None:
            raise UnsupportedSyntaxError(
                "Expected: **CREATE** [**OR REPLACE**] **TRIGGER** <name> **ON** "
                "<table> **EXECUTE** <task>. The table is the dataset whose commits "
                "fire it; the task is what it runs."
            )
        return [
            {
                "CreateTrigger": {
                    "trigger_name": match.group("name"),
                    "table_name": match.group("table"),
                    "task_name": match.group("task"),
                    "or_replace": match.group("or_replace") is not None,
                }
            }
        ]

    if _ALTER_TRIGGER_LEAD.match(clean_sql):
        match = _ALTER_TRIGGER_OWNER_RE.match(clean_sql)
        if match is not None:
            owner_raw = match.group("owner")
            return [
                {
                    "AlterTriggerOwner": {
                        "trigger_name": match.group("name"),
                        "table_name": match.group("table"),
                        "new_owner": _slot_value(owner_raw),
                        "owner_is_current_user": owner_raw.strip().upper() == "CURRENT_USER",
                    }
                }
            ]
        match = _ALTER_TRIGGER_RE.match(clean_sql)
        if match is None:
            raise UnsupportedSyntaxError(
                "Expected: **ALTER TRIGGER** <name> **ON** <table> "
                "**SUSPEND**|**RESUME**, or **... OWNER TO** <principal>. What a "
                "trigger runs is changed by recreating it, not altered in place."
            )
        return [
            {
                "AlterTriggerSuspended": {
                    "trigger_name": match.group("name"),
                    "table_name": match.group("table"),
                    "suspended": match.group("state").upper() == "SUSPEND",
                }
            }
        ]

    if not _DROP_TRIGGER_LEAD.match(clean_sql):
        return None
    match = _DROP_TRIGGER_RE.match(clean_sql)
    if match is None:
        # CASCADE/RESTRICT (or any other trailing modifier) lands here: the
        # grammar above accepts nothing after the table name.
        raise UnsupportedSyntaxError(
            "Expected: DROP TRIGGER [IF **EXISTS**] <name> ON <table> "
            "(no CASCADE/RESTRICT; the table name is required)"
        )
    return [
        {
            "DropTrigger": {
                "trigger_name": match.group("name"),
                "table_name": match.group("table"),
                "if_exists": match.group("if_exists") is not None,
            }
        }
    ]


# CREATE [OR REPLACE] TASK <name> AS <statement>
# DROP TASK [IF EXISTS] <name>
#
# sqlparser has no TASK object type at all - `CREATE TASK` fails with "Expected:
# an object type after CREATE" - so these take the same pre-parse route as
# REFRESH and DROP TRIGGER. `EXECUTE` needed none of this: sqlparser parses it
# natively, which is why only the DDL is intercepted here.
#
# The task's name is an IDENTIFIER slot, so it admits no placeholder, for the
# reason given at the top of this module: a parameterised name would let runtime
# data decide which task is defined. The STATEMENT is captured as raw text and
# not examined here - whether it parses, and whether it is something a task may
# run, is the planner's question, asked where the answer can be a useful error.
_CREATE_TASK_LEAD = re.compile(r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?TASK\b", re.IGNORECASE)
# `ON <table>` is optional and declares the dataset whose commits fire this
# task, creating the trigger alongside it - the way CREATE MATERIALIZED VIEW
# creates one per source. Omit it and the task is defined but nothing fires it,
# which is what a backfill or a replay is: EXECUTE by hand, on purpose.
#
# NOT derived from the statement's own sources, which is where this departs from
# a view. A view must track every source to stay consistent, so implying the
# triggers is right for one. A task's read set and its firing condition are
# different intents - a task joining a small event table to a large reference
# table wants to fire on the event table only - and an implication would give no
# way to say so.
_CREATE_TASK_RE = re.compile(
    r"^\s*CREATE\s+(?P<or_replace>OR\s+REPLACE\s+)?TASK\s+"
    r"(?P<name>[A-Za-z_][\w.$]*)\s+(?:ON\s+(?P<on>[A-Za-z_][\w.$]*)\s+)?"
    r"AS\s+(?P<statement>.+?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_DROP_TASK_LEAD = re.compile(r"^\s*DROP\s+TASK\b", re.IGNORECASE)
_DROP_TASK_RE = re.compile(
    r"^\s*DROP\s+TASK\s+(?P<if_exists>IF\s+EXISTS\s+)?"
    r"(?P<name>[A-Za-z_][\w.$]*)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


# ALTER TASK has no forms. A task is a statement and nothing else: what it runs
# is changed by redefining it, and the two things one might expect to alter -
# who it runs as, and whether it runs - belong to the TRIGGER, which is what
# fires unattended. Refused here by name so the reader is told that, rather than
# meeting a parser error pointing at the word TASK.
_ALTER_TASK_LEAD = re.compile(r"^\s*ALTER\s+TASK\b", re.IGNORECASE)


def _intercept_alter_task(clean_sql: str):
    """Refuse ALTER TASK, naming the statement that does what was meant."""
    from opteryx.exceptions import UnsupportedSyntaxError

    if not _ALTER_TASK_LEAD.match(clean_sql):
        return None
    raise UnsupportedSyntaxError(
        "**ALTER TASK** has no forms. Change what a task runs with **CREATE OR "
        "REPLACE TASK**. Who it runs as, and whether it runs, belong to the trigger "
        "that fires it: **ALTER TRIGGER** <name> **ON** <table> **OWNER TO** "
        "<principal>, or **... SUSPEND**|**RESUME**."
    )


def _intercept_task_statements(clean_sql: str):
    """Recognize `CREATE [OR REPLACE] TASK` and `DROP TASK` before the parser.

    Returns a synthesized single-statement AST list, or None when the statement
    is neither. Anything beginning with those words but not matching is rejected
    BY NAME here rather than left to sqlparser, which knows no TASK at all and
    would report a syntax error pointing at the word after it.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    if _CREATE_TASK_LEAD.match(clean_sql):
        match = _CREATE_TASK_RE.match(clean_sql)
        if match is None:
            raise UnsupportedSyntaxError(
                "Expected: **CREATE** [**OR REPLACE**] **TASK** <name> [**ON** <table>] "
                "**AS** <statement>. A task is a statement the platform runs for you; "
                "the statement is what it runs."
            )
        return [
            {
                "CreateTask": {
                    "name": match.group("name"),
                    "statement": match.group("statement"),
                    "or_replace": match.group("or_replace") is not None,
                    "on_table": match.group("on"),
                }
            }
        ]

    if _DROP_TASK_LEAD.match(clean_sql):
        match = _DROP_TASK_RE.match(clean_sql)
        if match is None:
            raise UnsupportedSyntaxError(
                "Expected: **DROP TASK** [**IF EXISTS**] <name>. **DROP TASK** takes "
                "no other options - a task owns no storage, so there is nothing for "
                "CASCADE or RESTRICT to decide."
            )
        return [
            {
                "DropTask": {
                    "name": match.group("name"),
                    "if_exists": match.group("if_exists") is not None,
                }
            }
        ]

    return None


# SHOW CREATE MATERIALIZED VIEW <name>
# SHOW CREATE TASK <name>
#
# sqlparser's ShowCreateObject enum has TABLE, VIEW, TRIGGER, FUNCTION,
# PROCEDURE and EVENT, and no way to add to it from a dialect - so these two
# take the same pre-parse route as CREATE TASK, and for the same reason. TABLE
# and VIEW parse natively and are deliberately NOT intercepted: the fewer
# spellings that come through here, the fewer places the grammar lives.
#
# The object name is an IDENTIFIER slot and admits no placeholder (see the top
# of this module): which object's definition is shown is not a runtime decision.
_SHOW_CREATE_LEAD = re.compile(r"^\s*SHOW\s+CREATE\s+(MATERIALIZED\s+VIEW|TASK)\b", re.IGNORECASE)
_SHOW_CREATE_RE = re.compile(
    r"^\s*SHOW\s+CREATE\s+(?P<kind>MATERIALIZED\s+VIEW|TASK)\s+"
    r"(?P<name>[A-Za-z_][\w.$]*)\s*;?\s*$",
    re.IGNORECASE,
)


def _intercept_show_create(clean_sql: str):
    """Recognize `SHOW CREATE MATERIALIZED VIEW` and `SHOW CREATE TASK`.

    Synthesized into the same ShowCreate shape sqlparser produces for TABLE and
    VIEW, down to the identifier-part list, so the logical planner has one path
    for all four object types rather than one per spelling.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    if not _SHOW_CREATE_LEAD.match(clean_sql):
        return None
    match = _SHOW_CREATE_RE.match(clean_sql)
    if match is None:
        raise UnsupportedSyntaxError(
            "Expected: **SHOW CREATE MATERIALIZED VIEW** <name> or **SHOW CREATE TASK** "
            "<name>. The statement takes one object name and nothing else."
        )
    kind = "MaterializedView" if match.group("kind").upper().startswith("MATERIALIZED") else "Task"
    return [
        {
            "ShowCreate": {
                "obj_type": kind,
                "obj_name": [
                    {"Identifier": {"value": part, "quote_style": None}}
                    for part in match.group("name").split(".")
                ],
            }
        }
    ]


# GRANT <role> ON <kind> <object> TO USER <user>
# REVOKE <role> ON <kind> <object> FROM USER <user>
# SHOW GRANTS ON <kind> <object>
#
# sqlparser has GRANT/REVOKE grammar, but it speaks in privileges (SELECT,
# INSERT) over tables - it has no reader/writer/owner role vocabulary and no
# WORKSPACE|COLLECTION|DATASET object kinds - so these take the same pre-parse
# route as REFRESH. The principal is `TO USER <user>` with USER mandatory: it
# reserves the grammar for TO ROLE/groups later without ambiguity. Bare
# `SHOW GRANTS` (the session's own grants) is untouched here and still parses
# through the ShowVariable catch-all.
_GRANT_LEAD = re.compile(r"^\s*(GRANT|REVOKE)\b", re.IGNORECASE)
_GRANT_RE = re.compile(
    r"^\s*(?P<verb>GRANT|REVOKE)\s+(?P<role>READER|WRITER|OWNER)\s+"
    r"ON\s+(?P<kind>WORKSPACE|COLLECTION|DATASET)\s+(?P<object>" + _OBJECT_SLOT + r")\s+"
    r"(?P<direction>TO|FROM)\s+USER\s+(?P<user>" + _PRINCIPAL_SLOT + r")\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
# Two statements, one grammar: `SHOW GRANTS ON <kind> <object>` lists what is
# stored AT the object, `SHOW EFFECTIVE GRANTS ON <kind> <object>` lists every
# policy that COVERS it. EFFECTIVE is a keyword from a closed set, matched here
# rather than left as a value, because it decides WHICH statement this is.
#
# The second lead exists so `SHOW EFFECTIVE ...` in any other shape is rejected
# by name here instead of reaching sqlparser, which knows no EFFECTIVE at all
# and would report a syntax error pointing at a token several words away. Bare
# `SHOW GRANTS` (the session's own grants) matches neither and still falls
# through to the parser's ShowVariable catch-all.
_SHOW_GRANTS_ON_LEAD = re.compile(
    r"^\s*SHOW\s+(?:EFFECTIVE\s+)?GRANTS\s+ON\b", re.IGNORECASE
)
_SHOW_EFFECTIVE_LEAD = re.compile(r"^\s*SHOW\s+EFFECTIVE\b", re.IGNORECASE)
_SHOW_GRANTS_ON_RE = re.compile(
    r"^\s*SHOW\s+(?P<effective>EFFECTIVE\s+)?GRANTS\s+ON\s+"
    r"(?P<kind>WORKSPACE|COLLECTION|DATASET)\s+"
    r"(?P<object>" + _OBJECT_SLOT + r")\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _intercept_grant_statements(clean_sql: str):
    """Recognize GRANT and REVOKE before the SQL parser.

        GRANT  READER|WRITER|OWNER ON WORKSPACE|COLLECTION|DATASET <object> TO USER <user>
        REVOKE READER|WRITER|OWNER ON WORKSPACE|COLLECTION|DATASET <object> FROM USER <user>

    Returns a synthesized single-statement AST list, or None when the statement
    does not begin with GRANT or REVOKE. Anything else beginning with those
    keywords is rejected here by name rather than handed to sqlparser's own
    GRANT grammar, which speaks in privileges over tables and would accept or
    misreport a statement this engine does not run.

    GRANT pairs with TO and REVOKE with FROM - the crossed forms are refused,
    since a statement whose preposition disagrees with its verb was not the
    statement someone meant to run.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    if not _GRANT_LEAD.match(clean_sql):
        return None
    match = _GRANT_RE.match(clean_sql)
    if match is None:
        raise UnsupportedSyntaxError(
            "Expected: **GRANT** READER|WRITER|OWNER **ON** "
            "WORKSPACE|COLLECTION|DATASET <object> **TO USER** <user>, or "
            "**REVOKE** READER|WRITER|OWNER **ON** WORKSPACE|COLLECTION|DATASET "
            "<object> **FROM USER** <user>."
        )
    verb = match.group("verb").upper()
    direction = match.group("direction").upper()
    if (verb == "GRANT") != (direction == "TO"):
        raise UnsupportedSyntaxError(
            "**GRANT** grants **TO USER** and **REVOKE** revokes **FROM USER** - "
            f"'{verb} ... {direction} USER' mixes the two."
        )
    root = "GrantAccess" if verb == "GRANT" else "RevokeAccess"
    # The role and the object kind are keywords from a closed set, so they stay
    # literal: a parameter there would make the SHAPE of the statement - which
    # authority it confers, how many parts its object name has - depend on runtime
    # data. The object and the principal are values, and take placeholders.
    return [
        {
            root: {
                "role": match.group("role").lower(),
                "object_kind": match.group("kind").lower(),
                "object_name": _slot_value(match.group("object")),
                "principal": _slot_value(match.group("user")),
            }
        }
    ]


def _intercept_show_grants_on(clean_sql: str):
    """Recognize the two grant listings before the SQL parser.

        SHOW GRANTS ON           WORKSPACE|COLLECTION|DATASET <object>
        SHOW EFFECTIVE GRANTS ON WORKSPACE|COLLECTION|DATASET <object>

    Returns a synthesized single-statement AST list, or None when the statement
    is neither - bare `SHOW GRANTS` (the session's own grants) falls through to
    the parser's ShowVariable catch-all untouched.

    They are two statements because they answer two questions: what is stored
    AT an object (1:1 with what a GRANT or REVOKE there would act on) and who
    can reach it at all (that policy plus every one above it that covers it).
    They are one grammar because everything else about them - the object kinds,
    the arity, the columns, the owner gate - is the same.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    if not (_SHOW_GRANTS_ON_LEAD.match(clean_sql) or _SHOW_EFFECTIVE_LEAD.match(clean_sql)):
        return None
    match = _SHOW_GRANTS_ON_RE.match(clean_sql)
    if match is None:
        raise UnsupportedSyntaxError(
            "Expected: **SHOW GRANTS ON** WORKSPACE|COLLECTION|DATASET <object> "
            "(the grants stored on the object), or **SHOW EFFECTIVE GRANTS ON** "
            "WORKSPACE|COLLECTION|DATASET <object> (those, plus the grants above "
            "it that cover it). For the session's own grants, use bare "
            "**SHOW GRANTS**."
        )
    root = "ShowEffectiveGrantsOn" if match.group("effective") else "ShowGrantsOn"
    return [
        {
            root: {
                "object_kind": match.group("kind").lower(),
                "object_name": _slot_value(match.group("object")),
            }
        }
    ]


# The order matters only in that each interceptor is asked about a statement it can
# rule out on its first keyword; a statement matching two of them does not exist.
_INTERCEPTORS = (
    _intercept_drop_statistics,
    _intercept_trigger_statements,
    _intercept_task_statements,
    _intercept_alter_task,
    _intercept_refresh_statements,
    _intercept_save_results,
    _intercept_alter_materialized_view,
    _intercept_grant_statements,
    _intercept_show_grants_on,
    _intercept_show_create,
)


def pre_parse(clean_sql: str):
    """The parse step for statements sqlparser has no grammar for.

    Parameters:
        clean_sql: the statement, after the SQL rewriter has run over it.

    Returns:
        A single-statement AST list in the shape sqlparser would have produced,
        or None when this is an ordinary statement for the parser to handle.

    Raises:
        UnsupportedSyntaxError: when the statement opens with one of these
            keywords but is not one of these statements. Rejecting it here names
            the statement it isn't; letting it fall through to the parser gets a
            syntax error pointing at a token several words away.
    """
    for interceptor in _INTERCEPTORS:
        parsed_statements = interceptor(clean_sql)
        if parsed_statements is not None:
            return parsed_statements
    return None
