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
        raise UnsupportedSyntaxError(
            "CREATE TRIGGER is not supported; triggers are created automatically "
            "by **CREATE MATERIALIZED VIEW**. A materialized view gets its trigger when it is created."
        )
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
    _intercept_refresh_statements,
    _intercept_alter_materialized_view,
    _intercept_grant_statements,
    _intercept_show_grants_on,
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
