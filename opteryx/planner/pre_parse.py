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

# DROP STATISTICS ON <table> [FOR COLUMNS <c1>, <c2>, ...]
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
# Opteryx dialect, so it takes the same pre-parse route DROP TRIGGER does.
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


# ALTER MATERIALIZED VIEW <name> OWNER TO <principal>|CURRENT_USER. Same route as
# REFRESH, but narrower: ALTER has other legitimate forms (ALTER TABLE, ALTER
# WORKSPACE), so anything not aimed at a materialized view falls through to the
# parser untouched.
_ALTER_MV_LEAD = re.compile(r"^\s*ALTER\s+MATERIALIZED\s+VIEW\b", re.IGNORECASE)
_ALTER_MV_OWNER_RE = re.compile(
    r"^\s*ALTER\s+MATERIALIZED\s+VIEW\s+(?P<name>[A-Za-z_][\w.$]*)\s+"
    r"OWNER\s+TO\s+(?P<owner>'[^']+'|\"[^\"]+\"|[\w.@:+-]+)\s*;?\s*$",
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
        owner = match.group("owner")
        quoted = owner[0] in "'\""
        if quoted:
            owner = owner[1:-1]
        # Bare CURRENT_USER means "me", resolved to the session identity when the
        # statement runs. Quoting it asks for a principal literally named
        # CURRENT_USER, which is the usual SQL distinction and worth keeping: it
        # is the only way to name such a principal if one ever exists.
        current_user = not quoted and owner.upper() == "CURRENT_USER"
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
    r"ON\s+(?P<kind>WORKSPACE|COLLECTION|DATASET)\s+(?P<object>[A-Za-z_][\w.$]*)\s+"
    r"(?P<direction>TO|FROM)\s+USER\s+(?P<user>'[^']+'|\"[^\"]+\"|[\w.@:+-]+)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_SHOW_GRANTS_ON_LEAD = re.compile(r"^\s*SHOW\s+GRANTS\s+ON\b", re.IGNORECASE)
_SHOW_GRANTS_ON_RE = re.compile(
    r"^\s*SHOW\s+GRANTS\s+ON\s+(?P<kind>WORKSPACE|COLLECTION|DATASET)\s+"
    r"(?P<object>[A-Za-z_][\w.$]*)\s*;?\s*$",
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
    principal = match.group("user")
    if principal[0] in "'\"":
        principal = principal[1:-1]
    root = "GrantAccess" if verb == "GRANT" else "RevokeAccess"
    return [
        {
            root: {
                "role": match.group("role").lower(),
                "object_kind": match.group("kind").lower(),
                "object_name": match.group("object"),
                "principal": principal,
            }
        }
    ]


def _intercept_show_grants_on(clean_sql: str):
    """Recognize `SHOW GRANTS ON <kind> <object>` before the SQL parser.

    Returns a synthesized single-statement AST list, or None when the statement
    is not a SHOW GRANTS ON - bare `SHOW GRANTS` (the session's own grants)
    falls through to the parser's ShowVariable catch-all untouched.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    if not _SHOW_GRANTS_ON_LEAD.match(clean_sql):
        return None
    match = _SHOW_GRANTS_ON_RE.match(clean_sql)
    if match is None:
        raise UnsupportedSyntaxError(
            "Expected: **SHOW GRANTS ON** WORKSPACE|COLLECTION|DATASET <object>. "
            "For the session's own grants, use bare **SHOW GRANTS**."
        )
    return [
        {
            "ShowGrantsOn": {
                "object_kind": match.group("kind").lower(),
                "object_name": match.group("object"),
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
