# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The SQL Rewriter is the first transformation in the planning pipeline. It operates on
the raw SQL string before it reaches the parser.

Input:  raw SQL string (bytes or str) as supplied by the caller
Output: a cleaned SQL string safe to hand to the parser

Responsibilities:
- Decodes escape sequences (\\n, \\t, etc.) to their real characters and normalises
  whitespace
- Rewrites TIMESTAMP[ns/ms/s/us/d] bracketed syntax to internal type names the parser
  accepts (e.g. TIMESTAMP[ns] → _TIMESTAMP_NS)
- Tokenises the statement and rewrites b-string literals (b'...' → CAST(... AS VARBINARY))
  and r-string literals (r'...' → BASE64_DECODE(...))
- Normalises EXPLAIN FORMAT MERMAID to FORMAT GRAPHVIZ so the parser accepts it;
  rejects FORMAT GRAPHVIZ and FORMAT JSON explicitly
- Rewrites COMMENT ON TABLE / COMMENT ON VIEW to COMMENT ON EXTENSION so the parser
  accepts the statement
- Rewrites CREATE/DROP COLLECTION to CREATE/DROP SCHEMA so the parser accepts the
  statement (sqlparser-rs has no COLLECTION object type)

The rewriter does NOT parse SQL into an AST; it only manipulates the text.
"""

import re

from opteryx.exceptions import UnsupportedSyntaxError

SQL_PARTS = {
    r"ANALYZE\sTABLE",
    r"ANTI\sJOIN",
    r"ALTER\sVIEW",
    r"COMMENT\sON",
    r"CREATE\sTABLE",
    r"CREATE\sVIEW",
    r"DROP\sVIEW",
    r"EXPLAIN\sANALYZE",
    r"FORMAT\sMERMAID",
    r"FORMAT\sTEXT",
    r"REPLACE",
    r"CROSS\sJOIN",
    r"FROM",
    r"FULL\sJOIN",
    r"FULL\sOUTER\sJOIN",
    r"INNER\sJOIN",
    r"JOIN",
    r"LEFT\sANTI\sJOIN",
    r"LEFT\sJOIN",
    r"LEFT\sOUTER\sJOIN",
    r"LEFT\sSEMI\sJOIN",
    r"NATURAL\sJOIN",
    r"RIGHT\sANTI\sJOIN",
    r"RIGHT\sJOIN",
    r"RIGHT\sOUTER\sJOIN",
    r"RIGHT\sSEMI\sJOIN",
    r"SEMI\sJOIN",
    r"GROUP\sBY",
    r"HAVING",
    r"LIKE",
    r"LIMIT",
    r"OFFSET",
    r"ON",
    r"ORDER\sBY",
    r"SHOW",
    r"SELECT",
    r"WHERE",
    r"WITH",
    r"USING",
    r";",
    r",",
    r"UNION",
    r"AS",
    r"AND",
    r"OR",
    r"NOT",
}


# Matches either a quoted span (captured, left untouched) or a literal/escaped
# newline, tab, or carriage return outside quotes (captured, collapsed to a
# single space). Quoted spans use the same doubled-quote escaping as
# _QUOTED_STRINGS_REGEX below ('don''t' is one literal, not two).
_WHITESPACE_NORMALIZE_REGEX = re.compile(
    r'("(?:[^"]|"")*"'
    r"|'(?:[^']|'')*'"
    r"|`[^`]*`)"
    r"|(\\r\\n|\\n|\\t|\\r|\r\n|\n|\t|\r)"
)


def _normalize_whitespace(statement: str) -> str:
    """
    Collapse literal newlines/tabs/CRs, and their backslash-escaped text forms
    (\\n, \\t, \\r), to a single space -- everywhere except inside quoted
    string literals, whose contents must reach the parser unchanged so it can
    apply standard SQL string-escape decoding itself.
    """

    def _replacer(match):
        if match.group(2) is not None:
            return " "
        return match.group(1)  # captured quoted-span, unchanged

    return _WHITESPACE_NORMALIZE_REGEX.sub(_replacer, statement)


# Precompile regex patterns at module level for performance
#
# `(?<![@$])` guards the leading `\b`: `@` and `$` are identifier-START characters
# in the Opteryx dialect (see `is_identifier_start` in src/opteryx_dialect.rs), but
# they are not word characters, so `\b` happily matches BETWEEN the sigil and the
# name. Without the guard a variable whose name is a keyword — `@OR`, `@WHERE`,
# `@SELECT` — was split into `@` + the keyword and rejoined as `@ OR`, which the
# parser then rejected with "Expected: identifier, found: @". Only the 17
# single-word SQL_PARTS entries could collide, which is why `@ORDER` (ORDER BY is
# two words) and `@my_or` (preceded by a word char, so `\b` never matched) worked.
_KEYWORDS_REGEX = re.compile(
    r"(\,|\(|\)|;|\t|\n|\->>|\->|@>|@>>|\&\&|@\?|"
    + r"|".join([r"(?<![@$])\b" + i.replace(r" ", r"\s") + r"\b" for i in SQL_PARTS])
    + r")",
    re.IGNORECASE,
)

# Match ", ', b", b', `
# We match b prefixes separately after the non-prefix versions
#
# A literal ends at the first quote that is NOT doubled: SQL escapes a quote by
# doubling it ('don''t'), so `'[^']*'` cannot express one — it ended the literal at the
# first inner quote, and `SELECT 'don''t'` tokenised as 'don' + 't', which the parts are
# then rejoined with a space between ('a''b''c' -> 'a' 'b' 'c'). The statement never
# reached the parser intact, so this looked like missing parser support for standard SQL
# escaping. `(?:[^']|'')*` consumes a doubled quote as one unit instead. The two
# alternatives are disjoint (the first cannot match a quote, the second only matches a
# pair), so there is no ambiguity for the engine to backtrack over.
_QUOTED_STRINGS_REGEX = re.compile(
    r'("(?:[^"]|"")*"'
    r'|\'(?:[^\']|\'\')*\''
    r'|\b[bB]"(?:[^"]|"")*"'
    r'|\b[bB]\'(?:[^\']|\'\')*\''
    r'|\b[rR]"(?:[^"]|"")*"'
    r'|\b[rR]\'(?:[^\']|\'\')*\''
    r"|`[^`]*`)"
)


def sql_parts(string):
    """
    Split a SQL statement into clauses
    """

    parts = []
    quoted_strings = _QUOTED_STRINGS_REGEX.split(string)
    for i, part in enumerate(quoted_strings):
        if part and part[-1] in ("'", '"', "`"):
            if part[0] in ("b", "B"):
                parts.append(f"CAST({part[1:]} AS VARBINARY)")
                # if there's no alias, we should add one to preserve the input
                if len(quoted_strings) > i + 1:
                    next_token = quoted_strings[i + 1]
                    if next_token.upper().strip().startswith(("FROM ", "JOIN ")):
                        parts.append("AS ")
                        parts.append(f"{part[2:-1]} ")
            elif part[0] in ("r", "R"):
                # We take the raw string and encode it, pass it into the
                # plan as the encoded string and let the engine decode it
                from opteryx.third_party.mabel import base64

                # part[2:-1] is the literal's VALUE, not SQL text — the parser never
                # sees it to un-escape, so undouble here. (A b-string's payload goes
                # back out as SQL, so its escapes stay doubled for the parser.)
                encoded_part = base64.encode(part[2:-1].replace("''", "'").encode()).decode()
                # if there's no alias, we should add one to preserve the input
                parts.append(f"BASE64_DECODE('{encoded_part}')")
                if len(quoted_strings) > i + 1:
                    next_token = quoted_strings[i + 1]
                    if next_token.upper().strip().startswith(("FROM ", "JOIN ")):
                        parts.append("AS ")
                        parts.append(f"{part[2:-1]} ")
            else:
                parts.append(part)
        else:
            for subpart in _KEYWORDS_REGEX.split(part):
                subpart = subpart.strip()
                if subpart:
                    parts.append(subpart)

    return parts


def rewrite_explain(parts: list) -> list:
    """
    Normalize EXPLAIN FORMAT handling.

    The parser's grammar accepts FORMAT GRAPHVIZ and FORMAT JSON but not
    FORMAT MERMAID. Users may write any of these forms; we need to:
      - Treat explicit FORMAT GRAPHVIZ or FORMAT JSON as unsupported and raise
      - Allow FORMAT MERMAID by rewriting it to GRAPHVIZ so the parser will
        accept it (the logical planner will convert GRAPHVIZ -> MERMAID)

    The tokenizer (sql_parts) may split things in different ways, so we
    check both the combined head token and separated tokens.
    """
    # Build a head string from the tokens up to the main body (e.g., SELECT)
    select_idx = None
    for i, token in enumerate(parts):
        if token.upper().startswith("SELECT") or token.upper().startswith("WITH"):
            select_idx = i
            break
    head_tokens = parts[:select_idx] if select_idx is not None else parts
    # Scan SYNTAX only, never string CONTENT. The head runs up to the first
    # SELECT/WITH, so for `SET @a = 'FORMAT JSON'; SELECT @a;` the literal sits
    # inside it, and matching on the raw text rejected the statement with
    # "JSON format is not supported" — a value the user is merely storing read
    # as a directive. Blanking quoted spans (rather than dropping whole tokens)
    # also covers a literal embedded in a larger token, e.g. the
    # `CAST('...' AS VARBINARY)` that sql_parts builds for a b-string.
    head = _QUOTED_STRINGS_REGEX.sub(" ", " ".join(head_tokens)).upper()

    # If the head explicitly requests GRAPHVIZ or JSON, they are unsupported
    if "FORMAT GRAPHVIZ" in head:
        raise UnsupportedSyntaxError("GRAPHVIZ format is not supported")
    if "FORMAT JSON" in head:
        raise UnsupportedSyntaxError("JSON format is not supported")

    # If the head requests MERMAID, rewrite it to GRAPHVIZ so the parser accepts it
    if "FORMAT MERMAID" in head:
        # replace the first occurrence in the token list
        for i, token in enumerate(parts):
            if token.upper().startswith("FORMAT MERMAID"):
                parts[i] = token.upper().replace("FORMAT MERMAID", "FORMAT GRAPHVIZ")
                return parts

    # Otherwise look for separate 'FORMAT' and value tokens (e.g., ['FORMAT', 'MERMAID'])
    for i, token in enumerate(parts):
        if token.upper() == "FORMAT":
            # ensure there's a following token for the format value
            if i + 1 < len(parts):
                fmt = parts[i + 1].upper().rstrip(";")
                if fmt == "GRAPHVIZ":
                    raise UnsupportedSyntaxError("GRAPHVIZ format is not supported")
                if fmt == "JSON":
                    raise UnsupportedSyntaxError("JSON format is not supported")
                if fmt == "MERMAID":
                    # rewrite to GRAPHVIZ so parser accepts it
                    parts[i + 1] = "GRAPHVIZ"
            break

    return parts


def rewrite_comment(parts: list) -> list:
    """
    Rewrite COMMENT ON TABLE to COMMENT ON EXTENSION.

    The parser supports COMMENT ON EXTENSION but not COMMENT ON TABLE.
    This transformation allows users to write COMMENT ON TABLE and have it
    work seamlessly.

    Example:
        COMMENT ON TABLE workspace.collection.table IS 'description'
        -> COMMENT ON EXTENSION workspace.collection.table IS 'description'
    """
    # The tokenizer may produce patterns like:
    # ['COMMENT ON', 'TABLE workspace...'] or
    # ['COMMENT IF EXISTS', 'ON', 'TABLE workspace...'] or
    # ['COMMENT ON', 'TABLE', '"schema"', ...]

    for i in range(len(parts)):
        part = parts[i]
        part_upper = part.upper()

        # Check if this token starts with TABLE or VIEW (with a space after)
        if part_upper.startswith("TABLE "):
            parts[i] = "EXTENSION " + part[6:]  # Replace "TABLE " with "EXTENSION "
            break
        elif part_upper.startswith("VIEW "):
            parts[i] = "EXTENSION " + part[5:]  # Replace "VIEW " with "EXTENSION "
            break
        # Check if this token is exactly TABLE or VIEW (standalone token)
        elif part_upper == "TABLE" or part_upper == "VIEW":
            parts[i] = "EXTENSION"
            break

    return parts


def rewrite_drop_collection(statement: str) -> str:
    """
    Rewrite DROP COLLECTION to DROP SCHEMA.

    The parser (sqlparser-rs) has no COLLECTION object type, so DROP COLLECTION
    cannot be parsed natively. DROP SCHEMA is accepted and otherwise unused by
    opteryx, so rewriting to it lets DROP COLLECTION reach the planner as a
    Statement::Drop AST node with object_type == "Schema", which plan_drop()
    maps to a DropCollection logical plan node.

    Example:
        DROP COLLECTION workspace.collection -> DROP SCHEMA workspace.collection
        DROP COLLECTION IF EXISTS workspace.collection -> DROP SCHEMA IF EXISTS workspace.collection
    """
    return re.sub(r"^(\s*DROP\s+)COLLECTION\b", r"\1SCHEMA", statement, count=1, flags=re.IGNORECASE)


def rewrite_create_collection(statement: str) -> str:
    """
    Rewrite CREATE COLLECTION to CREATE SCHEMA.

    The mirror of rewrite_drop_collection, for the same reason: the parser has no
    COLLECTION object type, and CREATE SCHEMA is accepted and otherwise unused by
    opteryx, so rewriting to it lets CREATE COLLECTION reach the planner as a
    Statement::CreateSchema AST node, which plan_create_collection() maps to a
    CreateCollection logical plan node.

    Example:
        CREATE COLLECTION workspace.collection -> CREATE SCHEMA workspace.collection
        CREATE COLLECTION IF NOT EXISTS ws.col -> CREATE SCHEMA IF NOT EXISTS ws.col
    """
    return re.sub(
        r"^(\s*CREATE\s+)COLLECTION\b", r"\1SCHEMA", statement, count=1, flags=re.IGNORECASE
    )


def rewrite_alter_workspace(statement: str) -> str:
    """
    Rewrite ALTER WORKSPACE to ALTER FUNCTION.

    The parser (sqlparser-rs) has no WORKSPACE object type, so ALTER WORKSPACE
    cannot be parsed natively. ALTER FUNCTION accepts the same
    `<name> SET <property> TO <value>` shape and is otherwise unused by opteryx,
    so rewriting to it lets ALTER WORKSPACE reach the planner as a
    Statement::AlterFunction AST node, which plan_alter_workspace() maps to an
    AlterWorkspace logical plan node.

    Example:
        ALTER WORKSPACE ws SET delete_protection TO OFF
        -> ALTER FUNCTION ws SET delete_protection TO OFF
    """
    return re.sub(
        r"^(\s*ALTER\s+)WORKSPACE\b", r"\1FUNCTION", statement, count=1, flags=re.IGNORECASE
    )


def rewrite_temporal_units(statement: str) -> str:
    """
    Rewrite temporal unit syntax to internal form for parser compatibility.

    User-facing syntax:  TIMESTAMP[ns], TIMESTAMP[ms], TIMESTAMP[s], TIMESTAMP[us], TIMESTAMP[d]
    Internal form:       _TIMESTAMP_NS, _TIMESTAMP_MS, _TIMESTAMP_S, _TIMESTAMP_US, _TIMESTAMP_DAYS

    This allows users to write familiar syntax that the parser doesn't natively
    support, while using internal forms that the parser accepts as custom types.

    Example:
        CAST(x AS TIMESTAMP[ns]) -> CAST(x AS _TIMESTAMP_NS)
        CAST(x AS TIMESTAMP[ms]) -> CAST(x AS _TIMESTAMP_MS)
        CAST(x AS TIMESTAMP[s])  -> CAST(x AS _TIMESTAMP_S)
        CAST(x AS TIMESTAMP[us]) -> CAST(x AS _TIMESTAMP_US)
        CAST(x AS TIMESTAMP[d])  -> CAST(x AS _TIMESTAMP_DAYS)
    """
    # Check for invalid forms (empty brackets)
    if re.search(r"\bTIMESTAMP\s*\[\s*\]", statement, re.IGNORECASE):
        raise UnsupportedSyntaxError(
            "TIMESTAMP[] with empty brackets is not supported. "
            "Use `TIMESTAMP[ns]`, `TIMESTAMP[ms]`, `TIMESTAMP[s]`, `TIMESTAMP[us]`, or `TIMESTAMP[d]`."
        )

    # Map user-facing forms to internal forms
    replacements = [
        (r"\bTIMESTAMP\s*\[\s*ns\s*\]", "_TIMESTAMP_NS", re.IGNORECASE),
        (r"\bTIMESTAMP\s*\[\s*ms\s*\]", "_TIMESTAMP_MS", re.IGNORECASE),
        (r"\bTIMESTAMP\s*\[\s*s\s*\]", "_TIMESTAMP_S", re.IGNORECASE),
        (r"\bTIMESTAMP\s*\[\s*us\s*\]", "_TIMESTAMP_US", re.IGNORECASE),
        (r"\bTIMESTAMP\s*\[\s*d\s*\]", "_TIMESTAMP_DAYS", re.IGNORECASE),
    ]

    for pattern, replacement, flags in replacements:
        statement = re.sub(pattern, replacement, statement, flags=flags)

    return statement


def do_sql_rewrite(statement):
    # Collapse structural newlines/tabs/CRs (and their backslash-escaped text
    # forms) to spaces so the rest of the rewriter -- sql_parts, and the
    # text-based rewrite_temporal_units/rewrite_explain/rewrite_comment that
    # run on it -- see a single-line statement. Quoted string literals are
    # left untouched so their escape sequences reach the parser intact.
    if isinstance(statement, bytes):
        statement = statement.decode("utf-8")

    statement = _normalize_whitespace(statement)

    # Rewrite temporal unit syntax before parsing
    statement = rewrite_temporal_units(statement)

    # Rewrite CREATE/DROP COLLECTION before parsing (parser has no COLLECTION object type)
    statement = rewrite_create_collection(statement)
    statement = rewrite_drop_collection(statement)

    # Rewrite ALTER WORKSPACE before parsing (parser has no WORKSPACE object type)
    statement = rewrite_alter_workspace(statement)

    parts = sql_parts(statement)
    parts = rewrite_explain(parts)
    if statement.lstrip().upper().startswith("COMMENT"):
        parts = rewrite_comment(parts)
    return " ".join(parts)
