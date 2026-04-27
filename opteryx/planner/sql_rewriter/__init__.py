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


COMBINE_WHITESPACE_REGEX = re.compile(r"[\r\n\t\f\v]+")

# Precompile regex patterns at module level for performance
_KEYWORDS_REGEX = re.compile(
    r"(\,|\(|\)|;|\t|\n|\->>|\->|@>|@>>|\&\&|@\?|"
    + r"|".join([r"\b" + i.replace(r" ", r"\s") + r"\b" for i in SQL_PARTS])
    + r")",
    re.IGNORECASE,
)

# Match ", ', b", b', `
# We match b prefixes separately after the non-prefix versions
_QUOTED_STRINGS_REGEX = re.compile(
    r'("[^"]*"|\'[^\']*\'|\b[bB]"[^"]*"|\b[bB]\'[^\']*\'|\b[rR]"[^"]*"|\b[rR]\'[^\']*\'|`[^`]*`)'
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

                encoded_part = base64.encode(part[2:-1].encode()).decode()
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
    head = " ".join(head_tokens).upper()

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
    # If the SQL was passed with escaped sequences (e.g. "\\n"),
    # interpret the common ones so the rewriter sees real newlines/tabs.
    if isinstance(statement, bytes):
        statement = statement.decode("utf-8")

    statement = (
        statement.replace("\\r\\n", " ")
        .replace("\\n", " ")
        .replace("\\t", " ")
        .replace("\\r", " ")
        .replace("\n", " ")
        .replace("\t", " ")
        .replace("\r", " ")
    )

    # Rewrite temporal unit syntax before parsing
    statement = rewrite_temporal_units(statement)

    parts = sql_parts(statement)
    parts = rewrite_explain(parts)
    parts = rewrite_comment(parts)
    return " ".join(parts)
