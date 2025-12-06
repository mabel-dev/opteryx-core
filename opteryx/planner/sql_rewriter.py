# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
~~~
                      ┌───────────┐
                      │   USER    │
         ┌────────────┤           ◄────────────┐
         │SQL         └───────────┘            │
  ───────┼─────────────────────────────────────┼──────
         │                                     │
   ┌─────▼─────┐                               │
   │ SQL       │                               │
   │ Rewriter  │                               │
   └─────┬─────┘                               │
         │SQL                                  │Results
   ┌─────▼─────┐                         ┌─────┴─────┐
   │           │                         │           │
   │ Parser    │                         │ Executor  │
   └─────┬─────┘                         └─────▲─────┘
         │AST                                  │Plan
   ┌─────▼─────┐      ┌───────────┐      ┌─────┴─────┐
   │ AST       │      │           │      │ Physical  │
   │ Rewriter  │      │ Catalogue │      │ Planner   │
   └─────┬─────┘      └───────────┘      └─────▲─────┘
         │AST               │Schemas           │Plan
   ┌─────▼─────┐      ┌─────▼─────┐      ┌─────┴─────┐
   │ Logical   │ Plan │           │ Plan │           │
   │   Planner ├──────► Binder    ├──────► Optimizer │
   └───────────┘      └───────────┘      └───────────┘

~~~

The SQL Rewriter does the following:
- strips comments
- normalizes whitespace
- temporal extraction (this is non-standard and not part of the parser)

This compensates for missing temporal table support in the SQL parser (sqlparser-rs).
This is relatively complex for what it appears to be doing - it needs to account for
a number of situations whilst being able to reconstruct the SQL query as the parser
would expect it.

For information on temporal tables see:
https://blog.devgenius.io/a-query-in-time-introduction-to-sql-server-temporal-tables-145ddb1355d9

This supports the following syntaxes:

- FOR <timestamp>
- FOR DATES BETWEEN <timestamp> AND <timestamp>
- FOR DATES IN <range>
- FOR DATES SINCE <timestamp>
- FOR LAST <number> DAYS

"""

import datetime
import re
from typing import List

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.utils import dates

COLLECT_RELATION = {
    r"ANALYZE\sTABLE",
    r"ANTI\sJOIN",
    r"CREATE\sTABLE",
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
}

COLLECT_TEMPORAL = {r"FOR"}

STOP_COLLECTING = {
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
}

COLLECT_ALIAS = {r"AS"}

BOUNDARIES = {r"(", r")"}

FOR_DATE_CLAUSES = {
    r"DATES\sIN\s\w+",
    r"DATES\sBETWEEN\s[^\r\n\t\f\v]AND\s[^\r\n\t\f\v]",
    r"DATES\sSINCE\s\w+",
    r"LAST\s\d+\sDAYS",
}

FUNCTIONS_WITH_FROM_SYNTAX = {"EXTRACT", "SUBSTRING", "TRIM"}

SQL_PARTS = (
    COLLECT_RELATION.union(COLLECT_TEMPORAL)
    .union(STOP_COLLECTING)
    .union(COLLECT_ALIAS)
    .union(FOR_DATE_CLAUSES)
)

COMBINE_WHITESPACE_REGEX = re.compile(r"\r\n\t\f\v+")

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

# states for the collection algorithm
WAITING: int = 1
RELATION: int = 2
TEMPORAL: int = 4
ALIAS: int = 8
FUNCTION_RELATION: int = 16

WEEKDAYS: List[str] = [
    "MONDAY",
    "TUESDAY",
    "WEDNESDAY",
    "THURSDAY",
    "FRIDAY",
    "SATURDAY",
    "SUNDAY",
]

# Get current time in UTC but without timezone info
NOW = (
    datetime.datetime.now(tz=datetime.timezone.utc)
    .replace(tzinfo=None)
    .replace(hour=0, minute=0, second=0, microsecond=0)
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
                from opteryx.third_party.alantsd import base64

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
    The parser does not support MERMAID format.

    We rewrite it to GRAPHVIZ, we don't support.
    """
    if parts[0] == "EXPLAIN ANALYZE FORMAT GRAPHVIZ":
        raise UnsupportedSyntaxError("GRAPHVIZ format is not supported")
    if parts[0] == "EXPLAIN ANALYZE FORMAT JSON":
        raise UnsupportedSyntaxError("JSON format is not supported")
    if parts[0].upper() == "EXPLAIN ANALYZE FORMAT MERMAID":
        parts[0] = "EXPLAIN ANALYZE FORMAT GRAPHVIZ"
    return parts


def do_sql_rewrite(statement):
    parts = sql_parts(statement)
    parts = rewrite_explain(parts)
    return " ".join(parts)
