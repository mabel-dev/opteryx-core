# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Turn a sqlparser failure into an error the reader can act on.

A parse failure is the first error many people meet, and what sqlparser hands back
is written for whoever is debugging the grammar:

    Query parsing failed.
        sql parser error: Expected: end of statement, found: FRM at Line: 1, Column: 10

Everything useful is in there - the location, and what the parser wanted - but it
is wrapped in the parser's own vocabulary, the location is glued to the end of a
sentence, and there is no sign of the statement it refers to. This module takes it
apart: pull out the position, cut the offending line out of the statement, point at
the column, and where the shape of the failure implies a likely cause, say so.

The guess uses `suggest_alternative` - the same typo detector the binder uses for
column and function names - against the SQL keywords. A misspelled keyword IS a
typo, so the detector already built for typos is the honest tool for it, and its
distance cap keeps it from inventing a keyword for a token that is simply wrong.
"""

import re
from typing import List
from typing import Optional

from opteryx.exceptions import QueryParseError
from opteryx.exceptions import SourcePosition
from opteryx.expression.intervals import INTERVAL_UNITS
from opteryx.planner.sql_rewriter import RewrittenStatement
from opteryx.utils.sql import offset_of

# sqlparser appends the position to the message text rather than carrying it as a
# field (see `parser_err!` in sqlparser's parser/mod.rs), and omits it entirely
# when the position is unknown. This lifts it back out.
_LOCATION = re.compile(r"\s*at Line:\s*(\d+),\s*Column:\s*(\d+)\s*\.?\s*$")

# The prefixes our own Rust binding and sqlparser add, which say nothing to a reader.
# Deliberately unanchored: the two arrive nested ("Query parsing failed.\n\tsql
# parser error: ..."), and an anchored pattern strips the outer one, consumes the
# newline with it, and then has nowhere for `^` to match the inner one.
_NOISE = re.compile(r"(Query parsing failed\.|sql parser error:)\s*")

# `found: X` is how sqlparser names the token it stopped on.
_FOUND = re.compile(r"found:\s*(\S+?)\s*$")

# The last bare word before a position - the token the parser had already accepted
# when it gave up on the next one.
_WORD_BEFORE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*$")

# Below this, a "typo" is mostly noise: `id` sits one edit from IN and `xx` two from
# IN, and neither reader meant a keyword. Every keyword worth misspelling is longer.
_MIN_TYPO_LENGTH = 3

# The keyword set the typo detector matches against. Listed most-used first -
# `suggest_alternative` keeps the FIRST best match, so when two keywords sit the
# same edit distance from a typo the more common one should be offered. That
# ordering is the whole reason this is a list and not a set.
#
# This used to be a top-up, with the clause, join and operator spellings read at
# runtime from the generated catalogs in `reference/`. `reference/` is a
# repo-root package that documentation tooling generates and the wheel does not
# ship, so in an installed deployment that import raised ModuleNotFoundError -
# from the error path, which meant every parse failure in production was
# reported as `No module named 'reference'` and the reader never saw which token
# the parser stopped on. The catalogs remain the source of truth for what the
# engine supports; `tests/unit/reference/test_parse_error_keywords.py` fails if
# one gains a word this list lacks, so nobody has to remember to add it here and
# nothing has to be importable at runtime for it to be suggestable.
_KEYWORDS: List[str] = [
    "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "BY", "HAVING", "LIMIT", "JOIN",
    "AS", "ON", "AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "DISTINCT",
    "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "USING", "UNION",
    "CASE", "WHEN", "THEN", "ELSE", "END", "BETWEEN", "EXISTS", "ALL", "ANY",
    "ASC", "DESC", "OFFSET", "WITH", "VALUES", "INSERT", "INTO", "UPDATE",
    "DELETE", "CREATE", "DROP", "TABLE", "VIEW", "SET", "SHOW", "EXPLAIN",
    "TRUE", "FALSE", "OVER", "PARTITION", "UNNEST", "CAST",
    # A keyword missing from this list is worse than merely un-suggestable: the
    # detector treats it as an unknown word and offers a near-miss FOR it. `FOR`
    # absent meant `... FOR TIMESTAMP AS OF ...` was answered "did you mean `OR`?".
    "FOR", "OF", "IF", "TO", "INTO", "ADD", "ALL", "SOME", "ROW", "ROWS", "RANGE",
    "RECURSIVE", "TEMPORARY", "REPLACE", "COLLECTION", "WORKSPACE", "TRIGGER",
    "STATISTICS", "COLUMNS", "MANIFEST", "GRANTS", "USER", "VARIABLES", "TRUNCATE",
    "ANALYZE", "COMMENT", "RENAME", "CLUSTER", "ALTER", "MATCH", "AGAINST", "ASOF",
    "EXCEPT", "INTERSECT", "NULLS", "FIRST", "LAST", "PRECEDING", "FOLLOWING",
    "CURRENT", "PRIMARY", "KEY", "DEFAULT", "ARRAY", "STRUCT", "INTERVAL",
    # Named by the clause, join and operator catalogs but not by the grammar
    # keywords above. Sorted, because none of them is the more common reading of
    # any near-miss - the ordering rule only decides ties.
    "ANTI", "EFFECTIVE", "GRANT", "MATERIALIZED", "NATURAL", "OPTIMIZE", "OWNER",
    "QUALIFY", "REFRESH", "MERGE", "RESUME", "REVOKE", "SEMI", "SNAPSHOTS",
    "SUSPEND", "TOP", "TRIGGERS", "VERSION", "WINDOW",
]


def _keywords() -> List[str]:
    """The keyword set the typo detector matches against."""
    return _KEYWORDS




def _word_before(sql: str, line: Optional[int], column: Optional[int]):
    """The word before the token the parser stopped on, and its 1-based column.

    The column comes back with it so the caret can be moved onto the word we are
    actually talking about: pointing at `BY` while asking "did you mean ORDER?" makes
    the reader check the wrong word.

    This is where a misspelled keyword actually is. sqlparser reports the position it
    gave up at, and a misspelled keyword does not fail there - it parses happily as an
    identifier or an alias, and the parser only objects at the NEXT token. So
    `... $planets ODER BY id` is reported at `BY`, and `... WEHRE id > 1` is reported
    at `id`, with the real mistake one word earlier in both.
    """
    if not sql or not line or not column or line < 1:
        return None
    lines = sql.split("\n")
    if line > len(lines):
        return None
    found = _WORD_BEFORE.search(lines[line - 1][: column - 1])
    if not found:
        return None
    return found.group(1), found.start(1) + 1


# The word starting at a position - the token the parser stopped ON, read out of the
# statement rather than out of the parser's message.
_WORD_AT = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)")


def _word_at(sql: str, line: Optional[int], column: Optional[int]) -> Optional[str]:
    """The word beginning at the reported position, or None.

    A companion to `_word_before`, for the failures sqlparser reports WITHOUT a
    `found:` clause. Those messages name what the grammar wanted and never quote what
    the reader actually wrote, so the token has to be read back out of the statement
    if the error is going to say which word is wrong.
    """
    if not sql or not line or not column or line < 1 or column < 1:
        return None
    lines = sql.split("\n")
    if line > len(lines):
        return None
    found = _WORD_AT.match(lines[line - 1][column - 1 :])
    return found.group(1) if found else None


def _keyword_suggestion(token: Optional[str]) -> Optional[str]:
    """A keyword this token was probably a misspelling of, or None.

    `EOF` is excluded explicitly: it is sqlparser's name for running out of input, not
    anything the reader typed, and it is two edits from ON.
    """
    if not token or len(token) < _MIN_TYPO_LENGTH or not token.isalpha():
        return None
    upper = token.upper()
    if upper == "EOF" or upper in _keywords():
        return None

    from opteryx.utils import suggest_alternative

    candidate = suggest_alternative(upper, _keywords())
    # A typo rarely changes a word's length by more than one. `HASH` sits two edits
    # from `AS` and would otherwise be offered as one, which reads as nonsense next
    # to a caret pointing at a word the reader typed deliberately. Narrowing only.
    if candidate is None or abs(len(candidate) - len(upper)) > 1:
        return None
    return candidate


def _guess(cause: str, found: Optional[str], at_token: Optional[str] = None) -> Optional[str]:
    """A likely cause, when the shape of the failure implies one.

    Returns a sentence, or None when nothing here fits - in which case the caller
    falls back to generic advice rather than inventing a diagnosis.

    `at_token` is the word at the reported position, for the failures whose message
    does not quote what the reader wrote.
    """
    lowered = cause.lower()

    # An INTERVAL unit fails in one of TWO places, and a reader cannot tell them apart:
    # a unit sqlparser knows but the engine does not implement (CENTURY, DOY) reaches
    # the planner and is rejected there by name, while a unit sqlparser does not know at
    # all (`MOMENTS`, `FORTNIGHT`, `QUARTERS`) dies in the grammar with a message that
    # never quotes the offending word - "INTERVAL requires a unit after the literal
    # value" and a bare column number. Same mistake, so the same answer: name the unit,
    # and quote the same list the planner would have quoted.
    if "interval requires a unit" in lowered:
        units = ", ".join(unit.upper() for unit in INTERVAL_UNITS)
        # Only when the reader actually OFFERED a unit. With none at all
        # (`INTERVAL '7' AS x`) the token at the position is the next keyword, and
        # naming it reports `AS` as a bad unit - a diagnosis of a word the reader used
        # correctly, which is worse than the generic message it replaced.
        if at_token and at_token.upper() not in _keywords():
            return f"`{at_token.upper()}` is not a supported INTERVAL unit - valid units are {units}"
        return (
            f"An INTERVAL needs a unit after its value, as in `INTERVAL '7' DAY` - "
            f"valid units are {units}"
        )

    if "unterminated string literal" in lowered:
        return (
            "A quote opens here and is never closed - string literals need a matching "
            "closing quote"
        )
    # sqlparser words this as "Unexpected EOF while in a multi-line comment" rather
    # than "unterminated", so match on the comment itself, not on the adjective.
    if "comment" in lowered:
        return "A block comment opens here and is never closed with `*/`"
    if "expected: an sql statement" in lowered:
        return (
            "A statement has to start with a keyword such as `SELECT`, `SHOW`, "
            "`INSERT` or `CREATE`"
        )
    if found == "EOF":
        return "The statement ends before it is complete"
    if lowered.startswith("expected: )"):
        return "There is an opening bracket without a matching closing one"
    if lowered.startswith("expected: ("):
        return "This form needs its arguments in brackets"
    if "expected an expression" in lowered or "expected: an expression" in lowered:
        if found and found.isalpha() and found.upper() in _keywords():
            return (
                f"A value was expected before `{found.upper()}` - this is usually a "
                f"trailing comma, or a column name that was left out"
            )
        return "A value or column name was expected here"
    return None


def _position(sql: str, line, column, token) -> Optional[SourcePosition]:
    """The range to underline: the offending token, or an empty range where it starts.

    sqlparser reports a POINT, so unlike an identifier's span there is no end to map -
    it has to be reconstructed. `token` is the word we are talking about (the one the
    parser stopped on, or the misspelled keyword before it), and its length is the
    extent. When we cannot name a token - the statement ended, the failure was about a
    bracket - the range collapses to nothing at the start, which an editor draws as a
    caret between two characters. That is the honest answer: we know where, not how far.
    """
    if line is None or column is None:
        return None
    start = offset_of(sql, line, column)
    if start is None:
        return None
    # Only when the token really is the text at that position. `found` is sqlparser's
    # NAME for what it saw ("EOF", "end of statement"), which is not always a substring
    # of the statement, and underlining by its length would mark the wrong characters.
    length = len(token) if token and sql[start : start + len(token)] == token else 0
    return SourcePosition(line, column, line, column + length, start, start + length)


def raise_parse_error(sql, error: BaseException) -> None:
    """Re-raise a sqlparser failure as a QueryParseError. Never returns.

    `sql` is what was handed to the parser. When it is a `RewrittenStatement` it also
    knows the text the READER wrote and how to map a position from one to the other, and
    that is the text everything below is built against - a position the reader cannot
    find in their own statement is worse than none. A plain `str` is its own source.
    """
    raw = _NOISE.sub("", str(error)).strip()

    line = column = None
    located = _LOCATION.search(raw)
    if located:
        line, column = int(located.group(1)), int(located.group(2))
        raw = raw[: located.start()].strip()

    if isinstance(sql, RewrittenStatement):
        line, column = sql.to_source_position(line, column)
        sql = sql.source
    sql = str(sql)

    found_match = _FOUND.search(raw)
    found = found_match.group(1) if found_match else None

    # Where we point. The parser's position is where it gave up; when the mistake is
    # the word before it, the reported position moves onto that word, so the message
    # and the thing it marks agree.
    point_at = column
    marked = found
    at_token = _word_at(sql, line, column)
    # sqlparser names no token for this failure, so `found` is None and the caret would
    # collapse to a zero-width mark. The offending unit IS in the statement at the
    # reported position - mark it, so the message and what it underlines agree.
    if (
        marked is None
        and "interval requires a unit" in raw.lower()
        and at_token
        and at_token.upper() not in _keywords()
    ):
        marked = at_token
    preceding = _word_before(sql, line, column)
    suggestion = _keyword_suggestion(preceding[0]) if preceding else None
    if suggestion:
        point_at, marked = preceding[1], preceding[0]
    else:
        suggestion = _keyword_suggestion(found)

    raise QueryParseError(
        sql=sql,
        line=line,
        column=point_at,
        cause=raw or None,
        suggestion=suggestion,
        hint=None if suggestion else _guess(raw, found, at_token),
        position=_position(sql, line, point_at, marked),
    ) from error
