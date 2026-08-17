# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The SQL Rewriter is the first transformation in the planning pipeline. It operates on
the raw SQL string before it reaches the parser.

Input:  raw SQL string (bytes or str) as supplied by the caller
Output: a `RewrittenStatement` - the text the parser will see, carrying enough
        provenance to map any position in it back to a position in the caller's text

Responsibilities:
- Rewrites TIMESTAMP[ns/ms/s/us/d] bracketed syntax to internal type names the parser
  accepts (e.g. TIMESTAMP[ns] -> _TIMESTAMP_NS)
- Rewrites b-string literals (b'...' -> CAST(... AS VARBINARY)) and r-string literals
  (r'...' -> BASE64_DECODE(...)), which the parser's tokenizer will not produce for
  this dialect
- Normalises EXPLAIN FORMAT MERMAID to FORMAT GRAPHVIZ so the parser accepts it;
  rejects FORMAT GRAPHVIZ and FORMAT JSON explicitly
- Rewrites CREATE/DROP COLLECTION to CREATE/DROP SCHEMA, and ALTER/DROP WORKSPACE
  to ALTER/DROP FUNCTION, so the parser accepts statements whose object types it
  has no grammar for

The rewriter does NOT parse SQL into an AST; it only manipulates the text.

EVERY REWRITE IS AN EDIT, NOT A REFORMAT
----------------------------------------
sqlparser reports a line and column for a parse failure, and hangs a `span` off every
identifier in the AST it returns. Both index the text the PARSER was given. This module
used to tokenise the whole statement on a keyword regex and rejoin it with single spaces
(`SELECT name, gravity` came back as `SELECT name , gravity`), which moved every
character to the right of the first comma and made those positions unusable for pointing
at anything the reader actually typed.

So the rewriter now only ever substitutes the spans it is actually changing, and records
each substitution as `(out_start, out_length, src_start, src_length)`. Text the rewriter
does not care about is passed through byte-for-byte. `RewrittenStatement.to_source_*`
walks those records - one list per pass, applied in reverse - to turn a position in the
parser's text back into a position in the reader's. A statement with no rewrites in it
at all (the overwhelming majority) maps one-to-one.

Comments and line structure are NOT stripped here, and must not be: the parser tokenises
comments itself, and the line breaks are what make a caret worth printing.
"""

import re
from typing import Callable
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.utils.sql import offset_of
from opteryx.utils.sql import position_of

# One edit: where the replacement landed in the output, and what it replaced in the input.
Edit = Tuple[int, int, int, int]

# A quoted span - a string literal or a quoted identifier. Every pattern in this module
# alternates this FIRST and declines to rewrite when it matches, so that a rewrite never
# reaches inside a literal. SQL escapes a quote by doubling it ('don''t' is one literal,
# not two), which `'[^']*'` cannot express - it ends the literal at the first inner quote
# and everything after it is read as bare SQL. `(?:[^']|'')*` consumes a doubled quote as
# one unit instead. The two alternatives are disjoint (the first cannot match a quote, the
# second only matches a pair), so there is nothing for the engine to backtrack over.
_QUOTED_SPAN = r'"(?:[^"]|"")*"' r"|'(?:[^']|'')*'" r"|`[^`]*`"


class RewrittenStatement(str):
    """The SQL handed to the parser, plus the provenance to get back from it.

    It IS the rewritten text - it is a `str` and every caller that just wants to parse
    it can keep treating it as one. The extra state is for the callers that need to say
    where something is: the parse-error formatter, and the binder reporting on a column.

    `source` is the caller's whole original statement (or batch); `source_offset` is
    where the text this rewriter was given starts inside it, so a statement pulled out
    of a semicolon-separated batch still reports positions in the text the reader wrote.
    """

    __slots__ = ("source", "source_offset", "_passes")

    def __new__(
        cls,
        text: str,
        source: str,
        source_offset: int = 0,
        passes: Sequence[Sequence[Edit]] = (),
    ) -> "RewrittenStatement":
        obj = super().__new__(cls, text)
        obj.source = source
        obj.source_offset = source_offset
        # Passes ran in order, each against the previous one's output, so mapping a
        # position backwards applies them in reverse. Empty passes are dropped - the
        # common case is that none of them fired and this is an empty tuple.
        obj._passes = tuple(tuple(edits) for edits in passes if edits)
        return obj

    def to_source_offset(self, offset: int) -> int:
        """Map a character offset in the parser's text to one in `source`."""
        for edits in reversed(self._passes):
            offset = _unmap_offset(offset, edits)
        return offset + self.source_offset

    def to_source_point(self, line: Optional[int], column: Optional[int]):
        """Map a 1-based (line, column) in the parser's text into `source`.

        Returns `(line, column, offset)` - both conventions, because the callers want
        different ones and computing an offset from a line and column is exactly the
        arithmetic worth doing once. `None` when there is no position to map, so a
        caller can pass a parser result straight through without testing it first.
        """
        if line is None or column is None:
            return None
        offset = offset_of(str(self), line, column)
        if offset is None:
            return None
        source_offset = self.to_source_offset(offset)
        line, column = position_of(self.source, source_offset)
        return (line, column, source_offset)

    def to_source_position(self, line: Optional[int], column: Optional[int]):
        """As `to_source_point`, without the offset. Returns `(None, None)` if unmappable."""
        point = self.to_source_point(line, column)
        return (None, None) if point is None else point[:2]


def _unmap_offset(offset: int, edits: Sequence[Edit]) -> int:
    """Map an offset in one pass's output back to an offset in its input.

    An offset that lands INSIDE a replacement maps to the start of what was replaced:
    the interior of `CAST('abc' AS VARBINARY)` has no counterpart in the `b'abc'` the
    reader wrote, and the start of that literal is the only honest thing to point at.
    """
    source_offset = offset
    for out_start, out_length, src_start, src_length in edits:
        if offset < out_start:
            break
        if offset < out_start + out_length:
            return src_start
        source_offset = offset - (out_start + out_length) + (src_start + src_length)
    return source_offset


def _substitute(text: str, pattern, replacer: Callable) -> Tuple[str, List[Edit]]:
    """`re.sub`, but recording where every replacement landed.

    `replacer` returns the replacement text, or None to leave the match alone - which is
    how each pattern's leading quoted-span alternative opts out without the caller having
    to strip literals out of the statement first.
    """
    pieces: List[str] = []
    edits: List[Edit] = []
    read_from = 0
    shift = 0

    for match in pattern.finditer(text):
        replacement = replacer(match)
        if replacement is None:
            continue
        start, end = match.start(), match.end()
        pieces.append(text[read_from:start])
        pieces.append(replacement)
        edits.append((start + shift, len(replacement), start, end - start))
        shift += len(replacement) - (end - start)
        read_from = end

    if not edits:
        return text, []
    pieces.append(text[read_from:])
    return "".join(pieces), edits


# --------------------------------------------------------------------------------------
# The rewrites
# --------------------------------------------------------------------------------------

# Backslash-escaped line breaks OUTSIDE a literal - the two characters `\` and `n`, not a
# newline. A caller that carried the statement through JSON or a shell can deliver them,
# and the parser has no meaning for a bare backslash. Real newlines and tabs are left
# exactly where they are: they are the statement's line structure, and destroying it was
# what made positions unusable.
_ESCAPED_BREAKS = re.compile(rf"(?P<quoted>{_QUOTED_SPAN})|(?P<escape>\\r\\n|\\n|\\t|\\r)")

# TIMESTAMP[ns] and friends. The parser has no grammar for a bracketed type argument, so
# the unit is folded into the type name. Note the lengths: `TIMESTAMP[ns]` and
# `_TIMESTAMP_NS` are both 13 characters, as are the `ms` and `us` forms, and `[s]` and
# `_TIMESTAMP_S` are both 12 - only the `[d]` form changes length, so in practice this
# rewrite moves nothing.
_TEMPORAL_UNITS = re.compile(
    rf"(?P<quoted>{_QUOTED_SPAN})"
    r"|(?P<empty>\bTIMESTAMP\s*\[\s*\])"
    r"|(?P<unit>\bTIMESTAMP\s*\[\s*(?P<name>ns|ms|us|s|d)\s*\])",
    re.IGNORECASE,
)
_TEMPORAL_INTERNAL = {
    "ns": "_TIMESTAMP_NS",
    "ms": "_TIMESTAMP_MS",
    "us": "_TIMESTAMP_US",
    "s": "_TIMESTAMP_S",
    "d": "_TIMESTAMP_DAYS",
}

# b'...' and r'...'. The parser's tokenizer DOES have both, but it gates them on a
# hardcoded dialect list (`dialect_of!(self is BigQuery | Postgres | MySQL | Generic)` in
# sqlparser's tokenizer.rs) rather than a dialect flag, so OpteryxDialect cannot opt in.
# This rewrite is load-bearing for CORRECTNESS, not convenience: without it `SELECT b'abc'`
# does not fail, it parses as `SELECT b AS "abc"` - a column reference with an alias, and a
# silently wrong query. It cannot move to the AST rewriter for the same reason: that parse
# is indistinguishable from one a reader could legitimately have meant. The fix belongs in
# the tokenizer, upstream, behind a `supports_byte_string_literal()` dialect method.
_PREFIXED_STRINGS = re.compile(
    rf"(?P<quoted>{_QUOTED_SPAN})"
    r"|(?P<bytes>\b[bB](?:\"(?:[^\"]|\"\")*\"|'(?:[^']|'')*'))"
    r"|(?P<raw>\b[rR](?:\"(?:[^\"]|\"\")*\"|'(?:[^']|'')*'))"
)

# A projection with no alias, immediately before FROM or JOIN. The rewrite above replaces
# the literal the reader wrote with a function call, and the output column would otherwise
# be named after the call rather than after the literal.
_UNALIASED = re.compile(r"\s*(?:FROM|JOIN)\s", re.IGNORECASE)

_EXPLAIN_FORMAT = re.compile(
    rf"(?P<quoted>{_QUOTED_SPAN})|\bFORMAT\s+(?P<format>[A-Za-z_]+)", re.IGNORECASE
)
# Where the statement's head ends. Everything before the first SELECT or WITH is the
# EXPLAIN preamble; `FORMAT` after that point belongs to the query, not to us.
_QUERY_BODY = re.compile(rf"(?P<quoted>{_QUOTED_SPAN})|\b(?:SELECT|WITH)\b", re.IGNORECASE)

_CREATE_COLLECTION = re.compile(r"^(\s*CREATE\s+)COLLECTION\b", re.IGNORECASE)
_DROP_COLLECTION = re.compile(r"^(\s*DROP\s+)COLLECTION\b", re.IGNORECASE)
_ALTER_WORKSPACE = re.compile(r"^(\s*ALTER\s+)WORKSPACE\b", re.IGNORECASE)
_DROP_WORKSPACE = re.compile(r"^(\s*DROP\s+)WORKSPACE\b", re.IGNORECASE)


def _rewrite_escaped_breaks(text: str) -> Tuple[str, List[Edit]]:
    def replace(match):
        if match.group("quoted") is not None:
            return None
        return " "

    return _substitute(text, _ESCAPED_BREAKS, replace)


def _rewrite_temporal_units(text: str) -> Tuple[str, List[Edit]]:
    """TIMESTAMP[ns] -> _TIMESTAMP_NS, and friends.

    An unrecognised unit is left alone deliberately - the parser rejects it by name, and
    inventing an error here would just be a worse version of the one it already gives.
    """

    def replace(match):
        if match.group("quoted") is not None:
            return None
        if match.group("empty") is not None:
            raise UnsupportedSyntaxError(
                "TIMESTAMP[] with empty brackets is not supported. "
                "Use `TIMESTAMP[ns]`, `TIMESTAMP[ms]`, `TIMESTAMP[s]`, `TIMESTAMP[us]`, or `TIMESTAMP[d]`."
            )
        return _TEMPORAL_INTERNAL[match.group("name").lower()]

    return _substitute(text, _TEMPORAL_UNITS, replace)


def _rewrite_prefixed_strings(text: str) -> Tuple[str, List[Edit]]:
    """b'...' -> CAST('...' AS VARBINARY), r'...' -> BASE64_DECODE('...')."""
    from opteryx.third_party.mabel import base64

    def replace(match):
        if match.group("quoted") is not None:
            return None

        literal = match.group("bytes") or match.group("raw")
        payload = literal[2:-1]

        if match.group("bytes") is not None:
            # The payload goes back out as SQL for the parser to re-read, so its doubled
            # quotes stay doubled.
            rewritten = f"CAST({literal[1:]} AS VARBINARY)"
        else:
            # This one does NOT go back out as SQL - it is the literal's VALUE, encoded
            # for the engine to decode. The parser never sees it to un-escape, so the
            # doubling has to come off here.
            encoded = base64.encode(payload.replace("''", "'").encode()).decode()
            rewritten = f"BASE64_DECODE('{encoded}')"

        if _UNALIASED.match(match.string, match.end()):
            rewritten = f"{rewritten} AS {payload}"
        return rewritten

    return _substitute(text, _PREFIXED_STRINGS, replace)


def _rewrite_explain_format(text: str) -> Tuple[str, List[Edit]]:
    """Normalize EXPLAIN FORMAT handling.

    The parser's grammar accepts FORMAT GRAPHVIZ and FORMAT JSON but not FORMAT MERMAID.
    Readers may write any of these, so:
      - explicit FORMAT GRAPHVIZ or FORMAT JSON are unsupported and raise
      - FORMAT MERMAID is rewritten to GRAPHVIZ so the parser will accept it (the logical
        planner converts GRAPHVIZ -> MERMAID)

    Only the statement's head is scanned, and only its SYNTAX: for
    `SET @a = 'FORMAT JSON'; SELECT @a;` the literal sits in the head, and reading it as
    a directive rejected a statement whose author was merely storing the value.
    """
    body_at = len(text)
    for match in _QUERY_BODY.finditer(text):
        if match.group("quoted") is None:
            body_at = match.start()
            break

    def replace(match):
        if match.group("quoted") is not None or match.start() >= body_at:
            return None
        requested = match.group("format").upper()
        if requested == "GRAPHVIZ":
            raise UnsupportedSyntaxError("GRAPHVIZ format is not supported")
        if requested == "JSON":
            raise UnsupportedSyntaxError("JSON format is not supported")
        if requested == "MERMAID":
            return f"{match.group(0)[: -len(match.group('format'))]}GRAPHVIZ"
        return None

    return _substitute(text, _EXPLAIN_FORMAT, replace)


def _rewrite_object_types(text: str) -> Tuple[str, List[Edit]]:
    """COLLECTION -> SCHEMA, WORKSPACE -> FUNCTION.

    sqlparser has no COLLECTION object type and no WORKSPACE statement, and both are
    Opteryx concepts rather than syntax any other engine would recognise, so there is
    nothing to send upstream. SCHEMA and ALTER/DROP FUNCTION accept the same shapes
    and are otherwise unused by opteryx, so the statements reach the planner as AST
    nodes that plan_create_collection / plan_drop / plan_alter_workspace map onward
    (DROP WORKSPACE's own object_type=="Function" branch lives in plan_drop,
    alongside DROP COLLECTION's object_type=="Schema" one).

    All four anchor at the start of the statement, so nothing inside a literal, an
    identifier or a subquery is at risk.
    """
    edits: List[Edit] = []
    for pattern, replacement in (
        (_CREATE_COLLECTION, "SCHEMA"),
        (_DROP_COLLECTION, "SCHEMA"),
        (_ALTER_WORKSPACE, "FUNCTION"),
        (_DROP_WORKSPACE, "FUNCTION"),
    ):
        match = pattern.match(text)
        if match is None:
            continue
        start, end = match.end(1), match.end()
        text = text[:start] + replacement + text[end:]
        edits.append((start, len(replacement), start, end - start))
        # Mutually exclusive - a statement cannot start with more than one of
        # CREATE, DROP and ALTER - so the first hit is the only hit.
        break
    return text, edits


def do_sql_rewrite(
    statement, source: Optional[str] = None, source_offset: int = 0
) -> RewrittenStatement:
    """Rewrite `statement` into the text the parser will be given.

    `source` and `source_offset` describe where `statement` came from, for callers that
    pulled it out of a larger body of text (a semicolon-separated batch). They default to
    the statement itself, at offset zero.
    """
    if isinstance(statement, bytes):
        statement = statement.decode("utf-8")
    if source is None:
        source = statement

    passes: List[List[Edit]] = []
    for rewrite in (
        _rewrite_escaped_breaks,
        _rewrite_temporal_units,
        _rewrite_object_types,
        _rewrite_prefixed_strings,
        _rewrite_explain_format,
    ):
        statement, edits = rewrite(statement)
        passes.append(edits)

    return RewrittenStatement(statement, source=source, source_offset=source_offset, passes=passes)
