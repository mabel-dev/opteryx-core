# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
GRAPH_TABLE (SQL/PGQ): a graph pattern in the FROM clause.

    SELECT manager, COUNT(*)
    FROM GRAPH_TABLE (hr.org.g
           MATCH (m:Person)-[:MANAGES]->(r:Person)
           COLUMNS (m.name AS manager, r.id AS report_id))
    GROUP BY manager

It is a table factor, so it composes with everything SQL already does: joins,
aggregates, CTEs, views. The pattern does the part SQL is bad at - naming a
path through relationships - and SQL does the rest.

sqlparser has no grammar for it, and forking sqlparser to add one was ruled out
(docs/PROPERTY_GRAPH_DESIGN.md). It needs no fork, because the work divides at
exactly the line between what needs the catalog and what does not:

1. TEXT, in the SQL rewriter (`rewrite_graph_tables`): the span the parser
   cannot read becomes a table-function call it can -
   `GRAPH_TABLE('<graph>', '<pattern>')` - with the edit recorded like any other
   rewrite's. No catalog, so `parse_statement` stays syntax-only and
   `analyze_query` can still name the graph (`graph_table_name`).

2. AST, in the logical planner (`expand_graph_table`): that call is replaced by
   the derived subquery the pattern lowers to. gqlparser emits the same
   sqlparser statement dict this engine's parser produces, so the subquery is
   spliced in dict-to-dict - there is no SQL text round trip - and is then
   planned, resolved and bound as the ordinary subquery it now is.

PERMISSIONS. Two separate things are read, and both are checked as the caller.
The graph's DEFINITION is read here, at READ on the graph, before a label or a
property is looked up - an error about the pattern would otherwise let a caller
with no access probe what the graph contains. The DATA is read by the scans of
the lowered subquery, each bound and READ-checked like any other scan. A graph
never lends access to its tables.

gqlparser is imported here and nowhere else in the engine, lazily. Standalone
GQL remains outside the engine (rulings D7/D8); what changed is that a pattern
INSIDE a SQL statement is the engine's to plan, and without the package this
one table factor is refused by name while everything else is unaffected.
"""

import re
from typing import List
from typing import Optional
from typing import Tuple

from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import SqlError
from opteryx.exceptions import UnsupportedSyntaxError

GRAPH_TABLE = "GRAPH_TABLE"

# The sqlparser release this engine parses with (Cargo.toml). gqlparser states
# the release its statement dicts are shaped for; a splice across two different
# shapes would fail somewhere far from here, so it is refused here instead.
SQLPARSER_FORMAT = "0.63"

_Edit = Tuple[int, int, int, int]  # (out_start, out_length, src_start, src_length)

_NAME = re.compile(r"GRAPH_TABLE\b", re.IGNORECASE)
_MATCH = re.compile(r"\bMATCH\b", re.IGNORECASE)
_COLUMNS = re.compile(r"COLUMNS\s*\(", re.IGNORECASE)
_WORD_CHARACTERS = "_$"


# --------------------------------------------------------------------------------------
# 1. Text
# --------------------------------------------------------------------------------------


def _skip_quoted(text: str, at: int) -> int:
    """`at` is on a quote character; the index just past its closing quote."""
    quote = text[at]
    cursor = at + 1
    while cursor < len(text):
        if text[cursor] == quote:
            if text[cursor + 1 : cursor + 2] == quote:  # a doubled quote is the quote itself
                cursor += 2
                continue
            return cursor + 1
        cursor += 1
    return len(text)  # unterminated: the parser reports it, with a position


def _skip_trivia(text: str, at: int) -> Optional[int]:
    """If `at` starts a quoted span or a comment, the index just past it."""
    character = text[at]
    if character in "'\"`":
        return _skip_quoted(text, at)
    if text.startswith("--", at):
        end = text.find("\n", at)
        return len(text) if end < 0 else end
    if text.startswith("/*", at):
        end = text.find("*/", at + 2)
        return len(text) if end < 0 else end + 2
    return None


def _closing_parenthesis(text: str, open_at: int) -> int:
    """Index of the `)` that closes the `(` at `open_at`, or -1."""
    depth = 0
    cursor = open_at
    while cursor < len(text):
        skipped = _skip_trivia(text, cursor)
        if skipped is not None:
            cursor = skipped
            continue
        if text[cursor] == "(":
            depth += 1
        elif text[cursor] == ")":
            depth -= 1
            if depth == 0:
                return cursor
        cursor += 1
    return -1


def _is_word_boundary(text: str, at: int) -> bool:
    if at == 0:
        return True
    previous = text[at - 1]
    return not (previous.isalnum() or previous in _WORD_CHARACTERS or previous == ".")


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def rewrite_graph_tables(text: str) -> Tuple[str, List[_Edit]]:
    """`GRAPH_TABLE (g MATCH ... COLUMNS (...))` -> `GRAPH_TABLE('g', 'MATCH ...')`.

    A rewriter pass: same contract as the others in `sql_rewriter`. Only the
    PGQ form is touched - a `GRAPH_TABLE(` whose brackets hold no `MATCH`, or
    never close, is left for the parser to report where it stands.
    """
    if _NAME.search(text) is None:
        return text, []

    pieces: List[str] = []
    edits: List[_Edit] = []
    read_from = 0
    shift = 0
    cursor = 0
    while cursor < len(text):
        skipped = _skip_trivia(text, cursor)
        if skipped is not None:
            cursor = skipped
            continue
        found = _NAME.match(text, cursor) if _is_word_boundary(text, cursor) else None
        if found is None:
            cursor += 1
            continue
        open_at = found.end()
        while open_at < len(text) and text[open_at].isspace():
            open_at += 1
        close_at = _closing_parenthesis(text, open_at) if text[open_at : open_at + 1] == "(" else -1
        if close_at < 0:
            cursor = found.end()
            continue
        inner = text[open_at + 1 : close_at]
        pattern_at = _first_outside_trivia(inner, _MATCH)
        if pattern_at < 0:
            cursor = found.end()
            continue
        graph = inner[:pattern_at].strip()
        pattern = inner[pattern_at:].strip()
        replacement = f"{GRAPH_TABLE}({_literal(graph)}, {_literal(pattern)})"
        start, end = found.start(), close_at + 1
        pieces.append(text[read_from:start])
        pieces.append(replacement)
        edits.append((start + shift, len(replacement), start, end - start))
        shift += len(replacement) - (end - start)
        read_from = cursor = end

    if not edits:
        return text, []
    pieces.append(text[read_from:])
    return "".join(pieces), edits


def _first_outside_trivia(text: str, pattern) -> int:
    """Where `pattern` first matches outside quotes and comments, or -1."""
    cursor = 0
    while cursor < len(text):
        skipped = _skip_trivia(text, cursor)
        if skipped is not None:
            cursor = skipped
            continue
        if pattern.match(text, cursor) and _is_word_boundary(text, cursor):
            return cursor
        cursor += 1
    return -1


# --------------------------------------------------------------------------------------
# 2. AST
# --------------------------------------------------------------------------------------


def _string_argument(argument) -> Optional[str]:
    try:
        return argument["Unnamed"]["Expr"]["Value"]["value"]["SingleQuotedString"]
    except (KeyError, TypeError):
        return None


def _arguments(table: dict) -> Optional[Tuple[str, str]]:
    """(graph, pattern) for a `GRAPH_TABLE('<graph>', '<pattern>')` table node, or None."""
    if not isinstance(table, dict) or not table.get("args"):
        return None
    name = table.get("name") or []
    if len(name) != 1 or str(name[0].get("Identifier", {}).get("value", "")).upper() != GRAPH_TABLE:
        return None
    arguments = [_string_argument(a) for a in table["args"].get("args") or []]
    if len(arguments) != 2 or None in arguments:
        raise UnsupportedSyntaxError(
            "**GRAPH_TABLE** is written `GRAPH_TABLE (<graph> MATCH <pattern> COLUMNS (<columns>))`."
        )
    return arguments[0], arguments[1]


def graph_table_name(table: dict) -> Optional[str]:
    """The graph a `GRAPH_TABLE` table node reads, or None if it is not one.

    For `analyze_query`: the graph is what the statement names, and READ on it
    is the first thing a caller needs. The tables behind the graph cannot be
    known without the catalog; they are checked when the statement is bound.
    """
    try:
        arguments = _arguments(table)
    except UnsupportedSyntaxError:
        return None
    return None if arguments is None else arguments[0]


def _columns_as_return(graph: str, pattern: str) -> str:
    """`MATCH ... COLUMNS (a.x AS x)` as the GQL `MATCH ... RETURN a.x AS x`.

    COLUMNS is PGQ's projection and RETURN is GQL's; the items are spelled the
    same. It must be the LAST clause: whatever follows it would be silently
    dropped by the rewrite, so it is refused.
    """
    last = -1
    cursor = 0
    while cursor < len(pattern):
        skipped = _skip_trivia(pattern, cursor)
        if skipped is not None:
            cursor = skipped
            continue
        if _COLUMNS.match(pattern, cursor) and _is_word_boundary(pattern, cursor):
            last = cursor
        cursor += 1
    if last < 0:
        raise UnsupportedSyntaxError(
            f"**GRAPH_TABLE** over {graph} needs a **COLUMNS** clause naming what it returns, "
            "for example `COLUMNS (a.name AS name)`."
        )
    open_at = pattern.index("(", last)
    close_at = _closing_parenthesis(pattern, open_at)
    if close_at < 0 or pattern[close_at + 1 :].strip():
        raise UnsupportedSyntaxError(
            f"In **GRAPH_TABLE** over {graph}, **COLUMNS (...)** must be the last clause."
        )
    return pattern[:last] + "RETURN " + pattern[open_at + 1 : close_at]


def _gqlparser():
    try:
        import gqlparser
    except ImportError:
        raise UnsupportedSyntaxError(
            "**GRAPH_TABLE** needs the `gqlparser` package, which is not installed here."
        ) from None
    if gqlparser.SQLPARSER_FORMAT != SQLPARSER_FORMAT:
        raise UnsupportedSyntaxError(
            f"**GRAPH_TABLE** needs a `gqlparser` built for sqlparser {SQLPARSER_FORMAT}; the "
            f"one installed is built for {gqlparser.SQLPARSER_FORMAT}."
        )
    return gqlparser


def _definition(graph: str) -> str:
    """The graph's canonical CREATE PROPERTY GRAPH text."""
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Eidetic

    if graph.count(".") < 1:
        raise UnsupportedSyntaxError(
            f"**GRAPH_TABLE** needs the graph's full name; `{graph}` is not one."
        )
    connector = connector_factory(graph, telemetry=None)
    if not isinstance(connector, Eidetic):
        raise DatasetNotFoundError(dataset=graph, connector="PROPERTY GRAPH")
    try:
        return connector.get_property_graph(graph).statement
    except DatasetNotFoundError:
        raise DatasetNotFoundError(dataset=graph, connector="PROPERTY GRAPH") from None
    except Exception as error:
        # The catalog's own not-found never travels into the engine under its own
        # name (the boundary rule `_commit` states); a table or view of this name is
        # equally "no such graph", which is what SHOW CREATE PROPERTY GRAPH says too.
        if type(error).__name__ == "PropertyGraphNotFound":
            raise DatasetNotFoundError(dataset=graph, connector="PROPERTY GRAPH") from None
        raise


def _refusal(graph: str, error) -> str:
    return f"In **GRAPH_TABLE** over {graph}: {str(error.message).rstrip('.')}."


def expand_graph_table(relation: dict, plan_context) -> dict:
    """A FROM item, with a `GRAPH_TABLE` table factor replaced by what it lowers to.

    `relation` is the planner's table-with-joins (or join) dict; anything that is
    not a GRAPH_TABLE comes back untouched. The replacement is a `Derived`
    subquery carrying the alias the reader gave the GRAPH_TABLE.
    """
    table = relation.get("relation", {}).get("Table") if isinstance(relation, dict) else None
    arguments = _arguments(table) if table is not None else None
    if arguments is None:
        return relation
    graph, pattern = arguments

    # READ on the graph BEFORE its definition is consulted: see PERMISSIONS above.
    from opteryx.managers.permissions import can_perform_action

    if not can_perform_action(plan_context.execution_context, graph, action="READ"):
        raise PermissionError(f"User does not have permission to read {graph}")

    gqlparser = _gqlparser()
    try:
        definition = gqlparser.parse_property_graph(_definition(graph))
        query = gqlparser.parse(f"USE {graph} " + _columns_as_return(graph, pattern))
        lowered = gqlparser.lower(query, definition)
    except gqlparser.GqlNotSupportedError as error:
        raise UnsupportedSyntaxError(_refusal(graph, error)) from None
    except gqlparser.GqlError as error:
        # Its line and column index the pattern as gqlparser was given it, not the
        # statement the reader wrote, so they are dropped rather than misreported.
        raise SqlError(_refusal(graph, error)) from None

    expanded = dict(relation)
    expanded["relation"] = {
        "Derived": {
            "lateral": False,
            "subquery": lowered["Query"],
            "alias": table.get("alias"),
            "sample": None,
        }
    }
    return expanded
