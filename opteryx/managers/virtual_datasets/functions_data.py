# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is a virtual dataset which is calculated at access time.

It is the function catalog behind `SHOW FUNCTIONS`: one row per name a query
can call, with the kind of function it is and a one-line summary.

The names are read from the engine's own registries, not from reference/
(which does not ship in the package):

* scalar functions and their aliases from the function catalog;
* aggregates from AGGREGATORS (opteryx/operators/aggregate/helpers.py);
* window functions from WINDOW_FUNCTIONS (opteryx/operators/window/helpers.py);
* the few names the planner handles itself and registers nowhere, in
  PLANNER_FUNCTIONS below.

It is the same set the exported catalogs describe - function_signatures.json,
aggregates.json and windows.json - and tests/unit/operators/test_show_functions.py
holds the two to each other, so a client generating a static name list from
reference/ (opteryx-sqlalchemy's FUNCTION_NAMES, for one) and a client running
SHOW FUNCTIONS see the same functions.
"""

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.types import logical_type as _lt
from opteryx.types.schema import ColumnDescriptor, RelationDescriptor

__all__ = ("read", "schema", "function_rows")

_COLUMNS = ("name", "kind", "summary")

# Spellings the planner rewrites or binds itself, so no registry holds them:
#
# * DATE_TRUNC/DATETRUNC are rewritten to TRUNC(value, unit) by the builder
#   (logical_planner_builders, `func in ("DATE_TRUNC", "DATETRUNC")`), so they
#   keep TRUNC's pruning; they are not registered in their own right.
# * GROUPING is parsed as an aggregator but is deliberately NOT in AGGREGATORS:
#   it is a lookup against the grouping set that produced the row, not a
#   per-group reduction.
# * MATCH (column) AGAINST (text) is built into the internal _MATCH_AGAINST,
#   whose leading underscore keeps it out of the scalar rows.
PLANNER_FUNCTIONS = {
    "DATE_TRUNC": ("scalar", "Truncate a date or timestamp to the given unit, written DATE_TRUNC(unit, value)."),
    "DATETRUNC": ("scalar", "Truncate a date or timestamp to the given unit, written DATETRUNC(unit, value)."),
    "GROUPING": (
        "aggregate",
        "1 when the GROUP BY column was rolled up to produce the row's subtotal, 0 when it holds a real group value.",
    ),
    "MATCH": ("scalar", "Text matching by embedding cosine similarity, written MATCH (column) AGAINST (text)."),
}


def function_rows() -> list:
    """`(name, kind, summary)` for every callable function name, sorted by name.

    A name the planner treats as internal (leading underscore) is never listed,
    and a scalar function whose lifecycle is not `active` is left out with its
    aliases, as reference/function_signatures.json's consumers do.
    """
    from opteryx.expression.functions import get_catalog
    from opteryx.operators.aggregate.helpers import AGGREGATE_SUMMARIES
    from opteryx.operators.aggregate.helpers import AGGREGATORS
    from opteryx.operators.window.helpers import WINDOW_FUNCTIONS
    from opteryx.operators.window.helpers import WINDOW_SUMMARIES

    rows = {}

    def add(name, kind, summary):
        if name.startswith("_"):
            return
        if name in rows:
            # Two registries claiming one name would make the call ambiguous;
            # better to fail the listing than to report one of them at random.
            raise RuntimeError(f"function name {name!r} is registered as both {rows[name][0]} and {kind}")
        rows[name] = (kind, summary or "")

    for function in get_catalog().list_functions():
        if function.lifecycle.status != "active":
            continue
        summary = function.summary or function.documentation or ""
        add(function.name, "scalar", summary)
        for alias in function.aliases:
            add(alias, "scalar", summary)

    for name in AGGREGATORS:
        add(name, "aggregate", AGGREGATE_SUMMARIES.get(name, ""))

    for name in WINDOW_FUNCTIONS:
        add(name, "window", WINDOW_SUMMARIES.get(name, ""))

    for name, (kind, summary) in PLANNER_FUNCTIONS.items():
        add(name, kind, summary)

    return [(name, *rows[name]) for name in sorted(rows)]


def read(at_date=None, variables=None) -> Morsel:
    rows = function_rows()
    vectors = [
        vector_from_sequence([row[index] for row in rows], dtype=DrakenType.VARCHAR)
        for index in range(len(_COLUMNS))
    ]
    return Morsel.from_vectors(list(_COLUMNS), vectors)


def schema():
    return RelationDescriptor(
        name="$functions",
        columns=[ColumnDescriptor(name=name, column_type=_lt.VARCHAR) for name in _COLUMNS],
        # Not exact (aliases and planner spellings are counted at read time);
        # declared so the planner never falls back to _UNKNOWN_ROW_COUNT.
        row_count_estimate=150,
    )
