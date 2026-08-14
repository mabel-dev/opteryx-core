"""Helpers for exporting the window-function catalog.

Window functions were in no catalog at all. They are not aggregates and not
scalar functions, so neither of the catalogs that could plausibly have held them
does:

* `aggregates.json` is generated from `AGGREGATORS`, and ROW_NUMBER has no
  aggregator kernel - it is executed by the dedicated Window operator
  (`opteryx/operators/window/window_node.pyx`).
* `function_signatures.json` is generated from the function registry, and a
  ranking function has no entry there either.

Hand-spiking either one would have meant hand-written rows in a wholly derived
file, so this is a catalog of its own.

The single most important fact recorded here is that the window forms have
OPPOSITE rules for ORDER BY:

* Ranking windows - ROW_NUMBER/RANK/DENSE_RANK - and navigation windows -
  LAG/LEAD - REQUIRE an ORDER BY inside OVER (...); PARTITION BY is optional.
* Aggregate windows - `aggregate(expr) OVER (...)` - REJECT an ORDER BY inside
  OVER (...); PARTITION BY is optional and `OVER ()` is legal.

Which aggregates are legal in which aggregate-window form is DERIVED, never
restated. `OVER ()` lowers to a global (ungrouped) aggregate and
`OVER (PARTITION BY ...)` to a grouped one, so `_GLOBAL_SUPPORTED` and
`_GROUPED_SUPPORTED` in `aggregate_catalog.py` already answer it - ANY_VALUE
and ARRAY_AGG are grouped-only, and correspondingly
`ARRAY_AGG(x) OVER (PARTITION BY y)` runs while `ARRAY_AGG(x) OVER ()` is
refused with "requires a GROUP BY clause". Importing those sets means a second
list cannot drift away from the first.
"""

from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Any

from opteryx.operators.aggregate.helpers import AGGREGATORS
from opteryx.operators.window.helpers import WINDOW_FUNCTIONS

from .aggregate_catalog import _GLOBAL_SUPPORTED
from .aggregate_catalog import _GROUPED_SUPPORTED

# The dedicated Window operator's functions: ranking (ROW_NUMBER/RANK/
# DENSE_RANK) and navigation (LAG/LEAD). The set is closed and small: the
# window-function registry (opteryx/operators/window/helpers.py) — which
# WindowNode's `_KIND_CODES` and the planner's routing both derive from — holds
# exactly these, and anything else is refused. export_window_catalog() fails
# fast if this prose table and the registry ever disagree.
#
# `deterministic` is a claim about the RESULT, not about the current
# implementation's incidental behaviour. RANK and DENSE_RANK give tied rows the
# same value, so the answer is fixed by the input multiset. ROW_NUMBER gives tied
# rows DISTINCT numbers and nothing in the window spec says which tied row gets
# which, so it is only reproducible when the ORDER BY is a total order over the
# partition - the same reason ANY_VALUE is flagged non-deterministic in
# aggregates.json. LAG/LEAD read a row at a fixed offset in that same tied
# ordering, so they carry ROW_NUMBER's caveat, not RANK's guarantee.
#
# Entries may carry optional `category` (default "ranking"), `sql_forms`,
# `parameters` (default none) and `returns` (default "INTEGER") overrides —
# the navigation functions differ from the ranking three on all four.
_WINDOW_FUNCTION_PROSE: dict[str, dict[str, Any]] = {
    "ROW_NUMBER": {
        "friendly_name": "Row Number",
        "summary": "Numbers the rows of each window partition 1..n in the window's ORDER BY order.",
        "documentation": (
            "Every row in a partition gets a distinct number, so the numbering is a "
            "permutation of 1..n. Rows that tie on the ORDER BY key are numbered in an "
            "unspecified order: the result is only reproducible when the ORDER BY is a "
            "total order over the partition. The operator emits rows in the window's "
            "sort order (partition keys, then order keys) - SQL guarantees no ordering "
            "without a top-level ORDER BY, so add one if the output order matters."
        ),
        "deterministic": False,
    },
    "RANK": {
        "friendly_name": "Rank",
        "summary": "Ranks the rows of each window partition, tied rows sharing a rank and the next rank skipping.",
        "documentation": (
            "1-based. Rows equal on the ORDER BY key share a rank, and the following "
            "rank skips the ties it jumped over, so a two-way tie for first yields "
            "1, 1, 3. Ties are resolved by the ORDER BY key alone, so the answer does "
            "not depend on the order tied rows arrive in."
        ),
        "deterministic": True,
    },
    "DENSE_RANK": {
        "friendly_name": "Dense Rank",
        "summary": "Ranks the rows of each window partition, tied rows sharing a rank and the next rank not skipping.",
        "documentation": (
            "1-based. Rows equal on the ORDER BY key share a rank and the following "
            "rank does not skip, so a two-way tie for first yields 1, 1, 2 - the ranks "
            "are contiguous. Ties are resolved by the ORDER BY key alone, so the answer "
            "does not depend on the order tied rows arrive in."
        ),
        "deterministic": True,
    },
    "LAG": {
        "friendly_name": "Lag",
        "category": "navigation",
        "summary": "The argument's value from the row `offset` rows earlier in the partition, in the window's ORDER BY order.",
        "documentation": (
            "LAG(expr) reads the previous row's value of `expr`; LAG(expr, offset) "
            "reads the row `offset` rows earlier. Rows closer to the start of their "
            "partition than the offset return NULL. The offset must be a non-negative "
            "integer literal and defaults to 1; offset 0 is the current row. The "
            "result's type is the ARGUMENT's type. The 3-argument default form is not "
            "supported - wrap the result: COALESCE(LAG(expr), default) - and neither "
            "is IGNORE NULLS / RESPECT NULLS. Rows that tie on the ORDER BY key sit in "
            "an unspecified order, so over a non-total ORDER BY the neighbouring row - "
            "and therefore the answer - is not deterministic."
        ),
        "deterministic": False,
        "sql_forms": [
            "LAG(expr) OVER ([PARTITION BY expr [, ...]] ORDER BY expr [ASC|DESC] [, ...])",
            "LAG(expr, offset) OVER ([PARTITION BY expr [, ...]] ORDER BY expr [ASC|DESC] [, ...])",
        ],
        "parameters": [
            {"label": "expr", "type": "any"},
            {"label": "offset", "type": "integer", "constant_only": True,
             "optional": True, "minimum": 0},
        ],
        "returns": "same as `expr`",
    },
    "LEAD": {
        "friendly_name": "Lead",
        "category": "navigation",
        "summary": "The argument's value from the row `offset` rows later in the partition, in the window's ORDER BY order.",
        "documentation": (
            "LEAD(expr) reads the next row's value of `expr`; LEAD(expr, offset) "
            "reads the row `offset` rows later. Rows closer to the end of their "
            "partition than the offset return NULL. The offset must be a non-negative "
            "integer literal and defaults to 1; offset 0 is the current row. The "
            "result's type is the ARGUMENT's type. The 3-argument default form is not "
            "supported - wrap the result: COALESCE(LEAD(expr), default) - and neither "
            "is IGNORE NULLS / RESPECT NULLS. Rows that tie on the ORDER BY key sit in "
            "an unspecified order, so over a non-total ORDER BY the neighbouring row - "
            "and therefore the answer - is not deterministic."
        ),
        "deterministic": False,
        "sql_forms": [
            "LEAD(expr) OVER ([PARTITION BY expr [, ...]] ORDER BY expr [ASC|DESC] [, ...])",
            "LEAD(expr, offset) OVER ([PARTITION BY expr [, ...]] ORDER BY expr [ASC|DESC] [, ...])",
        ],
        "parameters": [
            {"label": "expr", "type": "any"},
            {"label": "offset", "type": "integer", "constant_only": True,
             "optional": True, "minimum": 0},
        ],
        "returns": "same as `expr`",
    },
}

# The window spec each form accepts. Values are the vocabulary a generator can
# switch on: "required", "optional", "rejected". Ranking and navigation share
# the ordered spec — both require an ORDER BY inside OVER (...).
_ORDERED_WINDOW_SPEC = {
    "over": "required",
    "partition_by": "optional",
    "order_by": "required",
    "frame": "rejected",
}

_AGGREGATE_WINDOW_SPEC = {
    "over": "required",
    "partition_by": "optional",
    "order_by": "rejected",
    "frame": "rejected",
}


def _default_sql_forms(function: str) -> list[str]:
    # The argument-less ranking shape; navigation entries override sql_forms.
    return [
        f"{function}() OVER (ORDER BY expr [ASC|DESC] [, ...])",
        f"{function}() OVER (PARTITION BY expr [, ...] ORDER BY expr [ASC|DESC] [, ...])",
    ]


def _aggregate_window_support() -> OrderedDict[str, dict[str, bool]]:
    """Which aggregates are legal in which window form - derived, not restated.

    `OVER ()` is the global (ungrouped) aggregate path and
    `OVER (PARTITION BY ...)` the grouped one, so the aggregate catalog's own
    support sets are the answer.
    """
    support: OrderedDict[str, dict[str, bool]] = OrderedDict()
    for aggregate in sorted(AGGREGATORS):
        support[aggregate] = {
            "over_empty": aggregate in _GLOBAL_SUPPORTED,
            "over_partition_by": aggregate in _GROUPED_SUPPORTED,
        }
    return support


def export_window_catalog() -> OrderedDict[str, Any]:
    # The engine registry is the source of truth for WHICH functions exist; this
    # module holds their prose. If the sets diverge the catalog is lying about
    # the engine — refuse to generate rather than publish the lie.
    if set(_WINDOW_FUNCTION_PROSE) != set(WINDOW_FUNCTIONS):
        raise RuntimeError(
            "reference/window_catalog.py's prose table and the engine's window-function "
            "registry (opteryx/operators/window/helpers.py) disagree: "
            f"prose {sorted(_WINDOW_FUNCTION_PROSE)} vs registry {sorted(WINDOW_FUNCTIONS)}. "
            "Add the missing entry before regenerating."
        )

    functions: OrderedDict[str, dict[str, Any]] = OrderedDict()
    for function in sorted(_WINDOW_FUNCTION_PROSE):
        entry = _WINDOW_FUNCTION_PROSE[function]
        functions[function] = {
            "ast_symbol": function,
            "friendly_name": entry["friendly_name"],
            "category": entry.get("category", "ranking"),
            "status": "supported",
            "summary": entry["summary"],
            "documentation": entry["documentation"],
            "sql_forms": entry.get("sql_forms") or _default_sql_forms(function),
            "parameters": [dict(p) for p in entry.get("parameters", [])],
            "returns": entry.get("returns", "INTEGER"),
            "deterministic": entry["deterministic"],
            "window_spec": dict(_ORDERED_WINDOW_SPEC),
        }

    catalog: OrderedDict[str, Any] = OrderedDict()
    catalog["functions"] = functions
    catalog["aggregate_windows"] = {
        "status": "supported",
        "summary": "Compute an aggregate across a window of rows without collapsing them.",
        "documentation": (
            "`aggregate(expr) OVER ()` computes the aggregate across the whole result "
            "and repeats it on every row; `aggregate(expr) OVER (PARTITION BY ...)` "
            "computes it per partition. The result type is the aggregate's own - "
            "COUNT(*) OVER () is INTEGER, SUM(x) OVER (...) is SUM's type. An ORDER BY "
            "inside OVER (...) is REJECTED here, which is the opposite of the rule for "
            "the ranking functions, and there are no running or moving windows as a "
            "consequence."
        ),
        "sql_forms": [
            "aggregate(expr) OVER ()",
            "aggregate(expr) OVER (PARTITION BY expr [, ...])",
        ],
        "window_spec": dict(_AGGREGATE_WINDOW_SPEC),
        "support": _aggregate_window_support(),
    }
    catalog["restrictions"] = {
        "window_frames": {
            "supported": False,
            "clean_error": True,
            "detail": (
                "A frame specification (ROWS/RANGE BETWEEN) is rejected at plan time for "
                "both window forms."
            ),
        },
        "with_group_by": {
            "supported": False,
            "clean_error": True,
            "detail": (
                "A window function cannot appear in a statement that also has a GROUP BY; "
                "it is rejected at plan time."
            ),
        },
        "with_aggregate": {
            "supported": False,
            "clean_error": True,
            "detail": (
                "A window function cannot appear BESIDE a plain aggregate in the same "
                "statement - `SELECT COUNT(*), COUNT(*) OVER () FROM t` - and is rejected "
                "at plan time. A bare aggregate with no GROUP BY is still a group, and the "
                "Window step is planned UNDER the aggregate step, so the window would be "
                "computed over the rows the aggregate collapses and could never see the "
                "aggregated result. The aggregate need not be selected: one reached only "
                "through ORDER BY counts, as does a QUALIFY window beside a selected "
                "aggregate. Compute the aggregate in a subquery and apply the window to "
                "its result."
            ),
        },
        "aggregate_over_window": {
            "supported": False,
            "clean_error": True,
            "detail": (
                "A window function cannot appear inside an aggregate's ARGUMENT - "
                "`SUM(COUNT(*) OVER ())`, `MAX(ROW_NUMBER() OVER (ORDER BY id))` - and is "
                "rejected at plan time. Standard SQL forbids it outright, and the "
                "arrangement is the reverse of `with_aggregate` above, so the remedy is "
                "the reverse too: compute the WINDOW in a subquery and aggregate its "
                "result. It is the ancestry that is rejected, not adjacency, so the window "
                "may be only part of the argument (`SUM(mass + COUNT(*) OVER ())`) and the "
                "aggregate only part of the projection item. The same rule holds in "
                "QUALIFY."
            ),
        },
        "in_having": {
            "supported": False,
            "clean_error": True,
            "detail": (
                "A window function cannot appear in HAVING, and is rejected at plan time. "
                "This is the standard's rule, not a gap: HAVING filters GROUPS, and window "
                "functions are computed AFTER grouping and its filter, so the window's "
                "value does not exist yet when HAVING runs - there is no semantics to "
                "implement. Filter on a window function's output with QUALIFY instead. A "
                "ranking function is rejected there whether or not it carries an OVER, "
                "being window-only wherever it is written."
            ),
        },
        "nested_windows": {
            "supported": False,
            "clean_error": True,
            "detail": (
                "Window functions cannot be nested, and neither placement is accepted: "
                "not inside another window's ARGUMENT (`SUM(COUNT(*) OVER ()) OVER ()`) "
                "and not inside its OVER spec "
                "(`SUM(mass) OVER (PARTITION BY COUNT(*) OVER ())`, "
                "`ROW_NUMBER() OVER (ORDER BY COUNT(*) OVER ())`). Both are rejected at "
                "plan time. The legal way to write it is to CHAIN the windows across a "
                "subquery boundary, computing the inner one in a subquery and applying "
                "the outer one to its result - `SELECT SUM(x) OVER () FROM (SELECT "
                "COUNT(*) OVER () AS x FROM t) AS s`. Every combination of the two window "
                "forms runs, through a derived table or a CTE, and nests further than one "
                "level deep; each scope is rewritten as its own window chain, innermost "
                "first."
            ),
        },
        "nested_in_expression": {
            "supported": True,
            "detail": (
                "A window function does not have to be a whole projection item - it may "
                "sit anywhere inside a larger expression, and both window forms may: "
                "`COUNT(*) OVER (PARTITION BY gravity) + 0`, "
                "`CAST(COUNT(*) OVER () AS VARCHAR)`, "
                "`ROW_NUMBER() OVER (ORDER BY mass) + 1`. The window is computed first "
                "and the rest of the expression is computed over its output, one row "
                "at a time, so the nested form answers exactly what the un-nested one "
                "does. Base columns may be named in the same expression "
                "(`mass / SUM(mass) OVER ()`) and several windows may appear in one "
                "expression, over the same spec or different ones. Placement is the "
                "SELECT list or QUALIFY; a window in ORDER BY or HAVING is not "
                "supported - see `in_having` above for why those two differ. The one "
                "enclosing expression it may NOT sit inside is an "
                "aggregate's argument, or another window's - see "
                "`aggregate_over_window` and `nested_windows` above."
            ),
        },
    }
    return catalog


def write_window_catalog(path: str | Path) -> None:
    output_path = Path(path)
    output_path.write_text(
        json.dumps(export_window_catalog(), indent=4) + "\n",
        encoding="utf8",
    )
