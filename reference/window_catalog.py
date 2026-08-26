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

The single most important fact recorded here is that the two window forms take
DIFFERENT window specs, and that the aggregate form's spec varies BY AGGREGATE:

* Ranking windows - ROW_NUMBER/RANK/DENSE_RANK/NTILE/PERCENT_RANK/CUME_DIST -
  navigation windows - LAG/LEAD - and value windows - FIRST_VALUE/LAST_VALUE/
  NTH_VALUE - REQUIRE an ORDER BY inside OVER (...) and REJECT a frame;
  PARTITION BY is optional. They are always computed over the whole ordered
  partition.
* Aggregate windows - `aggregate(expr) OVER (...)` - take an optional PARTITION
  BY, and `OVER ()` is legal. An ORDER BY and a FRAME are ACCEPTED, but only for
  the five aggregates that have a running/framed implementation; every other
  aggregate rejects both.

This used to read "the window forms have OPPOSITE rules for ORDER BY", because
aggregate windows once rejected an ORDER BY outright and there were no running
or moving windows. That stopped being true when the framed-aggregate path
landed, and the opposition is no longer the organising fact: ORDER BY is
required on one form and optional-but-conditional on the other, and it is the
FRAME - rejected on every ranking/navigation/value function, supported on five
aggregates - that now separates them.

Which aggregates are legal in which aggregate-window form is DERIVED, never
restated. `OVER ()` lowers to a global (ungrouped) aggregate and
`OVER (PARTITION BY ...)` to a grouped one, so `_GLOBAL_SUPPORTED` and
`_GROUPED_SUPPORTED` in `aggregate_catalog.py` already answer it - ANY_VALUE
and ARRAY_AGG are grouped-only, and correspondingly
`ARRAY_AGG(x) OVER (PARTITION BY y)` runs while `ARRAY_AGG(x) OVER ()` is
refused with "requires a GROUP BY clause". Importing those sets means a second
list cannot drift away from the first.

The running/framed answer is derived the same way, from
`FRAMED_AGGREGATE_FUNCTIONS` in `opteryx/operators/window/helpers.py` - the very
set the logical planner tests a window aggregate against before accepting an
ORDER BY or a FRAME, and the set WindowNode builds its aggregate kind codes
from. It is the engine's own answer, not a transcription of it.

The per-form ORDER BY/FRAME rules for the ranking, navigation and value
functions are NOT derivable: the planner enforces them as inline `raise`
statements in `_hoist_windows` rather than as data a catalog could import (see
`opteryx/planner/logical_planner/logical_planner.py`). They are stated as
literals here, and `tests/sql/test_window_catalog_matches_engine.py` asserts
every one of them against the running engine so this file cannot silently rot
back into describing a surface the engine no longer has.
"""

from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Any

from opteryx.operators.aggregate.helpers import AGGREGATORS
from opteryx.operators.window.helpers import FRAMED_AGGREGATE_FUNCTIONS
from opteryx.operators.window.helpers import WINDOW_FUNCTIONS

from .aggregate_catalog import _GLOBAL_SUPPORTED
from .aggregate_catalog import _GROUPED_SUPPORTED

# The dedicated Window operator's functions: ranking (ROW_NUMBER/RANK/DENSE_RANK/
# NTILE/PERCENT_RANK/CUME_DIST), navigation (LAG/LEAD) and value (FIRST_VALUE/
# LAST_VALUE/NTH_VALUE). The set is closed and small: the
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
# the navigation and value functions differ from the argument-less ranking ones
# on all four, and PERCENT_RANK/CUME_DIST override `returns` alone.
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
    "NTILE": {
        "friendly_name": "N-Tile",
        "category": "ranking",
        "summary": "Divides each window partition into `buckets` contiguous groups in the window's ORDER BY order, numbered 1..buckets.",
        "documentation": (
            "NTILE(buckets) splits a partition of n rows into `buckets` groups as "
            "evenly as n divides: the first (n mod buckets) groups take one row more "
            "than the rest, and every group is contiguous in the ORDER BY order. When "
            "buckets is GREATER than n the first n buckets take one row each and the "
            "remainder are empty - no row is ever given a bucket number above n, so a "
            "10-bucket decile over 3 rows yields 1, 2, 3 and not 1, 4, 8. The bucket "
            "count must be an integer literal of 1 or more; it cannot be a column, "
            "because the bucket sizes depend on it before any row is read. Rows that "
            "tie on the ORDER BY key are NOT kept together - tied rows sit in an "
            "unspecified order and can fall either side of a bucket boundary - so the "
            "result is only reproducible when the ORDER BY is a total order over the "
            "partition."
        ),
        "deterministic": False,
        "sql_forms": [
            "NTILE(buckets) OVER (ORDER BY expr [ASC|DESC] [, ...])",
            "NTILE(buckets) OVER (PARTITION BY expr [, ...] ORDER BY expr [ASC|DESC] [, ...])",
        ],
        "parameters": [
            {"label": "buckets", "type": "integer", "constant_only": True, "minimum": 1},
        ],
        "returns": "INTEGER",
    },
    "PERCENT_RANK": {
        "friendly_name": "Percent Rank",
        "category": "ranking",
        "summary": "The row's RANK expressed as a fraction of the partition, from 0 for the first row to 1 for the last.",
        "documentation": (
            "(RANK - 1) / (partition rows - 1). The first row of every partition is 0 "
            "and the last is 1, so the value spans the CLOSED interval [0, 1] - unlike "
            "CUME_DIST, which is never 0. A partition of ONE row has no spread to be a "
            "fraction of and the result is 0, not a division by zero. Rows equal on the "
            "ORDER BY key share a RANK and therefore share a percent rank, so the answer "
            "does not depend on the order tied rows arrive in."
        ),
        "deterministic": True,
        "returns": "FLOAT",
    },
    "CUME_DIST": {
        "friendly_name": "Cumulative Distribution",
        "category": "ranking",
        "summary": "The proportion of the partition at or before the current row in the window's ORDER BY order, counting all of its tied peers.",
        "documentation": (
            "The number of rows up to and including the current row's LAST TIED PEER, "
            "divided by the number of rows in the partition. The value spans the "
            "HALF-OPEN interval (0, 1]: the last row is always 1, and no row is ever 0 "
            "because every row counts itself - which is the difference from "
            "PERCENT_RANK. Rows equal on the ORDER BY key all take the value of their "
            "group's last member, so the answer does not depend on the order tied rows "
            "arrive in."
        ),
        "deterministic": True,
        "returns": "FLOAT",
    },
    "FIRST_VALUE": {
        "friendly_name": "First Value",
        "category": "value",
        "summary": "The argument's value from the FIRST row of the partition, in the window's ORDER BY order.",
        "documentation": (
            "FIRST_VALUE(expr) evaluates `expr` on the partition's first row and repeats "
            "it on every row of that partition. The result's type is the ARGUMENT's "
            "type. IGNORE NULLS / RESPECT NULLS is not supported - it changes WHICH row "
            "is read, so it is refused rather than accepted and ignored; a NULL first "
            "row yields NULL. Computed over the WHOLE ordered partition: this engine "
            "rejects a frame clause on the window functions, so the standard's default "
            "frame (RANGE UNBOUNDED PRECEDING AND CURRENT ROW) has no spelling here. For "
            "FIRST_VALUE the two readings agree. Rows that tie on the ORDER BY key sit "
            "in an unspecified order, so over a non-total ORDER BY which row is first - "
            "and therefore the answer - is not deterministic."
        ),
        "deterministic": False,
        "sql_forms": [
            "FIRST_VALUE(expr) OVER (ORDER BY expr [ASC|DESC] [, ...])",
            "FIRST_VALUE(expr) OVER (PARTITION BY expr [, ...] ORDER BY expr [ASC|DESC] [, ...])",
        ],
        "parameters": [{"label": "expr", "type": "any"}],
        "returns": "same as `expr`",
    },
    "LAST_VALUE": {
        "friendly_name": "Last Value",
        "category": "value",
        "summary": "The argument's value from the LAST row of the partition, in the window's ORDER BY order.",
        "documentation": (
            "LAST_VALUE(expr) evaluates `expr` on the partition's last row and repeats it "
            "on every row of that partition. The result's type is the ARGUMENT's type. "
            "IGNORE NULLS / RESPECT NULLS is not supported - it changes WHICH row is "
            "read, so it is refused rather than accepted and ignored; a NULL last row "
            "yields NULL. IMPORTANT - this is the WHOLE-PARTITION reading, which "
            "DIFFERS from the SQL standard's default frame: under RANGE UNBOUNDED "
            "PRECEDING AND CURRENT ROW, LAST_VALUE returns the CURRENT row's last tied "
            "peer rather than the partition's last row, which is why LAST_VALUE is a "
            "well-known footgun elsewhere. This engine rejects a frame clause on the "
            "window functions, so that frame-relative reading has no spelling here and "
            "the whole-partition reading - what ROWS BETWEEN UNBOUNDED PRECEDING AND "
            "UNBOUNDED FOLLOWING means, and what callers almost always intend - is the "
            "only one. Rows that tie on the ORDER BY key sit in an unspecified order, so "
            "over a non-total ORDER BY which row is last - and therefore the answer - is "
            "not deterministic."
        ),
        "deterministic": False,
        "sql_forms": [
            "LAST_VALUE(expr) OVER (ORDER BY expr [ASC|DESC] [, ...])",
            "LAST_VALUE(expr) OVER (PARTITION BY expr [, ...] ORDER BY expr [ASC|DESC] [, ...])",
        ],
        "parameters": [{"label": "expr", "type": "any"}],
        "returns": "same as `expr`",
    },
    "NTH_VALUE": {
        "friendly_name": "Nth Value",
        "category": "value",
        "summary": "The argument's value from the nth row of the partition (1-based), in the window's ORDER BY order.",
        "documentation": (
            "NTH_VALUE(expr, n) evaluates `expr` on the partition's nth row - counting "
            "from 1 - and repeats it on every row of that partition. A partition with "
            "FEWER THAN n rows yields NULL for all of them. The position must be an "
            "integer literal of 1 or more; it cannot be a column, because the row it "
            "selects must be known before any row is read. The result's type is the "
            "ARGUMENT's type. IGNORE NULLS / RESPECT NULLS is not supported, and neither "
            "is FROM FIRST / FROM LAST - counting is always from the first row. Computed "
            "over the WHOLE ordered partition, which DIFFERS from the SQL standard's "
            "default frame - see LAST_VALUE for why. Rows that tie on the ORDER BY key "
            "sit in an unspecified order, so over a non-total ORDER BY which row is nth - "
            "and therefore the answer - is not deterministic."
        ),
        "deterministic": False,
        "sql_forms": [
            "NTH_VALUE(expr, n) OVER (ORDER BY expr [ASC|DESC] [, ...])",
            "NTH_VALUE(expr, n) OVER (PARTITION BY expr [, ...] ORDER BY expr [ASC|DESC] [, ...])",
        ],
        "parameters": [
            {"label": "expr", "type": "any"},
            {"label": "n", "type": "integer", "constant_only": True, "minimum": 1},
        ],
        "returns": "same as `expr`",
    },
}

# The window spec each form accepts. Values are the vocabulary a generator can
# switch on: "required", "optional", "rejected", "conditional". Ranking,
# navigation and value functions share the ordered spec — every one of them
# requires an ORDER BY inside OVER (...) and refuses a frame.
#
# These four values are LITERALS because the planner has no importable table to
# derive them from: `_hoist_windows` in
# opteryx/planner/logical_planner/logical_planner.py enforces them as inline
# `raise UnsupportedSyntaxError(...)` statements keyed off `_RANKING_FUNCTIONS`
# (which is just `tuple(WINDOW_FUNCTIONS)` — membership, carrying no spec).
# tests/sql/test_window_catalog_matches_engine.py runs each combination against
# the engine and asserts it matches what is written here.
_ORDERED_WINDOW_SPEC = {
    "over": "required",
    "partition_by": "optional",
    "order_by": "required",
    "frame": "rejected",
}

# "conditional" — accepted for SOME aggregates and refused for the rest. Which
# is per-aggregate and derived, not stated: see `_aggregate_window_support()`'s
# `over_order_by` / `over_frame`, both read off the engine's own
# FRAMED_AGGREGATE_FUNCTIONS. A generator that needs the yes/no answer must read
# the support map for the aggregate in hand rather than this summary.
_AGGREGATE_WINDOW_SPEC = {
    "over": "required",
    "partition_by": "optional",
    "order_by": "conditional",
    "frame": "conditional",
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

    `over_order_by` and `over_frame` are the running/framed question, and they
    are one answer, not two: the planner tests `FRAMED_AGGREGATE_FUNCTIONS`
    once and refuses an ORDER BY and a FRAME together, so an aggregate that
    accepts one accepts the other. They are reported as two keys because a
    generator asks two questions, and kept in lockstep here rather than by a
    reader's memory of the planner.
    """
    support: OrderedDict[str, dict[str, bool]] = OrderedDict()
    for aggregate in sorted(AGGREGATORS):
        framed = aggregate in FRAMED_AGGREGATE_FUNCTIONS
        support[aggregate] = {
            "over_empty": aggregate in _GLOBAL_SUPPORTED,
            "over_partition_by": aggregate in _GROUPED_SUPPORTED,
            "over_order_by": framed,
            "over_frame": framed,
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
            "COUNT(*) OVER () is INTEGER, SUM(x) OVER (...) is SUM's type. Adding an "
            "ORDER BY inside OVER (...) makes it a RUNNING aggregate - "
            "`SUM(x) OVER (ORDER BY d)` is a running total - and an explicit frame "
            "(ROWS/RANGE BETWEEN) makes it a MOVING one. An ORDER BY with no explicit "
            "frame gets the SQL standard's default frame, RANGE UNBOUNDED PRECEDING AND "
            "CURRENT ROW. Running and framed windows are supported for AVG, COUNT, MAX, "
            "MIN and SUM only; every other aggregate accepts PARTITION BY alone and "
            "refuses an ORDER BY or a frame - compute those in a subquery. Which "
            "aggregate is which is in `support` below, under `over_order_by` and "
            "`over_frame`. A frame REQUIRES an ORDER BY; see `window_frames` under "
            "`restrictions` for the frame shapes that are and are not accepted."
        ),
        "sql_forms": [
            "aggregate(expr) OVER ()",
            "aggregate(expr) OVER (PARTITION BY expr [, ...])",
            "aggregate(expr) OVER (ORDER BY expr [ASC|DESC] [, ...])",
            "aggregate(expr) OVER ([PARTITION BY expr [, ...]] ORDER BY expr [ASC|DESC] [, ...] [frame])",
        ],
        "window_spec": dict(_AGGREGATE_WINDOW_SPEC),
        "support": _aggregate_window_support(),
    }
    catalog["restrictions"] = {
        "window_frames": {
            "supported": True,
            "clean_error": True,
            "detail": (
                "A frame specification (ROWS/RANGE BETWEEN) is supported on AGGREGATE "
                "windows, for the five aggregates that have a running/framed "
                "implementation - AVG, COUNT, MAX, MIN, SUM - and rejected everywhere "
                "else, at plan time, in four distinct cases. (1) On a ranking, "
                "navigation or value function a frame is rejected outright: those are "
                "always computed over the whole ordered partition. (2) On any other "
                "aggregate the whole running/framed form is rejected, ORDER BY and "
                "frame alike - `STDDEV(x) OVER (ORDER BY d)` refuses with `only AVG, "
                "COUNT, MAX, MIN, SUM support a running/framed window`. (3) A frame "
                "with no ORDER BY in the same OVER (...) is rejected: a frame is "
                "relative to a current row, and with no ordering there is none. (4) The "
                "frame's own shape is restricted - the units must be ROWS or RANGE "
                "(GROUPS is rejected), a RANGE frame takes only UNBOUNDED PRECEDING, "
                "CURRENT ROW and UNBOUNDED FOLLOWING (a numeric PRECEDING/FOLLOWING "
                "offset is rejected; use ROWS), a ROWS offset must be a non-negative "
                "integer literal, and the start bound may not come after the end bound. "
                "An ORDER BY with no explicit frame is not one of these cases: it gets "
                "the standard's default frame, RANGE UNBOUNDED PRECEDING AND CURRENT "
                "ROW."
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
