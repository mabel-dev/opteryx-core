# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The window-function registry: the single Python-side source of truth for which
window functions exist and the integer kind codes the engine executes them by.

Everything Python that needs the names or codes imports THIS dict — the planner
(routing OVER-clause functions), the plan-time WindowNode config, the execution
compiler, and the reference-catalog generator (reference/window_catalog.py).

The one place that cannot import it is C++: the ``WinFn`` enum in
src/cpp/engine/native_sort.hpp is a hand-maintained mirror of these codes.
If you add a function here, add it there with the same value — a mismatch is a
silent wrong-function bug, not an error.
"""

# name -> engine kind code (must match WinFn in src/cpp/engine/native_sort.hpp)
WINDOW_FUNCTIONS: dict[str, int] = {
    "ROW_NUMBER": 0,
    "RANK": 1,
    "DENSE_RANK": 2,
    "LAG": 3,
    "LEAD": 4,
    "NTILE": 5,
    "PERCENT_RANK": 6,
    "CUME_DIST": 7,
    "FIRST_VALUE": 8,
    "LAST_VALUE": 9,
    "NTH_VALUE": 10,
}

# The rank-valued subset: outputs that ARE a rank over the window's ORDER BY,
# which is what makes a downstream `output <= K` filter a top-K and therefore
# fusable (WindowTopKFusionStrategy). LAG/LEAD outputs are VALUES from another
# row — `LAG(x) <= K` is an ordinary filter, and fusing it as a top-K would be
# a silent wrong answer.
#
# NTILE is deliberately NOT here. Its output is monotonic in the rank, so
# `NTILE(k) <= b` does describe a prefix — but the prefix LENGTH depends on the
# PARTITION SIZE, which a top-K rewrite does not know, so there is no constant K
# to fuse. PERCENT_RANK/CUME_DIST are excluded for the same reason plus a second:
# their outputs are fractions, not row counts.
RANK_VALUED: frozenset = frozenset({"ROW_NUMBER", "RANK", "DENSE_RANK"})

# Navigation functions: evaluate their argument expression on a row at a RELATIVE
# offset from the current row (in the window's ORDER BY order).
NAVIGATION_FUNCTIONS: frozenset = frozenset({"LAG", "LEAD"})

# Value functions: evaluate their argument expression on a row at a position
# ANCHORED TO THE PARTITION rather than relative to the current row.
#
# These are computed over the WHOLE ordered partition, which for LAST_VALUE and
# NTH_VALUE is a DELIBERATE DIVERGENCE from the SQL standard's default frame
# (RANGE UNBOUNDED PRECEDING AND CURRENT ROW, under which LAST_VALUE returns the
# current row's last peer rather than the partition's last row). This engine
# REJECTS a frame clause on every function in WINDOW_FUNCTIONS, so the
# standard's frame-relative reading has no spelling here and the
# whole-partition reading is the only coherent one — it is also what
# `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` means, which is
# what callers writing LAST_VALUE almost always intend. Documented in
# reference/windows.json rather than left to be discovered.
VALUE_FUNCTIONS: frozenset = frozenset({"FIRST_VALUE", "LAST_VALUE", "NTH_VALUE"})

# Every function whose output is a VALUE gathered from another row, and whose
# output type is therefore its ARGUMENT's type rather than a fixed type. The
# sink computes all of them through one path (a per-row source row id, then the
# canonical row gather), so this — not NAVIGATION_FUNCTIONS — is the set the
# binder and the compiler discriminate on.
GATHERED_FUNCTIONS: frozenset = NAVIGATION_FUNCTIONS | VALUE_FUNCTIONS

# The functions whose output is a FRACTION, not a count: FLOAT64, where every
# other non-gathered window output is INT64.
FLOAT_VALUED: frozenset = frozenset({"PERCENT_RANK", "CUME_DIST"})

# Functions taking a trailing constant-only integer that is NOT a row offset:
# NTILE's bucket count and NTH_VALUE's 1-based position. Both must be >= 1 (a
# zero bucket count has no meaning, and positions are 1-based), unlike LAG/LEAD's
# offset which may be 0. They travel in the same `offset` slot through the plan
# and into WindowFnSpec — one constant-integer parameter per function — so the
# minimum is the only thing that differs.
POSITIVE_INT_PARAM: frozenset = frozenset({"NTILE", "NTH_VALUE"})

# name -> engine kind code (must match WinAggFn in src/cpp/engine/native_window_frame.hpp).
# A SEPARATE registry from WINDOW_FUNCTIONS above, deliberately: a framed aggregate
# (SUM/COUNT/AVG/MIN/MAX OVER (... ROWS/RANGE BETWEEN ...)) is a different computation
# — a sliding-window reduction with its own per-function OUTPUT TYPE — executed by a
# separate native sink (FramedWindowSink), not WindowSink. See
# native_window_frame.hpp's header comment for why the two are not unified.
FRAMED_AGGREGATE_FUNCTIONS: dict[str, int] = {
    "SUM": 0,
    "COUNT": 1,
    "AVG": 2,
    "MIN": 3,
    "MAX": 4,
}

# Of those, the ones whose DISTINCT variant is a DIFFERENT computation — the sink
# runs its sliding distinct-multiset path for these (FramedAggFnSpec::distinct).
# MIN/MAX are deliberately absent, and that is not a gap: an extremum cannot be
# changed by removing duplicates, so MIN(DISTINCT x)/MAX(DISTINCT x) OVER (...) is
# the plain sliding-extremum answer and is lowered to it rather than paying for a
# multiset that could not alter the result. Anything NOT in FRAMED_AGGREGATE_FUNCTIONS
# is refused with or without DISTINCT, so this set only ever narrows that one.
FRAMED_DISTINCT_AGGREGATE_FUNCTIONS: frozenset = frozenset({"SUM", "COUNT", "AVG"})

# name -> engine kind code (must match FrameUnits in native_window_frame.hpp).
FRAME_UNITS: dict[str, int] = {
    "ROWS": 0,
    "RANGE": 1,
}

# name -> engine kind code (must match FrameBoundKind in native_window_frame.hpp).
FRAME_BOUND_KIND: dict[str, int] = {
    "UNBOUNDED_PRECEDING": 0,
    "PRECEDING": 1,
    "CURRENT_ROW": 2,
    "FOLLOWING": 3,
    "UNBOUNDED_FOLLOWING": 4,
}
