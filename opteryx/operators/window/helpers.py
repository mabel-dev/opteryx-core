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
}

# The rank-valued subset: outputs that ARE a rank over the window's ORDER BY,
# which is what makes a downstream `output <= K` filter a top-K and therefore
# fusable (WindowTopKFusionStrategy). LAG/LEAD outputs are VALUES from another
# row — `LAG(x) <= K` is an ordinary filter, and fusing it as a top-K would be
# a silent wrong answer.
RANK_VALUED: frozenset = frozenset({"ROW_NUMBER", "RANK", "DENSE_RANK"})

# Navigation functions: evaluate their argument expression on a row at a fixed
# offset within the partition (in the window's ORDER BY order). They take
# arguments; the ranking functions take none.
NAVIGATION_FUNCTIONS: frozenset = frozenset({"LAG", "LEAD"})
