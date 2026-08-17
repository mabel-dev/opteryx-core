# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

# Rewrite strategies are applied in declaration order.
# Strategies that must see the output of a prior strategy go after it.

from opteryx.planner.plan_rewriter.strategies.intersect_except_all_to_window_join import (
    IntersectExceptAllToWindowJoinStrategy,
)
from opteryx.planner.plan_rewriter.strategies.window_to_join import WindowToJoinStrategy

# FULL OUTER no longer needs a rewrite: the native engine implements it directly
# (JoinMode::FullOuter — LEFT OUTER probing with build-side match tracking plus an
# UnmatchedBuildSource tail pipeline; see native_join2.hpp). The old
# FullOuterToUnionStrategy (LEFT OUTER ∪ LEFT ANTI, restricted to explicit
# bare-identifier projections) was deleted with the wiring of that mode.
# INTERSECT / EXCEPT (the DISTINCT forms) are NOT rewritten here. They become semi /
# anti joins at BIND time — binder/set_ops._rewrite_setop_to_join — because their ON
# condition pairs the legs' output columns positionally, and which column a leg
# produces at each position is knowable only once the legs are bound. The two pre-bind
# strategies that used to do it built the ON as a cross product over relation NAMES,
# which was only ever correct for a leg over exactly one relation.
STRATEGIES: list = [
    WindowToJoinStrategy,          # runs first — aggregate Window nodes must be eliminated before join planning
    IntersectExceptAllToWindowJoinStrategy,  # INTERSECT/EXCEPT ALL -> ROW_NUMBER + semi/anti join
]
