# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

# Rewrite strategies are applied in declaration order.
# Strategies that must see the output of a prior strategy go after it.

from opteryx.planner.plan_rewriter.strategies.except_to_anti_join import (
    ExceptToAntiJoinStrategy,
)
from opteryx.planner.plan_rewriter.strategies.exists_subquery_to_join import (
    ExistsSubqueryToJoinStrategy,
)
from opteryx.planner.plan_rewriter.strategies.in_subquery_to_join import (
    InSubqueryToJoinStrategy,
)
from opteryx.planner.plan_rewriter.strategies.intersect_to_inner_join import (
    IntersectToSemiJoinStrategy,
)

STRATEGIES: list = [
    ExceptToAntiJoinStrategy,
    ExistsSubqueryToJoinStrategy,
    IntersectToSemiJoinStrategy,
    InSubqueryToJoinStrategy,
]
