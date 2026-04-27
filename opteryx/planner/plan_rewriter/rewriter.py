# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from opteryx.models import QueryTelemetry
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.plan_rewriter.strategies import STRATEGIES
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import PlanRewriteContext


class PlanRewriterVisitor:
    def __init__(self, telemetry: QueryTelemetry):
        self.strategies = [cls(telemetry) for cls in STRATEGIES]

    def traverse(self, plan: LogicalPlan, strategy, ctes: dict) -> LogicalPlan:
        exit_points = plan.get_exit_points()
        if not exit_points:
            return plan

        root_nid = exit_points.pop()
        context = PlanRewriteContext(plan, ctes)

        def _inner(nid, parent_nid, context):
            node = context.pre_rewrite_tree[nid]
            context.node_id = nid
            context.parent_nid = parent_nid
            context = strategy.visit(node, context)
            for child, _, _ in plan.ingoing_edges(nid):
                _inner(child, nid, context)

        _inner(root_nid, None, context)
        return strategy.complete(context.rewritten_plan, context)

    def rewrite(self, plan: LogicalPlan, ctes: dict) -> LogicalPlan:
        """
        Apply all strategies in a fixed-point loop. Each pass runs every strategy whose
        should_i_run() returns True. Looping continues until a full pass produces no
        eligible strategy — meaning all applicable rewrites have been exhausted.
        """
        current = plan
        changed = True
        while changed:
            changed = False
            for strategy in self.strategies:
                if strategy.should_i_run(current):
                    current = self.traverse(current, strategy, ctes)
                    changed = True
        return current
