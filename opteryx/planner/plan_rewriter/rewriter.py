# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import QueryTelemetry
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.plan_rewriter.strategies import STRATEGIES
from opteryx.planner.plan_rewriter.strategies.rewrite_strategy import PlanRewriteContext


class PlanRewriterVisitor:
    def __init__(self, telemetry: QueryTelemetry):
        self.telemetry = telemetry
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

    #  A pass that rewrites nothing means the fixed point is reached, so this only
    #  bounds genuinely non-converging loops. Far above any real plan's needs.
    MAX_PASSES = 100

    def rewrite(self, plan: LogicalPlan, ctes: dict) -> LogicalPlan:
        """
        Apply all strategies in a fixed-point loop. Each pass runs every strategy whose
        should_i_run() returns True. Looping continues until a full pass leaves the plan
        UNCHANGED — meaning all applicable rewrites have been exhausted.

        The loop continues on actual plan change, NOT on "a strategy ran". A strategy
        whose should_i_run() keeps returning True for something it cannot rewrite —
        e.g. a subquery form it does not own — stays eligible forever, so treating
        "ran" as progress spins indefinitely. Every strategy here is structural
        (subqueries and set operations become joins), so a rewrite that changes
        nothing leaves the node and edge counts identical.

        Non-convergence is a bug, so it fails loudly rather than hanging: a plan that
        is still changing after MAX_PASSES raises, naming the strategies still
        eligible.
        """
        current = plan
        changed = True
        passes = 0
        while changed:
            if passes >= self.MAX_PASSES:
                eligible = [
                    s.__class__.__name__ for s in self.strategies if s.should_i_run(current)
                ]
                raise InvalidInternalStateError(
                    f"plan rewriting did not converge after {self.MAX_PASSES} passes; "
                    f"still eligible: {', '.join(eligible) or 'none'}"
                )
            passes += 1
            changed = False
            for strategy in self.strategies:
                if strategy.should_i_run(current):
                    before = (len(current), len(current.edges()))
                    current = self.traverse(current, strategy, ctes)
                    after = (len(current), len(current.edges()))
                    self.telemetry.add_plan_rewrite(
                        "plan_rewriter",
                        strategy.__class__.__name__,
                        before,
                        after,
                    )
                    if before != after:
                        changed = True
        return current
