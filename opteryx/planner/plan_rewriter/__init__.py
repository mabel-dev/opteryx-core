# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The Plan Rewriter operates on the unbound logical plan — after the Logical Planner has
produced a plan tree from the AST, but before the Binder resolves column references against
relation schemas.

Because the plan is unbound at this stage, rewrites work on structure only: node types,
tree topology, raw identifiers, and CTE definitions. Column types and statistics are not
available. Strategies that require type information belong in the Optimizer.

The primary purpose is to eliminate query shapes that the Binder cannot process correctly,
or that are significantly cheaper to rewrite before schema resolution — most notably
correlated subqueries, which reference outer-scope columns from inside an inner scope.

Traversal is top-down (exit node → scans), mirroring the Optimizer. Strategies that need
bottom-up processing should accumulate state in visit() and act in complete().

Strategies are applied in a fixed-point loop: each pass runs every strategy whose
should_i_run() returns True. The loop terminates when a complete pass produces no
eligible strategy, meaning all applicable rewrites have been exhausted.
"""

from opteryx.models import QueryTelemetry
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.plan_rewriter.rewriter import PlanRewriterVisitor

__all__ = ["do_plan_rewrite"]


def do_plan_rewrite(
    plan: LogicalPlan,
    telemetry: QueryTelemetry,
) -> LogicalPlan:
    """
    Apply structural rewrites to the unbound logical plan.

    ONE pass over ONE plan. The Relation Resolver has already expanded every CTE and
    view reference, so a subquery inside a CTE or view body is in the plan we are handed
    and is rewritten by the same strategies as the main query. Nothing downstream of
    here eliminates a NodeType.SUBQUERY node, so anything this pass cannot see reaches
    the expression compiler and fails there.

    Parameters:
        plan: The fully-expanded logical plan.
        telemetry: Query telemetry for timing and diagnostics.

    Returns:
        The rewritten logical plan, ready for the Binder.
    """
    rewriter = PlanRewriterVisitor(telemetry)
    # Shared CTE bodies (see relation_resolver) live OUTSIDE the main plan and
    # execute once each — but they are query text like any other and carry the
    # same shapes this pass exists to eliminate (subquery expressions, windows).
    # Rewrite each body as a plan in its own right, then the main plan, and carry
    # the registry across — the rewriter can hand back a different plan object,
    # and Graph copies do not carry instance attributes.
    shared_ctes = getattr(plan, "shared_ctes", None) or {}
    recursive_ctes = getattr(plan, "recursive_ctes", None) or {}
    for key, body in shared_ctes.items():
        shared_ctes[key] = rewriter.rewrite(body, {})
    plan = rewriter.rewrite(plan, {})
    plan.shared_ctes = shared_ctes
    plan.recursive_ctes = recursive_ctes
    return plan
