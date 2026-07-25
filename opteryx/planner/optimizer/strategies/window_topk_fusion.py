# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Window Top-K Fusion

Type: Heuristic
Goal: Avoid emitting every ranked row from a ranking window when the query only
      keeps a bounded prefix per partition (`WHERE <rank> <= K`).

`ROW_NUMBER()/RANK()/DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...)` computes a
rank for every row before any outer filter runs — a query like:

    SELECT id6, v3 FROM (
        SELECT id6, v3, ROW_NUMBER() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS rn
        FROM t
    ) WHERE rn <= 2

still sorts and ranks every row of `t`, then a separate Filter drops all but the
top 2 per partition. This rule finds that shape — a Window node whose single
ranking output feeds, through zero or more pass-through Project/Subquery nodes,
a Filter of the form `<rank> <= K` (or the reversed/`<` spellings) — and fuses the
filter into the Window node itself (`top_k`), then removes the now-redundant
Filter node.

The native WindowNode still computes an exact rank for every row (correctness
requires it — ties for RANK/DENSE_RANK can only be resolved once every row's
rank is known), but filters to the kept prefix before gathering/emitting rows,
instead of materializing every ranked row for a downstream Filter to then throw
almost all of away. This does not avoid the O(n log n) sort; a bounded
per-partition top-K native sink (skipping the sort itself) is a separate,
larger piece of work.

Scope of this first cut (deliberately narrow):
- The Window node must have exactly one ranking output (no ambiguity about which
  output the filter applies to).
- The Filter must be a single comparison — `<rank> <= K` / `<rank> < K` (or the
  literal-first spellings `K >= <rank>` / `K > <rank>`) — not part of a larger
  AND/OR expression. Anything else falls through unfused (safe: the ordinary
  full-rank-then-filter behaviour is unchanged).
- K must be a non-negative integer literal; K < 1 is left unfused (a `rank < 1`
  or `rank <= 0` filter drops everything, which is not worth special-casing).
"""

from opteryx.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

# Node types the search may walk through looking for the qualifying Filter —
# purely column-reshaping (Project/Subquery) or independently-evaluated
# (Filter, when it doesn't itself match) — none of which can change what a
# surviving row's rank value is, or change the final result of applying the
# rank filter regardless of exactly where it runs (filters commute).
_TRANSPARENT_TYPES = (LogicalPlanStepType.Project, LogicalPlanStepType.Subquery)

_MAX_HOPS = 8


def _identity_of(node) -> str:
    if node.node_type != NodeType.IDENTIFIER:
        return None
    schema_column = getattr(node, "schema_column", None)
    return getattr(schema_column, "identity", None) if schema_column is not None else None


def _int_literal_of(node):
    if node.node_type != NodeType.LITERAL:
        return None
    value = node.value
    try:
        as_int = int(value)
    except (TypeError, ValueError):
        return None
    if as_int != value:
        return None  # e.g. 2.5 — not a valid rank boundary
    return as_int


def _match_topk_filter(condition, target_identity: str):
    """If `condition` is `<target_identity> <= K` (or an equivalent spelling),
    return K; otherwise None. Deliberately only matches a bare comparison —
    never descends into AND/OR, so a compound filter is left unfused."""
    if condition is None or condition.node_type != NodeType.COMPARISON_OPERATOR:
        return None
    op = condition.value
    left, right = condition.left, condition.right

    left_identity, right_identity = _identity_of(left), _identity_of(right)
    left_literal, right_literal = _int_literal_of(left), _int_literal_of(right)

    if left_identity == target_identity and right_literal is not None:
        if op == "LtEq":
            return right_literal
        if op == "Lt":
            return right_literal - 1
    elif right_identity == target_identity and left_literal is not None:
        if op == "GtEq":
            return left_literal
        if op == "Gt":
            return left_literal - 1
    return None


class WindowTopKFusionStrategy(OptimizationStrategy):
    def should_i_run(self, plan: LogicalPlan) -> bool:
        return len(get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Window,))) > 0

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore[arg-type]

        if node.node_type != LogicalPlanStepType.Window:
            return context

        outputs = getattr(node, "outputs", None)
        order_by = getattr(node, "order_by", None)
        if not outputs or not order_by or len(outputs) != 1:
            # No `outputs` = an aggregate window (already lowered to a join by the
            # plan rewriter); no `order_by` = the internal streaming ROW_NUMBER
            # path (INTERSECT/EXCEPT ALL rewrite), which has no outer SQL filter
            # to fuse; more than one output = ambiguous which one a filter targets.
            return context
        if getattr(node, "top_k", None) is not None:
            return context  # already fused

        target_identity = outputs[0][1].identity

        cur_nid = context.node_id
        for _ in range(_MAX_HOPS):
            edges = context.optimized_plan.outgoing_edges(cur_nid)
            if len(edges) != 1:
                return context
            next_nid = edges[0][1]
            next_node = context.optimized_plan[next_nid]
            if next_node is None:
                return context

            if next_node.node_type == LogicalPlanStepType.Filter:
                k = _match_topk_filter(next_node.condition, target_identity)
                if k is not None and k >= 1:
                    in_edges = context.optimized_plan.ingoing_edges(next_nid)
                    out_edges = context.optimized_plan.outgoing_edges(next_nid)
                    if len(in_edges) == 1 and len(out_edges) == 1:
                        window_node = context.optimized_plan[context.node_id]
                        window_node.top_k = k
                        context.optimized_plan[context.node_id] = window_node
                        context.optimized_plan.remove_node(next_nid, heal=True)
                        self.telemetry.optimization_window_topk_fuse += 1
                    return context
                # Non-matching filter: transparent to the search (filters
                # commute), keep walking past it.
                cur_nid = next_nid
                continue

            if next_node.node_type in _TRANSPARENT_TYPES:
                cur_nid = next_nid
                continue

            return context  # anything else (Join, Aggregate, Order, ...): stop

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan
