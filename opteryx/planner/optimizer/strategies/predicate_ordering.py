# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Predicate Ordering

Type: Cost
Goal: Faster Execution

We combine adjacent predicates into chains of ANDed conditions in a single
filtering step. We order the filters by estimated cost-per-row weighted by
selectivity, so the cheapest, most-reducing predicates run first.

Selectivity is statistics-driven (histograms / NDV / null fractions) when the
input relation carries refreshed ``RelationStatistics``; otherwise it falls
back to conservative operator-keyed constants.

NOTE: still limited for ORed conditions and complex sub-conditions, which are
appended after the simple predicates in their original order.
"""

from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.planner.cost_estimation import PredicateStats, order_predicates as _order_predicates
from opteryx.planner.cost_estimation.predicate_cost import (
    BASIC_COMPARISON_COSTS,
    OPERATION_COSTS,
    base_cost as _base_cost,
    predicate_cost as _predicate_cost,
)
from opteryx.planner.cost_estimation.fallback_selectivity import DEFAULT_SELECTIVITY
from opteryx.planner.cost_estimation.selectivity import estimate_selectivity
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.types.logical_type import LogicalCategory, ColumnType
from opteryx.types import logical_type as _lt
from opteryx.types.schema import ConstantColumn
from opteryx.utils import random_string
from .optimization_strategy import (
    OptimizationStrategy,
    OptimizerContext,
    get_nodes_of_type_from_logical_plan,
)
from opteryx.compiled.structures.expressions import Dnf

# If we have no data, we assume these default selectivities. Defined ONCE in
# fallback_selectivity, shared with the stats-informed estimator, so the same
# predicate is priced identically whether or not statistics are attached.


def _contains_function(node):
    """Return True if the comparison involves any function call on either side."""
    if node is None:
        return False
    return bool(get_all_nodes_of_type(node, (NodeType.FUNCTION,)))


def _estimate_selectivity(condition):
    """Conservative selectivity using defaults when no distribution is available."""

    op = getattr(condition, "value", None)
    return DEFAULT_SELECTIVITY.get(op, 0.5)


def _order_complex_predicates(predicates, telemetry, relation_stats=None):
    """Order complex predicates by selectivity/cost when a predicate's
    selectivity is estimable, falling back to cost alone otherwise.

    "Complex" here is everything ``order_predicates`` does not route to the
    simple path: FUNCTION-containing comparisons *and* non-comparison shapes
    (OR/NOT/DNF trees). Cost therefore comes from ``predicate_cost``, which
    sums catalog function costs for function-containing expressions and falls
    back to the type/operator cost model otherwise. Using
    ``catalog_function_cost`` directly here returned 0.0 for the
    non-comparison shapes — every one of them is function-free — which is
    both a meaningless cost and a divide-by-zero in the ranking key.

    Ranks by the same ``(selectivity - 1.0) / cost`` formula
    ``cost_estimation.predicate_ordering`` uses for simple predicates, with
    cost as an explicit secondary key. Predicates ``estimate_selectivity``
    has no model for (e.g. most FUNCTION calls) resolve to selectivity 1.0,
    so the primary key ties at 0 for all of them and the secondary (cost) key
    orders them cheapest-first. Only predicates with a real estimator
    (currently _STARTS_WITH/_CI_STARTS_WITH, bare or AND/NOT-wrapped) move
    based on actual selectivity.
    """
    if len(predicates) <= 1:
        return predicates

    costs = [_predicate_cost(p.condition) for p in predicates]
    if relation_stats is not None:
        selectivities = [estimate_selectivity(p.condition, relation_stats) for p in predicates]
    else:
        selectivities = [1.0] * len(predicates)
    order = sorted(
        range(len(predicates)),
        key=lambda i: ((selectivities[i] - 1.0) / costs[i], costs[i]),
    )
    ordered = [predicates[i] for i in order]

    if any(predicates[i] is not ordered[i] for i in range(len(ordered))):
        telemetry.optimization_cost_based_predicate_ordering += 1

    return ordered


def _resolve_predicate_stats(condition, relation_stats=None) -> PredicateStats:
    """Build pre-resolved selectivity/cost for a single simple predicate.

    Selectivity is statistics-driven when ``relation_stats`` (the input
    relation's ``RelationStatistics``) is available: ``estimate_selectivity``
    consults histograms, NDV and null fractions, degrading internally to
    textbook constants. When no statistics are attached we fall back to the
    operator-keyed ``DEFAULT_SELECTIVITY`` constants. Cost comes from
    ``OPERATION_COSTS`` (op-specific override) or ``BASIC_COMPARISON_COSTS``
    keyed on the column type.
    """
    if relation_stats is not None:
        selectivity = estimate_selectivity(condition, relation_stats)
    else:
        selectivity = _estimate_selectivity(condition)
    return PredicateStats(
        selectivity=selectivity,
        cost=_base_cost(condition),
    )


def _order_simple_predicates(predicates, telemetry, relation_stats=None):
    """Order simple (non-function) predicates via the cost-estimation module."""

    if len(predicates) <= 1:
        return predicates

    indexed = [
        (i, _resolve_predicate_stats(p.condition, relation_stats))
        for i, p in enumerate(predicates)
    ]
    order = _order_predicates(indexed)
    ordered = [predicates[i] for i in order]

    if any(predicates[i] is not ordered[i] for i in range(len(ordered))):
        telemetry.optimization_cost_based_predicate_ordering += 1

    return ordered


def rewrite_anded_any_eq_to_contains_all(predicate, telemetry):
    """
    Rewrite multiple AND'ed ANYOPEQ conditions on the same column into a single ArrayContainsAll (@>>) condition.

    Example:
      'a' = ANY(z) AND 'b' = ANY(z) AND 'c' = ANY(z)
      -->  z @>> ('a','b','c')     # BinaryOperator::Custom("ArrayContainsAll")

    Notes:
      - We only match: LITERAL = ANY(IDENTIFIER)
      - We group by the SAME column identity
      - Absorbed members are left out of a new DNF (X AND TRUE == X); inputs are
        not modified
    """
    anyeq_by_col = {}

    def collect_any_eq_and(node, grouped):
        # Only collect beneath ANDs (like your OR rewrite only walks ORs)
        if node.node_type == NodeType.DNF:
            for param in node.parameters:
                if param.node_type == NodeType.COMPARISON_OPERATOR and param.value == "AnyOpEq":
                    # literal = ANY(identifier)
                    if (
                        param.left.node_type == NodeType.LITERAL
                        and param.right.node_type == NodeType.IDENTIFIER
                    ):
                        col_id = param.right.schema_column.identity
                        if col_id not in grouped:
                            grouped[col_id] = {
                                "values": [],
                                "nodes": [],
                                "column_node": param.right,
                            }
                        # One object reached twice counts once.
                        if all(member is not param for member in grouped[col_id]["nodes"]):
                            grouped[col_id]["values"].append(param.left.value)
                            grouped[col_id]["nodes"].append(param)

    collect_any_eq_and(predicate, anyeq_by_col)

    # New nodes only. The matched `= ANY` nodes are the Filters' own condition
    # objects, still held by the Filter steps this pass removed and by the
    # pre-optimization plan; rewriting them in place turned every one after the
    # first into LITERAL TRUE for all of those holders - a dropped filter. The
    # fused node takes the first member's place and the absorbed members are left
    # out of the new DNF's list (X AND TRUE == X, so dropping is the same thing).
    replacements: dict = {}
    absorbed: set = set()
    for data in anyeq_by_col.values():
        # Only worth rewriting if we have 2+ literals against the same array column
        if len(data["values"]) > 1:
            telemetry.optimization_predicate_rewriter_anyeq_to_contains_all += 1
            first = data["nodes"][0]
            # An ARRAY constant of unique values (a set: order does not matter)
            values_set = set(data["values"])
            # Phase 2: build ARRAY ColumnType directly from old element type.
            _old_elem_ct_po = first.left.type
            _arr_ct_po = (
                _lt.ARRAY(_old_elem_ct_po)
                if isinstance(_old_elem_ct_po, ColumnType)
                else _lt.ARRAY(_lt.VARIANT)
            )
            values_literal = first.left.replace(
                value=values_set,
                type=_arr_ct_po,
                schema_column=ConstantColumn(name=None, column_type=_arr_ct_po, value=values_set),
            )
            # column @>> ARRAY[...] - the column (array) on the left
            replacements[id(first)] = first.replace(
                value="ArrayContainsAll",
                left=data["column_node"],
                right=values_literal,
            )
            absorbed.update(id(node) for node in data["nodes"][1:])

    if not replacements:
        return predicate
    return predicate.replace(
        parameters=[
            replacements.get(id(param), param)
            for param in predicate.parameters
            if id(param) not in absorbed
        ],
    )


def order_predicates(predicates: list, telemetry, relation_stats=None) -> list:
    """
    Order predicates using selectivity/cost heuristics.

    - Simple column-vs-literal comparisons are ordered first using brute-force
      (up to small N). Selectivity is statistics-driven via ``relation_stats``
      (the input relation's ``RelationStatistics``) when available, else
      conservative constants.
    - Predicates involving functions (or non-comparison forms) are appended
      after the ordered simple predicates, preserving their original order.
    """
    simple = []
    complex_preds = []

    for pred in predicates:
        cond = getattr(pred, "condition", None)
        if cond is None or cond.node_type != NodeType.COMPARISON_OPERATOR:
            complex_preds.append(pred)
            continue

        if _contains_function(cond):
            complex_preds.append(pred)
            continue

        simple.append(pred)

    ordered_simple = _order_simple_predicates(simple, telemetry, relation_stats)
    ordered_complex = _order_complex_predicates(complex_preds, telemetry, relation_stats)

    # Maintain original order for complex/function predicates appended after simples
    return ordered_simple + ordered_complex


class PredicateOrderingStrategy(OptimizationStrategy):
    optimization_technique = "cost"

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if node.node_type == LogicalPlanStepType.Filter:
            context.collected_nids[id(node)] = context.node_id
            context.collected_predicates.append(node)
            return context

        if node.node_type != LogicalPlanStepType.Filter and context.collected_predicates:
            if len(context.collected_predicates) == 1:
                context.collected_predicates = []
                return context

            new_node = LogicalPlanNode(LogicalPlanStepType.Filter)
            new_node.condition = Dnf()
            # `node` is the node feeding the collected filter chain; its refreshed
            # statistics are the input relation the predicates filter against.
            relation_stats = context.plan_context.statistics(node)
            context.collected_predicates = order_predicates(
                context.collected_predicates, self.telemetry, relation_stats
            )
            new_node.condition.parameters = [c.condition for c in context.collected_predicates]
            new_node.columns = []
            new_node.relations = set()
            new_node.all_relations = set()

            for predicate in context.collected_predicates:
                new_node.columns.extend(predicate.columns)
                new_node.relations.update(predicate.relations)
                new_node.all_relations.update(predicate.all_relations)
                self.telemetry.optimization_flatten_filters += 1
                context.optimized_plan.remove_node(context.collected_nids[id(predicate)], heal=True)

            new_node.condition = rewrite_anded_any_eq_to_contains_all(
                new_node.condition, self.telemetry
            )

            context.optimized_plan.insert_node_after(random_string(), new_node, context.node_id)
            context.collected_predicates.clear()

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def should_i_run(self, plan):
        # Check if predicate ordering is disabled via feature flag
        from opteryx import config

        if config.features.disable_predicate_ordering:
            return False

        # only run if there are Filter nodes in the plan
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Filter,))
        return len(candidates) > 0
