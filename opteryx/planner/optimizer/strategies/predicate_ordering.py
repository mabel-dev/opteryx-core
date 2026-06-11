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
from opteryx.models import Node
from opteryx.planner.cost_estimation import PredicateStats, order_predicates as _order_predicates
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

# Approximate of the time in seconds (3sf) to compare 1 million records
# These are the core comparisons, Eq, NotEq, Gt, GtEq, Lt, LtEq
BASIC_COMPARISON_COSTS = {
    LogicalCategory.ARRAY: 10.00,  # expensive
    LogicalCategory.VARBINARY: 0.058,  # varies based on length, this is 50 bytes
    LogicalCategory.NVARCHAR: 10.00,  # JSON/complex text (treat as expensive)
    LogicalCategory.BOOLEAN: 0.003,
    LogicalCategory.DATE: 0.008,
    LogicalCategory.DECIMAL: 1.533,
    LogicalCategory.FLOAT: 0.002,
    LogicalCategory.INTEGER: 0.001,
    LogicalCategory.INTERVAL: 10.00,  # expensive
    LogicalCategory.TIMESTAMP: 0.008,
    LogicalCategory.TIME: 10.00,  # expensive
    LogicalCategory.VARCHAR: 0.231,  # varies based on length, this is 50 chars
    LogicalCategory.NULL: 10.00,  # for completeness
    None: 10.00,  # unknown type — treat as expensive
}

# Operation-specific costs (override type-based costs)
# Pattern matching operations are significantly more expensive than simple comparisons
OPERATION_COSTS = {
    "InStr": 2.5,  # substring search (Volnitsky algorithm)
    "IInStr": 2.5,  # case-insensitive substring search
    "NotInStr": 2.5,
    "NotIInStr": 2.5,
    "Like": 2.5,  # pattern matching
    "ILike": 2.5,
    "NotLike": 2.5,
    "NotILike": 2.5,
    "RLike": 3.0,  # regex is even more expensive
    "NotRLike": 3.0,
}

# If we have no data, we assume these default selectivities
DEFAULT_SELECTIVITY = {
    "Eq": 0.1,
    "NotEq": 0.9,
    "Gt": 0.5,
    "GtEq": 0.5,
    "Lt": 0.5,
    "LtEq": 0.5,
    "InStr": 0.3,
    "IInStr": 0.3,
    "NotInStr": 0.7,
    "NotIInStr": 0.7,
    "Like": 0.3,
    "ILike": 0.3,
    "NotLike": 0.7,
    "NotILike": 0.7,
    "RLike": 0.3,
    "NotRLike": 0.7,
}


def _contains_function(node):
    """Return True if the comparison involves any function call on either side."""
    if node is None:
        return False
    return bool(get_all_nodes_of_type(node, (NodeType.FUNCTION,)))


def _estimate_selectivity(condition):
    """Conservative selectivity using defaults when no distribution is available."""

    op = getattr(condition, "value", None)
    return DEFAULT_SELECTIVITY.get(op, 0.5)


def _base_cost(condition):
    op = getattr(condition, "value", None)
    if op in OPERATION_COSTS:
        return OPERATION_COSTS[op]
    col = getattr(condition, "left", None)
    col_type = getattr(col, "schema_column", None)
    if col_type is None:
        return 10.0
    return BASIC_COMPARISON_COSTS.get(col_type.category, 10.0)


def _catalog_function_cost(node) -> float:
    """Sum catalog cost estimates for all FUNCTION nodes in the expression subtree.

    Falls back to 100.0 µs/million for any function with cost 0.0 (legacy backfill entries
    that don't have measured costs should be treated as expensive, not free).
    """
    from opteryx.expression.functions import get_catalog

    _UNKNOWN_COST = 100.0
    total = 0.0
    for func_node in get_all_nodes_of_type(node, (NodeType.FUNCTION,)):
        cost = get_catalog().get_cost(func_node.value) or 0.0
        total += cost if cost > 0.0 else _UNKNOWN_COST
    return total


def _order_complex_predicates(predicates, telemetry):
    """Order function-containing predicates by estimated catalog cost."""
    if len(predicates) <= 1:
        return predicates

    costs = [_catalog_function_cost(p.condition) for p in predicates]
    order = sorted(range(len(predicates)), key=lambda i: costs[i])
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
      - Remaining AND nodes are neutralized to TRUE (since X AND TRUE == X)
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
                        grouped[col_id]["values"].append(param.left.value)
                        grouped[col_id]["nodes"].append(param)

    collect_any_eq_and(predicate, anyeq_by_col)

    for data in anyeq_by_col.values():
        # Only worth rewriting if we have 2+ literals against the same array column
        if len(data["values"]) > 1:
            telemetry.optimization_predicate_rewriter_anyeq_to_contains_all += 1

            # Reuse the first matched node as the replacement site
            new_node = data["nodes"][0]

            # Build right-hand side as an ARRAY constant of unique values
            # (use a set to dedupe; order doesn't matter)
            values_set = set(data["values"])
            new_node.left.value = values_set
            # Phase 2: build ARRAY ColumnType directly from old element type.
            _old_elem_ct_po = new_node.left.type
            _arr_ct_po = _lt.ARRAY(_old_elem_ct_po) if isinstance(_old_elem_ct_po, ColumnType) else _lt.ARRAY(_lt.VARIANT)
            new_node.left.type = _arr_ct_po
            new_node.left.schema_column = ConstantColumn(
                name=new_node.left.name,
                column_type=_arr_ct_po,
                value=new_node.left.value,
            )

            # Turn node into: column @>> ARRAY[...]
            new_node.value = "ArrayContainsAll"  # your @>> operator
            new_node.node_type = NodeType.COMPARISON_OPERATOR
            new_node.right = data["column_node"]

            # Swap so LHS is the column (array), RHS is the values array
            new_node.left, new_node.right = new_node.right, new_node.left

            # Neutralize the remaining AND'ed ANYOPEQ nodes to TRUE
            for node in data["nodes"][1:]:
                node.node_type = NodeType.LITERAL
                node.type = _lt.BOOLEAN
                node.value = True

    return predicate


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
    ordered_complex = _order_complex_predicates(complex_preds, telemetry)

    # Maintain original order for complex/function predicates appended after simples
    return ordered_simple + ordered_complex


class PredicateOrderingStrategy(OptimizationStrategy):
    optimization_technique = "cost"

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()  # type: ignore

        if node.node_type == LogicalPlanStepType.Filter:
            node.nid = context.node_id
            context.collected_predicates.append(node)
            return context

        if node.node_type != LogicalPlanStepType.Filter and context.collected_predicates:
            if len(context.collected_predicates) == 1:
                context.collected_predicates = []
                return context

            new_node = LogicalPlanNode(LogicalPlanStepType.Filter)
            new_node.condition = Node(node_type=NodeType.DNF)
            # `node` is the node feeding the collected filter chain; its refreshed
            # statistics are the input relation the predicates filter against.
            relation_stats = getattr(node, "statistics", None)
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
                context.optimized_plan.remove_node(predicate.nid, heal=True)

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
