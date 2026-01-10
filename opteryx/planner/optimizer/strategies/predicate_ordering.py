# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Predicate Ordering

Type: Cost
Goal: Faster Execution

We combine adjacent predicates into chains of ANDed conditions in a single
filtering step. We use cost estimates based on the data type to determine
a good order to execute the filters.

NOTE: This currently doesn't account for a few very important things which
means this may create an order that is slower than the order that would have
been run if this strategy didn't run.

- The selectivity of the filter
- How to handle complex sub conditions or ORed conditions
"""

from itertools import permutations

from orso.schema import ConstantColumn
from orso.tools import random_string

# pragma: no cover
from orso.types import OrsoTypes

from opteryx.managers.expression import NodeType
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan

# Approximate of the time in seconds (3sf) to compare 1 million records
# These are the core comparisons, Eq, NotEq, Gt, GtEq, Lt, LtEq
BASIC_COMPARISON_COSTS = {
    OrsoTypes.ARRAY: 10.00,  # expensive
    OrsoTypes.BLOB: 0.058,  # varies based on length, this is 50 bytes
    OrsoTypes.JSONB: 10.00,  # JSONB (treat as expensive)
    OrsoTypes.BOOLEAN: 0.004,
    OrsoTypes.DATE: 0.01,
    OrsoTypes.DECIMAL: 2.871,
    OrsoTypes.DOUBLE: 0.003,
    OrsoTypes.INTEGER: 0.002,
    OrsoTypes.INTERVAL: 10.00,  # expensive
    OrsoTypes.STRUCT: 10.00,  # expensive
    OrsoTypes.TIMESTAMP: 0.009,
    OrsoTypes.TIME: 10.00,  # expensive
    OrsoTypes.VARCHAR: 0.375,  # varies based on length, this is 50 chars
    OrsoTypes.NULL: 10.00,  # for completeness
    getattr(OrsoTypes, "_MISSING_TYPE", 0): 10.00,  # for completeness
    0: 10.00,  # for completeness
}

# If we have no data, we assume these default selectivities
DEFAULT_SELECTIVITY = {
    "Eq": 0.1,
    "NotEq": 0.9,
    "Gt": 0.5,
    "GtEq": 0.5,
    "Lt": 0.5,
    "LtEq": 0.5,
}


def _contains_function(node):
    """Return True if the comparison involves any function call on either side."""

    def is_fn(n):
        return getattr(n, "node_type", None) == NodeType.FUNCTION

    return is_fn(node) or is_fn(getattr(node, "left", None)) or is_fn(getattr(node, "right", None))


def _estimate_selectivity(condition):
    """Conservative selectivity using defaults when no distribution is available."""

    op = getattr(condition, "value", None)
    return DEFAULT_SELECTIVITY.get(op, 0.5)


def _base_cost(condition):
    col = getattr(condition, "left", None)
    col_type = getattr(col, "schema_column", None)
    if col_type is None:
        return 10.0
    return BASIC_COMPARISON_COSTS.get(col_type.type, 10.0)


def _order_simple_predicates(predicates, telemetry):
    """Order simple (non-function) predicates by brute-force cost if small, else by cost heuristic."""

    if len(predicates) <= 1:
        return predicates

    selectivities = [_estimate_selectivity(p.condition) for p in predicates]
    execution = [_base_cost(p.condition) for p in predicates]

    if len(predicates) <= 6:
        best_order = _brute_force_order(selectivities, execution)
        ordered = [predicates[i] for i in best_order]
    else:
        # Greedy: lowest execution cost first
        order = sorted(range(len(predicates)), key=lambda i: execution[i])
        ordered = [predicates[i] for i in order]

    # Telemetry if order changed
    if any(predicates[i] is not ordered[i] for i in range(len(ordered)) if i < len(predicates)):
        telemetry.optimization_cost_based_predicate_ordering += 1

    return ordered


def _brute_force_order(predicate_selectivity, predicate_execution_time):
    """Return the permutation with the lowest estimated cost using simple selectivity/cost model."""

    best_order = tuple(range(len(predicate_selectivity)))
    best_cost = float("inf")

    for arrangement in permutations(range(len(predicate_selectivity))):
        cumulative_size = 1.0
        execution_cost = 0.0

        for idx in arrangement:
            execution_cost += predicate_execution_time[idx] * cumulative_size
            cumulative_size *= predicate_selectivity[idx]

        if execution_cost < best_cost:
            best_cost = execution_cost
            best_order = arrangement

    return best_order


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
            new_node.left.element_type = new_node.left.type
            new_node.left.type = OrsoTypes.ARRAY
            new_node.left.schema_column = ConstantColumn(
                name=new_node.left.name,
                type=OrsoTypes.ARRAY,
                element_type=new_node.left.element_type,
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
                node.type = OrsoTypes.BOOLEAN
                node.value = True

    return predicate


def order_predicates(predicates: list, telemetry) -> list:
    """
    Order predicates using simple selectivity/cost heuristics.

    - Simple column-vs-literal comparisons are ordered first using brute-force
      (up to small N) with conservative selectivities.
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

    ordered_simple = _order_simple_predicates(simple, telemetry)

    # Maintain original order for complex/function predicates appended after simples
    return ordered_simple + complex_preds


class PredicateOrderingStrategy(OptimizationStrategy):
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
            context.collected_predicates = order_predicates(
                context.collected_predicates, self.telemetry
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
