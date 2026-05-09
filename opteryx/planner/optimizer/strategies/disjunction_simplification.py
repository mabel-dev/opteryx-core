# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# AS IS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""
Optimization Strategy: Disjunction Simplification

Global DNF normalisation across the OR-branches of a filter condition.

Conceptually distinct from BooleanSimplificationStrategy: that strategy applies
local algebraic rewrites (De Morgan's, double-negation, constant folding,
single-level redundancy) — it preserves the shape of the boolean tree.
This one *reshapes* the tree by reasoning across OR branches:

  1. Within-clause dedup:   A AND A AND B            → A AND B
  2. Cross-clause dedup:    (A AND B) OR (A AND B)   → A AND B
  3. Absorption:            A OR (A AND B)           → A
  4. Common factoring:      (J AND A) OR (J AND B)   → J AND (A OR B)

After this strategy runs, SplitConjunctivePredicatesStrategy can separate any
factored common predicates into their own filter nodes, which in turn enables
predicate pushdown and CrossJoinFilterPushdownStrategy to recognise them as
join conditions. Without it, queries like TPC-H Q19 — three OR branches each
containing the same `p_partkey = l_partkey` — execute as full cartesian
products.
"""

from typing import Dict, FrozenSet, List, Optional

from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.expression.formatter import format_expression
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy, OptimizerContext


def _split_or(node: Optional[Node]) -> List[Node]:
    """Recursively split OR (and NESTED-OR) nodes into a flat list of branches."""
    if node is None:
        return []
    if node.node_type == NodeType.NESTED:
        return _split_or(node.centre)
    if node.node_type != NodeType.OR:
        return [node]
    return _split_or(node.left) + _split_or(node.right)


def _split_and(node: Optional[Node]) -> List[Node]:
    """Recursively split AND (and NESTED-AND) nodes into a flat list of predicates."""
    if node is None:
        return []
    if node.node_type == NodeType.NESTED:
        return _split_and(node.centre)
    if node.node_type != NodeType.AND:
        return [node]
    return _split_and(node.left) + _split_and(node.right)


def _build_and(predicates: List[Node]) -> Optional[Node]:
    if not predicates:
        return None
    result = predicates[0]
    for pred in predicates[1:]:
        n = Node(node_type=NodeType.AND)
        n.left = result
        n.right = pred
        result = n
    return result


def _build_or(predicates: List[Node]) -> Optional[Node]:
    if not predicates:
        return None
    result = predicates[0]
    for pred in predicates[1:]:
        n = Node(node_type=NodeType.OR)
        n.left = result
        n.right = pred
        result = n
    return result


def _simplify_disjunction(condition: Node) -> Optional[Node]:
    """
    Apply DNF simplification to an OR-rooted condition. Returns a new condition
    if any rewrite occurred, or None if the condition is already in simplest form.
    """
    branches = _split_or(condition)
    if len(branches) < 2:
        return None

    # Decompose each branch into AND-conjuncts keyed by canonical string.
    # Within-clause dedup happens implicitly via the dict.
    branch_clauses: List[Dict[str, Node]] = []
    for branch in branches:
        clause: Dict[str, Node] = {}
        for pred in _split_and(branch):
            clause[format_expression(pred)] = pred
        if clause:
            branch_clauses.append(clause)

    if not branch_clauses:
        return None

    # Cross-clause dedup: drop OR-branches with an identical key set.
    seen: List[FrozenSet[str]] = []
    deduped: List[Dict[str, Node]] = []
    for clause in branch_clauses:
        key = frozenset(clause.keys())
        if key in seen:
            continue
        seen.append(key)
        deduped.append(clause)
    branch_clauses = deduped

    # Absorption: drop any clause that is a strict superset of another clause.
    # `(A AND B) OR A` → `A`, because A subsumes A AND B.
    keysets = [frozenset(c.keys()) for c in branch_clauses]
    keep_idx = []
    for i, ki in enumerate(keysets):
        absorbed = False
        for j, kj in enumerate(keysets):
            if i != j and kj < ki:  # strict subset
                absorbed = True
                break
        if not absorbed:
            keep_idx.append(i)
    branch_clauses = [branch_clauses[i] for i in keep_idx]

    # If absorption collapsed everything to a single branch, that branch IS the
    # whole condition (no OR needed).
    if len(branch_clauses) == 1:
        only = list(branch_clauses[0].values())
        return _build_and(only)

    # Common-predicate factoring: keys present in every remaining branch.
    common_keys = set(branch_clauses[0].keys())
    for clause in branch_clauses[1:]:
        common_keys &= set(clause.keys())

    if common_keys:
        # If any branch is exactly the common keys, the per-branch remainder is
        # empty: `J AND (... OR TRUE OR ...)` collapses to `J`.
        common_preds = [branch_clauses[0][k] for k in sorted(common_keys)]
        remainder_nodes: List[Node] = []
        any_empty_remainder = False
        for clause in branch_clauses:
            rem = [v for k, v in clause.items() if k not in common_keys]
            if not rem:
                any_empty_remainder = True
                break
            remainder_nodes.append(_build_and(rem))

        if any_empty_remainder:
            return _build_and(common_preds)

        or_part = _build_or(remainder_nodes)
        return _build_and(common_preds + [or_part])

    # No common factoring possible — but we may still have rewritten via dedup
    # or absorption. Only rebuild if the structure actually changed.
    rebuilt_branches = [_build_and(list(clause.values())) for clause in branch_clauses]

    if len(rebuilt_branches) == len(branches):
        original_sizes = [len(_split_and(b)) for b in branches]
        new_sizes = [len(c) for c in branch_clauses]
        if original_sizes == new_sizes:
            return None

    return _build_or(rebuilt_branches)


class DisjunctionSimplificationStrategy(OptimizationStrategy):
    """
    Global DNF normalisation: within-clause dedup, cross-clause dedup, absorption,
    and common-predicate factoring across the OR branches of a filter condition.
    """

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()

        if node.node_type == LogicalPlanStepType.Filter and node.condition is not None:
            condition = node.condition
            while condition is not None and condition.node_type == NodeType.NESTED:
                condition = condition.centre

            if condition is not None and condition.node_type == NodeType.OR:
                simplified = _simplify_disjunction(condition)
                if simplified is None:
                    # No logical simplification, but flatten nested binary OR to CNF
                    # when there are 3+ branches for efficient n-ary evaluation.
                    branches = _split_or(condition)
                    if len(branches) >= 3:
                        cnf = Node(node_type=NodeType.CNF)
                        cnf.parameters = branches
                        simplified = cnf

                if simplified is not None:
                    new_node = context.optimized_plan[context.node_id]
                    new_node.condition = simplified
                    new_node.columns = get_all_nodes_of_type(
                        simplified, select_nodes=(NodeType.IDENTIFIER,)
                    )
                    context.optimized_plan[context.node_id] = new_node
                    self.telemetry.optimization_disjunction_simplification = (
                        getattr(
                            self.telemetry, "optimization_disjunction_simplification", 0
                        )
                        + 1
                    )

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        for node in plan._nodes.values():
            if node.node_type == LogicalPlanStepType.Filter and node.condition is not None:
                cond = node.condition
                while cond is not None and cond.node_type == NodeType.NESTED:
                    cond = cond.centre
                if cond is not None and cond.node_type == NodeType.OR:
                    return True
        return False
