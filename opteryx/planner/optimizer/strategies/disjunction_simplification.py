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
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy, OptimizerContext, predicate_key


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
            clause[predicate_key(pred)] = pred
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


def _unwrap_nested(node: Optional[Node]) -> Optional[Node]:
    while node is not None and node.node_type == NodeType.NESTED:
        node = node.centre
    return node


def _simplify_or_conjunct(condition: Node) -> Optional[Node]:
    """`_simplify_disjunction`, plus the CNF-flattening fallback `visit` used to
    apply only at the top level. Returns None if nothing changed."""
    simplified = _simplify_disjunction(condition)
    if simplified is None:
        # No logical simplification, but flatten nested binary OR to CNF
        # when there are 3+ branches for efficient n-ary evaluation.
        branches = _split_or(condition)
        if len(branches) >= 3:
            cnf = Node(node_type=NodeType.CNF)
            cnf.parameters = branches
            simplified = cnf
    return simplified


class DisjunctionSimplificationStrategy(OptimizationStrategy):
    """
    Global DNF normalisation: within-clause dedup, cross-clause dedup, absorption,
    and common-predicate factoring across the OR branches of a filter condition.

    Runs before `SplitConjunctivePredicatesStrategy`, so a Filter's condition at
    this point is still the whole WHERE clause — an OR this strategy needs to
    reach is not always the top-level node itself; it is at least as often one
    top-level-AND conjunct alongside others (`d_year = 2001 AND (a AND x OR a
    AND y) AND (...)`, TPC-DS Q13/Q48's shape). `DisjunctiveDomainPushdownStrategy`,
    which runs immediately after this one for the same reason, already walks
    every top-level-AND conjunct looking for an OR to work on — this strategy
    must do the same, or a common equi-join key ANDed into every branch of a
    non-top-level OR is invisible to every join-key-detection site downstream
    (`cross_join_chain_reorder`, the DPccp adapter, `cross_join_filter_pushdown`),
    silently degrading the comma join into a real, unfiltered cross join — a
    20+ minute hang on TPC-DS Q13 at SF1 (see runner.py's TPC-DS smoke suite
    docstring).
    """

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()

        if node.node_type == LogicalPlanStepType.Filter and node.condition is not None:
            conjuncts = _split_and(node.condition)
            changed = False
            new_conjuncts: List[Node] = []
            for conjunct in conjuncts:
                unwrapped = _unwrap_nested(conjunct)
                if unwrapped is not None and unwrapped.node_type == NodeType.OR:
                    simplified = _simplify_or_conjunct(unwrapped)
                    if simplified is not None:
                        new_conjuncts.append(simplified)
                        changed = True
                        continue
                new_conjuncts.append(conjunct)

            if changed:
                new_condition = _build_and(new_conjuncts)
                new_node = context.optimized_plan[context.node_id]
                new_node.condition = new_condition
                new_node.columns = get_all_nodes_of_type(
                    new_condition, select_nodes=(NodeType.IDENTIFIER,)
                )
                context.optimized_plan[context.node_id] = new_node
                self.telemetry.optimization_disjunction_simplification = (
                    getattr(self.telemetry, "optimization_disjunction_simplification", 0) + 1
                )

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        for node in plan._nodes.values():
            if node.node_type == LogicalPlanStepType.Filter and node.condition is not None:
                for conjunct in _split_and(node.condition):
                    unwrapped = _unwrap_nested(conjunct)
                    if unwrapped is not None and unwrapped.node_type == NodeType.OR:
                        return True
        return False
