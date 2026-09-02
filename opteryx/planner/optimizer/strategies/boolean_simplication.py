# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Demorgan's Laws

Type: Heuristic
Goal: Preposition for following actions
"""

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.models import QueryTelemetry
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType

from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext
from .optimization_strategy import get_nodes_of_type_from_logical_plan
from .optimization_strategy import predicate_key

# Operations safe to invert.
HALF_INVERSIONS: dict = {
    "Eq": "NotEq",
    "Gt": "LtEq",
    "GtEq": "Lt",
    "Like": "NotLike",
    "ILike": "NotILike",
    "RLike": "NotRLike",
    "InStr": "NotInStr",
    "IInStr": "NotIInStr",
    # NOT (x IN (...)) is ONE NotInList probe — the native kernel the connectors
    # push (PUSHABLE_OPS) — not an unbounded chain of `!=` conjuncts. It used to
    # be expanded to N NotEq terms "for pushability", which gave `NOT (x IN (...))`
    # N filter passes while the equivalent `x NOT IN (...)` kept one.
    "InList": "NotInList",
    # Unary tests. These are UNARY_OPERATOR nodes whose `value` is the test name;
    # the same lookup inverts them. Without these `NOT (x IS NULL)` kept a NOT
    # root, which no pushdown gate admits, so it was stranded above every join.
    "IsNull": "IsNotNull",
    "IsTrue": "IsNotTrue",
    "IsFalse": "IsNotFalse",
    "IsEmpty": "IsNotEmpty",
    # Any to All conversions (De Morgan's laws)
    "AnyOpEq": "AllOpNotEq",  # NOT(ANY x = y) → ALL x != y
    "AnyOpGtEq": "AllOpLt",  # NOT(ANY x >= y) → ALL x < y
}

INVERSIONS = {**HALF_INVERSIONS, **{v: k for k, v in HALF_INVERSIONS.items()}}

# Node types whose `value` is an operator name INVERSIONS can look up. A LITERAL's
# value may be an unhashable list and a BETWEEN's is a tuple of flags — neither is
# an operator, so the lookup is gated on the node type, not just on the value.
_INVERTIBLE_NODE_TYPES = (NodeType.COMPARISON_OPERATOR, NodeType.UNARY_OPERATOR)


def _directly_invertible(node: LogicalPlanNode) -> bool:
    """True when `NOT node` collapses to a single node with no NOT left behind."""
    while node is not None and node.node_type == NodeType.NESTED:
        node = node.centre
    if node is None:
        return False
    if node.node_type == NodeType.NOT:
        return True
    return node.node_type in _INVERTIBLE_NODE_TYPES and node.value in INVERSIONS


class BooleanSimplificationStrategy(OptimizationStrategy):  # pragma: no cover
    """
    This action aims to rewrite and simplify expressions.

    This has two purposes:
     1) Reduce the work to evaluate expressions by removing steps
     2) Express conditions in ways that other strategies can act on, e.g. pushing
        predicates.

    The core of this action takes advantage of the following:

        Demorgan's Laws (Binary)
            not (A or B) = (not A) and (not B)

        De Morgan's Laws (N-ary Extension)
            not (A or B or C ...) = (not A) and (not B) and (not C) ...
            Creates multiple AND conditions for better predicate pushdown

        De Morgan's Laws (AND)
            not (A and B) = (not A) or (not B)
            Applied only when every conjunct inverts cleanly (a comparison or a
            unary test), so the result is an OR of plain predicates — the shape
            the parser already emits for NOT BETWEEN — rather than an OR of NOTs.

        Negative Reduction:
            not (A = B) = A != B
            not (A != B) = A = B
            not (x IN (...)) = x NOT IN (...)
            not (x IS NULL) = x IS NOT NULL   (and IS TRUE / IS FALSE / IS EMPTY)
            not (not (A)) = A

        Constant Folding:
            A AND TRUE => A
            A AND FALSE => FALSE
            A OR TRUE => TRUE
            A OR FALSE => A

        AND Chain Flattening:
            ((A AND B) AND C) => (A AND (B AND C))

        Redundant Condition Removal:
            A AND A => A

    These simplifications help prepare conditions for predicate pushdown by creating
    simple chains of AND conditions that can be more easily pushed down through the
    query plan.
    """

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if node.node_type == LogicalPlanStepType.Filter:
            # do the work
            node.condition = update_expression_tree(node.condition, self.telemetry)
            context.optimized_plan[context.node_id] = node

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # No finalization needed for this strategy
        return plan

    def should_i_run(self, plan):
        # only run if there are FILTER clauses in the plan
        candidates = get_nodes_of_type_from_logical_plan(plan, (LogicalPlanStepType.Filter,))
        return len(candidates) > 0


def _is_literal_true(node: LogicalPlanNode) -> bool:
    """Check if a node is a literal TRUE value."""
    if node is None:
        return False
    if node.node_type == NodeType.LITERAL:
        return node.value is True
    return False


def _is_literal_false(node: LogicalPlanNode) -> bool:
    """Check if a node is a literal FALSE value."""
    if node is None:
        return False
    if node.node_type == NodeType.LITERAL:
        return node.value is False
    return False


def _flatten_and_chain(node: LogicalPlanNode, telemetry: QueryTelemetry) -> list:
    """
    Flatten nested AND chains into a list of conditions.
    e.g., ((A AND B) AND C) becomes [A, B, C]
    """
    if node is None:
        return []
    if node.node_type != NodeType.AND:
        return [node]

    left_conditions = _flatten_and_chain(node.left, telemetry)
    right_conditions = _flatten_and_chain(node.right, telemetry)
    return left_conditions + right_conditions


def _rebuild_and_chain(conditions: list) -> LogicalPlanNode:
    """
    Rebuild AND chain from a list of conditions.
    [A, B, C] becomes ((A AND B) AND C)
    """
    if not conditions:
        return None
    if len(conditions) == 1:
        return conditions[0]

    result = conditions[0]
    for condition in conditions[1:]:
        result = Node(NodeType.AND, left=result, right=condition)
    return result


def _simplify_and_chain(node: LogicalPlanNode, telemetry: QueryTelemetry):
    """Simplify an AND chain in a single pass.

    Flattens the whole chain once, simplifies each conjunct once, then folds
    constants / removes duplicates / rebuilds a single time. Doing all of this at
    the top of the chain avoids the previous behaviour, where the generic
    post-order walk re-flattened the accumulated left subtree at *every* AND node
    (O(n^2) in chain length, with an O(n^2) dedup on top). A WHERE with dozens of
    ANDed predicates made that dominate bind+optimize time; this is O(n).

    Semantics preserved from the prior pairwise implementation:
      * a literal FALSE conjunct short-circuits the whole chain to FALSE;
      * literal TRUE conjuncts are dropped (an all-TRUE chain collapses to TRUE);
      * duplicate conjuncts are removed only when the flattened chain has >2
        members (the original only deduped on the >2 flatten path).
    """
    # One flatten of the whole chain, then simplify each conjunct exactly once.
    # A conjunct may itself expand into an AND (e.g. De Morgan on NOT(OR ...),
    # NOT IN expansion); splice those back in flat so the chain stays flat.
    processed: list = []
    for conjunct in _flatten_and_chain(node, telemetry):
        simplified = update_expression_tree(conjunct, telemetry)
        if simplified is None:
            continue
        if simplified.node_type == NodeType.AND:
            processed.extend(_flatten_and_chain(simplified, telemetry))
        else:
            processed.append(simplified)

    # Constant folding: FALSE short-circuits; TRUE conjuncts drop out.
    kept: list = []
    true_node = None
    for cond in processed:
        if _is_literal_false(cond):
            telemetry.optimization_boolean_rewrite_and_false += 1
            return cond
        if _is_literal_true(cond):
            telemetry.optimization_boolean_rewrite_and_true += 1
            true_node = cond
            continue
        kept.append(cond)

    # All conjuncts were TRUE — the chain is TRUE.
    if not kept:
        return true_node

    # Dedup only kicks in for chains longer than two, matching the prior >2 gate.
    # Keyed on the conjunct's CURRENT rendered content, never on Node.uuid — by
    # this point conjuncts have been rewritten in place (NOT (A = B) inverts the
    # comparison node and keeps its uuid), and the binder hands out uuid-
    # preserving copies of expressions that rendered alike at bind time. See
    # predicate_key() for why uuid is not an expression identity.
    if len(kept) > 2:
        unique: list = []
        seen: set = set()
        for cond in kept:
            key = predicate_key(cond)
            if key in seen:
                telemetry.optimization_boolean_rewrite_and_redundant += 1
                continue
            seen.add(key)
            unique.append(cond)
        telemetry.optimization_boolean_rewrite_and_flatten += 1
        return _rebuild_and_chain(unique)

    return _rebuild_and_chain(kept)


def _flatten_or_chain(node: LogicalPlanNode) -> list:
    """
    Flatten nested OR chains into a list of conditions.
    e.g., ((A OR B) OR C) becomes [A, B, C]
    """
    if node is None:
        return []
    if node.node_type != NodeType.OR:
        return [node]

    left_conditions = _flatten_or_chain(node.left)
    right_conditions = _flatten_or_chain(node.right)
    return left_conditions + right_conditions


def _rebuild_or_chain(conditions: list) -> LogicalPlanNode:
    """
    Rebuild OR chain from a list of conditions.
    [A, B, C] becomes ((A OR B) OR C)
    """
    if not conditions:
        return None
    if len(conditions) == 1:
        return conditions[0]

    result = conditions[0]
    for condition in conditions[1:]:
        result = Node(NodeType.OR, left=result, right=condition)
    return result


def update_expression_tree(node: LogicalPlanNode, telemetry: QueryTelemetry):
    # break out of nests
    if node.node_type == NodeType.NESTED:
        return update_expression_tree(node.centre, telemetry)

    # handle rules relating to NOTs
    if node.node_type == NodeType.NOT:
        centre_node = node.centre

        # break out of nesting
        if centre_node.node_type == NodeType.NESTED:
            centre_node = centre_node.centre

        # De Morgan's n-ary: NOT (A OR B OR C ...) => (NOT A) AND (NOT B) AND (NOT C) ...
        # This creates more AND conditions that can be pushed down
        if centre_node.node_type == NodeType.OR:
            # Flatten the OR chain to handle all conditions
            or_conditions = _flatten_or_chain(centre_node)

            # If we have 2+ conditions, apply De Morgan's to all
            if len(or_conditions) >= 2:
                # Create NOT of each condition
                not_conditions = [
                    Node(NodeType.NOT, centre=condition) for condition in or_conditions
                ]

                # Rebuild as AND chain (highly pushable!)
                result = not_conditions[0]
                for condition in not_conditions[1:]:
                    result = Node(NodeType.AND, left=result, right=condition)

                # Track statistic based on chain length
                if len(or_conditions) > 2:
                    telemetry.optimization_boolean_rewrite_demorgan_nary += 1
                else:
                    telemetry.optimization_boolean_rewrite_demorgan += 1

                return update_expression_tree(result, telemetry)

        # De Morgan's for AND: NOT (A AND B) => (NOT A) OR (NOT B), only when every
        # conjunct inverts to a plain predicate. The OR that results is a single-
        # relation shape pushdown can carry to a leg (an OR root is admitted; a NOT
        # root is not), and the same shape the parser emits for NOT BETWEEN, so
        # `NOT (x BETWEEN a AND b)` and `x NOT BETWEEN a AND b` now plan alike.
        if centre_node.node_type == NodeType.AND:
            and_conditions = _flatten_and_chain(centre_node, telemetry)
            if len(and_conditions) >= 2 and all(
                _directly_invertible(condition) for condition in and_conditions
            ):
                not_conditions = [
                    Node(NodeType.NOT, centre=condition) for condition in and_conditions
                ]
                telemetry.optimization_boolean_rewrite_demorgan_and += 1
                return update_expression_tree(_rebuild_or_chain(not_conditions), telemetry)

        # NOT(A = B) => A != B, NOT(x IN (..)) => x NOT IN (..), NOT(x IS NULL) => x IS NOT NULL
        if centre_node.node_type in _INVERTIBLE_NODE_TYPES and centre_node.value in INVERSIONS:
            centre_node.value = INVERSIONS[centre_node.value]
            telemetry.optimization_boolean_rewrite_inversion += 1
            return update_expression_tree(centre_node, telemetry)

        # NOT(NOT(A)) => A
        if centre_node.node_type == NodeType.NOT:
            telemetry.optimization_boolean_rewrite_double_not += 1
            return update_expression_tree(centre_node.centre, telemetry)

    # AND chains: handle the whole chain in one pass (flatten, simplify each
    # conjunct, fold/dedup/rebuild once) rather than re-flattening at every AND
    # node during the generic post-order walk below. See _simplify_and_chain.
    if node.node_type == NodeType.AND:
        return _simplify_and_chain(node, telemetry)

    # traverse the expression tree
    node.left = None if node.left is None else update_expression_tree(node.left, telemetry)
    node.centre = None if node.centre is None else update_expression_tree(node.centre, telemetry)
    node.right = None if node.right is None else update_expression_tree(node.right, telemetry)
    if node.parameters:
        node.parameters = [
            (
                parameter
                if not isinstance(parameter, Node)
                else update_expression_tree(parameter, telemetry)
            )
            for parameter in node.parameters
        ]

    return node
