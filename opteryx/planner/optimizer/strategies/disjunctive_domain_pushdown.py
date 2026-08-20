# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# AS IS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

"""
Optimization Strategy: Disjunctive Domain Pushdown

Derives a WEAKER, per-column predicate from an OR-of-AND filter, so a
correlated disjunction that can never be pushed down whole still gets *some*
of its filtering pushed to the scan.

This is deliberately NOT the same job as DisjunctionSimplificationStrategy.
That strategy finds predicates repeated IDENTICALLY across every OR-branch
and factors them out (`(J AND A) OR (J AND B) -> J AND (A OR B)`) — an exact,
equivalence-preserving rewrite. It does nothing when every branch's leaves are
distinct, which is exactly the shape of a "bilateral" filter such as TPC-H Q7:

    (n1.n_name = 'KENYA' AND n2.n_name = 'PERU')
    OR
    (n1.n_name = 'PERU' AND n2.n_name = 'KENYA')

No leaf is shared between the branches, so there is nothing to factor, and the
whole OR is stuck evaluating after every join, over the full unfiltered
cross-product of nation rows. But every branch DOES constrain `n1.n_name` to
one of {KENYA, PERU}, and `n2.n_name` to one of {KENYA, PERU} — so:

    n1.n_name IN ('KENYA', 'PERU') AND n2.n_name IN ('KENYA', 'PERU')

is IMPLIED by the original filter (every branch satisfies it) but WEAKER than
it (e.g. n1='KENYA' AND n2='KENYA' passes the derived predicate but not the
real one). That makes it safe to push down as an extra, additive filter — it
can only discard rows the original filter would also discard — but it can
NEVER replace the original filter, which stays in the plan unmodified and
still does the real correctness work post-join. This strategy only ADDS
predicates; it never removes or rewrites the OR.

The same reasoning extends to range predicates: if every branch bounds a
column between two literals, the UNION of those per-branch ranges is implied
by the whole filter and is pushable. This strategy pushes the loosest sound
bound — the convex hull of the per-branch ranges (min of the lows, max of the
highs) — not the tighter exact union of (possibly disjoint) intervals. The
hull is enough to get the win that matters here (fewer bytes read: row-group
and file min/max pruning only ever check overlap with a [lo, hi] envelope
anyway), without needing the scan layer to support a pushed-down per-column
OR.

Scope (v1): only Eq/InList leaves (-> IN-list) and Gt/GtEq/Lt/LtEq leaves
(-> range) are recognised. A branch that mixes point and range constraints on
the same column, wraps the column in a function/CAST, or simply doesn't
constrain a given column at all, makes that column ineligible — the strategy
silently derives nothing for it rather than guess.

Runs after DisjunctionSimplificationStrategy (so it only has to deal with
whatever OR shape survives that pass) and before
SplitConjunctivePredicatesStrategy, so the derived predicates this strategy
ANDs onto the original condition get split into their own Filter steps and
become independently pushable by PredicatePushdownStrategy.

A WHERE clause with join conditions and a disjunction (the normal shape —
e.g. `p_partkey = l_partkey AND (A OR B OR C)`) reaches this strategy as ONE
Filter node whose condition is a single top-level AND tree; the OR is one
AND-conjunct, not the root. So detection here looks at each top-level
AND-conjunct for one that is OR-rooted, rather than requiring the whole
condition to be OR-rooted.
"""

from typing import Dict, List, Optional, Set, Tuple

from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan, LogicalPlanNode, LogicalPlanStepType
from opteryx.types.logical_type import ARRAY as _CT_ARRAY

from .disjunction_simplification import _split_and, _split_or
from .optimization_strategy import OptimizationStrategy, OptimizerContext
from .predicate_pushdown import _normalize_col_op_lit

# (value, inclusive, literal_node)
_Bound = Tuple[object, bool, Node]


def _classify_leaf(leaf: Node) -> Optional[Tuple[str, Node, str, tuple]]:
    """Classify one AND-conjunct leaf as a point or range constraint on a column.

    Returns (column_identity, identifier_node, kind, payload) or None if the
    leaf isn't a plain `col = / IN / </<=/>/>= literal` shape this strategy
    understands. kind is "points" (payload = (values: set, element_type)) or
    "range" (payload = (value, inclusive, literal_node, is_lower)).
    """
    if leaf.node_type != NodeType.COMPARISON_OPERATOR:
        return None
    if get_all_nodes_of_type(leaf, (NodeType.FUNCTION, NodeType.CAST, NodeType.AGGREGATOR)):
        return None

    if leaf.value in ("Eq", "InList"):
        left, right = leaf.left, leaf.right
        if (
            left is None
            or right is None
            or left.node_type != NodeType.IDENTIFIER
            or right.node_type != NodeType.LITERAL
            or left.schema_column is None
        ):
            return None
        if leaf.value == "Eq":
            values, element_type = {right.value}, right.type
        else:
            values, element_type = set(right.value), (
                right.type.element if right.type is not None else None
            )
        return left.schema_column.identity, left, "points", (values, element_type)

    if leaf.value in ("Gt", "GtEq", "Lt", "LtEq"):
        ident, op, lit = _normalize_col_op_lit(leaf)
        if ident is None or ident.schema_column is None:
            return None
        is_lower = op in ("Gt", "GtEq")
        inclusive = op in ("GtEq", "LtEq")
        return ident.schema_column.identity, ident, "range", (lit.value, inclusive, lit, is_lower)

    return None


def _tighter(a: _Bound, b: _Bound, is_lower: bool) -> _Bound:
    """Pick whichever bound is MORE RESTRICTIVE (larger for a lower bound,
    smaller for an upper bound; on a tie, exclusive is tighter than inclusive).
    Used to AND-narrow multiple leaves on the same column within one branch."""
    a_key = (a[0], (0 if a[1] else 1) if is_lower else (1 if a[1] else 0))
    b_key = (b[0], (0 if b[1] else 1) if is_lower else (1 if b[1] else 0))
    bigger = a_key > b_key
    return a if (bigger if is_lower else not bigger) else b


def _looser(a: _Bound, b: _Bound, is_lower: bool) -> _Bound:
    """Pick whichever bound is MORE PERMISSIVE (smaller for a lower bound,
    larger for an upper bound; on a tie, inclusive is looser than exclusive).
    Used to OR-widen a column's per-branch bound into the cross-branch hull."""
    a_key = (a[0], (0 if a[1] else 1) if is_lower else (1 if a[1] else 0))
    b_key = (b[0], (0 if b[1] else 1) if is_lower else (1 if b[1] else 0))
    smaller = a_key < b_key
    return a if (smaller if is_lower else not smaller) else b


def _branch_column_domains(branch: Node) -> Dict[str, tuple]:
    """Reduce one OR-branch to {column_identity: (identifier, kind, payload)},
    AND-narrowing multiple same-column leaves within the branch. A column with
    both point and range leaves in the same branch is dropped (out of scope)."""
    per_col: Dict[str, list] = {}
    for leaf in _split_and(branch):
        classified = _classify_leaf(leaf)
        if classified is None:
            continue
        identity, ident, kind, payload = classified
        per_col.setdefault(identity, []).append((ident, kind, payload))

    result: Dict[str, tuple] = {}
    for identity, entries in per_col.items():
        kinds = {kind for _, kind, _ in entries}
        if len(kinds) != 1:
            continue
        kind = kinds.pop()
        ident = entries[0][0]

        if kind == "points":
            values: Set = set()
            element_type = None
            for _, _, (vals, et) in entries:
                values |= vals
                element_type = element_type or et
            result[identity] = (ident, "points", (values, element_type))
            continue

        lo: Optional[_Bound] = None
        hi: Optional[_Bound] = None
        for _, _, (value, inclusive, lit, is_lower) in entries:
            bound = (value, inclusive, lit)
            if is_lower:
                lo = bound if lo is None else _tighter(lo, bound, is_lower=True)
            else:
                hi = bound if hi is None else _tighter(hi, bound, is_lower=False)
        result[identity] = (ident, "range", (lo, hi))

    return result


def _domain_for_column(branch_domains: List[Dict[str, tuple]], identity: str) -> Optional[tuple]:
    """Union a column's per-branch domain across ALL branches into the hull
    that's implied by (but weaker than) the whole OR. None if branches
    disagree on kind, or the hull ends up unbounded on both sides."""
    entries = [bd[identity] for bd in branch_domains]
    kinds = {kind for _, kind, _ in entries}
    if len(kinds) != 1:
        return None
    kind = kinds.pop()
    ident = entries[0][0]

    if kind == "points":
        values: Set = set()
        element_type = None
        for _, _, (vals, et) in entries:
            values |= vals
            element_type = element_type or et
        return ident, "points", (values, element_type)

    lo: Optional[_Bound] = None
    hi: Optional[_Bound] = None
    lo_unbounded = hi_unbounded = False
    for _, _, (branch_lo, branch_hi) in entries:
        if branch_lo is None:
            lo_unbounded = True
        elif not lo_unbounded:
            lo = branch_lo if lo is None else _looser(lo, branch_lo, is_lower=True)
        if branch_hi is None:
            hi_unbounded = True
        elif not hi_unbounded:
            hi = branch_hi if hi is None else _looser(hi, branch_hi, is_lower=False)

    if lo_unbounded:
        lo = None
    if hi_unbounded:
        hi = None
    if lo is None and hi is None:
        return None
    return ident, "range", (lo, hi)


def _build_points_node(ident: Node, values: Set, element_type) -> Node:
    ordered = sorted(values, key=str)
    if len(ordered) == 1:
        lit = Node(node_type=NodeType.LITERAL, type=element_type, value=ordered[0])
        return Node(NodeType.COMPARISON_OPERATOR, value="Eq", left=ident.copy(), right=lit)
    lit = Node(node_type=NodeType.LITERAL, type=_CT_ARRAY(element_type), value=ordered)
    return Node(NodeType.COMPARISON_OPERATOR, value="InList", left=ident.copy(), right=lit)


def _build_range_nodes(ident: Node, lo: Optional[_Bound], hi: Optional[_Bound]) -> List[Node]:
    """1 or 2 leaf comparisons. PredicateCompactionStrategy (later in the
    pipeline) recombines a lo+hi pair on the same column into one BETWEEN, so
    there's no need to build that node shape here."""
    nodes = []
    if lo is not None:
        _, inclusive, lit = lo
        op = "GtEq" if inclusive else "Gt"
        nodes.append(Node(NodeType.COMPARISON_OPERATOR, value=op, left=ident.copy(), right=lit.copy()))
    if hi is not None:
        _, inclusive, lit = hi
        op = "LtEq" if inclusive else "Lt"
        nodes.append(Node(NodeType.COMPARISON_OPERATOR, value=op, left=ident.copy(), right=lit.copy()))
    return nodes


def _derive_domain_predicates(condition: Node) -> List[Node]:
    branches = _split_or(condition)
    if len(branches) < 2:
        return []

    branch_domains = [_branch_column_domains(branch) for branch in branches]

    candidate_identities = set(branch_domains[0].keys())
    for domains in branch_domains[1:]:
        candidate_identities &= set(domains.keys())
    if not candidate_identities:
        return []

    derived: List[Node] = []
    for identity in sorted(candidate_identities):
        domain = _domain_for_column(branch_domains, identity)
        if domain is None:
            continue
        ident, kind, payload = domain
        if kind == "points":
            values, element_type = payload
            derived.append(_build_points_node(ident, values, element_type))
        else:
            lo, hi = payload
            derived.extend(_build_range_nodes(ident, lo, hi))
    return derived


class DisjunctiveDomainPushdownStrategy(OptimizationStrategy):
    """
    Adds implied, weaker per-column domain predicates (IN-list or range) ANDed
    onto an OR-of-AND filter, so a correlated disjunction that can never be
    pushed down whole still gets some of its filtering pushed to the scan. See
    the module docstring for the full reasoning and the TPC-H Q7 example.
    """

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if node.node_type == LogicalPlanStepType.Filter and node.condition is not None:
            derived: List[Node] = []
            for conjunct in _split_and(node.condition):
                while conjunct is not None and conjunct.node_type == NodeType.NESTED:
                    conjunct = conjunct.centre
                if conjunct is not None and conjunct.node_type == NodeType.OR:
                    derived.extend(_derive_domain_predicates(conjunct))

            if derived:
                new_node = context.optimized_plan[context.node_id]
                new_condition = new_node.condition
                for predicate in derived:
                    wrapper = Node(node_type=NodeType.AND)
                    wrapper.left = new_condition
                    wrapper.right = predicate
                    new_condition = wrapper
                new_node.condition = new_condition
                new_node.columns = get_all_nodes_of_type(
                    new_condition, select_nodes=(NodeType.IDENTIFIER,)
                )
                context.optimized_plan[context.node_id] = new_node
                self.telemetry.optimization_disjunctive_domain_pushdown += len(derived)

        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        for node in plan._nodes.values():
            if node.node_type == LogicalPlanStepType.Filter and node.condition is not None:
                for conjunct in _split_and(node.condition):
                    while conjunct is not None and conjunct.node_type == NodeType.NESTED:
                        conjunct = conjunct.centre
                    if conjunct is not None and conjunct.node_type == NodeType.OR:
                        return True
        return False
