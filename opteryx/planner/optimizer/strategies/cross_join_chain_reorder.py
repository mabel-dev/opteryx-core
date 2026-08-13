# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Strategy: Cross Join Chain Reorder

A `FROM A, B, C, D` clause is built by the logical planner as a left-deep chain
of implicit cross joins in the order written:

    ((((A x B) x C) x D)

`CrossJoinFilterPushdownStrategy` later converts each cross join to an inner
join when it can find an equality predicate spanning that join's two sides.
But equalities are only direct edges in the join graph — the strategy can't
rewrite a chain whose adjacent operands are not directly connected.

For TPC-H Q02 the outer FROM is `part, supplier, partsupp, ...`. There is no
direct predicate between `part` and `supplier` (they connect via `partsupp`),
so the bottom `part x supplier` cross join cannot be converted and produces a
40M-row Cartesian.

This strategy runs *before* `CrossJoinFilterPushdownStrategy` and reorders the
chain of cross joins so that each adjacent pair shares an equality edge in the
join graph derived from the WHERE clause. After reordering, the pushdown
strategy converts every cross join into an inner join.

Connectivity is the hard constraint. Original FROM-order is preserved as the
tie-breaker so plans for queries that already work today are unchanged.
"""

from typing import Dict, List, Optional, Set, Tuple

from opteryx.expression import NodeType
from opteryx.models import Node
from opteryx.planner.logical_planner.logical_planner import (
    LogicalPlan,
    LogicalPlanNode,
    LogicalPlanStepType,
)

from .optimization_strategy import OptimizationStrategy, OptimizerContext


def _split_and_conditions(node: Optional[Node]) -> List[Node]:
    if node is None:
        return []
    if node.node_type != NodeType.AND:
        return [node]
    return _split_and_conditions(node.left) + _split_and_conditions(node.right)


def _identifier_source(node: Optional[Node]) -> Optional[str]:
    if node is None:
        return None
    if node.node_type == NodeType.IDENTIFIER:
        return node.source
    return None


def _is_unconverted_cross_join(node: LogicalPlanNode) -> bool:
    # A window join (`agg OVER ()`, window_to_join.py) is a cross join with no ON, so it
    # matches everything below — but it is not part of a `FROM a, b, c` chain and must not
    # be reordered into one. Its legs are labelled and its right leg is a synthetic one-row
    # aggregate that the Project above the chain reads by alias; moving it changes which
    # relation that Project is expanding. Excluded by intent, not by accident.
    return (
        node.node_type == LogicalPlanStepType.Join
        and node.type == "cross join"
        and not getattr(node, "on", None)
        and not getattr(node, "using", None)
        and not getattr(node, "is_window_join", False)
    )


def _subplan_relation_names(
    plan: LogicalPlan, root_id: str, visited: Optional[Set[str]] = None
) -> Set[str]:
    """
    Walk the subplan rooted at root_id and return the set of relation names it
    exposes. Subqueries are opaque — their alias is included but their interior
    is not descended into.
    """
    if visited is None:
        visited = set()
    if root_id in visited:
        return set()
    visited.add(root_id)

    node = plan[root_id]
    names: Set[str] = set()
    alias = getattr(node, "alias", None)
    if alias:
        names.add(alias)

    if node.node_type == LogicalPlanStepType.Subquery:
        return names

    for child_id, _, _ in plan.ingoing_edges(root_id):
        names |= _subplan_relation_names(plan, child_id, visited)
    return names


def _collect_chain_top_down(
    plan: LogicalPlan, root_id: str
) -> List[Tuple[str, LogicalPlanNode]]:
    """
    Starting at root_id (the topmost cross join), walk down the chain of
    unconverted cross joins where each step has exactly one cross-join child.
    Returns the chain top-down. The bottom join in the returned list has both
    children as leaves.
    """
    chain: List[Tuple[str, LogicalPlanNode]] = []
    current_id = root_id
    while True:
        node = plan[current_id]
        if not _is_unconverted_cross_join(node):
            break
        chain.append((current_id, node))

        cross_children = [
            child_id
            for child_id, _, _ in plan.ingoing_edges(current_id)
            if _is_unconverted_cross_join(plan[child_id])
        ]
        # A well-formed left-deep chain has 0 or 1 cross-join children.
        # Anything else is an unusual shape we leave alone.
        if len(cross_children) != 1:
            break
        current_id = cross_children[0]

    return chain


class _Leaf:
    __slots__ = ("subplan_id", "rel_names", "readers", "original_index")

    def __init__(
        self,
        subplan_id: str,
        rel_names: List[str],
        readers: Optional[List[str]],
        original_index: int,
    ) -> None:
        self.subplan_id = subplan_id
        self.rel_names = list(rel_names)
        self.readers = list(readers) if readers else []
        self.original_index = original_index


def _gather_leaves(
    plan: LogicalPlan, chain: List[Tuple[str, LogicalPlanNode]]
) -> Optional[List[_Leaf]]:
    """
    Walk the chain bottom-up and collect every leaf relation in user-written
    FROM order. Returns None if the chain has any shape we don't recognise
    (defensive: the strategy must be a no-op rather than corrupt the plan).

    User-written order:
        FROM A, B, C, D  ->  ((A x B) x C) x D
        leaves[0] = A (bottom-left)
        leaves[1] = B (bottom-right)
        leaves[2] = C (J2's right)
        leaves[3] = D (J3's right)
    """
    if not chain:
        return None

    leaves: List[_Leaf] = []
    chain_ids = {jid for jid, _ in chain}

    bottom_id, bottom_node = chain[-1]

    # Bottom: both children must be non-chain leaves.
    bottom_children = [
        child_id
        for child_id, _, _ in plan.ingoing_edges(bottom_id)
        if child_id not in chain_ids
    ]
    if len(bottom_children) != 2:
        return None

    left_target = set(bottom_node.left_relation_names or [])
    right_target = set(bottom_node.right_relation_names or [])

    # Match each child to left/right by walking its subplan's relation names.
    bottom_left_id: Optional[str] = None
    bottom_right_id: Optional[str] = None
    for child_id in bottom_children:
        child_names = _subplan_relation_names(plan, child_id)
        if left_target and child_names == left_target and bottom_left_id is None:
            bottom_left_id = child_id
        elif right_target and child_names == right_target and bottom_right_id is None:
            bottom_right_id = child_id

    # If matching failed (e.g. metadata stale), fall back to original order.
    if bottom_left_id is None or bottom_right_id is None:
        # Defensive bail — don't reorder if we can't be sure of left/right.
        return None

    leaves.append(
        _Leaf(
            subplan_id=bottom_left_id,
            rel_names=list(bottom_node.left_relation_names or []),
            readers=bottom_node.left_readers,
            original_index=0,
        )
    )
    leaves.append(
        _Leaf(
            subplan_id=bottom_right_id,
            rel_names=list(bottom_node.right_relation_names or []),
            readers=bottom_node.right_readers,
            original_index=1,
        )
    )

    # Walk chain bottom-up (skip bottom, already handled).
    # chain is top-down, so reversed(chain[:-1]) goes bottom-up.
    next_index = 2
    for jid, jnode in reversed(chain[:-1]):
        leaf_children = [
            child_id
            for child_id, _, _ in plan.ingoing_edges(jid)
            if child_id not in chain_ids
        ]
        if len(leaf_children) != 1:
            return None
        leaves.append(
            _Leaf(
                subplan_id=leaf_children[0],
                rel_names=list(jnode.right_relation_names or []),
                readers=jnode.right_readers,
                original_index=next_index,
            )
        )
        next_index += 1

    return leaves


def _build_join_graph(
    where_predicates: List[Node], leaves: List[_Leaf]
) -> Dict[int, Set[int]]:
    """
    Build an undirected adjacency map keyed by leaf index.

    Edges = direct equality predicates between identifiers belonging to two
    different leaves in the chain.
    """
    # Map relation name -> leaf index. A leaf may carry multiple relation names
    # (a CTE / subquery composed of joins). All map to the same leaf index.
    rel_to_leaf: Dict[str, int] = {}
    for i, leaf in enumerate(leaves):
        for rel in leaf.rel_names:
            rel_to_leaf[rel] = i

    adj: Dict[int, Set[int]] = {i: set() for i in range(len(leaves))}

    for pred in where_predicates:
        if not (
            pred.node_type == NodeType.COMPARISON_OPERATOR
            and pred.value == "Eq"
        ):
            continue
        left_src = _identifier_source(pred.left)
        right_src = _identifier_source(pred.right)
        if left_src is None or right_src is None:
            continue
        left_leaf = rel_to_leaf.get(left_src)
        right_leaf = rel_to_leaf.get(right_src)
        if left_leaf is None or right_leaf is None:
            continue
        if left_leaf == right_leaf:
            continue
        adj[left_leaf].add(right_leaf)
        adj[right_leaf].add(left_leaf)

    return adj


def _choose_order(adj: Dict[int, Set[int]], num_leaves: int) -> List[int]:
    """
    Pick a leaf order such that every leaf after the first is connected to the
    set of already-picked leaves. Original FROM order is the tie-breaker, so
    plans that already work today are unchanged.

    Disconnected components stay disconnected: we finish one component, then
    start the next from its lowest-index leaf. The resulting cross joins
    between components correspond to genuine Cartesians the user wrote and
    cannot be converted to inner joins anyway.
    """
    placed: List[int] = []
    placed_set: Set[int] = set()
    remaining: Set[int] = set(range(num_leaves))

    while remaining:
        if not placed:
            seed = min(remaining)
            placed.append(seed)
            placed_set.add(seed)
            remaining.discard(seed)
            continue

        candidates = [i for i in remaining if adj[i] & placed_set]
        if candidates:
            pick = min(candidates)
        else:
            pick = min(remaining)
        placed.append(pick)
        placed_set.add(pick)
        remaining.discard(pick)

    return placed


def _rewire_chain(
    plan: LogicalPlan,
    chain: List[Tuple[str, LogicalPlanNode]],
    leaves: List[_Leaf],
    new_order: List[int],
) -> None:
    """
    Reattach the leaves of the cross-join chain in `new_order` and refresh
    each join node's relation_names / readers / schemas so that later
    strategies see a consistent left-deep tree.

    Topology of internal chain edges (J1 -> J2 -> J3 -> ...) is unchanged.
    Only the leaf-feeding edges and per-node metadata are rewritten.
    """
    chain_ids = {jid for jid, _ in chain}
    new_leaves = [leaves[i] for i in new_order]

    # The top join was bound with the full set of relation schemas under the
    # chain. We use it as the master schema map so that bottom-up rebuilds get
    # the correct {relation_name: schema} entries even after rearrangement.
    top_node = chain[0][1]
    master_schemas = dict(getattr(top_node, "schemas", None) or {})

    def _schemas_for(rel_names: List[str]) -> dict:
        # Preserve any non-relation entries (e.g. "$derived") on every join in
        # the chain — projection / binder code that follows expects them on
        # every join node.
        out = {
            k: v
            for k, v in master_schemas.items()
            if not k or k.startswith("$") or k.startswith("$derived")
        }
        for name in rel_names:
            if name in master_schemas:
                out[name] = master_schemas[name]
        return out

    # Step 1: drop every leaf-feeding edge in the chain.
    for jid, _ in chain:
        for child_id, target, relationship in list(plan.ingoing_edges(jid)):
            if child_id in chain_ids:
                continue
            plan.remove_edge(child_id, target, relationship)

    bottom_id, bottom_node = chain[-1]

    # Step 2: rewire bottom join with two new leaves.
    bottom_left = new_leaves[0]
    bottom_right = new_leaves[1]
    plan.add_edge(bottom_left.subplan_id, bottom_id, None)
    plan.add_edge(bottom_right.subplan_id, bottom_id, None)

    bottom_node.left_relation_names = list(bottom_left.rel_names)
    bottom_node.left_readers = list(bottom_left.readers)
    bottom_node.right_relation_names = list(bottom_right.rel_names)
    bottom_node.right_readers = list(bottom_right.readers)
    bottom_node.relation_names = [
        bottom_node.left_relation_names,
        bottom_node.right_relation_names,
    ]
    bottom_node.schemas = _schemas_for(
        list(bottom_left.rel_names) + list(bottom_right.rel_names)
    )
    plan[bottom_id] = bottom_node

    accumulated_names: List[str] = list(bottom_left.rel_names) + list(bottom_right.rel_names)
    accumulated_readers: List[str] = list(bottom_left.readers) + list(bottom_right.readers)

    # Step 3: walk chain bottom-up (excluding bottom), attaching one leaf per
    # join as the right side. Refresh left side from the running accumulator.
    leaf_cursor = 2
    for jid, jnode in reversed(chain[:-1]):
        leaf = new_leaves[leaf_cursor]
        plan.add_edge(leaf.subplan_id, jid, None)

        jnode.left_relation_names = list(accumulated_names)
        jnode.left_readers = list(accumulated_readers)
        jnode.right_relation_names = list(leaf.rel_names)
        jnode.right_readers = list(leaf.readers)
        jnode.relation_names = [jnode.left_relation_names, jnode.right_relation_names]
        accumulated_names = accumulated_names + list(leaf.rel_names)
        accumulated_readers = accumulated_readers + list(leaf.readers)
        jnode.schemas = _schemas_for(list(accumulated_names))
        plan[jid] = jnode
        leaf_cursor += 1


def _collect_predicates_above(plan: LogicalPlan, chain_top_id: str) -> List[Node]:
    """
    Walk up from chain_top_id collecting predicates from every Filter node
    that sits directly above the chain. Stops at the first non-Filter parent
    (Project, Aggregate, Subquery, etc.) — predicates above those don't
    constrain operands of this chain.

    SplitConjunctivePredicatesStrategy runs before us and explodes one WHERE
    into many Filter nodes (one predicate each), so we must collect across all
    of them.
    """
    predicates: List[Node] = []
    seen: Set[str] = set()
    frontier = [chain_top_id]

    while frontier:
        nid = frontier.pop()
        for _, parent_id, _ in plan.outgoing_edges(nid):
            if parent_id in seen:
                continue
            seen.add(parent_id)
            parent = plan[parent_id]
            if parent.node_type == LogicalPlanStepType.Filter:
                if parent.condition is not None:
                    predicates.extend(_split_and_conditions(parent.condition))
                frontier.append(parent_id)
    return predicates


def _is_already_well_ordered(
    leaves: List[_Leaf], adj: Dict[int, Set[int]]
) -> bool:
    """
    True iff every leaf after the first has at least one edge to a previous
    leaf. When true, the chain is already convertible by the pushdown strategy
    and reordering would be a no-op rewrite — skip to avoid touching the plan.
    """
    if len(leaves) < 2:
        return True
    placed: Set[int] = {0}
    for i in range(1, len(leaves)):
        if not (adj[i] & placed):
            return False
        placed.add(i)
    return True


class CrossJoinChainReorderStrategy(OptimizationStrategy):
    """
    Reorder chains of implicit cross joins (`FROM A, B, C`) so that adjacent
    operands share an equality edge in the join graph.

    Runs before CrossJoinFilterPushdownStrategy so the pushdown can convert
    every cross join in the reordered chain to an inner join.
    """

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        if not context.optimized_plan:
            context.optimized_plan = context.pre_optimized_tree.copy()
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # A chain top is an unconverted cross join whose parent is NOT itself
        # an unconverted cross join — i.e. it is the highest cross join in
        # its left-deep chain.
        chain_tops: List[str] = []
        for nid, node in plan.nodes(True):
            if not _is_unconverted_cross_join(node):
                continue
            parents = list(plan.outgoing_edges(nid))
            if any(_is_unconverted_cross_join(plan[p[1]]) for p in parents):
                continue
            chain_tops.append(nid)

        rewired_top_ids: Set[str] = set()

        for top_id in chain_tops:
            if top_id in rewired_top_ids:
                continue

            chain = _collect_chain_top_down(plan, top_id)
            if len(chain) < 1:
                continue

            leaves = _gather_leaves(plan, chain)
            if leaves is None or len(leaves) < 2:
                continue

            # Predicates that constrain this chain live in any Filter nodes
            # above the chain top (potentially split across multiple Filter
            # nodes by SplitConjunctivePredicates). Collect them all before
            # building the join graph — picking from a single filter would
            # underspecify connectivity.
            predicates = _collect_predicates_above(plan, top_id)
            if not predicates:
                continue

            adj = _build_join_graph(predicates, leaves)
            if _is_already_well_ordered(leaves, adj):
                continue

            new_order = _choose_order(adj, len(leaves))
            if new_order == list(range(len(leaves))):
                continue

            _rewire_chain(plan, chain, leaves, new_order)
            rewired_top_ids.add(top_id)

        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        for node in plan._nodes.values():
            if _is_unconverted_cross_join(node):
                return True
        return False
