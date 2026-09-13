# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Strategy: Cost-based join planning (DPccp).

For each chain of unconverted cross joins:
  1. Identify the chain (reusing helpers from `cross_join_chain_reorder`).
  2. Build a JoinGraph from the leaves and the predicates above the chain.
  3. Run `enumerate_join_tree` (DPccp / greedy fallback) to pick a join tree.
  4. Rewrite the chain's internal nodes to match the chosen tree, which may
     be bushy (not just left-deep).

The rewritten internal nodes stay as ``cross join`` shells; the immediately
following ``CrossJoinFilterPushdownStrategy`` converts each one to an inner
join with the appropriate equi-key predicate from the upper Filter. Predicates
themselves are not moved by this strategy.

Outer joins are not rewritten: ``_collect_chain_top_down`` only walks
unconverted cross joins, so outer joins naturally bound the chain. No
explicit guard is needed.

Gated behind the ``enable_dpccp_join_planning`` feature flag — when off,
the strategy is a no-op.
"""

from typing import List
from typing import Optional
from typing import Tuple

from opteryx.config import features
from opteryx.planner.cost_estimation import JoinTree
from opteryx.planner.cost_estimation import JoinTreeLeaf
from opteryx.planner.cost_estimation import JoinTreeNode
from opteryx.planner.cost_estimation import enumerate_join_tree
from opteryx.planner.cost_estimation.plan_adapter import build_join_graph
from opteryx.planner.logical_planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner.logical_planner import LogicalPlanNode

from .cross_join_chain_reorder import _Leaf
from .cross_join_chain_reorder import _collect_chain_top_down
from .cross_join_chain_reorder import _collect_predicates_above
from .cross_join_chain_reorder import _gather_leaves
from .cross_join_chain_reorder import _is_unconverted_cross_join
from .optimization_strategy import OptimizationStrategy
from .optimization_strategy import OptimizerContext


def _tree_is_left_deep_in_leaf_order(
    tree: JoinTree, expected_leaf_ids: List[int]
) -> bool:
    """True iff ``tree`` is the left-deep chain matching ``expected_leaf_ids``.

    Used to skip rewriting when DPccp's chosen plan is structurally the same
    as the existing chain (a no-op rewrite still produces a different plan
    object; the cheapest equivalence is a tandem walk).
    """
    if len(expected_leaf_ids) == 1:
        return isinstance(tree, JoinTreeLeaf) and tree.vertex_id == expected_leaf_ids[0]
    if not isinstance(tree, JoinTreeNode):
        return False
    if not isinstance(tree.right, JoinTreeLeaf):
        return False
    if tree.right.vertex_id != expected_leaf_ids[-1]:
        return False
    return _tree_is_left_deep_in_leaf_order(tree.left, expected_leaf_ids[:-1])


def _apply_join_tree(
    plan: LogicalPlan,
    chain: List[Tuple[str, LogicalPlanNode]],
    leaves: List[_Leaf],
    tree: JoinTree,
) -> None:
    """Rewrite the chain to match ``tree``.

    Internal-node ids are reused from the existing chain so downstream
    strategies that key off node ids during the same pass keep working.
    The chain top's id is preserved as the new tree's root, so the parent
    connection above the chain is unchanged.

    Leaf subplans are opaque — their internal edges and nodes are not
    touched. Only the leaf-feeding edges of the chain's internal nodes are
    rewired.
    """
    chain_ids = [jid for jid, _ in chain]
    chain_id_set = set(chain_ids)

    # Master schema map sourced from the chain top — every join in the chain
    # was bound with the full set of relation schemas underneath, so binder
    # / projection follow-ups expect the same shape on every internal node.
    top_node = chain[0][1]
    master_schemas = dict(getattr(top_node, "schemas", None) or {})

    def _schemas_for(rel_names: List[str]) -> dict:
        out = {
            k: v
            for k, v in master_schemas.items()
            if not k or k.startswith("$") or k.startswith("$derived")
        }
        for name in rel_names:
            if name in master_schemas:
                out[name] = master_schemas[name]
        return out

    # Step 1: drop every incoming edge to every chain join — including
    # chain-internal edges. We rebuild them with proper "left"/"right"
    # labels in step 2; leaving stale labels in place would survive
    # ``add_edge`` (which updates by source+target) and confuse the
    # physical-plan builder.
    for jid in chain_ids:
        for child_id, target, relationship in list(plan.ingoing_edges(jid)):
            plan.remove_edge(child_id, target, relationship)

    # Step 2: materialize the new tree pre-order, consuming chain ids from
    # the top down. ``chain_ids`` is top-down, so chain_ids[0] is the tree
    # root, preserving the parent edge above the chain.
    id_pool = list(chain_ids)

    def _materialize(subtree: JoinTree) -> Tuple[str, List[str], List[str]]:
        """Return ``(subplan_id, accumulated_rel_names, accumulated_readers)``."""
        if isinstance(subtree, JoinTreeLeaf):
            leaf = leaves[subtree.vertex_id]
            return leaf.subplan_id, list(leaf.rel_names), list(leaf.readers)

        # Pop next id (pre-order: root before children).
        join_id = id_pool.pop(0)
        join_node = plan[join_id]

        left_id, left_rels, left_readers = _materialize(subtree.left)
        right_id, right_rels, right_readers = _materialize(subtree.right)

        join_node.type = "cross join"
        join_node.on = None
        join_node.using = None
        join_node.condition = None
        join_node.left_columns = None
        join_node.right_columns = None
        join_node.columns = None
        join_node.left_relation_names = list(left_rels)
        join_node.right_relation_names = list(right_rels)
        join_node.left_readers = list(left_readers)
        join_node.right_readers = list(right_readers)
        join_node.relation_names = [
            join_node.left_relation_names,
            join_node.right_relation_names,
        ]
        accumulated = list(left_rels) + list(right_rels)
        join_node.schemas = _schemas_for(accumulated)
        plan[join_id] = join_node

        plan.add_edge(left_id, join_id, "left")
        plan.add_edge(right_id, join_id, "right")

        return join_id, accumulated, list(left_readers) + list(right_readers)

    _materialize(tree)


class JoinPlanningStrategy(OptimizationStrategy):
    optimization_technique = "cost"
    provides = ("joins-planned",)

    """Cost-based join enumeration via DPccp.

    Sits before ``CrossJoinFilterPushdownStrategy`` so the pushdown can
    convert each cross-join shell into an inner join with the appropriate
    equi-key predicate.
    """

    def visit(self, node: LogicalPlanNode, context: OptimizerContext) -> OptimizerContext:
        return context

    def complete(self, plan: LogicalPlan, context: OptimizerContext) -> LogicalPlan:
        # Outer-join refusal is implicit: _collect_chain_top_down only walks
        # unconverted cross joins, so any outer join naturally bounds the
        # chain. No explicit guard needed.
        chain_tops: List[str] = []
        for nid, node in plan.nodes(True):
            if not _is_unconverted_cross_join(node):
                continue
            parents = list(plan.outgoing_edges(nid))
            if any(_is_unconverted_cross_join(plan[p[1]]) for p in parents):
                continue
            chain_tops.append(nid)

        for top_id in chain_tops:
            # EVERY decline below is recorded. The six ways this strategy can walk
            # away point at six different pieces of work, and a plan that got no
            # join ordering at all is indistinguishable in optimizer_trace from one
            # that was considered and kept ({'changed': False} either way). TPC-H
            # Q21 at SF100 ran with NO join ordering for exactly that reason, and
            # finding out required monkeypatching this method.
            label = f"cost-based join planning ({top_id})"

            chain = _collect_chain_top_down(plan, top_id)
            if len(chain) < 1:
                self.record_decision(
                    label,
                    "declined, no cross-join chain collected below the chain top",
                )
                continue

            leaves = _gather_leaves(plan, chain)
            # Two distinct declines: an UNRECOGNISED chain shape is a gap in
            # _gather_leaves, a chain with one leaf is nothing to order.
            if leaves is None:
                self.record_decision(
                    label,
                    f"declined, chain shape not recognised by _gather_leaves:"
                    f" {len(chain)} join(s) in chain",
                )
                continue
            if len(leaves) < 2:
                self.record_decision(
                    label,
                    f"declined, fewer than 2 leaves to order: {len(leaves)} leaf"
                    f" ({len(chain)} join(s) in chain)",
                )
                continue

            predicates = _collect_predicates_above(plan, top_id)
            if not predicates:
                # NOT benign. _collect_predicates_above stops at nodes it does not
                # walk through, so anything it declines to traverse hides every join
                # predicate from this strategy and DPccp never runs. It DOES now walk
                # through Join parents (the decorrelated EXISTS/NOT EXISTS case that
                # cost Q21 a 1.9x); a decline here means a Project/Aggregate/Subquery
                # boundary, or a genuine cartesian. Named separately so the next such
                # blind spot is visible in telemetry instead of looking like
                # "nothing to do".
                self.record_decision(
                    label,
                    f"declined, no predicates above the chain: {len(leaves)} leaves,"
                    " DPccp did not run",
                )
                continue

            graph = build_join_graph(plan, leaves, predicates)
            if graph is None:
                # build_join_graph refuses for three reasons it does not currently
                # distinguish: no cross-leaf equi predicate, a leaf with no row
                # count, or a disconnected graph. The counts below narrow it.
                self.record_decision(
                    label,
                    f"declined, no usable join graph: {len(leaves)} leaves,"
                    f" {len(predicates)} predicate(s) above the chain"
                    " (no equi edge, missing row statistics, or disconnected)",
                )
                continue

            # No guard around the enumerator: build_join_graph has ALREADY enforced
            # every precondition it can refuse on (>= 1 vertex, connected), so a
            # raise here is an invariant violation, not a shape this strategy may
            # decline. It must fail loudly rather than silently cost nothing.
            tree = enumerate_join_tree(graph)

            # Skip when DPccp picks the same left-deep chain we already have.
            existing_order = [leaf.original_index for leaf in leaves]
            if _tree_is_left_deep_in_leaf_order(tree, existing_order):
                self.record_decision(
                    label,
                    f"kept, enumerator chose the existing left-deep order:"
                    f" {graph.n} vertices, {len(graph.edges)} edge(s)",
                )
                continue

            _apply_join_tree(plan, chain, leaves, tree)
            self.record_decision(
                label,
                f"reordered, enumerator chose a different tree:"
                f" {graph.n} vertices, {len(graph.edges)} edge(s)",
            )

        return plan

    def should_i_run(self, plan: LogicalPlan) -> bool:
        if not features.enable_dpccp_join_planning:
            return False
        for node in plan._nodes.values():
            if _is_unconverted_cross_join(node):
                return True
        return False
