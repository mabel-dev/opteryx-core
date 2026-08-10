# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The Physical Plan is a tree of nodes that represent the execution plan for a query.
"""

from typing import Optional

from opteryx.exceptions import InvalidInternalStateError
from opteryx.third_party.travers import Graph

# Traversal rank for an edge's relationship. Unlabelled edges (relationship is
# None -- the `Graph.add_edge` default) rank last, so a join's left leg is always
# traversed before its right leg.
_LEG_ORDER = {"left": 0, "right": 1}
_UNLABELLED_ORDER = 2


class PhysicalPlan(Graph):
    """
    The execution tree is defined separately to the planner to simplify the
    complex code which is the planner from the tree that describes the plan.
    """

    def depth_first_search_flat(
        self, node: Optional[str] = None, visited: Optional[set] = None
    ) -> list:
        """
        Returns a flat list representing the depth-first traversal of the graph with left/right ordering.

        We do this so we always evaluate the left side of a join before the right side. It technically
        doesn't need the entire plan flattened DFS-wise, but this is what we are doing here to achieve
        the outcome we're after.
        """
        if node is None:
            node = self.get_exit_points()[0]

        if visited is None:
            visited = set()

        visited.add(node)

        # Collect this node's information in a flat list format
        traversal_list = [
            (
                node,
                self[node],
            )
        ]

        # Sort neighbors based on relationship to ensure left, right, then unlabelled order.
        # The sort is stable, so unlabelled edges retain their insertion order.
        neighbors = sorted(
            self.ingoing_edges(node), key=lambda x: _LEG_ORDER.get(x[2], _UNLABELLED_ORDER)
        )

        # left semi and anti joins we hash the right side first, usually we want the left side first
        if self[node].is_join and self[node].join_type in (
            "left anti",
            "left semi",
            "left anti null-aware",
            "left semi not-distinct",
            "left anti not-distinct",
        ):
            neighbors.reverse()

        # Traverse each child, prioritizing left, then right, then unlabelled
        for neighbor, _, _ in neighbors:
            if neighbor not in visited:
                child_list = self.depth_first_search_flat(neighbor, visited)
                traversal_list.extend(child_list)

        return traversal_list

    def label_join_legs(self):
        """Ensure every join's ingoing edges carry a left/right label.

        A label already on an edge is authoritative. The logical planner sets it
        when it builds the join, ``JoinOrderingStrategy`` flips it when it swaps
        the build side, and ``remove_node(heal=True)`` carries it across removed
        nodes. Labels are only *inferred* for edges an optimizer rewrite left
        unlabelled — cross-join filter pushdown, cross-join chain reorder and
        the set-op / IN-subquery rewrites rewire join inputs without naming the
        sides.

        Inference must never overrule an existing label: doing so discards the
        swap decision and silently rebuilds the hash table on the larger leg.
        """
        joins = ((nid, node) for nid, node in self.nodes(True) if node.is_join)
        for nid, join in joins:
            ingoing = list(self.ingoing_edges(nid))
            assignments: list = [
                relation if relation in ("left", "right") else None
                for _source, _target, relation in ingoing
            ]

            if any(side is None for side in assignments):
                if join.left_readers is None:
                    # No reader UUIDs. Joins synthesised from INTERSECT/EXCEPT/IN-
                    # subquery rewrites still carry left/right relation names — resolve
                    # each leg by the scan aliases reachable from it. UnnestJoinNode has
                    # neither readers nor relation names, and only one leg → leave it.
                    if not (join.left_relation_names and join.right_relation_names):
                        continue
                    self._assign_legs_by_relation(nid, join, ingoing, assignments)
                else:
                    self._assign_legs_by_reader(nid, join, ingoing, assignments)

                self._assign_legs_by_complement(assignments)

                for (provider, _target, _relation), side in zip(ingoing, assignments):
                    self.add_edge(provider, nid, side)

            if len(ingoing) > 1:
                sides = sorted(side for side in assignments if side is not None)
                if sides != ["left", "right"]:
                    raise InvalidInternalStateError(
                        f"Join legs are ambiguous: expected one LEFT and one RIGHT, got {sides or 'none'}."
                    )

    def _assign_legs_by_reader(self, nid, join, ingoing, assignments):
        """Resolve unlabelled legs by the scan reader UUIDs each branch reaches."""
        for idx, (provider, provider_target, provider_relation) in enumerate(ingoing):
            if assignments[idx] is not None:
                continue
            reader_edges = set(self.breadth_first_search(provider, reverse=True))
            reader_edges.add((provider, provider_target, provider_relation))

            for source, _target, _relation in reader_edges:
                uuid = self[source].uuid
                if uuid is None:
                    continue
                if uuid in join.left_readers:
                    assignments[idx] = "left"
                    break
                if uuid in join.right_readers:
                    assignments[idx] = "right"
                    break

    def _assign_legs_by_relation(self, nid, join, ingoing, assignments):
        """Resolve unlabelled legs by the scan aliases each branch reaches.

        For joins that have no reader UUIDs (set-operation / IN-subquery
        rewrites) the leg of each input is determined by the relations the input
        branch reaches. Each branch is expected to reach exactly one side's
        relations; branches that hit both or neither stay unresolved.
        """
        left_rel = set(join.left_relation_names)
        right_rel = set(join.right_relation_names)
        for idx, (provider, _target, _relation) in enumerate(ingoing):
            if assignments[idx] is not None:
                continue
            # `alias` is declared by individual operators, not by BasePlanNode,
            # so it is genuinely absent on most nodes rather than None.
            aliases = {getattr(self[provider], "alias", None)}
            for source, _t, _r in self.breadth_first_search(provider, reverse=True):
                aliases.add(getattr(self[source], "alias", None))
            aliases.discard(None)

            hits_left = bool(aliases & left_rel)
            hits_right = bool(aliases & right_rel)
            if hits_right and not hits_left:
                assignments[idx] = "right"
            elif hits_left and not hits_right:
                assignments[idx] = "left"

    @staticmethod
    def _assign_legs_by_complement(assignments):
        """Fill still-unresolved legs with the side no other edge has claimed.

        Only one side claimed → the remaining edge is the other side. With both
        or neither claimed there is nothing to deduce, so we fall back to
        ingoing-edge order — the last resort, and the only step here that can
        be wrong.
        """
        claimed = {side for side in assignments if side is not None}
        for idx, side in enumerate(assignments):
            if side is not None:
                continue
            if claimed == {"left"}:
                assignments[idx] = "right"
            elif claimed == {"right"}:
                assignments[idx] = "left"
            else:
                assignments[idx] = "left" if idx == 0 else "right"

    def sensors(self):
        readings = {}
        for nid in self.nodes():
            node = self[nid]
            readings[node.identity] = node.sensors()
        return readings

    def __del__(self):
        pass
