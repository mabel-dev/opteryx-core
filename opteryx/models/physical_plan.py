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

        # Sort neighbors based on relationship to ensure left, right, then unlabelled order
        neighbors = sorted(self.ingoing_edges(node), key=lambda x: (x[2] == "right", x[2] == ""))

        # left semi and anti joins we hash the right side first, usually we want the left side first
        if self[node].is_join and self[node].join_type in ("left anti", "left semi", "left anti null-aware"):
            neighbors.reverse()

        # Traverse each child, prioritizing left, then right, then unlabelled
        for neighbor, _, _ in neighbors:
            if neighbor not in visited:
                child_list = self.depth_first_search_flat(neighbor, visited)
                traversal_list.extend(child_list)

        return traversal_list

    def label_join_legs(self):
        # add the left/right labels to the edges coming into the joins
        joins = ((nid, node) for nid, node in self.nodes(True) if node.is_join)
        for nid, join in joins:
            if join.left_readers is None:
                # No reader UUIDs. Joins synthesised from INTERSECT/EXCEPT/IN-
                # subquery rewrites still carry left/right relation names — label
                # each leg by the scan aliases reachable from it. This is robust
                # to ingoing-edge ordering, which a redundant-operator removal
                # (remove_node heal) can flip and which would otherwise silently
                # swap the build/probe sides of a non-commutative anti/semi join.
                # UnnestJoinNode has neither readers nor relation names → skip.
                if join.left_relation_names and join.right_relation_names:
                    self._label_join_legs_by_relation(nid, join)
                continue

            # Iterate through incoming edges and label them based on join sides
            # If left_readers contains ANY Scan node UUID that the provider reaches,
            # label that provider edge as "left"
            ingoing = list(self.ingoing_edges(nid))
            for idx, (provider, provider_target, provider_relation) in enumerate(ingoing):
                # Use UUID matching to honour join_ordering swaps.
                # Position (idx) is used only as a fallback when readers are not populated.
                reader_edges = {
                    (source, target, relation)
                    for source, target, relation in self.breadth_first_search(
                        provider, reverse=True
                    )
                }
                if getattr(self[provider], "uuid", None) is not None:
                    reader_edges.add((provider, provider_target, provider_relation))

                labelled = False
                for s, t, r in reader_edges:
                    node = self[s]
                    if getattr(node, "uuid", None) is None:
                        continue
                    if node.uuid in join.left_readers:
                        self.add_edge(provider, nid, "left")
                        labelled = True
                        break
                    elif node.uuid in join.right_readers:
                        self.add_edge(provider, nid, "right")
                        labelled = True
                        break

                if not labelled:
                    # Fallback: no UUID match — use insertion order
                    self.add_edge(provider, nid, "left" if idx == 0 else "right")

            tester = self.breadth_first_search(nid, reverse=True)
            if not any(r == "left" for s, t, r in tester):
                raise InvalidInternalStateError("Unable to determine LEFT side of join.")
            if not any(r == "right" for s, t, r in tester):
                raise InvalidInternalStateError("Join has no RIGHT leg")

    def _label_join_legs_by_relation(self, nid, join):
        """Label a join's input edges using its left/right relation names.

        For joins that have no reader UUIDs (set-operation / IN-subquery
        rewrites) the leg of each input is determined by the scan aliases the
        input branch reaches, not by ingoing-edge insertion order. Each branch
        is expected to reach exactly one side's relations; when it cannot be
        resolved unambiguously we fall back to insertion order.
        """
        left_rel = set(join.left_relation_names)
        right_rel = set(join.right_relation_names)
        for idx, (provider, _target, _relation) in enumerate(self.ingoing_edges(nid)):
            aliases = set()
            alias = getattr(self[provider], "alias", None)
            if alias is not None:
                aliases.add(alias)
            for source, _t, _r in self.breadth_first_search(provider, reverse=True):
                alias = getattr(self[source], "alias", None)
                if alias is not None:
                    aliases.add(alias)

            hits_left = bool(aliases & left_rel)
            hits_right = bool(aliases & right_rel)
            if hits_right and not hits_left:
                self.add_edge(provider, nid, "right")
            elif hits_left and not hits_right:
                self.add_edge(provider, nid, "left")
            else:
                # Ambiguous or undetermined — preserve historical positional rule.
                self.add_edge(provider, nid, "left" if idx == 0 else "right")

        tester = self.breadth_first_search(nid, reverse=True)
        if not any(r == "left" for s, t, r in tester):
            raise InvalidInternalStateError("Unable to determine LEFT side of join.")
        if not any(r == "right" for s, t, r in tester):
            raise InvalidInternalStateError("Join has no RIGHT leg")

    def sensors(self):
        readings = {}
        for nid in self.nodes():
            node = self[nid]
            readings[node.identity] = node.sensors()
        return readings

    def __del__(self):
        pass
