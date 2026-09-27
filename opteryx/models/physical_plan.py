# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The Physical Plan is a tree of nodes that represent the execution plan for a query.
"""

from typing import Optional

from opteryx.compiled.planner.plan_graph import PlanGraph


class PhysicalPlan(PlanGraph):
    """
    The execution tree is defined separately to the planner to simplify the
    complex code which is the planner from the tree that describes the plan.

    It carries the query's PlanContext (the PlanGraph base binds it, copies
    included): the compiler lowers it at execution time and mints the columns it
    needs (join-key coercions, zone-map terms) in the same column table planning
    used (architect, 2026-09-26).
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

        # A two-input node's legs by label, left then right (plan.legs refuses
        # unlabelled legs); a single input is just that input.
        ingoing = self.ingoing_edges(node)
        neighbors = list(self.legs(node)) if len(ingoing) > 1 else [source for source, _t, _r in ingoing]

        # left semi and anti joins we hash the right side first, usually we want the left side first
        if self[node].is_join and self[node].join_type in (
            "left anti",
            "left semi",
            "left anti null-aware",
            "left semi not-distinct",
            "left anti not-distinct",
        ):
            neighbors.reverse()

        # Traverse each child, left before right
        for neighbor in neighbors:
            if neighbor not in visited:
                child_list = self.depth_first_search_flat(neighbor, visited)
                traversal_list.extend(child_list)

        return traversal_list

    def sensors(self):
        readings = {}
        for nid in self.nodes():
            node = self[nid]
            readings[node.identity] = node.sensors()
        return readings
