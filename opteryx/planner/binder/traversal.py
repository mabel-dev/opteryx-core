# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.models import Node
from opteryx.planner.binder.binder import merge_schemas
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.logical_planner import LogicalPlan


def post_bind(self, node):
    # The binder skips calculated fields when it performs binding because
    # sometimes it doesn't have access to all of the fields used in the
    # calculation - so we bind these now
    seen: dict = {}

    def _inner(branch):
        if branch.fully_bound is False:
            if branch.schema_column.identity in seen:
                branch = seen[branch.schema_column.identity]
        elif branch.schema_column:
            seen[branch.schema_column.identity] = branch.copy()
        for attr in ("left", "right", "centre"):
            if hasattr(branch, attr) and getattr(branch, attr) is not None:
                setattr(branch, attr, _inner(getattr(branch, attr)))
        if branch.parameters:
            branch.parameters = [_inner(p) for p in branch.parameters]
        return branch

    if node.condition:
        node.condition = _inner(node.condition)
    if node.columns:
        # if it doesn't have a schema column here - we can remove it
        node.columns = [_inner(c) for c in node.columns if c.schema_column is not None]
    return node


def traverse(
    self, graph: LogicalPlan, node: Node, context: BindingContext
) -> Tuple[LogicalPlan, BindingContext]:
    """
    Traverses the given graph starting at the given node and calling the
    appropriate visit methods for each node in the graph. This method uses
    a post-order traversal, which visits the children of a node before
    visiting the node itself.

    Args:
        graph: The graph to traverse.
        node: The node to start the traversal from.
        context: An optional context object to pass to each visit method.
    Returns:
        A tuple containing the updated graph and the context.
    """
    # Expose the graph on the visitor so handlers (e.g. visit_insert) can
    # reach already-bound upstream nodes by id when needed.
    self.graph = graph

    # Recursively visit children
    children = graph.ingoing_edges(node)

    if children:
        exit_context = context.copy()
        for child in children:
            # Each peer gets the exact copy of the context so they don't affect each other
            _, child_context = traverse(self, graph, child[0], context.copy())
            # merges the schemas from two contexts
            exit_context.schemas = merge_schemas(child_context.schemas, exit_context.schemas)

            # Update relations if necessary
            merged_relations = {
                **context.relations,
                **exit_context.relations,
                **child_context.relations,
            }
            context.relations = merged_relations

        context.schemas = merge_schemas(context.schemas, exit_context.schemas)

    # Visit node and return updated context
    return_node, context = self.visit_node(graph[node], context=context)

    # We keep track of the relations which are 'visible' along each branch
    if return_node.all_relations is None:
        return_node.all_relations = set()  # Initialize as an empty set if None

    return_node.all_relations.update(
        {value for value in [return_node.relation, return_node.alias] if value is not None}
    )

    children = graph.ingoing_edges(node)
    for plan_node_id, _, _ in children:
        plan_node = graph[plan_node_id]
        if plan_node.all_relations:
            return_node.all_relations.update(plan_node.all_relations)

    return_node = post_bind(self, return_node)
    graph[node] = return_node
    return graph, context
