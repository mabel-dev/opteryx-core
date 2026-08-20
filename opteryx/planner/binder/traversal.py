# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.exceptions import AmbiguousDatasetError
from opteryx.models import Node
from opteryx.planner.binder.binder import merge_schemas
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType


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
        if branch.left is not None:
            branch.left = _inner(branch.left)
        if branch.right is not None:
            branch.right = _inner(branch.right)
        if branch.centre is not None:
            branch.centre = _inner(branch.centre)
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

    # A Subquery node is a NAMING-SCOPE BOUNDARY: a derived table (and a spliced CTE
    # reference, which is the same node) owns a private relation namespace. Its
    # internal aliases are not addressable from outside, and - the point of clearing
    # them here rather than on the way out - an enclosing alias is not addressable
    # from inside either, so `FROM t p, (SELECT p.id FROM t p) q` shadows rather than
    # collides. This mirrors `BindingContext.open_correlated_scope`, which opens the
    # equivalent scope for a nested subquery; the difference is only that a derived
    # table resolves no names outwards, so there is nothing to carry across.
    #
    # `schemas` needs no equivalent: peers are already isolated from each other (each
    # gets a copy below, and their schemas are accumulated into `exit_context`, not
    # into the shared `context`). `relations` alone is shared across peers, and
    # deliberately - see the merge in the loop.
    enclosing_relations = None
    if graph[node].node_type == LogicalPlanStepType.Subquery:
        enclosing_relations = context.relations
        context.relations = {}

    if len(children) == 1:
        # Linear plan (no branching): pass context directly — no copies needed.
        # The child updates context.schemas and context.relations in-place, which
        # is exactly what the parent needs. Avoids expensive deepcopy of schema.
        graph, context = traverse(self, graph, children[0][0], context)
    elif children:
        exit_context = context.copy()
        for child in children:
            # Each peer is bound against its own copy, so a peer's SCHEMAS never reach
            # another peer - they are accumulated into `exit_context` and only merged
            # back into `context` once every peer has been bound.
            #
            # `relations` is the exception, and is cumulative ON PURPOSE: the peers of
            # one branching node are the relations of ONE `FROM` scope, and the only
            # thing that makes `FROM t, t` ambiguous is the second scan finding the
            # first scan's name already registered (see visit_scan). Peers are opaque
            # to each other for name RESOLUTION and transparent for name COLLISION.
            _, child_context = traverse(self, graph, child[0], context.copy())
            # merges the schemas from two contexts. The accumulator goes FIRST so the
            # merged dict keeps children in edge order (left leg, then right leg) —
            # `SELECT *` expands schemas in dict order, and SQL requires the left
            # relation's columns before the right's.
            exit_context.schemas = merge_schemas(exit_context.schemas, child_context.schemas)

            merged_relations = {
                **context.relations,
                **exit_context.relations,
                **child_context.relations,
            }
            context.relations = merged_relations

        context.schemas = merge_schemas(context.schemas, exit_context.schemas)

    # Visit node and return updated context
    return_node, context = self.visit_node(graph[node], context=context)

    if enclosing_relations is not None:
        # Closing the scope opened above. `visit_subquery` has replaced `relations`
        # with the single name this boundary exports, so the enclosing scope's names
        # come back here and the private ones do not.
        # Case-folded to match visit_scan's collision check (dataset.py) and
        # locate_identifier's resolution (binder.py `_candidates`) - an alias is an
        # unquoted SQL identifier, so `FROM (SELECT ...) Y, (SELECT ...) y` collides
        # exactly as matching-case would.
        if return_node.alias and return_node.alias.lower() in {
            r.lower() for r in enclosing_relations
        }:
            # The exported name collides with a relation already in the enclosing
            # scope - `FROM (SELECT ...) y, (SELECT ...) y`, or the two references of
            # `WITH c AS (...) SELECT * FROM c, c`. Genuinely ambiguous, and the same
            # rule visit_scan applies one level down.
            raise AmbiguousDatasetError(dataset=return_node.alias)
        context.relations = {**enclosing_relations, **context.relations}

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
