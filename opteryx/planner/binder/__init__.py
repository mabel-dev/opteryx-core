# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The Binder sits between the Plan Rewriter and the Optimizer. It is responsible for
resolving all names, types, and references in the logical plan against the live
catalogues and execution context.

Input:  unbound LogicalPlan — nodes carry raw identifiers and AST fragments
Output: bound LogicalPlan — nodes carry resolved column identities, types, and schemas

The Binder performs three passes:

1. Relation expansion (bind_logical_relations) — Scan nodes that reference a VIEW or
   CTE are replaced with the corresponding sub-plan. Relation names are randomised to
   avoid conflicts when the same view or CTE is referenced more than once.

2. Visibility filter injection — row-level security predicates are inserted as Filter
   nodes immediately above the relevant Scan nodes.

3. Node binding (BinderVisitor) — a bottom-up traversal resolves every column reference
   against the relation schemas accumulated from the scans upward, validates types,
   checks function signatures, and attaches schema_column metadata to each identifier
   node. The bound plan carries enough information for the Optimizer and Physical
   Planner to operate without further catalogue access.

The Binder does NOT restructure the plan or make cost-based decisions; that is the
Optimizer's responsibility.
"""

from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import Node
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.binder.common import BinderVisitor
from opteryx.planner.logical_planner import (
    LogicalPlan,
    LogicalPlanStepType,
    apply_visibility_filters,
)


def _copy_cte_plan(plan: LogicalPlan) -> LogicalPlan:
    """
    Copy a CTE sub-plan with fresh node IDs so repeated expansions of the
    same CTE (e.g. once directly and once inside a chained CTE) don't share
    node IDs and cause dict-update collisions when merged into the main plan.
    """
    from opteryx.utils import random_string

    id_map = {old_id: random_string() for old_id in plan.nodes(data=False)}

    base = plan.copy()
    new_plan = plan.__class__.__new__(plan.__class__)
    new_plan._nodes = {id_map[old_id]: node for old_id, node in base._nodes.items()}

    new_plan._edges = {}
    for src, targets in base._edges.items():
        new_src = id_map.get(src, src)
        new_plan._edges[new_src] = tuple(
            (id_map.get(tgt, tgt), rel) for tgt, rel in targets
        )

    new_plan._cached_edges = None
    return new_plan


def rename_relations(plan: LogicalPlan, prefix: str = "$view-"):
    """
    When we include VIEWs and CTEs in a plan, we randomize the name of the
    relations to avoid conflicts.
    """
    from opteryx.models import LogicalColumn
    from opteryx.utils import random_string

    relations = {}
    uuid_remap = {}  # old_uuid -> new_uuid for updating join readers

    # first we collection the relations
    for nid, node in [
        (nid, node)
        for (nid, node) in plan.nodes(True)
        if node.node_type == LogicalPlanStepType.Scan
    ]:
        alias = f"{prefix}{random_string(4)}"
        unique_id = random_string(32)
        relations[node.alias] = (node.relation, alias, unique_id)
        uuid_remap[node.uuid] = unique_id
        node.alias = alias
        node.uuid = unique_id
        plan[nid] = node

    def _prop(property):
        if isinstance(property, LogicalColumn) and property.source in relations:
            property.source = relations[property.source][1]
        if isinstance(property, list):
            return [_prop(p) for p in property]
        if isinstance(property, dict):
            return {k: _prop(v) for k, v in property.items()}
        if isinstance(property, Node):
            for p in property.properties:
                property.properties[p] = _prop(property.properties[p])
        return property

    for nid, node in plan.nodes(True):
        for property in node.properties:
            node.properties[property] = _prop(node.properties[property])

    # Remap left/right relation name lists and reader UUID lists on join nodes.
    # _prop only handles LogicalColumn.source; plain string lists need explicit remapping.
    # Set-operation nodes (Union/Intersect/Except) also carry left/right relation
    # name lists referencing scan aliases — a nested set operation's lists must be
    # remapped when its scans are renamed, or the outer set op fails to resolve its
    # side's columns at bind time.
    for nid, node in plan.nodes(True):
        if node.node_type in (
            LogicalPlanStepType.Join,
            LogicalPlanStepType.Union,
            LogicalPlanStepType.Intersect,
            LogicalPlanStepType.Except,
        ):
            if node.left_relation_names:
                node.left_relation_names = [
                    relations[n][1] if n in relations else n
                    for n in node.left_relation_names
                ]
            if node.right_relation_names:
                node.right_relation_names = [
                    relations[n][1] if n in relations else n
                    for n in node.right_relation_names
                ]
            if node.left_readers:
                node.left_readers = [
                    uuid_remap.get(u, u) for u in node.left_readers
                ]
            if node.right_readers:
                node.right_readers = [
                    uuid_remap.get(u, u) for u in node.right_readers
                ]
            plan[nid] = node

    return plan


def join_leg_preprocess(plan: LogicalPlan):
    for nid, node in (
        (nid, node)
        for (nid, node) in plan.nodes(True)
        if node.node_type == LogicalPlanStepType.Scan
    ):
        uuid = node.uuid

        location_nid = nid
        location_node = plan[location_nid]
        leg = None
        while location_nid:
            if location_node.node_type == LogicalPlanStepType.Join:
                if leg == "left":
                    location_node.left_readers.append(uuid)
                    location_node.left_relation_names.append(node.alias)
                elif leg == "right":
                    location_node.right_readers.append(uuid)
                    location_node.right_relation_names.append(node.alias)
                plan[location_nid] = location_node
            incoming = plan.outgoing_edges(location_nid)
            if incoming:
                location_nid = incoming[0][1]
                location_node = plan[location_nid]
                leg = incoming[0][2]
            else:
                location_nid = None

    return plan


def bind_logical_relations(plan: LogicalPlan, ctes: dict, telemetry) -> LogicalPlan:
    """
    Bind the logical relations in the logical plan.

    Parameters:
        plan: LogicalPlan
            The logical plan.
        context: BindingContext
            The context needed for the binding phase.

    Returns:
        LogicalPlan: The logical plan with the logical relations bound.
    """
    from opteryx.expression import NodeType
    from opteryx.models import Node
    from opteryx.planner.logical_planner import LogicalPlanStepType
    from opteryx.managers.views import resolve_relation

    if ctes is None:
        ctes = {}

    # Iterative expansion: after merging a CTE sub-plan, newly added Scan nodes
    # inside it may themselves reference other CTEs (chained CTEs). Re-scan until
    # no resolvable Scan nodes remain. Use .copy() to avoid mutating the shared
    # CTE plan objects when rename_relations modifies node aliases in-place.
    while True:
        expanded = False
        for nid, node in list(plan.nodes(True)):
            if node.node_type != LogicalPlanStepType.Scan:
                continue
            relation = node.relation
            if relation in ctes:
                sub_plan = _copy_cte_plan(ctes[relation])
            elif getattr(node, "resolved_dataset", None) is not None:
                # Already resolved to a table on a previous pass; leave as Scan.
                sub_plan = None
            else:
                # Catalog resolution step: one round trip resolves view-or-table.
                # This is the Firestore catalog lookup — a per-relation cloud round
                # trip, distinct from the GCS manifest/footer fetch timed in
                # dataset.py (time_binding_metadata). Timed so the two cloud costs
                # in the binder are visible separately (time_ prefix → seconds).
                import time as _cat_time

                _cat0 = _cat_time.monotonic_ns()
                kind, resolved = resolve_relation(relation, telemetry)
                if telemetry is not None:
                    telemetry.time_binding_catalog += _cat_time.monotonic_ns() - _cat0
                if kind == "view":
                    sub_plan = resolved
                else:
                    if kind == "dataset":
                        # Stash on the node so the dataset-bind pass reuses it
                        # instead of re-reading the catalog.
                        node.resolved_dataset = resolved
                    sub_plan = None
            if sub_plan:
                sub_plan = rename_relations(sub_plan)
                sub_plan_head = sub_plan.get_exit_points()[0]
                consumer = plan.outgoing_edges(nid)[0]
                node.node_type = LogicalPlanStepType.Subquery
                node.columns = sub_plan[sub_plan_head].columns or [Node(NodeType.WILDCARD)]
                plan += sub_plan
                plan.add_edge(sub_plan_head, nid, consumer[2])
                plan = join_leg_preprocess(plan)
                expanded = True
                break  # restart scan; plan topology has changed
        if not expanded:
            break

    return plan


def do_bind_phase(
    plan: LogicalPlan,
    execution_context=None,
    query_id: str = None,
    common_table_expressions: dict = None,
    visibility_filters: dict = None,
    telemetry=None,
) -> LogicalPlan:
    """
    Execute the bind phase of the query engine.

    Parameters:
        plan: Any
            The logical plan.
        context: BindingContext
            The context needed for the binding phase.

    Returns:
        Modified logical plan after the binding phase.

    Raises:
        InvalidInternalStateError: Raised when the logical plan has more than one root node.
    """
    if common_table_expressions is None:
        common_table_expressions = {}

    plan = bind_logical_relations(plan, common_table_expressions, telemetry=telemetry)

    if visibility_filters:
        plan = apply_visibility_filters(plan, visibility_filters, telemetry)

    binder_visitor = BinderVisitor()
    root_node = plan.get_exit_points()
    context = BindingContext.initialize(query_id=query_id, execution_context=execution_context)

    if len(root_node) > 1:
        raise InvalidInternalStateError(
            f"{context.query_id} - logical plan has {len(root_node)} heads - this is an error"
        )

    plan, _ = binder_visitor.traverse(plan, root_node[0], context=context)

    return plan
