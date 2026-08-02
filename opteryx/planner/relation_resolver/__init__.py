# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The Relation Resolver turns a NAME in a FROM clause into a PLAN.

It runs between the Logical Planner and the Plan Rewriter, on the unbound plan:

    parse -> logical plan -> RESOLVE -> plan rewrite -> bind -> optimize -> physical

Resolution is purely STRUCTURAL — it needs names and plans, never schemas, types or
statistics. That is why it is not the Binder's job. Doing it here means the Plan
Rewriter sees ONE fully-expanded plan: subquery expressions inside a CTE or a view
body are eliminated by the same single pass that handles the main query. When
expansion lived in the Binder (i.e. AFTER the rewriter), a view body carrying an
IN-subquery reached the expression compiler as an opaque NodeType.SUBQUERY and the
query died at execution with "unsupported node type 39".

Scoping
-------
Names are resolved against a scope, innermost first:

  1. a CTE visible in the current scope   (a CTE shadows a catalog relation)
  2. a view in the catalog                (expand it)
  3. a dataset in the catalog             (leave the Scan alone; the Binder handles it)

A CTE body sees its own scope, so it can reference CTEs declared alongside it.
A VIEW body opens a FRESH scope carrying only the view's own CTEs: a view is a closed
unit and MUST NOT see the CTEs of the query that called it.

Termination
-----------
Every expansion is tagged with the path of relation names that produced it. Expanding a
relation already on its own path is a cycle and fails loud; so does exceeding
MAX_EXPANSION_DEPTH. Without this the expansion loop cannot terminate: a spliced Scan's
ALIAS is randomised by rename_relations but its RELATION is not, and `relation` is the
lookup key — so a self-referencing view, a view cycle, or a recursive CTE regenerated an
identical lookup on every pass and hung the planner forever.

WITH RECURSIVE is rejected at logical planning (see extract_ctes). Supporting it needs a
native fixpoint operator in the engine, which is a separate piece of work.
"""

from typing import Dict
from typing import Optional
from typing import Tuple

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import LogicalColumn
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType

__all__ = ["do_resolve_relations", "rename_relations", "join_leg_preprocess"]

# The deepest chain of view/CTE expansions we will follow. A legitimate plan nests a
# handful deep; anything beyond this is a runaway and is failed rather than followed.
MAX_EXPANSION_DEPTH = 16


def copy_sub_plan(plan: LogicalPlan) -> LogicalPlan:
    """
    Copy a sub-plan with fresh node IDs — including any plan EMBEDDED in an expression.

    Merging is a dict update (Graph.__add__), so two expansions of the SAME view or CTE
    would share node IDs and silently overwrite each other in the merged plan. Fresh IDs
    make each expansion independent.

    An un-rewritten subquery expression (IN / EXISTS / scalar) hangs a whole LogicalPlan
    off the expression node. Those inner nodes need fresh IDs too: the Plan Rewriter later
    merges the inner plan into the main plan when it turns the subquery into a join, and
    if both copies carry inner nodes with identical IDs the second merge overwrites the
    first — silently truncating one leg to a childless Subquery node.
    """
    from opteryx.utils import random_string

    id_map = {old_id: random_string() for old_id in plan.nodes(data=False)}

    base = plan.copy()
    new_plan = plan.__class__.__new__(plan.__class__)
    new_plan._nodes = {id_map[old_id]: node for old_id, node in base._nodes.items()}

    new_plan._edges = {}
    for src, targets in base._edges.items():
        new_src = id_map.get(src, src)
        new_plan._edges[new_src] = tuple((id_map.get(tgt, tgt), rel) for tgt, rel in targets)

    new_plan._cached_edges = None
    new_plan._cached_ingoing_edges = None

    # NOTE: Node.properties returns a FRESH dict on every access, so
    # `node.properties[key] = value` writes into a throwaway and is silently discarded.
    # Replacing a property value requires setattr.
    def _rekey_embedded(value):
        if isinstance(value, LogicalPlan):
            return copy_sub_plan(value)
        if isinstance(value, list):
            return [_rekey_embedded(v) for v in value]
        if isinstance(value, dict):
            return {k: _rekey_embedded(v) for k, v in value.items()}
        if isinstance(value, Node):
            for prop, val in list(value.properties.items()):
                replacement = _rekey_embedded(val)
                if replacement is not val:
                    setattr(value, prop, replacement)
        return value

    for _nid, node in new_plan.nodes(True):
        for prop, val in list(node.properties.items()):
            replacement = _rekey_embedded(val)
            if replacement is not val:
                setattr(node, prop, replacement)

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
    #
    # Scan is the common case (a real dataset), but FunctionDataset (VALUES, UNNEST,
    # GENERATE_SERIES, ...) also introduces a relation alias that column references and
    # join/union relation-name lists can point at — it must be renamed too, or a clone of
    # a subplan whose only relations are FunctionDataset nodes (e.g. a FULL OUTER JOIN
    # between two VALUES clauses) collides with the original names it was cloned from.
    # Guard on a non-empty alias: some FunctionDataset nodes (READ_JSONL/READ_PARQUET/
    # READ_CSV) are not required to carry one, and mapping `None` as a relations-dict
    # key would make every unrelated `.source is None` column reference match it.
    for nid, node in [
        (nid, node)
        for (nid, node) in plan.nodes(True)
        if node.node_type in (LogicalPlanStepType.Scan, LogicalPlanStepType.FunctionDataset)
        and node.alias
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
                    relations[n][1] if n in relations else n for n in node.left_relation_names
                ]
            if node.right_relation_names:
                node.right_relation_names = [
                    relations[n][1] if n in relations else n for n in node.right_relation_names
                ]
            if node.left_readers:
                node.left_readers = [uuid_remap.get(u, u) for u in node.left_readers]
            if node.right_readers:
                node.right_readers = [uuid_remap.get(u, u) for u in node.right_readers]
            plan[nid] = node

    return plan


def join_leg_preprocess(plan: LogicalPlan):
    """Teach each Join node which scans feed its left and right legs.

    A spliced sub-plan introduces new Scan nodes beneath an existing Join; the Join must
    learn their uuids/aliases or the Binder cannot attribute columns to the correct side.
    """
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


def _cycle_error(relation: str, path: Tuple[str, ...]) -> UnsupportedSyntaxError:
    trail = " -> ".join((*path, relation))
    return UnsupportedSyntaxError(
        f"Relation '{relation}' is defined in terms of itself: {trail}. "
        "Recursive views and self-referencing CTEs are not supported."
    )


def _apply_column_aliases(head, relation: str) -> None:
    """Apply a relation's column-alias list — the `(a, b)` in `WITH t(a, b) AS (...)`.

    The names rename the relation's OUTPUT columns positionally. Applied here, at the
    splice, because this is where the alias list and the body's projection meet.
    """
    from opteryx.expression import NodeType

    column_aliases = head.column_aliases
    if not column_aliases:
        return

    columns = head.columns or []

    # `WITH t(a, b) AS (SELECT * FROM ...)` — a wildcard body has no projection list to
    # line the names up against until binding resolves the schema (for a bare `SELECT *`
    # the plan head is the Scan itself, carrying no columns at all). Refuse, rather than
    # drop the names on the floor, which is what used to happen.
    if not columns or any(column.node_type == NodeType.WILDCARD for column in columns):
        raise UnsupportedSyntaxError(
            f"Relation '{relation}' declares column aliases over a wildcard projection. "
            "Name the columns in the body instead of using SELECT *."
        )

    if len(column_aliases) != len(columns):
        raise UnsupportedSyntaxError(
            f"Relation '{relation}' declares {len(column_aliases)} column alias(es) "
            f"but its body produces {len(columns)} column(s)."
        )

    for column, alias in zip(columns, column_aliases):
        column.alias = alias


def _splice(plan: LogicalPlan, nid: str, node, sub_plan: LogicalPlan) -> LogicalPlan:
    """Replace a Scan node with a sub-plan, in place.

    The Scan becomes a Subquery boundary node keeping its alias — that alias is how the
    outer query addresses the expanded relation.
    """
    from opteryx.expression import NodeType

    sub_plan = rename_relations(sub_plan)
    sub_plan_head = sub_plan.get_exit_points()[0]
    _apply_column_aliases(sub_plan[sub_plan_head], node.relation)

    outgoing = plan.outgoing_edges(nid)
    if not outgoing:
        raise UnsupportedSyntaxError(
            f"Relation '{node.relation}' cannot be expanded here — it has no consumer."
        )

    node.node_type = LogicalPlanStepType.Subquery
    node.columns = sub_plan[sub_plan_head].columns or [Node(NodeType.WILDCARD)]
    plan += sub_plan
    plan.add_edge(sub_plan_head, nid, outgoing[0][2])
    return join_leg_preprocess(plan)


def _expression_subqueries(node) -> list:
    """
    Every NodeType.SUBQUERY expression node hanging off a plan node's properties.

    A subquery in an expression (`WHERE x IN (SELECT ...)`) carries its sub-plan as the
    `value` of an expression node, NOT as a member of the plan graph — the Plan Rewriter
    is what later splices it in. So the graph walk cannot see the Scans inside it and the
    sub-plan needs resolving in its own right.
    """
    from opteryx.expression import NodeType
    from opteryx.expression import get_all_nodes_of_type

    roots = []
    for key, value in node.properties.items():
        if key in ("node_type", "uuid"):
            continue
        if isinstance(value, (Node, LogicalColumn)):
            roots.append(value)
        elif isinstance(value, (list, tuple, set)):
            roots.extend(v for v in value if isinstance(v, (Node, LogicalColumn)))

    return get_all_nodes_of_type(roots, (NodeType.SUBQUERY,))


def _resolve(
    plan: LogicalPlan,
    root_scope: Dict[str, LogicalPlan],
    root_path: Tuple[str, ...],
    telemetry,
) -> LogicalPlan:
    """
    Expand every CTE and view reference in one plan, then recurse into the sub-plans of
    any subquery expressions it carries. `root_scope`/`root_path` are the scope and
    expansion trail this plan is being resolved under.
    """
    from opteryx.managers.views import resolve_relation

    # nid -> (scope, expansion path). Held here rather than on the nodes: node properties
    # are deep-copied and walked by rename_relations, and a scope holds whole sub-plans.
    # Tracked for EVERY node, not just Scans: a Filter carrying an IN-subquery needs the
    # scope of the body it was spliced in from, so its sub-plan resolves against the same
    # CTEs the surrounding relation sees.
    scopes: Dict[str, Tuple[Dict[str, LogicalPlan], Tuple[str, ...]]] = {}
    for nid in plan.nodes():
        scopes[nid] = (root_scope, root_path)

    settled = set()  # scans known to be real datasets — never probed again

    while True:
        expanded = False

        for nid, node in list(plan.nodes(True)):
            if node.node_type != LogicalPlanStepType.Scan or nid in settled:
                continue

            relation = node.relation
            if relation is None:
                settled.add(nid)
                continue

            scope, path = scopes.get(nid, (root_scope, root_path))

            if relation in scope:
                if relation in path:
                    raise _cycle_error(relation, path)
                sub_plan = copy_sub_plan(scope[relation])
                # a CTE body may reference CTEs declared alongside it
                child_scope = scope
            else:
                kind, resolved = resolve_relation(relation, telemetry)
                if kind == "view":
                    if relation in path:
                        raise _cycle_error(relation, path)
                    view_plan, view_ctes = resolved
                    sub_plan = copy_sub_plan(view_plan)
                    # a view is a closed unit: it sees its own CTEs, never the caller's
                    child_scope = view_ctes
                else:
                    if kind == "dataset":
                        # stash it so the Binder doesn't re-read the catalog
                        node.resolved_dataset = resolved
                    settled.add(nid)
                    continue

            if len(path) >= MAX_EXPANSION_DEPTH:
                trail = " -> ".join((*path, relation))
                raise UnsupportedSyntaxError(
                    f"Relations are nested more than {MAX_EXPANSION_DEPTH} deep: {trail}."
                )

            child_path = path + (relation,)
            for sub_nid in sub_plan.nodes():
                scopes[sub_nid] = (child_scope, child_path)

            plan = _splice(plan, nid, node, sub_plan)
            expanded = True
            break  # topology changed — restart the scan

        if not expanded:
            break

    # Every relation NAMED IN THE GRAPH is now real. Expression subqueries hold their own
    # plans off to the side, so resolve each against the scope of the node carrying it.
    # Done after the fixpoint above: splicing a CTE body in can introduce more of them.
    for nid, node in list(plan.nodes(True)):
        scope, path = scopes.get(nid, (root_scope, root_path))
        for subquery in _expression_subqueries(node):
            subquery.value = _resolve(subquery.value, scope, path, telemetry)

    return plan


def do_resolve_relations(
    plan: LogicalPlan,
    common_table_expressions: Optional[Dict[str, LogicalPlan]],
    telemetry,
) -> LogicalPlan:
    """
    Expand every CTE and view reference in the plan until only real datasets remain.

    Returns the expanded plan. Fails loud on relation cycles and on runaway nesting.
    """
    return _resolve(plan, common_table_expressions or {}, (), telemetry)
