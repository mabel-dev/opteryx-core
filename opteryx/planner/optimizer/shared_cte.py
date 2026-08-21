# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Cross-reference coordination for shared (materialize-once) CTE bodies.

A shared CTE body executes once; its references are leaves in the plans that read
it (see relation_resolver / MaterializedCteRef). Because references are leaves,
the main optimizer cannot push anything INTO the body — this module is the one
sanctioned crossing of that boundary, and it only moves work that is provably
shared by every reference:

- PROJECTIONS: the body's output is narrowed to the UNION of the columns any
  reference reads. A column no reference reads is never materialized; a column
  any reference reads must be (union, never intersection — dropping a column one
  consumer needs is a wrong answer, not an optimization).

- PREDICATES: a filter sitting directly above a reference is moved into the body
  only when an EQUIVALENT filter (equal after translating each reference's column
  identities to the body's) sits above EVERY reference. Filtering the body's
  output by P and then reading it is the same relation as reading it and then
  filtering by P, so when all readers filter by P the move is an identity — the
  per-reference copies are removed.

Order matters: the MAIN plan (and any dependent shared body) is optimized first,
so filters and projections have already settled against the reference leaves;
then this coordination runs per body, dependents before dependencies; then each
body is optimized as a plan in its own right, which carries the moved predicates
and the narrowed projection down onto its scans.
"""

from typing import Dict
from typing import List
from typing import Tuple

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import LogicalColumn
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanNode
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.relation_resolver import iter_plan_forest

__all__ = ["coordinate_shared_cte", "strip_body_boundary", "stamp_reference_estimates"]


def _refs_of(plans: List[LogicalPlan], cte_key: str):
    """(plan, nid, node) for every reference to `cte_key` across `plans`.

    Traverses the plan FOREST, not just each plan's own graph: an expression
    subquery holds a whole plan off to the side, and a reference living in one is
    a reference like any other. This is the resolver's own traversal
    (`iter_plan_forest`) deliberately — the resolver counts references to decide
    inline-vs-share, and if the two rules disagree this module coordinates a
    body against a subset of the references the resolver shared it for.

    The yielded plan is the FOREST MEMBER holding the reference, which is the
    graph `_push_common_predicates` must edit to remove a hoisted filter.
    """
    seen_members: set = set()
    for plan in plans:
        for member in iter_plan_forest(plan):
            # `plans` are distinct graphs, but one embedded plan can be reachable
            # from more than one of them; yielding a reference twice would have
            # `_push_common_predicates` try to remove the same filter node twice.
            if id(member) in seen_members:
                continue
            seen_members.add(id(member))
            for nid, node in list(member.nodes(True)):
                if (
                    node.node_type == LogicalPlanStepType.MaterializedCteRef
                    and node.cte_key == cte_key
                ):
                    yield member, nid, node


def strip_body_boundary(body: LogicalPlan):
    """Remove the body's Subquery boundary head, returning its bound schema.

    The boundary existed so the Binder could derive the body's output schema the
    same way it does for any derived table; the schema (and each reference's
    `cte_column_map` onto it) is settled now, and the physical planner has no
    Subquery operator — the boundary must not survive to compilation.
    """
    head_nid = body.get_exit_points()[0]
    boundary = body[head_nid]
    if boundary.node_type != LogicalPlanStepType.Subquery:
        # already stripped (or never added — a body built by a future producer);
        # the caller needs a schema, so reconstruct it from the head's columns
        return None
    schema = boundary.schema
    body.remove_node(head_nid, heal=True)
    return schema


def _projection_head(body: LogicalPlan):
    """The body's projection node (nid, node): the first node from the head down
    that carries columns, provided it is a Project — Order/Limit heads carry no
    columns of their own and are walked through. Returns (None, None) when the
    head with columns is not a Project (an aggregate head prunes via its own
    optimization instead)."""
    nid = body.get_exit_points()[0]
    while True:
        node = body[nid]
        if node.columns:
            if node.node_type == LogicalPlanStepType.Project:
                return nid, node
            return None, None
        below = body.ingoing_edges(nid)
        if len(below) != 1:
            return None, None
        nid = below[0][0]


def _narrow_body_projection(body: LogicalPlan, refs, telemetry) -> None:
    """Prune the body's output to the union of what its references read."""
    needed: set = set()
    for _plan, _nid, ref in refs:
        columns = ref.columns
        if not columns:
            # an unpruned/unknown reference demands everything — do not narrow
            return
        mapping = ref.cte_column_map or {}
        for column in columns:
            body_identity = mapping.get(column.schema_column.identity)
            if body_identity is None:
                return
            needed.add(body_identity)
    if not needed:
        # a zero-column demand is the UNKNOWN sentinel elsewhere in the planner;
        # never narrow to nothing
        return
    project_nid, project = _projection_head(body)
    if project is None:
        return
    kept = [
        column
        for column in project.columns
        if column.schema_column is not None and column.schema_column.identity in needed
    ]
    if kept and len(kept) < len(project.columns):
        telemetry.optimization_shared_cte_projection_narrowed += len(project.columns) - len(kept)
        project.columns = kept
        body[project_nid] = project


def _canonical_key(condition, mapping: Dict[bytes, bytes]):
    """A structural key for a predicate, with each reference-local column
    identity translated to the body identity it maps to. Two references' filters
    get the SAME key exactly when they apply the same test to the same body
    output columns. Returns None when the predicate reads anything that is not a
    mapped reference column (it then belongs to that reference alone)."""
    if condition is None:
        return None
    if isinstance(condition, (LogicalColumn,)) or condition.node_type in (
        NodeType.IDENTIFIER,
        NodeType.AGGREGATOR,
        NodeType.EVALUATED,
    ):
        schema_column = getattr(condition, "schema_column", None)
        identity = getattr(schema_column, "identity", None)
        body_identity = mapping.get(identity)
        if body_identity is None:
            return None
        return ("col", body_identity)
    if condition.node_type == NodeType.LITERAL:
        return ("lit", str(condition.type), repr(condition.value))
    children = []
    for child in (condition.left, condition.centre, condition.right):
        if child is None:
            children.append(None)
            continue
        child_key = _canonical_key(child, mapping)
        if child_key is None:
            return None
        children.append(child_key)
    parameters = []
    for parameter in condition.parameters or []:
        parameter_key = _canonical_key(parameter, mapping)
        if parameter_key is None:
            return None
        parameters.append(parameter_key)
    return (
        str(condition.node_type),
        str(condition.value),
        tuple(children),
        tuple(parameters),
    )


def _filters_above(plan: LogicalPlan, nid: str):
    """The chain of Filter nodes sitting directly above `nid` (nearest first)."""
    chain = []
    current = nid
    while True:
        outgoing = plan.outgoing_edges(current)
        if len(outgoing) != 1:
            return chain
        parent_nid = outgoing[0][1]
        parent = plan[parent_nid]
        if parent.node_type != LogicalPlanStepType.Filter:
            return chain
        chain.append((parent_nid, parent))
        current = parent_nid


def _translate_condition(condition, mapping: Dict[bytes, bytes], body_columns_by_identity):
    """Deep-copy `condition`, re-pointing every identifier at the body's own
    bound schema column (by mapped identity). The copy is what gets inserted
    into the body — the original, still keyed by the reference's identities,
    stays with its reference until removal."""
    translated = condition.copy()
    for identifier in get_all_nodes_of_type(
        translated, (NodeType.IDENTIFIER, NodeType.AGGREGATOR, NodeType.EVALUATED)
    ):
        body_identity = mapping[identifier.schema_column.identity]
        identifier.schema_column = body_columns_by_identity[body_identity]
        identifier.source = None
    return translated


def _push_common_predicates(body: LogicalPlan, refs, body_schema, telemetry) -> None:
    """Move filters shared by EVERY reference into the body (and off the refs)."""
    from opteryx.utils import random_string

    per_ref: List[Dict[Tuple, Tuple]] = []  # key -> (plan, filter_nid, filter_node, mapping)
    for plan, nid, ref in refs:
        mapping = ref.cte_column_map or {}
        candidates: Dict[Tuple, Tuple] = {}
        for filter_nid, filter_node in _filters_above(plan, nid):
            key = _canonical_key(filter_node.condition, mapping)
            if key is not None and key not in candidates:
                candidates[key] = (plan, filter_nid, filter_node, mapping)
        per_ref.append(candidates)

    common = set(per_ref[0])
    for candidates in per_ref[1:]:
        common &= set(candidates)
    if not common:
        return

    body_columns_by_identity = {c.identity: c for c in (body_schema.columns if body_schema else [])}
    head_nid = body.get_exit_points()[0]

    for key in common:
        _donor_plan, _donor_nid, donor, donor_mapping = per_ref[0][key]
        # a KeyError here would mean a mapping onto a column the boundary schema
        # does not carry — impossible by construction (the mapping was minted
        # FROM that schema), so it fails loud rather than being smoothed over
        translated = _translate_condition(
            donor.condition, donor_mapping, body_columns_by_identity
        )
        body_filter = LogicalPlanNode(LogicalPlanStepType.Filter)
        body_filter.condition = translated
        body_filter.columns = get_all_nodes_of_type(
            translated, (NodeType.IDENTIFIER, NodeType.AGGREGATOR)
        )
        body_filter.relations = set()
        body_filter.all_relations = set()
        filter_nid = random_string()
        body.add_node(filter_nid, body_filter)
        body.add_edge(head_nid, filter_nid)
        head_nid = filter_nid

        # every reference carried an equivalent copy — remove each one
        for candidates in per_ref:
            ref_plan, ref_filter_nid, _node, _mapping = candidates[key]
            ref_plan.remove_node(ref_filter_nid, heal=True)
        telemetry.optimization_shared_cte_predicate_pushed += 1


def coordinate_shared_cte(
    body: LogicalPlan, consumer_plans: List[LogicalPlan], cte_key: str, telemetry
):
    """Run both coordinations for one shared body. `consumer_plans` must already
    be optimized (main plan plus any dependent shared bodies). Returns the body
    (boundary stripped), ready for its own optimization pass."""
    refs = list(_refs_of(consumer_plans, cte_key))
    body_schema = strip_body_boundary(body)
    if refs:
        _push_common_predicates(body, refs, body_schema, telemetry)
        _narrow_body_projection(body, refs, telemetry)
    return body


def stamp_reference_estimates(consumer_plans: List[LogicalPlan], cte_key: str, body_statistics):
    """Attach the body's output estimate to each reference leaf, so the main
    plan's cost-based strategies (join ordering, build-shape) see a real
    cardinality instead of an unknown. See statistics_refresh's
    MaterializedCteRef branch."""
    for _plan, _nid, node in _refs_of(consumer_plans, cte_key):
        node.cte_statistics = body_statistics
