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

WITH RECURSIVE (docs/RECURSIVE_CTE_DESIGN.md) is the ONE sanctioned exemption: a
recursive CTE arrives from extract_ctes as a RecursiveCteDefinition (anchor plan +
recursive-term plan, already split at its topmost UNION ALL) and is NEVER spliced —
every reference, the term's self-reference included, becomes a MaterializedCteRef on
the one definition, so no cycle ever exists in the plan graph. The legs ride
`plan.shared_ctes` as ordinary bodies (bound, optimized and compiled on the existing
shared-CTE rail) and `plan.recursive_ctes` carries the fixpoint metadata the plan
compiler turns into the engine's LoopSpan.
"""

from typing import Dict
from typing import Optional
from typing import Tuple

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import LogicalColumn
from opteryx.models import Node
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import RecursiveCteDefinition

__all__ = [
    "do_resolve_relations",
    "iter_plan_forest",
    "rename_relations",
    "join_leg_preprocess",
    "subplan_rooted_at",
    "RELATION_STEP_TYPES",
]

# The plan steps that INTRODUCE a relation name — the names a column reference, a join's
# left/right_relation_names, or a qualified wildcard can address. Everything else in a
# plan passes its input's names through unchanged.
RELATION_STEP_TYPES = (
    LogicalPlanStepType.Scan,
    LogicalPlanStepType.FunctionDataset,
    LogicalPlanStepType.Subquery,
    LogicalPlanStepType.MaterializedCteRef,
)

# The deepest chain of view/CTE expansions we will follow. A legitimate plan nests a
# handful deep; anything beyond this is a runaway and is failed rather than followed.
MAX_EXPANSION_DEPTH = 16

# Alias prefixes the planner MINTS when it splices a sub-plan in, to keep two copies of
# one relation apart. They name nothing the reader wrote and nothing they can type, so
# a surface reporting relations back to a person (see opteryx/planner/query_check.py)
# must not offer them. Named here so that side and the minting side cannot drift.
VIEW_ALIAS_PREFIX = "$view-"
UNION_ALIAS_PREFIX = "$union-"
SYNTHETIC_ALIAS_PREFIXES = (VIEW_ALIAS_PREFIX, UNION_ALIAS_PREFIX)


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
    new_plan._mutation_epoch = 0

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


def subplan_rooted_at(plan: LogicalPlan, root_nid: str) -> LogicalPlan:
    """
    Extract the subtree feeding `root_nid` (inclusive) as a standalone plan.

    The nodes are the SAME objects as in `plan` — `copy_sub_plan` deep-copies them on
    the way out, so nothing here may be mutated before that happens. The pairing is the
    point: this says WHICH nodes, `copy_sub_plan` makes them independent, and
    `rename_relations` stops the copy claiming the original's relation names.
    """
    sub = LogicalPlan()
    seen: set = set()
    stack = [root_nid]
    while stack:
        nid = stack.pop()
        if nid in seen:
            continue
        seen.add(nid)
        sub.add_node(nid, plan[nid])
        for child, _target, _relation in plan.ingoing_edges(nid):
            stack.append(child)
    for nid in seen:
        for child, _target, relation in plan.ingoing_edges(nid):
            if child in seen:
                sub.add_edge(child, nid, relation)
    return sub


def rename_relations(plan: LogicalPlan, prefix: str = VIEW_ALIAS_PREFIX):
    """
    When we include VIEWs and CTEs in a plan, we randomize the name of the
    relations to avoid conflicts.
    """
    from opteryx.expression import NodeType
    from opteryx.models import LogicalColumn
    from opteryx.utils import random_string

    relations = {}  # old_alias.lower() -> new_alias
    uuid_remap = {}  # old_uuid -> new_uuid for updating join readers

    # first we collection the relations
    #
    # Scan is the common case (a real dataset), but FunctionDataset (VALUES, UNNEST,
    # GENERATE_SERIES, ...) also introduces a relation alias that column references and
    # join/union relation-name lists can point at — it must be renamed too, or a clone of
    # a subplan whose only relations are FunctionDataset nodes (e.g. a FULL OUTER JOIN
    # between two VALUES clauses) collides with the original names it was cloned from.
    # Subquery is the third: a derived table (`FROM (SELECT ...) AS s`) and an already-
    # expanded CTE/view reference both present as a Subquery node, and its alias is a
    # relation name in exactly the same sense — `get_subplan_schemas` stops AT a Subquery
    # and returns its alias, so it reaches a join's left/right_relation_names too. A copy
    # that kept the original's subquery alias put two relations of one name in front of
    # the Binder (a window over a CTE died with a misleading SEMI-join error).
    # Guard on a non-empty alias: some FunctionDataset nodes (READ_JSONL/READ_PARQUET/
    # READ_CSV) are not required to carry one, and mapping `None` as a relations-dict
    # key would make every unrelated `.source is None` column reference match it.
    for nid, node in [
        (nid, node)
        for (nid, node) in plan.nodes(True)
        if node.node_type in RELATION_STEP_TYPES and node.alias
    ]:
        alias = f"{prefix}{random_string(4)}"
        # Keyed by folded case: a relation alias is an unquoted SQL identifier, so
        # `FROM (SELECT ...) CATALOG ... WHERE catalog.x` (the declaration and its
        # reference spelled differently) must remap to the SAME new alias — the
        # same fold `locate_identifier` applies at bind time (binder.py's
        # `_candidates`), done here too because this runs BEFORE binding, on a
        # spliced CTE/view/set-op-leg copy the binder never sees pre-rename.
        relations[node.alias.lower()] = alias
        # Only scan-like nodes are READERS: join_leg_preprocess walks Scan nodes and
        # collects their uuids into left_readers/right_readers, so those are the uuids
        # that have to be made unique per copy. A Subquery's uuid reaches no such list,
        # and minting a new one for it would be churn with nothing reading it.
        if node.node_type != LogicalPlanStepType.Subquery:
            unique_id = random_string(32)
            uuid_remap[node.uuid] = unique_id
            node.uuid = unique_id
        node.alias = alias
        plan[nid] = node

    def _prop(property):
        if isinstance(property, LogicalColumn) and property.source is not None:
            mapped = relations.get(property.source.lower())
            if mapped is not None:
                property.source = mapped
        # A QUALIFIED wildcard (`p.*`) names a relation too, and names it as a plain
        # string in `value` rather than as a LogicalColumn.source — so nothing above
        # reaches it, and the tuple branch below walks straight past a bare string. Left
        # unmapped it points at the alias the copy was renamed AWAY from, matches no
        # relation in scope, and expands to NOTHING: every source column silently
        # disappears from the copy, and the Project above it fails on a column that is
        # plainly there in the original. `WITH c AS (SELECT p.* FROM $planets AS p)
        # SELECT name FROM c` died with a raw `ValueError: not enough values to unpack`
        # from the binder's `zip(*...)` over an empty expansion.
        if (
            isinstance(property, Node)
            and property.node_type == NodeType.WILDCARD
            and property.value
        ):
            property.value = type(property.value)(
                relations.get(q.lower(), q) for q in property.value
            )
        if isinstance(property, list):
            return [_prop(p) for p in property]
        if isinstance(property, tuple):
            # ORDER BY entries are (expr, ascending) tuples — recurse into them too,
            # or a spliced view/CTE keeps a dangling reference to its old alias
            # (e.g. `ORDER BY o.observed_at` after `o`'s Scan is renamed away).
            return tuple(_prop(p) for p in property)
        if isinstance(property, dict):
            return {k: _prop(v) for k, v in property.items()}
        if isinstance(property, Node):
            for p in property.properties:
                property.properties[p] = _prop(property.properties[p])
        return property

    for nid, node in plan.nodes(True):
        for property in node.properties:
            node.properties[property] = _prop(node.properties[property])

    # Window and FramedWindow nodes carry a pre-minted output relation
    # (`$window-XXXXXX` / `$framedwindow-XXXXXX`) and pre-minted SchemaColumn
    # identities for their outputs, fixed at logical-planning time. A CTE
    # referenced twice splices two copies of one body, and identity is the
    # engine's per-column handle — two copies sharing one identity means a
    # self-join's `a.rn = b.rn + 1` binds both sides to the SAME column and
    # silently collapses (TPC-DS Q57 answered 0 rows, "ok"). Re-mint both per
    # copy. Nothing in the unbound sub-plan references the relation string or
    # the identities — references to a window output are by NAME until the
    # binder registers the schema — so no fixup pass is needed.
    for nid, node in plan.nodes(True):
        # Guard on output_relation: an UNFRAMED aggregate window is also a Window
        # node at this stage, but carries `aggregates` only — the plan rewriter
        # lowers it to a join and it never mints an output relation.
        if (
            node.node_type
            in (
                LogicalPlanStepType.Window,
                LogicalPlanStepType.FramedWindow,
            )
            and node.output_relation
        ):
            import dataclasses

            from opteryx.types.schema import mint_column_identity

            new_rel = f"{node.output_relation.rsplit('-', 1)[0]}-{random_string(6)}"
            node.output_relation = new_rel
            # REPLACE the SchemaColumns rather than mutating them: copy_sub_plan's
            # node copy shares any non-Node property object between the copy and
            # its source (see _inner_copy in compiled/structures/node.pyx), so an
            # in-place identity write would hit the CTE template and every other
            # copy of it too — the very sharing this re-mint exists to break.
            node.outputs = [
                (
                    output[0],
                    dataclasses.replace(
                        output[1],
                        identity=mint_column_identity(new_rel, output[1].name),
                    ),
                    *output[2:],
                )
                for output in node.outputs
            ]
            plan[nid] = node

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
                    relations.get(n.lower(), n) for n in node.left_relation_names
                ]
            if node.right_relation_names:
                node.right_relation_names = [
                    relations.get(n.lower(), n) for n in node.right_relation_names
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

    Both lists are SETS in all but type: a name means "this leg includes that relation",
    so a name already present must not be added again. `_splice` runs this over the WHOLE
    plan on every expansion, so a Join whose legs the logical planner already computed
    (or an earlier splice already taught) is walked repeatedly. Appending unconditionally
    made `right_relation_names` read `['b', 'b']`, and the binder's USING handler pops the
    named column out of each listed relation in turn — the second pop of an already-popped
    column returns None, and setting `.origin` on it died with an AttributeError. That is
    a JOIN ... USING inside any view or CTE body.
    """
    for nid, node in (
        (nid, node)
        for (nid, node) in plan.nodes(True)
        # A MaterializedCteRef is a reader in exactly the Scan sense: it has a
        # uuid and an alias, and a join leg that includes it must list both.
        if node.node_type
        in (LogicalPlanStepType.Scan, LogicalPlanStepType.MaterializedCteRef)
    ):
        uuid = node.uuid

        location_nid = nid
        location_node = plan[location_nid]
        leg = None
        while location_nid:
            if location_node.node_type == LogicalPlanStepType.Join:
                if leg == "left":
                    if uuid not in location_node.left_readers:
                        location_node.left_readers.append(uuid)
                    if node.alias not in location_node.left_relation_names:
                        location_node.left_relation_names.append(node.alias)
                elif leg == "right":
                    if uuid not in location_node.right_readers:
                        location_node.right_readers.append(uuid)
                    if node.alias not in location_node.right_relation_names:
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


# The plan node types whose `columns` property holds a PROJECTION (a list of
# expression Nodes). Every other node either carries no columns or carries
# something else under the same name — see `_output_columns`.
_PROJECTION_NODES = frozenset(
    {
        LogicalPlanStepType.Project,
        LogicalPlanStepType.Exit,
        LogicalPlanStepType.Subquery,
        LogicalPlanStepType.Union,
    }
)


def _output_columns(sub_plan: LogicalPlan, head_nid: str):
    """The projection a relation's body emits, as seen from its plan head.

    The head is only the Project when nothing sits above it. A body with ORDER BY or
    LIMIT leaves an Order/Limit node at the head, and those carry no columns of their
    own — reading them made the relation look like a bare `SELECT *` and lost the
    body's names. Walk down (edges run leaf -> head) past the column-less nodes to the
    projection they wrap.

    Only the node types in `_PROJECTION_NODES` are read for that projection. `columns`
    is not one property with one meaning: a FunctionDataset (VALUES, UNNEST,
    GENERATE_SERIES) puts its output NAMES there, as plain strings. `SELECT * FROM
    (VALUES ...) AS v(c)` leaves no Project at all, so a CTE body of exactly that has
    the FunctionDataset AT its head — the walk used to return `('c',)` and the Binder
    died reading `node_type` off a `str`. A body headed by one of those projects
    everything it produces, which the wildcard says exactly.

    Found by walking THIS sub-plan rather than carried from the logical planner: the
    plan is copied per reference and `LogicalColumn.copy()` takes no memo, so a list
    stashed elsewhere would hold objects distinct from this copy's own Project.
    """
    nid = head_nid
    while True:
        node = sub_plan[nid]
        if node.node_type in _PROJECTION_NODES:
            columns = node.columns
            if columns:
                return columns
        elif node.columns:
            # `columns` does NOT mean the same thing on every node type: on a
            # FunctionDataset (VALUES, UNNEST, GENERATE_SERIES) it is a tuple of
            # output NAMES, not projection expressions. Reading it here stamped bare
            # strings onto the Subquery boundary and the binder died on
            # `'str' object has no attribute 'node_type'`. The wildcard is the
            # honest answer for a body headed by one of these.
            return None
        below = sub_plan.ingoing_edges(nid)
        # a branch (join) or a leaf (bare `SELECT *`, head is the Scan) has no single
        # projection to descend to — the wildcard is the honest answer
        if len(below) != 1:
            return None
        nid = below[0][0]


def _boundary_columns(sub_plan: LogicalPlan, head_nid: str, relation: str) -> list:
    """The projection to stamp on a relation's Subquery boundary node.

    The Binder reads these as expression Nodes (`column.node_type`). Anything else
    reaching it dies as a bare `AttributeError` inside `binder/project.py` with no
    mention of the SQL that produced it, so the contract is checked HERE, where the
    relation is still named.
    """
    from opteryx.exceptions import InvalidInternalStateError
    from opteryx.expression import NodeType

    columns = _output_columns(sub_plan, head_nid) or [Node(NodeType.WILDCARD)]
    bad = [column for column in columns if not isinstance(column, (Node, LogicalColumn))]
    if bad:
        raise InvalidInternalStateError(
            f"Relation '{relation}' produced a projection the binder cannot read: "
            f"{', '.join(repr(column) for column in bad)}. "
            "A relation body's output columns must be expression nodes."
        )
    return list(columns)


def _splice(plan: LogicalPlan, nid: str, node, sub_plan: LogicalPlan) -> LogicalPlan:
    """Replace a Scan node with a sub-plan, in place.

    The Scan becomes a Subquery boundary node keeping its alias — that alias is how the
    outer query addresses the expanded relation.
    """
    sub_plan = rename_relations(sub_plan)
    sub_plan_head = sub_plan.get_exit_points()[0]

    outgoing = plan.outgoing_edges(nid)
    if not outgoing:
        raise UnsupportedSyntaxError(
            f"Relation '{node.relation}' cannot be expanded here — it has no consumer."
        )

    node.node_type = LogicalPlanStepType.Subquery
    node.columns = _boundary_columns(sub_plan, sub_plan_head, node.relation or node.alias)
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
    catalog_cache=None,
    root_via_view: Optional[str] = None,
    cte_registry: Optional[Dict[str, LogicalPlan]] = None,
    cte_body_keys: Optional[Dict[Tuple[int, str], str]] = None,
    cte_names: Optional[Dict[str, str]] = None,
    recursive_defs: Optional[Dict[str, "RecursiveCteDefinition"]] = None,
) -> LogicalPlan:
    """
    Expand every view reference in one plan, resolve every CTE reference to a
    once-resolved body in `cte_registry`, then recurse into the sub-plans of
    any subquery expressions it carries. `root_scope`/`root_path` are the scope and
    expansion trail this plan is being resolved under; `root_via_view` is the innermost
    view this plan came out of, if any.

    CTE references are NOT spliced here. Each distinct CTE definition — keyed by
    (scope object, name), so one definition resolved through however many references —
    is resolved exactly once into `cte_registry`, and the referencing Scan becomes a
    pending marker (`pending_cte_key`). `_finalize_cte_sharing` then counts references:
    a single-reference body is spliced inline (today's behaviour, one copy), a
    multiply-referenced body stays in the registry and its markers become
    MaterializedCteRef leaves that share the one body.
    """
    from opteryx.managers.views import resolve_relation

    # nid -> (scope, expansion path). Held here rather than on the nodes: node properties
    # are deep-copied and walked by rename_relations, and a scope holds whole sub-plans.
    # Tracked for EVERY node, not just Scans: a Filter carrying an IN-subquery needs the
    # scope of the body it was spliced in from, so its sub-plan resolves against the same
    # CTEs the surrounding relation sees.
    # The third element is the innermost VIEW the node came out of, carried so a
    # refusal can name the view the caller actually wrote rather than only the
    # relation behind it - see the Scan gate in the Binder.
    scopes: Dict[str, Tuple[Dict[str, LogicalPlan], Tuple[str, ...], Optional[str]]] = {}
    for nid in plan.nodes():
        scopes[nid] = (root_scope, root_path, root_via_view)

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

            scope, path, via_view = scopes.get(nid, (root_scope, root_path, root_via_view))

            if relation in scope:
                if isinstance(scope[relation], RecursiveCteDefinition):
                    # A recursive CTE is never spliced: every reference (the term's
                    # self-reference included) becomes a pending marker on the ONE
                    # definition. The key is registered BEFORE the legs resolve, so
                    # the self-reference inside the term lands here on the memoized
                    # key instead of recursing — the one sanctioned exemption from
                    # the cycle check below.
                    definition = scope[relation]
                    body_key = cte_body_keys.get((id(scope), relation))
                    if body_key is None:
                        if relation in path:
                            raise _cycle_error(relation, path)
                        from opteryx.utils import random_string

                        body_key = f"$rcte-{random_string(8)}"
                        cte_body_keys[(id(scope), relation)] = body_key
                        cte_names[body_key] = relation
                        legs = []
                        for leg in (definition.anchor, definition.term):
                            legs.append(
                                _resolve(
                                    copy_sub_plan(leg),
                                    scope,
                                    path + (relation,),
                                    telemetry,
                                    catalog_cache,
                                    via_view,
                                    cte_registry=cte_registry,
                                    cte_body_keys=cte_body_keys,
                                    cte_names=cte_names,
                                    recursive_defs=recursive_defs,
                                )
                            )
                        recursive_defs[body_key] = RecursiveCteDefinition(
                            anchor=legs[0],
                            term=legs[1],
                            distinct=definition.distinct,
                            name=relation,
                        )
                    node.pending_cte_key = body_key
                    settled.add(nid)
                    continue
                if relation in path:
                    raise _cycle_error(relation, path)
                if len(path) >= MAX_EXPANSION_DEPTH:
                    trail = " -> ".join((*path, relation))
                    raise UnsupportedSyntaxError(
                        f"Relations are nested more than {MAX_EXPANSION_DEPTH} deep: {trail}. Flatten some of the views or **WITH** clauses feeding this query."
                    )
                # One resolution per CTE DEFINITION: the scope dict object plus the
                # name identifies the definition across all its references.
                body_key = cte_body_keys.get((id(scope), relation))
                if body_key is None:
                    from opteryx.utils import random_string

                    body_key = f"$cte-{random_string(8)}"
                    cte_body_keys[(id(scope), relation)] = body_key
                    cte_names[body_key] = relation
                    body = copy_sub_plan(scope[relation])
                    # a CTE body may reference CTEs declared alongside it, and a CTE
                    # inside a view is still read through that view
                    cte_registry[body_key] = _resolve(
                        body,
                        scope,
                        path + (relation,),
                        telemetry,
                        catalog_cache,
                        via_view,
                        cte_registry=cte_registry,
                        cte_body_keys=cte_body_keys,
                        cte_names=cte_names,
                        recursive_defs=recursive_defs,
                    )
                node.pending_cte_key = body_key
                settled.add(nid)
                continue
            else:
                kind, resolved = resolve_relation(relation, telemetry, catalog_cache)
                if kind == "view":
                    if relation in path:
                        raise _cycle_error(relation, path)
                    view_plan, view_ctes = resolved
                    sub_plan = copy_sub_plan(view_plan)
                    # a view is a closed unit: it sees its own CTEs, never the caller's
                    child_scope = view_ctes
                    # everything spliced in from here is read through THIS view; the
                    # innermost one wins, since that is the one naming the relation
                    child_via_view = relation
                else:
                    if kind == "dataset":
                        # stash it so the Binder doesn't re-read the catalog
                        node.resolved_dataset = resolved
                    # Settled as a real relation, whatever the connector called
                    # it - `kind` is None for connectors that prefetch nothing,
                    # so this must not hang off the branch above.
                    if via_view is not None:
                        node.via_view = via_view
                    settled.add(nid)
                    continue

            if len(path) >= MAX_EXPANSION_DEPTH:
                trail = " -> ".join((*path, relation))
                raise UnsupportedSyntaxError(
                    f"Relations are nested more than {MAX_EXPANSION_DEPTH} deep: {trail}. Flatten some of the views or **WITH** clauses feeding this query."
                )

            child_path = path + (relation,)
            for sub_nid in sub_plan.nodes():
                scopes[sub_nid] = (child_scope, child_path, child_via_view)

            plan = _splice(plan, nid, node, sub_plan)
            expanded = True
            break  # topology changed — restart the scan

        if not expanded:
            break

    # Every relation NAMED IN THE GRAPH is now real. Expression subqueries hold their own
    # plans off to the side, so resolve each against the scope of the node carrying it.
    # Done after the fixpoint above: splicing a CTE body in can introduce more of them.
    for nid, node in list(plan.nodes(True)):
        scope, path, via_view = scopes.get(nid, (root_scope, root_path, root_via_view))
        for subquery in _expression_subqueries(node):
            subquery.value = _resolve(
                subquery.value,
                scope,
                path,
                telemetry,
                catalog_cache,
                via_view,
                cte_registry=cte_registry,
                cte_body_keys=cte_body_keys,
                cte_names=cte_names,
                recursive_defs=recursive_defs,
            )

    return plan


def iter_plan_forest(plan: LogicalPlan, _seen: Optional[set] = None):
    """Yield `plan` and every LogicalPlan embedded in its nodes' properties
    (expression subqueries hold whole plans off to the side), recursively.

    Deduplicated by object identity: one expression object (and so one embedded
    plan) is routinely reachable from SEVERAL plan nodes — a Project and the
    Exit above it share their column expression objects — and counting a plan
    twice inflates CTE reference counts."""
    if _seen is None:
        _seen = set()
    if id(plan) in _seen:
        return
    _seen.add(id(plan))
    yield plan
    for _nid, node in plan.nodes(True):
        for embedded in _embedded_plans(node):
            yield from iter_plan_forest(embedded, _seen)


def _embedded_plans(node) -> list:
    """Every LogicalPlan hanging off a plan node's properties (see
    `_expression_subqueries` — the plan is the SUBQUERY expression node's value)."""
    found: list = []

    def _walk(value):
        if isinstance(value, LogicalPlan):
            found.append(value)
        elif isinstance(value, (list, tuple, set)):
            for v in value:
                _walk(v)
        elif isinstance(value, dict):
            for v in value.values():
                _walk(v)
        elif isinstance(value, (Node, LogicalColumn)):
            props = value.properties
            for key, val in (props or {}).items():
                if key in ("node_type", "uuid"):
                    continue
                _walk(val)

    for key, value in node.properties.items():
        if key in ("node_type", "uuid"):
            continue
        _walk(value)
    return found


def _pending_refs(plan: LogicalPlan):
    """(plan, nid, node) for every pending CTE marker in `plan`'s forest."""
    for member in iter_plan_forest(plan):
        for nid, node in list(member.nodes(True)):
            if getattr(node, "pending_cte_key", None) is not None:
                yield member, nid, node


def _reject_unsupported_term_shapes(term: LogicalPlan, key: str, name: str) -> None:
    """v1 shape gates on the recursive term (docs/RECURSIVE_CTE_DESIGN.md §5.1):
    the self-reference must feed the term's head through row-producing steps
    whose semantics survive per-iteration re-execution. Aggregates/windows over
    the working table are semantically contested across engines; ORDER BY /
    LIMIT inside the term would need per-iteration reset the engine's operators
    do not have (quota state is cumulative across passes); outer joins can
    null-pad the frontier. Each is rejected by name, never computed wrongly."""
    ref_nid = None
    for nid, node in term.nodes(True):
        if node.node_type == LogicalPlanStepType.MaterializedCteRef and node.cte_key == key:
            ref_nid = nid
            break
    if ref_nid is None:
        raise UnsupportedSyntaxError(
            f"Recursive CTE '{name}' references itself inside a subquery expression "
            "of the recursive term; reference it directly in the FROM clause."
        )
    blocking = {
        LogicalPlanStepType.Aggregate: "an aggregation",
        LogicalPlanStepType.AggregateAndGroup: "a GROUP BY",
        LogicalPlanStepType.Window: "a window function",
        LogicalPlanStepType.FramedWindow: "a window function",
        LogicalPlanStepType.Limit: "a LIMIT",
        LogicalPlanStepType.Order: "an ORDER BY",
        LogicalPlanStepType.HeapSort: "an ORDER BY with LIMIT",
    }
    nid = ref_nid
    while True:
        outgoing = term.outgoing_edges(nid)
        if not outgoing:
            return
        nid = outgoing[0][1]
        node = term[nid]
        label = blocking.get(node.node_type)
        if label is not None:
            raise UnsupportedSyntaxError(
                f"Recursive CTE '{name}' applies {label} over its own reference in "
                "the recursive term, which is not supported; apply it in the query "
                "that reads the CTE."
            )
        if (
            node.node_type == LogicalPlanStepType.Join
            and node.type not in ("inner", "cross join")
        ):
            raise UnsupportedSyntaxError(
                f"Recursive CTE '{name}' feeds its own reference through a "
                f"{str(node.type).upper()} join in the recursive term; only INNER "
                "joins over the self-reference are supported."
            )


def _finalize_cte_sharing(
    plan: LogicalPlan,
    registry: Dict[str, LogicalPlan],
    names: Dict[str, str],
    recursive_defs: Optional[Dict[str, RecursiveCteDefinition]] = None,
) -> LogicalPlan:
    """Decide, per CTE definition, between inline expansion and result sharing.

    Reference counts are taken over the final structure: the main plan's forest plus
    each REACHABLE registry body, counted once — a body executes once however many
    references it carries. Then:

    - refcount 1  -> splice the body inline at its single reference (exactly the plan
      shape the resolver produced before sharing existed, minus redundant copies).
    - refcount 2+ -> the markers become MaterializedCteRef leaves and the body stays
      in `plan.shared_ctes` (topologically ordered, dependencies first), headed by a
      Subquery boundary node so the Binder derives its output schema the same way it
      does for any derived relation.

    Unreachable bodies (a CTE declared but never referenced) are dropped.

    Recursive CTEs (`recursive_defs`, keyed like the registry) never take the
    inline path — their references share the fixpoint's one accumulated result by
    definition. Each one's anchor/term legs enter `plan.shared_ctes` as ordinary
    bodies and `plan.recursive_ctes` carries the metadata binding them together.
    """
    recursive_defs = recursive_defs or {}

    # ---- reachability + reference counts ------------------------------------
    def _refs_in(p: LogicalPlan):
        return [node.pending_cte_key for _m, _nid, node in _pending_refs(p)]

    def _body_refs(key: str):
        if key in recursive_defs:
            return _refs_in(recursive_defs[key].anchor) + _refs_in(recursive_defs[key].term)
        return _refs_in(registry[key])

    counts: Dict[str, int] = {}
    reachable: list = []  # discovery order
    frontier = _refs_in(plan)
    while frontier:
        key = frontier.pop()
        counts[key] = counts.get(key, 0) + 1
        if key not in reachable:
            reachable.append(key)
            frontier.extend(_body_refs(key))
        # already-reachable bodies were counted when first discovered

    # A body's OWN references were counted exactly once, on discovery, which is the
    # once-per-execution the count is defined over.

    # ---- refcount 1: splice inline ------------------------------------------
    # A single reference can live in the main plan's forest OR inside another
    # (not-yet-spliced) registry body — search both. Splicing merges the body's
    # nodes into the referencing plan, so markers it carried are found by later
    # searches wherever they ended up; the order keys are processed in does not
    # matter.
    def _all_sites(key: str, exclude_body: str):
        for site in _pending_refs(plan):
            if site[2].pending_cte_key == key:
                yield site
        for other_key, body in registry.items():
            if other_key == exclude_body:
                continue
            for site in _pending_refs(body):
                if site[2].pending_cte_key == key:
                    yield site
        # a recursive CTE's legs are bodies too — a single-referenced ordinary
        # CTE read only from inside one is spliced into that leg
        for d in recursive_defs.values():
            for leg in (d.anchor, d.term):
                for site in _pending_refs(leg):
                    if site[2].pending_cte_key == key:
                        yield site

    for key in [k for k in reachable if counts[k] == 1 and k not in recursive_defs]:
        sites = list(_all_sites(key, exclude_body=key))
        if len(sites) != 1:  # pragma: no cover — counts and sites derive identically
            raise UnsupportedSyntaxError(
                f"CTE '{names.get(key, key)}' reference bookkeeping is inconsistent."
            )
        member, nid, node = sites[0]
        node.pending_cte_key = None
        _splice(member, nid, node, registry.pop(key))

    # ---- refcount 2+ (and every recursive CTE): shared, materialized once ----
    shared: Dict[str, LogicalPlan] = {}
    recursive_used: list = []  # discovery order
    remaining = [_pending_refs(plan)] + [_pending_refs(body) for body in registry.values()]
    for d in recursive_defs.values():
        remaining.append(_pending_refs(d.anchor))
        remaining.append(_pending_refs(d.term))
    for site_iter in remaining:
        for member, nid, node in site_iter:
            key = node.pending_cte_key
            node.pending_cte_key = None
            node.node_type = LogicalPlanStepType.MaterializedCteRef
            node.cte_key = key
            node.cte_name = names.get(key)
            if key in recursive_defs:
                if key not in recursive_used:
                    recursive_used.append(key)
            else:
                shared.setdefault(key, registry[key])

    # Reference-shape obligations the fixpoint depends on: exactly one
    # self-reference, in the term. An indirect self-reference (through a CTE
    # spliced into a leg) surfaces here too — the splice landed the reference
    # in the leg's own forest.
    for key in recursive_used:
        d = recursive_defs[key]
        name = names.get(key, key)

        def _self_refs(p: LogicalPlan):
            return [
                node
                for member in iter_plan_forest(p)
                for _nid, node in member.nodes(True)
                if node.node_type == LogicalPlanStepType.MaterializedCteRef
                and node.cte_key == key
            ]

        if _self_refs(d.anchor):
            raise UnsupportedSyntaxError(
                f"Recursive CTE '{name}' references itself in the anchor term. "
                "Only the term after UNION ALL may reference the CTE."
            )
        term_self_refs = len(_self_refs(d.term))
        if term_self_refs != 1:
            raise UnsupportedSyntaxError(
                f"Recursive CTE '{name}' references itself {term_self_refs} times in "
                "the recursive term; exactly one self-reference is supported."
            )
        _reject_unsupported_term_shapes(d.term, key, name)

    # Head each shared body (and each recursive leg) with a Subquery boundary
    # (alias = the CTE's declared name) so binding it standalone produces the
    # body's output schema exactly as visit_subquery does for a derived table.
    from opteryx.planner.logical_planner import LogicalPlanNode
    from opteryx.utils import random_string

    def _add_boundary(body: LogicalPlan, alias: str):
        head = body.get_exit_points()[0]
        boundary = LogicalPlanNode(LogicalPlanStepType.Subquery)
        boundary.alias = alias
        boundary.columns = _boundary_columns(body, head, boundary.alias)
        boundary_nid = random_string()
        body.add_node(boundary_nid, boundary)
        body.add_edge(head, boundary_nid)

    for key, body in shared.items():
        _add_boundary(body, names.get(key, key))
    for key in recursive_used:
        _add_boundary(recursive_defs[key].anchor, names.get(key, key))
        _add_boundary(recursive_defs[key].term, names.get(key, key))

    # Topological order, dependencies first — a shared body referencing another
    # shared CTE must be bound/compiled after the body it reads. A recursive
    # CTE contributes its two legs (anchor immediately before term) plus a
    # metadata record binding them; its self-reference is not a dependency.
    ordered: Dict[str, LogicalPlan] = {}
    recursive_meta: Dict[str, dict] = {}
    emitted: set = set()

    def _converted_refs_in(p: LogicalPlan):
        return [
            node.cte_key
            for member in iter_plan_forest(p)
            for _nid, node in member.nodes(True)
            if node.node_type == LogicalPlanStepType.MaterializedCteRef
        ]

    def _emit(key: str, trail: Tuple[str, ...]):
        if key in emitted:
            return
        if key in trail:  # pragma: no cover — the resolver's cycle check fires first
            raise _cycle_error(names.get(key, key), trail)
        if key in recursive_defs:
            d = recursive_defs[key]
            deps = set(_converted_refs_in(d.anchor)) | set(_converted_refs_in(d.term))
            deps.discard(key)  # the self-reference
            for dep in deps:
                if dep in recursive_defs:
                    raise UnsupportedSyntaxError(
                        f"Recursive CTEs '{names.get(key, key)}' and "
                        f"'{names.get(dep, dep)}' reference each other; mutual "
                        "recursion is not supported."
                    )
                _emit(dep, trail + (key,))
            emitted.add(key)
            anchor_key, term_key = f"{key}#anchor", f"{key}#term"
            ordered[anchor_key] = d.anchor
            ordered[term_key] = d.term
            names[anchor_key] = names[term_key] = names.get(key, key)
            recursive_meta[key] = {
                "anchor_key": anchor_key,
                "term_key": term_key,
                "distinct": d.distinct,
                "name": names.get(key, key),
            }
        else:
            for dep in set(_converted_refs_in(shared[key])):
                _emit(dep, trail + (key,))
            emitted.add(key)
            ordered[key] = shared[key]

    for key in list(shared) + recursive_used:
        _emit(key, ())

    plan.shared_ctes = ordered
    plan.recursive_ctes = recursive_meta
    return plan


def do_resolve_relations(
    plan: LogicalPlan,
    common_table_expressions: Optional[Dict[str, LogicalPlan]],
    telemetry,
    catalog_cache=None,
) -> LogicalPlan:
    """
    Expand every CTE and view reference in the plan until only real datasets remain.

    Returns the expanded plan, carrying `shared_ctes` — the once-materialized bodies
    of CTEs referenced two or more times (empty dict when there are none). Fails loud
    on relation cycles and on runaway nesting.

    `catalog_cache` is passed by the edit-time check path ONLY - it makes catalog
    lookups up to a minute stale, which a statement that reads rows must not be. The
    query planner does not accept one; see `opteryx.CatalogCache`.
    """
    registry: Dict[str, LogicalPlan] = {}
    body_keys: Dict[Tuple[int, str], str] = {}
    names: Dict[str, str] = {}
    recursive_defs: Dict[str, RecursiveCteDefinition] = {}
    plan = _resolve(
        plan,
        common_table_expressions or {},
        (),
        telemetry,
        catalog_cache,
        cte_registry=registry,
        cte_body_keys=body_keys,
        cte_names=names,
        recursive_defs=recursive_defs,
    )
    return _finalize_cte_sharing(plan, registry, names, recursive_defs)
