# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from copy import copy
from typing import List, Tuple

from opteryx.expression import ExpressionColumn, NodeType, get_all_nodes_of_type
from opteryx.models import LogicalColumn, Node
from opteryx.planner.binder.binder import (
    _bound_cast_node,
    _descriptor_carries_meaning,
    merge_schemas,
)
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.logical_planner import LogicalPlanNode, LogicalPlanStepType
from opteryx.types.logical_type import LogicalCategory, ColumnType, find_compatible_type
from opteryx.types import logical_type as _lt
from opteryx.types.schema import ConstantColumn, SchemaColumn, RelationSchema, mint_column_identity


def visit_set(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    node.variables = context.execution_context.variables
    node.columns = []
    return node, context


# The node types whose `.columns` are an OUTPUT projection. Everything else states
# columns for its own purposes — a Filter's predicate, a Join's ON condition, a
# Scan's read set — and none of those describe what the leg above them emits.
_PROJECTING_STEPS = (
    LogicalPlanStepType.Project,
    LogicalPlanStepType.Union,
    LogicalPlanStepType.Intersect,
    LogicalPlanStepType.Except,
)


def _setop_leg_columns(self, node: Node, relation_names: List[str], context: BindingContext):
    """One set-op leg's OUTPUT columns, in order, as (relation, SchemaColumn) pairs.

    THE ONE DEFINITION of what a leg produces. INTERSECT and EXCEPT match their legs
    POSITIONALLY - column i of the left against column i of the right - so order is
    the point, and the only node that states the leg's order is the leg's own top
    projection. Summing `context.schemas[rel]` across `relation_names` cannot: that
    list is not in projection order (it is collected by walking down to the scans),
    and for a leg over two relations it interleaves columns the projection never
    named.

    Returns None when any output column cannot be tied to a relation on this side -
    a computed or literal projection (`SELECT id + 1`, `SELECT 1`) is bound into the
    shared `$project` key and carries no source, and `extract_join_fields` keys the
    join by relation name. The caller then leaves the node alone rather than
    building a join whose keys silently do not resolve.

    ONLY a node that states a PROJECTION counts. Most nodes carry a `.columns` list
    and almost none of them are an output list: a Filter's is the predicate's
    identifiers, a Join's is its ON condition's. Accepting the first node that had
    any columns made `SELECT * FROM t WHERE ... INTERSECT SELECT * FROM t WHERE ...`
    compare the two legs on the FILTER's column alone and call the rest equal.
    """
    graph = getattr(self, "graph", None)
    if graph is None:
        return None

    set_op_nid = None
    for nid, candidate in graph.nodes(True):
        if candidate is node:
            set_op_nid = nid
            break
    if set_op_nid is None:
        return None

    names = set(relation_names)
    for child_nid, _, _ in graph.ingoing_edges(set_op_nid):
        if not _branch_owns_a_relation(graph, child_nid, names):
            continue

        descent = [child_nid]
        descended = set()
        while descent:
            current = descent.pop(0)
            if current in descended:
                continue
            descended.add(current)
            branch_node = graph[current]

            # A nested set operation this function already rewrote records what it
            # exports (see `_rewrite_setop_to_join`). Read it rather than
            # re-deriving: the rewritten node is a Join, whose `.columns` are the
            # identifiers of its ON condition - both legs' keys, not an output list.
            carried = getattr(branch_node, "setop_leg_columns", None)
            if carried is not None:
                return carried

            if branch_node.node_type in _PROJECTING_STEPS and branch_node.columns:
                leg_columns = []
                for column in branch_node.columns:
                    schema_column = getattr(column, "schema_column", None)
                    if schema_column is None:
                        # An unexpanded wildcard, or anything else not yet bound to
                        # a column: this IS the leg's projection, so there is
                        # nothing further down to consult.
                        return None
                    source = getattr(column, "source", None)
                    if source not in names:
                        # A column can reach the projection without its `source`
                        # set (`SELECT id` over a single relation); its origin
                        # names the relation it came from.
                        origins = [o for o in (schema_column.origin or []) if o in names]
                        source = origins[-1] if origins else None
                    if source is None:
                        # A computed projection (`SUBSTRING(ca_zip, 1, 5) AS ca_zip`)
                        # has no `.source` of its own, and its schema_column (an
                        # ExpressionColumn/FunctionColumn) carries no `.origin` either
                        # — that field is populated for aggregate outputs, not plain
                        # projected expressions. `.relations` (binder.py's
                        # `inner_binder`, "node.relations = set(sources)") is the
                        # general answer: every relation an identifier inside the
                        # expression resolves to. Only trust it when it names exactly
                        # ONE relation on this side — a bare `SELECT 1` (no
                        # identifiers) leaves it empty, and a genuinely cross-relation
                        # computed column leaves it ambiguous; both must still decline
                        # rather than guess.
                        relations = [r for r in (getattr(column, "relations", None) or ()) if r in names]
                        source = relations[0] if len(relations) == 1 else None
                    if source is None:
                        return None
                    leg_columns.append((source, schema_column))
                return leg_columns or None

            for upstream_nid, _, _ in graph.ingoing_edges(current):
                descent.append(upstream_nid)

    # No projection anywhere in the branch: `SELECT * FROM t`, whose wildcard the
    # planner resolves at the scan. The leg is then the relation itself, in schema
    # order. Only for a SINGLE relation — `SELECT * FROM a, b` outputs both, and
    # this function has no way to say which order they arrive in, so it declines
    # rather than compare one relation's columns and report the other's as equal.
    if len(relation_names) == 1:
        schema = context.schemas.get(relation_names[0])
        if schema is not None and schema.columns:
            return [(relation_names[0], schema_column) for schema_column in schema.columns]

    return None


def _branch_owns_a_relation(graph, start_nid: str, relation_names: set) -> bool:
    """Does the branch rooted at `start_nid` contain one of these relations?"""
    stack = [start_nid]
    seen: set = set()
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        if getattr(graph[current], "alias", None) in relation_names:
            return True
        for upstream_nid, _, _ in graph.ingoing_edges(current):
            stack.append(upstream_nid)
    return False


def _positional_setop_on_condition(left_columns, right_columns) -> Node:
    """AND-tree of `left[i] = right[i]`, one equality per output position.

    The identifiers are handed over ALREADY BOUND - `inner_binder` returns early on
    a node that has a `schema_column`, so nothing here is resolved by name. That is
    the point: a name is not a reliable handle on a leg's output column (two legs
    can both offer `id`, and a leg's own alias may have renamed it), whereas the
    bound column is the thing itself.

    `source` is still set, because it is not decoration: `extract_join_fields`
    decides which side of the join each key belongs to by testing the identifier's
    source against the join's relation-name lists.
    """
    conditions = []
    for (left_relation, left_column), (right_relation, right_column) in zip(
        left_columns, right_columns
    ):
        equality = Node(
            node_type=NodeType.COMPARISON_OPERATOR,
            value="Eq",
            do_not_create_column=True,
        )
        equality.left = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source=left_relation,
            source_column=left_column.name,
            schema_column=left_column,
        )
        equality.right = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source=right_relation,
            source_column=right_column.name,
            schema_column=right_column,
        )
        conditions.append(equality)

    while len(conditions) > 1:
        paired = []
        for i in range(0, len(conditions), 2):
            if i + 1 < len(conditions):
                and_node = Node(node_type=NodeType.AND, do_not_create_column=True)
                and_node.left = conditions[i]
                and_node.right = conditions[i + 1]
                paired.append(and_node)
            else:
                paired.append(conditions[i])
        conditions = paired

    return conditions[0]


def _rewrite_setop_to_join(self, node: Node, context: BindingContext, join_type: str):
    """Convert an INTERSECT/EXCEPT node to a `left semi` / `left anti` Join.

    THE set-op -> join rewrite, and the only one for the DISTINCT forms. It lives at
    bind time because the ON condition needs to know which column each leg produces
    at each output position, and that is knowable only once the legs are bound.

    It replaces three pre-bind constructions (plan_rewriter's
    `intersect_to_inner_join` and `except_to_anti_join`, deleted, and this module's
    own wildcard-only builder) which all built the ON as the CROSS PRODUCT of
    `left_relation_names x right_relation_names x projected column names`. That is
    only correct when each leg is exactly ONE relation. A leg that joins two -
    `... FROM store_sales, date_dim, customer ... EXCEPT ...`, TPC-DS Q87 - got a
    predicate per relation PAIR, referencing relations whose columns the leg's own
    projection had already narrowed away, and failed to bind. Positional matching is
    also what SQL actually specifies, so it additionally retires those modules'
    documented "column matching is by name" limitation.

    Returns None when the legs cannot be paired - an unequal column count (the
    validator has already refused that), or a leg whose output cannot be tied to a
    relation (see `_setop_leg_columns`). The caller then leaves the node exactly as
    it was: a set op the physical planner has no builder for still fails loud there,
    which is what it did before this path existed.

    Runs BEFORE the caller pops `right_relation_names`' schemas: visit_join binds
    the ON against both sides, exactly as it would a hand-written one.
    """
    left_columns = _setop_leg_columns(self, node, node.left_relation_names, context)
    right_columns = _setop_leg_columns(self, node, node.right_relation_names, context)
    if left_columns is None or right_columns is None:
        return None
    if len(left_columns) != len(right_columns):
        return None

    join_node = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
    join_node.type = join_type
    join_node.on = _positional_setop_on_condition(left_columns, right_columns)
    join_node.using = None
    join_node.left_relation_names = node.left_relation_names
    join_node.right_relation_names = node.right_relation_names
    join_node.columns = []
    # What this node exports, for an ENCLOSING set operation to read instead of
    # re-deriving it from a node that is no longer a projection. A semi/anti join
    # emits its left leg and nothing else, so the left leg's columns ARE the output
    # of the set operation this node replaces.
    join_node.setop_leg_columns = left_columns

    from opteryx.planner.binder.join import visit_join

    return visit_join(self, join_node, context)


def _columns_for_side(
    self,
    node: Node,
    relation_names: List[str],
    context: BindingContext,
):
    """Resolve the schema columns produced by one side of a set operation.

    Normally each side's relation names are registered in `context.schemas`.
    When a branch has no FROM clause (e.g. `SELECT 1`), the project step pops
    the synthetic `$no_table` source because none of its columns are projected,
    and the projected literals end up under a shared `$project` key that gets
    merged across branches. In that case fall back to walking the plan to find
    the branch's direct Project child of the set-op node and use its columns.

    `relation_names` can also over-report: `get_subplan_schemas` walks down to
    every scan in the branch, but a nested set operation collapses its legs into
    a single surviving relation at bind time (the others are popped). When that
    happens, some names resolve and some don't — the resolvable schemas already
    hold the merged columns, so trust them and ignore the collapsed siblings.
    Only fall back to the graph walk when *nothing* resolves.

    A relation's `context.schemas` entry can carry duplicate SchemaColumn entries
    for the same underlying column: `visit_project` merges one context snapshot
    per projected column (`merge_schemas(*[ctx.schemas for ctx in group_contexts])`),
    and `inner_binder` returns the *same* context object, unchanged, for every
    already-resolvable column (a plain identifier, or a column bound earlier) —
    so a Project with N such columns over one relation merges that relation's
    schema with itself N times. Most consumers of `context.schemas` are immune
    (they search-and-stop, or dedupe by identity during wildcard expansion), but
    this function returns a flat, ordered column list whose *length* the caller
    validates positionally — so the duplication must be undone here, by identity,
    rather than fixed at the merge site (which many other paths depend on).

    Summing per-relation schemas also can't see a projected column that isn't
    backed by any relation — a literal or other computed expression (`1 AS a`,
    `NULL AS a`) mixed in alongside real relation columns. `resolved_any` stays
    True (the branch's real relations DO resolve), so the "nothing resolved"
    fallback below never fires, and the literal silently drops out of the count.
    Guard against that by also trying the graph-walk-to-Project lookup (the
    branch's own bound `.columns`, unambiguously its true output) and preferring
    it ONLY when it reports MORE columns than the schema-sum — i.e. only in
    exactly this under-count case. This leaves every path where the schema-sum
    already agrees (the common case, and the nested-set-op leg-collapse
    tolerance this function was built for) byte-for-byte unchanged.
    """
    columns = []
    resolved_any = False
    for rel_name in relation_names:
        schema = context.schemas.get(rel_name)
        if schema is not None:
            columns.extend(schema.columns)
            resolved_any = True

    deduped = None
    if resolved_any:
        seen_identities = set()
        deduped = []
        for col in columns:
            if col.identity in seen_identities:
                continue
            seen_identities.add(col.identity)
            deduped.append(col)

    branch_columns = _branch_project_columns(self, node, relation_names, context)

    if deduped is not None:
        if branch_columns is not None and len(branch_columns) > len(deduped):
            return branch_columns
        return deduped

    if branch_columns is not None:
        return branch_columns

    raise KeyError(relation_names)


def _branch_project_columns(self, node: Node, relation_names: List[str], context: BindingContext):
    """Find a set-op branch's own bound Project columns by walking the graph.

    Returns None if the branch (or a Project within it) cannot be located —
    the caller decides what that means for its own resolution strategy.
    """
    graph = getattr(self, "graph", None)
    if graph is None:
        return None

    set_op_nid = None
    for nid, n in graph.nodes(True):
        if n is node:
            set_op_nid = nid
            break
    if set_op_nid is None:
        return None

    rel_set = set(relation_names)
    for child_nid, _, _ in graph.ingoing_edges(set_op_nid):
        if _branch_owns_a_relation(graph, child_nid, rel_set):
            # The branch's output columns live on its top node — usually the
            # direct Project child. With chained set operations the direct child
            # is a column-less wrapper (e.g. DISTINCT over a nested set op), so
            # descend through column-less nodes to the first node that carries
            # schema columns and use those.
            descent = [child_nid]
            descent_seen = set()
            while descent:
                cur = descent.pop(0)
                if cur in descent_seen:
                    continue
                descent_seen.add(cur)
                cur_node = graph[cur]
                # A nested set operation, already rewritten to a semi/anti Join by
                # `_rewrite_setop_to_join`, states what it exports. A Join's own
                # `.columns` are the identifiers of its ON condition — BOTH legs'
                # keys — so reading them here counts a two-column output for a
                # one-column set operation.
                carried = getattr(cur_node, "setop_leg_columns", None)
                if carried is not None:
                    return [schema_column for _, schema_column in carried]
                # Only a node that STATES a projection counts — same restriction
                # `_setop_leg_columns` applies via `_PROJECTING_STEPS`. Without it, a
                # HAVING Filter sitting directly below the set-op (any leg ending
                # `GROUP BY ... HAVING ...`) is mistaken for the leg's own Project:
                # a Filter's `.columns` are its predicate's referenced identifiers
                # (e.g. `sum(mass) > 0` -> `[mass]`), not an output list, and that
                # short leg-arity got reported as the branch's true column count.
                if cur_node.node_type in _PROJECTING_STEPS:
                    branch_columns = []
                    for col in (cur_node.columns or []):
                        schema_column = getattr(col, "schema_column", None)
                        if schema_column is not None:
                            branch_columns.append(schema_column)
                    if branch_columns:
                        return branch_columns
                for upstream_nid, _, _ in graph.ingoing_edges(cur):
                    descent.append(upstream_nid)

    return None


def _branch_project_node(self, node: Node, relation_names: List[str]):
    """Find a set-op branch's own Project (or Project-like) node by walking the graph.

    Same matching/descent as `_branch_project_columns`, but returns the actual
    graph node — so its `.columns` can be mutated in place — instead of a copy
    of its bound SchemaColumns. Returns None if no such node can be located.
    """
    graph = getattr(self, "graph", None)
    if graph is None:
        return None

    set_op_nid = None
    for nid, n in graph.nodes(True):
        if n is node:
            set_op_nid = nid
            break
    if set_op_nid is None:
        return None

    rel_set = set(relation_names)
    for child_nid, _, _ in graph.ingoing_edges(set_op_nid):
        if _branch_owns_a_relation(graph, child_nid, rel_set):
            descent = [child_nid]
            descent_seen = set()
            while descent:
                cur = descent.pop(0)
                if cur in descent_seen:
                    continue
                descent_seen.add(cur)
                cur_node = graph[cur]
                # Skipped for the same reason as in `_branch_project_columns`: a
                # Join's `.columns` are its ON condition's identifiers, so it is
                # never the node whose column list a caller wants to read or cast.
                if cur_node.node_type != LogicalPlanStepType.Join and any(
                    getattr(col, "schema_column", None) is not None for col in (cur_node.columns or [])
                ):
                    return cur_node
                for upstream_nid, _, _ in graph.ingoing_edges(cur):
                    descent.append(upstream_nid)

    return None


def _cast_leg_columns_to(columns: List[Node], coerced_types: List[ColumnType]) -> None:
    """Wrap each of a UNION leg's bound columns in a CAST when it doesn't already
    match the position's coerced (unified-across-both-legs) type.

    Inserted fully bound (own `schema_column`) rather than through the binder's
    CAST handling in `inner_binder`: this runs after the leg's own Project has
    already been bound, so nothing will traverse into a freshly-inserted raw
    CAST node to bind it. Mirrors the same pattern used for the CONCAT-argument
    CAST wrapper in optimizer/strategies/predicate_rewriter.py.

    The CAST node itself is built by `_bound_cast_node` (binder.py), which emits the
    shape the lowering reads: BARE type name in `.value` plus LITERAL parameters.
    `str(target)` was used here, and its parametrized display form ("DECIMAL(22, 2)")
    matches no resolver arm — a UNION coercing any leg to DECIMAL died with
    "CAST INT64 → DECIMAL(22, 2) is not supported".

    A NULL-typed LITERAL is retyped in place instead of CAST-wrapped: there is
    no NULL-to-anything native cast kernel (a NULL literal carries no value to
    convert), and none is needed — `CAST(NULL AS VARCHAR)` and "a VARCHAR-typed
    NULL literal" are the same thing. `visit_case`'s LITERAL-branch coercion
    (binder.py, "Coerce LITERAL branches to the resolved result type") is the
    same idea for CASE branches; this is that pattern's NULL case.
    """
    for i, col in enumerate(columns):
        if i >= len(coerced_types):
            return
        target = coerced_types[i]
        if target is None:
            continue
        schema_column = getattr(col, "schema_column", None)
        if schema_column is None:
            continue
        current_type = schema_column.column_type
        # Skip the cast only when the PHYSICAL tag already matches — the tag is what
        # Morsel.combine concatenates on. Matching CATEGORY is not enough: every
        # integer width shares LogicalCategory.INTEGER and FLOAT32/FLOAT64 share
        # FLOAT, so `SELECT int8_col ... UNION ALL SELECT int64_col ...` computed the
        # right target (INT64) and then skipped BOTH legs' casts, leaving one leg at
        # INT8 for draken's concat to reject with "all inputs must share one type".
        # COUNT(*) (always INT64) unioned with MAX(int8_col) is the everyday shape.
        #
        # The category test was right about the case it was written for and wrong
        # only in its closing premise — an unparameterized type CAN hide a
        # difference, it just hides it in the physical tag instead of the logical
        # descriptor. Comparing physical keeps that original case intact: legs at
        # DECIMAL(10,2) and DECIMAL(10,4), or timestamp[ms] and timestamp[us], share
        # one physical tag and so still fall through to a real rescale cast via
        # _descriptor_carries_meaning — without it they concatenated raw payloads
        # under ONE declared scale/unit (SUM 100x wrong; "mismatched unit").
        if current_type is not None and current_type.physical == target.physical:
            if current_type == target or not _descriptor_carries_meaning(target):
                continue
        if col.node_type == NodeType.LITERAL and col.value is None:
            col.type = target
            schema_column.column_type = target
            continue
        columns[i] = _bound_cast_node(col, target)


_SET_OP_STEP_TYPES = (
    LogicalPlanStepType.Union,
    LogicalPlanStepType.Intersect,
    LogicalPlanStepType.Except,
)


def _retype_declared_columns(columns: List[Node], context: BindingContext, coerced_types) -> None:
    """Point a set-op node's DECLARED output columns at the types its legs were just
    coerced to, in `columns` and in `context.schemas` alike.

    `_cast_leg_columns_to` only rewrites the LEGS. The set-op node's own output
    columns are bound to the FIRST leg's ORIGINAL, pre-cast SchemaColumn, so without
    this the node keeps declaring a type it no longer produces —
    `SELECT id AS n FROM $planets UNION ALL SELECT CAST(gravity AS FLOAT64) AS n ...`
    ran, but declared INT8 at EXIT while delivering FLOAT64. `A UNION B UNION C` is
    nested binary unions, so the outer union READS that lie when it reconciles C
    against `union(A, B)` — it picked its target from the stale type and skipped the
    cast the legs actually needed, and draken's concat rejected the result ("all
    inputs must share one type").

    Retypes a COPY of the SchemaColumn, never the SchemaColumn itself. That object is
    the LEG's — for `SELECT id AS n FROM $planets` the union's output column IS the
    reader's `id` column, so retyping it in place would retype the scan. The reader
    still produces INT8; it is the leg's inserted CAST that produces the coerced
    type. The copy keeps the identity, which is what the executor keys on
    (`UnionNode.column_ids`) and what the schema update below matches on.

    PRECONDITION: `columns` must be a set-op node's POST-`visit_exit` output columns.
    Those are freshly built LogicalColumns (see project.visit_exit), owned by this
    node alone. A set-op node's PRE-exit columns are the very node objects the left
    leg projects, and `_cast_leg_columns_to` keeps each as its CAST's source operand
    (`_bound_cast_node(col, target)` sets `left=col`) while replacing it only in the
    leg's own list — so assigning a retyped `schema_column` there told that CAST its
    input was already coerced, and `CAST(id AS INT64)` compiled a float→int64 kernel
    over INT8 data ("expected FLOAT, got 1").

    The NULL-literal arm of `_cast_leg_columns_to` retypes `schema_column.column_type`
    in place instead; that is sound there because that arm retypes the leg's own
    projected node, which is the thing whose type it is changing.

    Positional over `columns` — the same alignment the executor uses (each leg's
    first N columns become the union's N `column_ids`). `context.schemas` is NOT in
    projection order (see `_columns_for_side`), so it is matched by identity, and an
    identity appearing at two positions with two different targets (`SELECT id AS a,
    id AS b ...`) is ambiguous there and left alone rather than resolved by guess.
    """
    if not columns or len(columns) != len(coerced_types):
        return

    retyped_by_identity = {}
    claimed_identities = set()
    for i, column in enumerate(columns):
        target = coerced_types[i]
        schema_column = getattr(column, "schema_column", None)
        if target is None or schema_column is None:
            continue
        identity = schema_column.identity
        if schema_column.column_type == target and identity not in claimed_identities:
            claimed_identities.add(identity)
            continue
        replacement = copy(schema_column)
        replacement.column_type = target
        if identity in claimed_identities:
            # Two OUTPUT positions cannot share one identity once they carry different
            # columns: identity is the engine's column key, so `UnionNode.column_ids`
            # would name both legs' columns the same and the second would resolve to
            # the first. `SELECT id AS a, id AS b ... UNION ALL SELECT CAST(id AS
            # INT64) AS a, CAST(gravity AS FLOAT64) AS b ...` answered `b` with `a`'s
            # data — the leg projected the two casts correctly (they get their own
            # identities from `_bound_cast_node`), and the union's duplicated ids threw
            # one of them away.
            #
            # The duplication is legitimate up to here: `SELECT id AS a, id AS b` IS
            # one column named twice, and the binder deliberately folds those onto one
            # SchemaColumn. It only stops being expressible where a set operation makes
            # the positions DIVERGE, which is exactly here, so re-identify here and
            # nowhere else — the first occurrence keeps the leg's identity, which
            # projection pushdown and the aggregate-key emit rely on ("the union's
            # output identities ARE the first leg's").
            replacement.identity = mint_column_identity(None, schema_column.name)
        claimed_identities.add(replacement.identity)
        column.schema_column = replacement
        retyped_by_identity.setdefault(identity, replacement)

    if not retyped_by_identity:
        return

    for schema in context.schemas.values():
        for position, schema_column in enumerate(schema.columns):
            replacement = retyped_by_identity.get(schema_column.identity)
            if replacement is not None and replacement.identity == schema_column.identity:
                schema.columns[position] = replacement


def _publish_declared_columns(bound_columns: List[Node], exit_columns: List[Node]) -> None:
    """Make the query's EXIT read this set operation's settled output columns.

    A set operation's output IS the query's output, and logical_planner says so by
    handing ONE list object to both the set-op node and the EXIT node. This node then
    gets its own list from visit_exit and settles its columns (coercion, and the
    re-identification in `_retype_declared_columns`); `exit_columns` is that original
    shared list, still what the EXIT node will iterate when the binder reaches it.

    Republishing positionally is the only way to carry a RE-IDENTIFIED column across.
    The EXIT's own entries are the left leg's projection nodes, already bound, and
    `inner_binder` short-circuits on those — so it would keep whatever they resolved to
    when the LEG was bound. For `SELECT id AS a, id AS b ... UNION ALL SELECT CAST(id AS
    INT64) AS a, CAST(gravity AS FLOAT64) AS b ...` both entries are bound to the one
    folded `id` SchemaColumn, so the EXIT asked the union for that identity twice and
    got column `a` back for `b` — the union had the two columns right by then, and the
    EXIT threw one away.

    Slots are replaced, never the nodes in them: those nodes are also the CAST source
    operands in the leg's own (now separate) column list, and assigning a settled
    `schema_column` onto one tells its CAST the input is already coerced.

    Length disagreement means the EXIT is not a mirror of this node after all — a
    wildcard set-op EXIT, which expands from the schemas on its own — so leave it be.
    """
    if len(bound_columns) != len(exit_columns):
        return
    for position, bound_column in enumerate(bound_columns):
        exit_columns[position] = bound_column


def _coerce_branch_to(self, branch: Node, context: BindingContext, coerced_types) -> None:
    """Make one side of a set operation actually produce `coerced_types`.

    For an ordinary leg this is `_cast_leg_columns_to` on its Project.

    For a leg that is ITSELF a set operation (`A UNION B UNION C` parses as nested
    binary unions, so the outer union's left side is the inner union node) the cast
    cannot go on that node: `UnionNode` evaluates nothing — the compiler only
    positionally selects each leg's first N columns into a shared buffer (see
    compiler.py's UnionNode branch) — so a CAST written onto a union's own columns
    is never executed, and the declared type would be a second lie on top of the one
    this function exists to fix. Push it down to the LEAVES instead, then retype the
    inner node's declared output to match what its legs now emit.
    """
    if branch.node_type in _SET_OP_STEP_TYPES:
        left = _branch_project_node(self, branch, branch.left_relation_names)
        right = _branch_project_node(self, branch, branch.right_relation_names)
        if left is None or right is None:
            # Best-effort, exactly as the caller is: a branch we cannot confidently
            # locate is left exactly as it was rather than half-coerced.
            return
        _coerce_branch_to(self, left, context, coerced_types)
        _coerce_branch_to(self, right, context, coerced_types)
        _retype_declared_columns(branch.columns, context, coerced_types)
        return

    _cast_leg_columns_to(branch.columns, coerced_types)


def _set_op_common_type(left_type, right_type):
    """The type both legs of a set operation must arrive at for this column position.

    Legs that ALREADY carry the identical ColumnType need no coercion, and saying so
    here rather than deferring to `find_compatible_type` is not just an optimization:
    that function deliberately widens INT8/INT16/INT32 to INT64 when resolving a
    MIXED set of types, which is right for `INT8 ∪ INT16` but turns `INT8 ∪ INT8`
    into a pointless 8x widening of a union whose legs never disagreed. Nothing is
    being reconciled in that case, so nothing should be cast.

    Only the identical case short-circuits — any genuine disagreement still goes to
    `find_compatible_type` and gets its ladder (DECIMAL > FLOAT > INTEGER > BOOLEAN,
    narrow ints to INT64).
    """
    if left_type is not None and left_type == right_type:
        return left_type
    return find_compatible_type([left_type, right_type])


def _validate_set_operation_types(
    self,
    node: Node,
    context: BindingContext,
    operation_name: str = "SET OPERATION",
) -> None:
    """Both sides of a set operation must present the same number of columns.

    It also used to return a per-position coerced type, which every caller stored on
    the node as `coerced_types` and NOTHING ever read — the leg coercion in
    visit_union computes its own (see the note there on why that list could not be
    reused). Dead, so cut; the count check is what is left, and it is real.
    """
    left_columns = _columns_for_side(self, node, node.left_relation_names, context)
    right_columns = _columns_for_side(self, node, node.right_relation_names, context)

    if len(left_columns) != len(right_columns):
        raise ValueError(
            f"{operation_name}: column count mismatch — left has {len(left_columns)}, right has {len(right_columns)}"
        )


def visit_union(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    _validate_set_operation_types(self, node, context, "UNION")

    # The list object this node's columns live in is SHARED with the query's EXIT node
    # (logical_planner assigns one list to both), and the EXIT is bound after this node.
    # Captured before visit_exit below swaps this node onto its own list, so that what
    # this node settles on can be published back into it — see `_publish_declared_columns`.
    exit_columns = node.columns

    # Physically enforce the coercion: the executor concatenates each leg's
    # columns by position with no type check of its own (UnionNode just selects
    # column indices into a shared buffer — see compiler.py's UnionNode handling),
    # so two legs whose actual column types differ (most commonly: a NULL literal
    # on one side, a real typed column on the other) crash at morsel-combine time.
    #
    # Per-position types come from each side's own located Project
    # (`_branch_project_node`), which is unambiguously in the branch's real output
    # order — NOT from `_columns_for_side`, which the count check above uses. That
    # one (in its primary, non-fallback path) sums `context.schemas[rel].columns` —
    # RELATION SCHEMA order, which coincides with the SELECT-list order only when a
    # branch happens to project columns in the underlying table's declared order;
    # `SELECT name, id FROM $planets` (the schema's order is id-then-name) already
    # disagrees. Applying that mis-ordered list positionally against
    # `project.columns` silently pairs the wrong target type with the wrong column —
    # caught by `make q` regressing `... UNION ...` with a `name, id` projection into
    # an "Invalid digit in integer literal" CAST failure.
    #
    # Best-effort: only touches a leg whose own Project node can be confidently
    # located AND whose sibling side is too — anything else is left exactly as it
    # was, rather than half-coerced.
    left_project = _branch_project_node(self, node, node.left_relation_names)
    right_project = _branch_project_node(self, node, node.right_relation_names)
    leg_coerced_types = None
    if left_project is not None and right_project is not None:
        left_cols, right_cols = left_project.columns, right_project.columns
        if len(left_cols) == len(right_cols):
            leg_coerced_types = []
            for left_col, right_col in zip(left_cols, right_cols):
                left_sc = getattr(left_col, "schema_column", None)
                right_sc = getattr(right_col, "schema_column", None)
                left_type = left_sc.column_type if left_sc is not None else None
                right_type = right_sc.column_type if right_sc is not None else None
                leg_coerced_types.append(_set_op_common_type(left_type, right_type))
            _coerce_branch_to(self, left_project, context, leg_coerced_types)
            _coerce_branch_to(self, right_project, context, leg_coerced_types)

    for relation in node.right_relation_names:
        context.schemas.pop(relation, None)
    context.relations = {n: "union" for n in node.left_relation_names}

    if len(node.columns) == 1 and node.columns[0].node_type == NodeType.WILDCARD:
        columns = []
        for schema_name in node.left_relation_names:
            for schema_column in context.schemas[schema_name].columns:
                columns.append(
                    LogicalColumn(
                        node_type=NodeType.IDENTIFIER,  # column type
                        source_column=schema_column.name,  # the source column
                        schema_column=schema_column,
                    )
                )
        node.columns = columns

    from opteryx.planner.binder.project import visit_exit

    node, context = visit_exit(self, node, context)

    # AFTER visit_exit, which is what makes these columns this node's own — see the
    # precondition in _retype_declared_columns. It is also after the wildcard
    # expansion above, so a wildcard union's columns (schema order, which is the
    # order the branch node below a wildcard reports too) line up with the leg
    # types positionally.
    if leg_coerced_types is not None:
        _retype_declared_columns(node.columns, context, leg_coerced_types)
        _publish_declared_columns(node.columns, exit_columns)

    return node, context


def visit_intersect(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    # Every non-ALL INTERSECT is a semi join, and the rewrite runs here rather than
    # pre-bind because the ON condition pairs the legs' output columns positionally
    # — see `_rewrite_setop_to_join`. The wildcard case is not special any more; it
    # takes the same path as every other projection.
    #
    # INTERSECT ALL has no join-based rewrite here: multiset semantics need each
    # row's occurrence index, which plan_rewriter's
    # `intersect_except_all_to_window_join` supplies by inserting a ROW_NUMBER
    # Window into the plan — a structural change the binder cannot make mid-
    # traversal (it visits bottom-up; a node inserted below it is never bound).
    #
    # AHEAD of the column-count check, which resolves each leg the older, less
    # direct way (`_columns_for_side`, summing per-relation schemas). The rewrite
    # declines an unequal pairing rather than zipping it short, so a genuine
    # mismatch still lands on the validator and is still reported there.
    if node.modifier != "All":
        rewritten = _rewrite_setop_to_join(self, node, context, "left semi not-distinct")
        if rewritten is not None:
            return rewritten

    _validate_set_operation_types(self, node, context, "INTERSECT")

    is_wildcard = len(node.columns) == 1 and node.columns[0].node_type == NodeType.WILDCARD

    for relation in node.right_relation_names:
        context.schemas.pop(relation, None)
    context.relations = {n: "intersect" for n in node.left_relation_names}

    if is_wildcard:
        columns = []
        for schema_name in node.left_relation_names:
            for schema_column in context.schemas[schema_name].columns:
                columns.append(
                    LogicalColumn(
                        node_type=NodeType.IDENTIFIER,
                        source_column=schema_column.name,
                        schema_column=schema_column,
                    )
                )
        node.columns = columns

    from opteryx.planner.binder.project import visit_exit

    node, context = visit_exit(self, node, context)
    return node, context


def visit_except(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    # See the matching comment in visit_intersect — same reasoning, and the same
    # ordering ahead of the count check. "left anti" instead of "left semi", and
    # EXCEPT ALL falls through for the same reason INTERSECT ALL does.
    if node.modifier != "All":
        rewritten = _rewrite_setop_to_join(self, node, context, "left anti not-distinct")
        if rewritten is not None:
            return rewritten

    _validate_set_operation_types(self, node, context, "EXCEPT")

    is_wildcard = len(node.columns) == 1 and node.columns[0].node_type == NodeType.WILDCARD

    for relation in node.right_relation_names:
        context.schemas.pop(relation, None)
    context.relations = {n: "except" for n in node.left_relation_names}

    if is_wildcard:
        columns = []
        for schema_name in node.left_relation_names:
            for schema_column in context.schemas[schema_name].columns:
                columns.append(
                    LogicalColumn(
                        node_type=NodeType.IDENTIFIER,
                        source_column=schema_column.name,
                        schema_column=schema_column,
                    )
                )
        node.columns = columns

    from opteryx.planner.binder.project import visit_exit

    node, context = visit_exit(self, node, context)
    return node, context


# Python element type -> ColumnType for a LITERAL array's elements. `bool` must
# precede `int` conceptually — the lookup is by EXACT type, never isinstance, so a
# bool never resolves as an integer.
_LITERAL_ELEMENT_TYPES = {
    bool: _lt.BOOLEAN,
    int: _lt.INT64,
    float: _lt.FLOAT64,
    str: _lt.VARCHAR,
    bytes: _lt.VARBINARY,
}


def _literal_array_element_type(values):
    """ColumnType of a literal array's elements, inferred from the first non-NULL
    value. Mixed or unrecognised element types stay VARIANT (fail late, not wrong)."""
    if not isinstance(values, (list, tuple, set)):
        return _lt.VARIANT
    for value in values:
        if value is None:
            continue
        return _LITERAL_ELEMENT_TYPES.get(type(value), _lt.VARIANT)
    return _lt.VARIANT


def visit_unnest(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    node.columns = []

    # we create a new schema for the unnested column
    unnest_schema = node.alias

    if node.unnest_function == "CIDR_UNNEST":
        # CIDR_UNNEST's output type is FIXED at IPV4 whatever its input, so unlike
        # UNNEST there is no element type to infer and no VARIANT fallback. The
        # IPV4 descriptor is load-bearing rather than cosmetic: without it the
        # column renders as an integer and CIDR_AGG refuses to take it back, so
        # the round trip would not close.
        from opteryx.exceptions import IncorrectTypeError
        from opteryx.types.schema import mint_column_identity

        # A literal source is bound like any other expression rather than
        # special-cased. That keeps ONE compile path: the literal becomes a
        # projected constant column and streams through the same operator, so
        # nothing is materialized at plan time (a literal /0 would be 4.3 billion
        # addresses) and the IPV4 descriptor is attached by the operator that
        # generates the values.
        from opteryx.planner.binder.binder import inner_binder

        node.unnest_column, context = inner_binder(node.unnest_column, context)
        category = node.unnest_column.schema_column.category
        if category not in (None, LogicalCategory.VARCHAR, LogicalCategory.NVARCHAR,
                            LogicalCategory.NULL):
            raise IncorrectTypeError(
                "**CROSS JOIN CIDR_UNNEST** requires a text CIDR block such as "
                f"'10.0.0.0/24', not {category}."
            )
        node.columns += [node.unnest_column]

        schema_column = SchemaColumn(
            name=node.unnest_alias,
            column_type=_lt.IPV4,
            identity=mint_column_identity(node.unnest_alias, node.unnest_alias),
        )
        node.unnest_target = LogicalColumn(
            alias=node.unnest_alias,
            node_type=NodeType.IDENTIFIER,
            source_column=node.unnest_alias,
            source=unnest_schema,
            schema_column=schema_column,
        )
        context.schemas[unnest_schema] = RelationSchema(
            name=unnest_schema, columns=[schema_column]
        )
        node.columns.append(node.unnest_target)
        return node, context

    # this is the column which is being unnested
    if node.unnest_column.node_type == NodeType.LITERAL:
        # Phase 2: node.unnest_column.type is ARRAY(element) ColumnType.
        # UNNEST produces the element type as the output column.
        arr_ct = node.unnest_column.type
        if isinstance(arr_ct, ColumnType) and arr_ct.element is not None:
            elem_ct = arr_ct.element
        else:
            # A literal array carries no declared element type. Infer it from the
            # values: VARIANT has no physical vector type, so leaving it VARIANT
            # makes the unnested column unmaterializable (and unfilterable)
            # downstream. Falls back to VARIANT only when nothing is inferable.
            elem_ct = _literal_array_element_type(node.unnest_column.value)
        schema_column = ConstantColumn(
            name=node.unnest_alias,
            column_type=elem_ct,
            value=node.unnest_column.value,
        )
        node.unnest_target = LogicalColumn(
            alias=node.unnest_alias,
            node_type=NodeType.IDENTIFIER,
            source_column=node.unnest_alias,
            source=unnest_schema,
            schema_column=schema_column,
        )
        # create the schema for the unnested column
        context.schemas[unnest_schema] = RelationSchema(name=unnest_schema, columns=[schema_column])
        # reference the new column in the node
        node.columns.append(node.unnest_target)
    else:
        from opteryx.planner.binder.binder import inner_binder

        node.unnest_column, context = inner_binder(node.unnest_column, context)
        node.columns += [node.unnest_column]

        # The source array must survive the bind-time schema narrowing even when no
        # projection or aggregate names it. UNNEST reads it STRUCTURALLY — the output
        # row count is the sum of its array lengths — so `SELECT COUNT(*) FROM t CROSS
        # JOIN UNNEST(arr) AS v`, which references no column anywhere, still depends on
        # it entirely. Without this the narrowing in aggregate.py/project.py dropped
        # the column and the compiler refused the query outright ("a CROSS JOIN UNNEST
        # source array the engine could not resolve here").
        #
        # Every identifier the source expression reads is retained, not just a bare
        # column: the source can be computed (`UNNEST(SPLIT(s, ','))`), and it is `s`
        # that has to reach the scan.
        for identifier in get_all_nodes_of_type(node.unnest_column, (NodeType.IDENTIFIER,)):
            if identifier.schema_column is not None:
                context.retained_columns.add(identifier.schema_column.identity)

        # we can only UNNEST an ARRAY type column, we need to find it before we know its type
        if node.unnest_column.schema_column.category not in (
            None,
            LogicalCategory.ARRAY,
            LogicalCategory.VECTOR,
            LogicalCategory.NULL,
        ):
            from opteryx.exceptions import IncorrectTypeError

            raise IncorrectTypeError(
                f"**CROSS JOIN UNNEST** requires an ARRAY or VECTOR type column, not {node.unnest_column.schema_column.category}."
            )

        # Phase 2: resolve UNNEST element type from column_type (ARRAY carries element).
        # VECTOR unnests to FLOAT64. Unknown arrays produce VARCHAR.
        unnest_sc = node.unnest_column.schema_column
        if unnest_sc and unnest_sc.category == LogicalCategory.VECTOR:
            elem_ct_unnest = _lt.FLOAT64
        elif (
            unnest_sc is not None
            and unnest_sc.column_type is not None
            and unnest_sc.column_type.element is not None
        ):
            elem_ct_unnest = unnest_sc.column_type.element
        else:
            elem_ct_unnest = _lt.VARCHAR

        from opteryx.types.schema import mint_column_identity
        schema_column = SchemaColumn(name=node.unnest_alias, column_type=elem_ct_unnest, identity=mint_column_identity(node.unnest_alias, node.unnest_alias))
        node.unnest_target = LogicalColumn(
            alias=node.unnest_alias,
            node_type=NodeType.IDENTIFIER,
            source_column=node.unnest_alias,
            source=unnest_schema,
            schema_column=schema_column,
        )

        # create the schema for the unnested column
        context.schemas[unnest_schema] = RelationSchema(name=unnest_schema, columns=[schema_column])

        # reference the new column in the node
        node.columns.append(node.unnest_target)

    return node, context
