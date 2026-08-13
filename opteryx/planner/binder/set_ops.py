# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

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
from opteryx.types.schema import ConstantColumn, SchemaColumn, RelationSchema


def visit_set(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    node.variables = context.execution_context.variables
    node.columns = []
    return node, context


def _build_setop_on_condition(
    left_relations: List[str],
    right_relations: List[str],
    col_names: List[str],
) -> Node:
    """AND-tree of equality conditions covering every (left_rel, right_rel, col)
    triple, unbound (source/source_column only) — mirrors
    plan_rewriter.strategies.intersect_to_inner_join._build_on_condition /
    except_to_anti_join._build_on_condition exactly, so that delegating to
    visit_join below resolves it exactly as it would a hand-written ON clause.

    Not imported from plan_rewriter: binder and plan_rewriter are sibling
    planning phases with no existing cross-import in either direction (plan_rewriter
    runs BEFORE binding; reaching backward into it here would be a new, backwards
    layering dependency for one small helper). The two plan_rewriter copies
    already don't share this with each other either, so a third copy here follows
    the codebase's existing convention for this specific helper, not a new one.
    """
    conditions = []
    for left_rel in left_relations:
        for right_rel in right_relations:
            for col_name in col_names:
                eq = Node(
                    node_type=NodeType.COMPARISON_OPERATOR,
                    value="Eq",
                    do_not_create_column=True,
                )
                eq.left = LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source=left_rel,
                    source_column=col_name,
                )
                eq.right = LogicalColumn(
                    node_type=NodeType.IDENTIFIER,
                    source=right_rel,
                    source_column=col_name,
                )
                conditions.append(eq)

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


def _rewrite_wildcard_setop_to_join(
    self, node: Node, context: BindingContext, join_type: str
) -> Tuple[Node, BindingContext]:
    """Convert a wildcard (`SELECT * INTERSECT/EXCEPT SELECT *`) set-op node to a
    `left semi` / `left anti` Join, at bind time, then delegate to visit_join.

    plan_rewriter.strategies.intersect_to_inner_join / except_to_anti_join already
    perform this exact rewrite, but only pre-bind, and explicitly skip wildcard
    projections: column names aren't resolvable before the binder has fetched each
    relation's schema (see those modules' docstrings — "The binder expands
    wildcards and handles those nodes directly"). That claim was only half true:
    visit_intersect/visit_except previously expanded the wildcard's `.columns`
    but left `node.node_type` as Intersect/Except, which physical_planner has no
    builder for — `SELECT * INTERSECT SELECT *` reached physical planning and
    failed with `InvalidInternalStateError: Unexpected logical node encountered`.

    Runs BEFORE this function pops `right_relation_names`' schemas (unlike the
    ordinary wildcard-expansion path below) because visit_join's own
    inner_binder(node.on, ...) call needs both sides' schemas present to resolve
    the ON condition, exactly as it would for a hand-written ON clause.

    Known gap: uses `node.left_relation_names`/`right_relation_names` directly,
    not reduced via `_set_op_join_common.live_relations` the way the pre-bind
    rewrite is (that reduction exists for CHAINED/nested set-ops, where a nested
    set-op's own legs must not be double-counted). A single non-nested wildcard
    set-op is unaffected; a chained wildcard case
    (`SELECT * FROM a INTERSECT SELECT * FROM b INTERSECT SELECT * FROM c`) is not
    verified against this path and may misbehave — flagged rather than solved
    speculatively, matching how the parallel UNION-side gap was handled.
    """
    col_names = []
    for schema_name in node.left_relation_names:
        col_names.extend(schema_column.name for schema_column in context.schemas[schema_name].columns)

    on_condition = _build_setop_on_condition(
        node.left_relation_names,
        node.right_relation_names,
        col_names,
    )

    join_node = LogicalPlanNode(node_type=LogicalPlanStepType.Join)
    join_node.type = join_type
    join_node.on = on_condition
    join_node.using = None
    join_node.left_relation_names = node.left_relation_names
    join_node.right_relation_names = node.right_relation_names
    join_node.columns = []

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
        stack = [child_nid]
        seen = set()
        matched = False
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            cur_node = graph[cur]
            if getattr(cur_node, "alias", None) in rel_set:
                matched = True
                break
            for upstream_nid, _, _ in graph.ingoing_edges(cur):
                stack.append(upstream_nid)
        if matched:
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
        stack = [child_nid]
        seen = set()
        matched = False
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            cur_node = graph[cur]
            if getattr(cur_node, "alias", None) in rel_set:
                matched = True
                break
            for upstream_nid, _, _ in graph.ingoing_edges(cur):
                stack.append(upstream_nid)
        if matched:
            descent = [child_nid]
            descent_seen = set()
            while descent:
                cur = descent.pop(0)
                if cur in descent_seen:
                    continue
                descent_seen.add(cur)
                cur_node = graph[cur]
                if any(getattr(col, "schema_column", None) is not None for col in (cur_node.columns or [])):
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
        # A matching CATEGORY is enough only for types whose tag tells the whole
        # story (binder._descriptor_carries_meaning). Legs at DECIMAL(10,2) and
        # DECIMAL(10,4), or at timestamp[ms] and timestamp[us], all passed this
        # guard UNCAST and then concatenated raw payloads under ONE declared
        # scale/unit: SUM over the decimal union was silently 100x wrong, and the
        # timestamp pair hit Morsel.combine's "mismatched unit/offset_minutes".
        # Those legs now go through a real rescale cast.
        if current_type is not None and current_type.category == target.category:
            if current_type == target or not _descriptor_carries_meaning(target):
                continue
        if col.node_type == NodeType.LITERAL and col.value is None:
            col.type = target
            schema_column.column_type = target
            continue
        columns[i] = _bound_cast_node(col, target)


def _validate_set_operation_types(
    self,
    node: Node,
    context: BindingContext,
    operation_name: str = "SET OPERATION",
) -> list:
    """Validate and find compatible types for columns in set operations.

    For each column position across left and right relations, find a compatible type.
    Returns list of coerced ColumnTypes in column order (None where type is unresolvable).
    """
    left_columns = _columns_for_side(self, node, node.left_relation_names, context)
    right_columns = _columns_for_side(self, node, node.right_relation_names, context)

    if len(left_columns) != len(right_columns):
        raise ValueError(
            f"{operation_name}: column count mismatch — left has {len(left_columns)}, right has {len(right_columns)}"
        )

    coerced_types = []
    for left_col, right_col in zip(left_columns, right_columns):
        coerced_type = find_compatible_type([left_col.column_type, right_col.column_type])
        coerced_types.append(coerced_type)

    return coerced_types


def visit_union(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    # Validate and determine coerced types for UNION/INTERSECT/EXCEPT
    coerced_types = _validate_set_operation_types(self, node, context, "UNION")
    node.coerced_types = coerced_types

    # Physically enforce the coercion: the executor concatenates each leg's
    # columns by position with no type check of its own (UnionNode just selects
    # column indices into a shared buffer — see compiler.py's UnionNode handling),
    # so two legs whose actual column types differ (most commonly: a NULL literal
    # on one side, a real typed column on the other) crash at morsel-combine time.
    # `coerced_types` was computed but never applied anywhere else in the
    # codebase.
    #
    # NOT reused here: `coerced_types` above is built from `_columns_for_side`,
    # which (in its primary, non-fallback path) sums `context.schemas[rel].columns`
    # — RELATION SCHEMA order, not the branch's actual SELECT-list order. Those
    # only coincide when a branch happens to project columns in the underlying
    # table's declared order; `SELECT name, id FROM $planets` (name before id,
    # the schema's order is id-then-name) already disagrees. Applying that
    # mis-ordered list positionally against `project.columns` silently pairs the
    # wrong target type with the wrong column — caught by `make q` regressing
    # `... UNION ...` with a `name, id` projection into an
    # "Invalid digit in integer literal" CAST failure.
    #
    # Recompute per-position types directly from each side's own located Project
    # (`_branch_project_node`), which is unambiguously in the branch's real
    # output order. Best-effort: only touches a leg whose own Project node can
    # be confidently located AND whose sibling side is too — anything else is
    # left exactly as before, i.e. no behaviour change beyond fixing the
    # concrete mismatch case.
    left_project = _branch_project_node(self, node, node.left_relation_names)
    right_project = _branch_project_node(self, node, node.right_relation_names)
    if left_project is not None and right_project is not None:
        left_cols, right_cols = left_project.columns, right_project.columns
        if len(left_cols) == len(right_cols):
            leg_coerced_types = []
            for left_col, right_col in zip(left_cols, right_cols):
                left_sc = getattr(left_col, "schema_column", None)
                right_sc = getattr(right_col, "schema_column", None)
                left_type = left_sc.column_type if left_sc is not None else None
                right_type = right_sc.column_type if right_sc is not None else None
                leg_coerced_types.append(find_compatible_type([left_type, right_type]))
            _cast_leg_columns_to(left_cols, leg_coerced_types)
            _cast_leg_columns_to(right_cols, leg_coerced_types)

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
    return node, context


def visit_intersect(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    # Validate and determine coerced types for INTERSECT
    coerced_types = _validate_set_operation_types(self, node, context, "INTERSECT")
    node.coerced_types = coerced_types

    is_wildcard = len(node.columns) == 1 and node.columns[0].node_type == NodeType.WILDCARD

    # Delegate wildcard, non-ALL INTERSECT to the semi-join rewrite BEFORE popping
    # right_relation_names' schemas below — visit_join needs both sides present to
    # resolve the ON condition. INTERSECT ALL has no join-based rewrite (multiset
    # semantics; matches plan_rewriter.strategies.intersect_to_inner_join's own
    # exclusion) — it falls through unchanged and still fails loud at physical
    # planning, exactly as before this fix, by design, not a regression.
    if is_wildcard and node.modifier != "All":
        return _rewrite_wildcard_setop_to_join(self, node, context,
                                              "left semi not-distinct")

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
    # Validate and determine coerced types for EXCEPT
    coerced_types = _validate_set_operation_types(self, node, context, "EXCEPT")
    node.coerced_types = coerced_types

    is_wildcard = len(node.columns) == 1 and node.columns[0].node_type == NodeType.WILDCARD

    # See the matching comment in visit_intersect — same reasoning, "left anti"
    # instead of "left semi". EXCEPT ALL falls through unchanged for the same
    # multiset-semantics reason.
    if is_wildcard and node.modifier != "All":
        return _rewrite_wildcard_setop_to_join(self, node, context,
                                              "left anti not-distinct")

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
