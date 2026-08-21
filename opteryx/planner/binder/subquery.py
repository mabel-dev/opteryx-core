# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import copy
from typing import Tuple

from opteryx.managers.virtual_datasets import derived
from opteryx.models import Node
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.types.schema import RelationSchema


def visit_comment(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the COMMENT node to determine which connector should handle
    storing the comment on the view/table.

    This is a pass-through binder - COMMENT nodes don't need schema resolution,
    but we do need to determine the connector for storage.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action
    from opteryx.managers.virtual_datasets import derived

    # Get connector gateway (cached by prefix)
    node.connector = connector_factory(node.object_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.object_name} does not support COMMENT ON"
        )

    # Ensure this user can write to the object location
    if not can_perform_action(context.execution_context, node.object_name, action="WRITE"):
        raise PermissionError(f"User does not have permission to comment on {node.object_name}")

    if "variables" in dir(node.connector):
        node.connector.variables = context.execution_context.variables

    # COMMENT nodes don't have columns (non-tabular result)
    node.columns = []
    return node, context


def visit_materialized_cte_ref(
    self, node: Node, context: BindingContext
) -> Tuple[Node, BindingContext]:
    """A reference to a shared, materialize-once CTE body.

    The body was bound BEFORE the main plan (see do_bind_phase), and its boundary
    schema is the one set of output columns the single execution produces. Each
    reference exposes that schema under its OWN alias with FRESHLY MINTED column
    identities: identity is the handle the execution stream is keyed by, and a
    self-join of two references must present two distinct relations — sharing the
    body's identities across references is exactly the wrong-answer shape the old
    per-reference copies suffered when a copy leaked an identity.

    `cte_column_map` records reference identity -> body output identity; the plan
    compiler uses it to select-and-rename each reference's columns out of the one
    shared result buffer.
    """
    from opteryx.exceptions import AmbiguousDatasetError
    from opteryx.exceptions import InvalidInternalStateError
    from opteryx.expression import NodeType
    from opteryx.models import LogicalColumn
    from opteryx.types.schema import mint_column_identity

    if node.alias and node.alias.lower() in {r.lower() for r in context.relations}:
        raise AmbiguousDatasetError(dataset=node.alias)

    boundary_schema = context.shared_cte_schemas.get(node.cte_key)
    if boundary_schema is None:
        raise InvalidInternalStateError(
            f"Reference to CTE '{node.cte_name}' found no bound shared body - "
            "shared CTE bodies must be bound before the plan that reads them."
        )

    columns = []
    mapping = {}
    for body_column in boundary_schema.columns:
        out_column = copy.copy(body_column)
        out_column.identity = mint_column_identity(node.alias, body_column.name)
        out_column.origin = [node.alias]
        out_column.aliases = []
        columns.append(out_column)
        mapping[out_column.identity] = body_column.identity

    schema = RelationSchema(name=node.alias, columns=columns)
    context.schemas[node.alias] = schema
    context.relations[node.alias] = "materialized_cte"
    context.manifests[node.alias] = None
    node.schema = schema
    node.cte_column_map = mapping
    if context.schema_only:
        node.unpruned_columns = list(schema.columns)
    # Same contract as a bound Scan: the node reads its whole schema until
    # ProjectionPushdownStrategy narrows `columns`.
    node.columns = [
        LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source_column=column.name,
            source=node.alias,
            schema_column=column,
        )
        for column in schema.columns
    ]
    return node, context


def visit_subquery(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    from opteryx.planner.binder.project import visit_exit

    node, context = visit_exit(self, node, context)

    # Extract the column names to check for duplicates
    column_names = (n.current_name for n in node.columns)
    seen = set()
    duplicates = [name for name in column_names if name in seen or seen.add(name)]  # type: ignore

    # Now you can check if there are any duplicates and take action accordingly
    if duplicates:
        from opteryx.exceptions import AmbiguousIdentifierError

        raise AmbiguousIdentifierError(
            identifier=duplicates,
            message=f"Column name collision in subquery '{node.alias}'; Column(s) {', '.join(duplicates)} is ambiguous in the outer query, use AS to provide unique names for these columns.",
        )

    # A subquery exposes its SELECT list and nothing else. The plan feeding this node
    # can carry MORE than that: the Project emits pass-through columns for ORDER BY and
    # HAVING expressions the SELECT list does not name (see logical_planner), and those
    # are still live in the incoming schemas here because the Filter/Order above the
    # Project reads them. At top level the Exit node prunes them back to the projection;
    # a subquery has no Exit (create_node_relation and the CTE splice both strip it), so
    # without this filter an internal column — `COUNT(*)` from `HAVING COUNT(*) <= n`,
    # or the ORDER BY key — leaked into the subquery's schema and out of the query.
    projected_identities = {column.schema_column.identity for column in node.columns}

    # we sack all the tables we previously knew and create a new set of schemas here
    columns: list = []
    source_relations: list = []
    # One underlying column can be reachable under SEVERAL `context.schemas` keys (the
    # scan's relation and `$project`/`$shared` copies all hold the same object). Every
    # key contributes its origins to the node's source relations, but the schema column
    # itself is recorded once by identity — without that dedup the boundary emitted the
    # column once PER KEY, so the derived relation carried duplicates — invisible while
    # `SELECT *` deduped by identity, an AmbiguousIdentifierError from `s.*` once it
    # stopped.
    schema_columns_by_identity: dict = {}
    for schema in context.schemas.values():
        for schema_column in schema.columns:
            if schema_column.identity not in projected_identities:
                continue
            source_relations.extend(schema_column.origin or [])
            if schema_column.identity not in schema_columns_by_identity:
                schema_columns_by_identity[schema_column.identity] = schema_column

    # ONE OUTPUT COLUMN PER PROJECTION ENTRY, IN PROJECTION ORDER. The projection list
    # drives emission — walking `context.schemas` here instead put the boundary's
    # columns in underlying-schema order, so `SELECT * FROM (SELECT name, id ...) x`
    # came back (id, name). `SELECT id AS x, id` names two columns of the derived
    # relation, not one column carrying an alias — the same for `id AS x, id AS y` and
    # for `n1.n_name AS supp, n2.n_name AS cust` over a self-join, where both legs
    # resolve to one underlying identity. Emitting a single column per identity (with
    # the siblings' names demoted to `aliases`) kept every name RESOLVABLE but left the
    # relation one column short: `SELECT *` over it expands the schema, so the
    # un-aliased copy silently vanished when the query was wrapped.
    #
    # These outputs deliberately SHARE the underlying identity. Identity is the
    # handle the stream is keyed by, and the subquery has no physical operator
    # to duplicate a vector with (the optimizer removes it); the duplication
    # happens where it already happens for the un-nested spelling — the Exit's
    # `add_select`, which is free to point two output names at one input index.
    for projection_column in node.columns:
        schema_column = schema_columns_by_identity.get(projection_column.schema_column.identity)
        if schema_column is None:
            continue
        # The subquery's OUTPUT column carries the user-facing alias as its
        # name. It must be a SEPARATE object from the underlying scan's column:
        # the scan column keeps its physical name (e.g. `id`) so the reader can
        # map the connector's physically-named data back to this identity, while
        # the output column below is renamed to the alias (e.g. `k`) for outer
        # resolution. Mutating the shared column in place renamed the scan column
        # too, leaving the reader unable to find the physical column (it then
        # emitted a NULL placeholder of the wrong width).
        out_column = copy.copy(schema_column)
        projection_column.source = node.alias
        out_column.origin = list(schema_column.origin or []) + [node.alias]

        out_column.name = projection_column.current_name

        if "." in out_column.name:
            # a qualified reference (`t.id`) names the output column `id`
            out_column.name = out_column.name.split(".")[-1]

        # The output name is the only name this column answers to; the
        # underlying column's aliases are not the derived relation's.
        out_column.aliases = []
        columns.append(out_column)

    schema = RelationSchema(name=node.alias, columns=columns)

    # A derived relation exposes ONE name - its alias - and the relations it was
    # built from are not among them. Both dicts are replaced wholesale for the same
    # reason: `traverse` re-attaches the ENCLOSING scope's relations on the way out
    # of the boundary (see traversal.py), so anything left here is an internal name
    # escaping into a scope that cannot address it.
    #
    # This previously popped only the relations still present as `context.schemas`
    # keys, which is not the same set: the Project below this node narrows schemas to
    # the columns it emits, so a relation contributing NO projected column - the `d`
    # of `(SELECT p.id FROM t p, t d WHERE d.id = p.id)` - had already lost its schema
    # and survived the pop. It then collided with the identically-named private alias
    # of a SIBLING derived table and raised a false AmbiguousDatasetError.
    context.schemas = {"$derived": derived.schema(), node.alias: schema}
    context.relations = {node.alias: "subquery"}
    node.schema = schema
    if context.schema_only:
        # What this boundary EXPOSES, before an enclosing projection narrows it. The
        # schema object above is the one that goes into `context.schemas`, so the
        # outer visit_project rebinds its `columns` in place exactly as it does for a
        # Scan's - see the same snapshot in dataset.visit_scan. For a CTE this is the
        # only place the name the reader wrote (`c`, not the spliced `$view-XXXX`)
        # meets the columns it offers.
        node.unpruned_columns = list(schema.columns)
    node.source_relations = set(source_relations)
    return node, context
