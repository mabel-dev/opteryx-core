# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.exceptions import SqlError
from opteryx.exceptions import md_table
from opteryx.expression import NodeType
from opteryx.models import LogicalColumn
from opteryx.models import Node
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.models import current_name_of


def visit_show_columns(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    node.schema = context.schemas[node.relation]
    node.columns = []
    for schema_column in node.schema.columns:
        column_reference = LogicalColumn(
            node_type=NodeType.IDENTIFIER,  # column type
            source_column=schema_column.name,  # the source column
            source=node.relation,  # the source relation
            schema_column=schema_column,
        )
        node.columns.append(column_reference)
    return node, context


def visit_show_manifest(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind SHOW MANIFEST FOR: consume the Manifest the Scan below already
    loaded (visit_scan populates context.manifests, gated on the owner-only
    MANIFEST permission — see dataset.py's for_manifest_only check) and fix
    the output to the manifest's own schema, never the scanned dataset's
    column schema, which is unrelated.
    """
    from opteryx.exceptions import UnsupportedSyntaxError
    from opteryx.models.manifest_io import manifest_output_schema

    if context.schema_only:
        # The Manifest IS this statement's result, and a schema-only bind deliberately
        # did not read one. That is not a reason to refuse: the SHAPE of the answer is
        # `manifest_output_schema`, which is fixed and knowable without reading
        # anything, and the shape is all a check is being asked for. Only the ROWS are
        # unknowable here, and no caller of a schema-only bind executes the plan, so
        # `node.manifest` is left unset rather than faked.
        #
        # Refusing instead reported a valid statement as an error, and "no manifest
        # support" - the message below - would have been a true-sounding sentence
        # about the wrong thing.
        node.schema = manifest_output_schema(node.relation)
    else:
        manifest = context.manifests.get(node.relation)
        if manifest is None:
            raise UnsupportedSyntaxError(
                f"'{node.relation}' has no manifest support (its connector does not "
                "expose file-level metadata)."
            )
        node.manifest = manifest
        node.schema = manifest_output_schema(node.relation)
    node.columns = []
    for schema_column in node.schema.columns:
        column_reference = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source_column=schema_column.name,
            source=node.relation,
            schema_column=schema_column,
        )
        node.columns.append(column_reference)
    return node, context


def visit_show_snapshots(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind SHOW [ALL] SNAPSHOTS FOR: consume the commit history the Scan below
    already fetched (visit_scan populates context.snapshots for a
    `for_snapshots_only` Scan, gated at READ — or at MANIFEST for the ALL form,
    see dataset.py) and fix the output to the history's own schema, never the
    scanned relation's column schema, which is unrelated.

    `history_view` is the planner's, set on this node and on the Scan from one
    value, so the schema fixed here and the rows the Scan loaded are the same
    shape by construction.
    """
    from opteryx.exceptions import UnsupportedSyntaxError
    from opteryx.models.snapshot_history import snapshots_output_schema

    include_expiry = getattr(node, "history_view", None) == "snapshots_all"

    if context.schema_only:
        # The history IS this statement's result, and a schema-only bind
        # deliberately did not read one — the same reasoning as SHOW MANIFEST:
        # the output schema is fixed, so the statement checks clean and only the
        # rows (and with them the row-count estimate) are left unknown.
        node.schema = snapshots_output_schema(node.relation, include_expiry=include_expiry)
    else:
        snapshots = context.snapshots.get(node.relation)
        if snapshots is None:
            # None is "this connector keeps no commit log", which is not the same
            # answer as an empty list ("it does, and nothing has been committed").
            raise UnsupportedSyntaxError(
                f"'{node.relation}' has no snapshot history (its connector does not "
                "keep a commit log)."
            )
        node.snapshots = snapshots
        node.schema = snapshots_output_schema(node.relation, include_expiry=include_expiry)
        node.schema.row_count_estimate = len(snapshots)
    node.columns = []
    for schema_column in node.schema.columns:
        column_reference = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source_column=schema_column.name,
            source=node.relation,
            schema_column=schema_column,
        )
        node.columns.append(column_reference)
    return node, context


def visit_show_lineage(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind SHOW LINEAGE FOR: consume the receipts the Scan below already
    fetched (visit_scan populates context.snapshots for a `for_snapshots_only`
    Scan whose `history_view` is "lineage", gated at READ on the relation) and
    fix the output to the lineage shape.

    EVERY SOURCE IS NAMED (decision 2026-09-09). The gate is READ on the
    relation being asked about, and nothing further: a name in a receipt is a
    citation, not access, and the caller still needs a grant of their own to
    read anything it names. The binder used to null each name the caller could
    not READ; it no longer asks, because an answer that will not say what a
    table was built from does not answer the question, and the same names are
    what make impact analysis worth having.
    """
    from opteryx.exceptions import UnsupportedSyntaxError
    from opteryx.models.lineage_history import lineage_output_schema

    if context.schema_only:
        # The shape is fixed and knowable without reading anything; only the
        # rows are unknowable in a schema-only bind - as for SHOW SNAPSHOTS.
        node.schema = lineage_output_schema(node.relation)
    else:
        rows = context.snapshots.get(node.relation)
        if rows is None:
            raise UnsupportedSyntaxError(
                f"'{node.relation}' has no lineage (its connector does not keep a "
                "commit log)."
            )
        node.lineage = rows
        node.schema = lineage_output_schema(node.relation)
        node.schema.row_count_estimate = len(rows)
    node.columns = []
    for schema_column in node.schema.columns:
        column_reference = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source_column=schema_column.name,
            source=node.relation,
            schema_column=schema_column,
        )
        node.columns.append(column_reference)
    return node, context


def visit_show_sources(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind SHOW SOURCES FOR: consume the standing source list the Scan below
    already read off the dataset (visit_scan, `history_view` "sources") and
    fix the output to the source-list shape. Every source is named, as for
    SHOW LINEAGE.
    """
    from opteryx.exceptions import UnsupportedSyntaxError
    from opteryx.models.source_list import sources_output_schema

    if context.schema_only:
        node.schema = sources_output_schema(node.relation)
    else:
        rows = context.snapshots.get(node.relation)
        if rows is None:
            raise UnsupportedSyntaxError(
                f"'{node.relation}' has no source list (its connector does not "
                "keep a commit log)."
            )
        node.sources = rows
        node.schema = sources_output_schema(node.relation)
        node.schema.row_count_estimate = len(rows)
    node.columns = []
    for schema_column in node.schema.columns:
        column_reference = LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source_column=schema_column.name,
            source=node.relation,
            schema_column=schema_column,
        )
        node.columns.append(column_reference)
    return node, context


# What SHOW CREATE asks for, per object type - see `visit_show`. TABLE is
# absent deliberately and falls through to READ.
_SHOW_CREATE_ACTIONS = {
    "VIEW": "WRITE",
    "MATERIALIZED VIEW": "WRITE",
    "TASK": "AUTOMATE",
    # Same tier as TASK: a trigger's definition includes the identity its
    # unattended runs carry, which is exactly what makes a task's definition
    # AUTOMATE-gated.
    "TRIGGER": "AUTOMATE",
}


def visit_show(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind SHOW CREATE.

    Gated per object type, at the tier of whoever authors that kind of thing -
    the same tiers `information_schema` shows the same text at, so there is no
    side door where a definition the listing withholds is one statement away:

      TABLE - READ. The DDL is the column list, the declared relationships and
        the clustering, all of which information_schema already answers at
        READ. It is NOT the manifest: no file path or storage layout is
        disclosed here, which is what puts SHOW MANIFEST at owner instead.
      VIEW, MATERIALIZED VIEW - WRITE. The definition is SQL that names the
        relations it reads, which the caller may hold no grant on: the binder
        refuses to RUN such a view for them, so the text is not "a read they
        may already make". A writer may replace the definition, so a writer
        may see it.
      TASK - AUTOMATE. A task is automation; only an owner may create, drop or
        alter one, and only an owner may read what it runs.
      TRIGGER - AUTOMATE. A trigger's definition names the identity its
        unattended runs carry, same sensitivity as a task's.

    Without this the statement reached its operator with no authorization at
    all, because a node type with no visitor was silently passed through (see
    BinderVisitor.visit_node).
    """
    from opteryx.connectors import connector_factory
    from opteryx.managers.permissions import can_perform_action

    action = _SHOW_CREATE_ACTIONS.get(node.object_type, "READ")
    if not can_perform_action(context.execution_context, node.object_name, action=action):
        raise PermissionError(
            f"User does not have permission to show the definition of "
            f"{node.object_type.lower()} {node.object_name} ({action.lower()} required)"
        )

    if node.object_type == "VIEW":
        # Read back from where CREATE VIEW wrote it - the view store, not the
        # workspace's data binding. A MATERIALIZED VIEW is storage and stays on
        # the data binding with the rest of the object types.
        node.connector = _view_store(node.object_name, context)
    else:
        node.connector = connector_factory(node.object_name, telemetry=context.telemetry)

    # Every object type but VIEW is read back through the Writable capability -
    # the definition stores hang off it - so a connector without it cannot
    # answer, and must say so rather than fail later with a missing attribute.
    if node.object_type != "VIEW":
        from opteryx.connectors.capabilities import Writable
        from opteryx.exceptions import UnsupportedSyntaxError

        if not isinstance(node.connector, Writable):
            raise UnsupportedSyntaxError(
                f"connector for {node.object_name} cannot show a "
                f"**{node.object_type}** definition."
            )

    node.columns = []
    return node, context


def _view_output_schema(node, context: BindingContext):
    """The RelationSchema a view's defining statement produces.

    Stored on the view so a catalog reader - `information_schema`, the OData
    metadata document - can describe a view's columns without planning its SQL.
    That means TYPES as well as names, which is why this is a bind and not a
    read of the projection: a name carries no type, and a consumer that needs
    one would have to invent it.

    Bound from the SQL TEXT the statement is about to store, not from the AST in
    hand. That text is what gets planned every time the view is read, so binding
    anything else would describe a statement the catalog does not hold.

    `schema_only=True` because this needs names and types and nothing a name
    cannot be resolved without - in particular not each relation's Manifest,
    which is the larger of binding's two cloud reads and describes rows, which a
    definition has none of.

    A failure here is NOT swallowed: the sources must resolve, and the creator
    must be able to read them. A view whose shape cannot be determined is one no
    reader can be told the shape of, and recording it anyway would leave the
    catalog holding a definition it silently cannot describe.

    A wildcard is bound like anything else. Its column list is a SNAPSHOT of the
    sources as they are now, and the source gaining a column later leaves this
    stale - the view itself still expands the wildcard at read time, so this can
    misdescribe the view but can never change what it returns.
    """
    from opteryx.planner import bind_statement
    from opteryx.types.schema import RelationSchema
    from opteryx.types.schema import SchemaColumn
    from opteryx.types.schema import mint_column_identity

    bound_plan, _clean_sql, _ast = bind_statement(
        operation=node.view_sql,
        parameters=None,
        # Row visibility filters restrict which ROWS a reader sees; they cannot
        # add, remove or retype a column, so they have no bearing on the shape
        # being recorded here.
        visibility_filters=None,
        execution_context=context.execution_context,
        query_id=context.query_id,
        telemetry=context.telemetry,
        schema_only=True,
    )

    heads = bound_plan.get_exit_points()
    head = bound_plan[heads[0]]

    columns = []
    for column in head.columns:
        # `current_name` is `alias or source_column`, and is recorded as a list
        # when one expression was named more than once - the reader sees the
        # first. This is the name the view answers to, which is not necessarily
        # the bound column's own name.
        name = current_name_of(column)
        if isinstance(name, (list, tuple)):
            name = name[0] if name else None
        name = str(name)
        columns.append(
            SchemaColumn(
                name=name,
                column_type=column.schema_column.column_type,
                nullable=column.schema_column.nullable,
                # A fresh identity, not the bound column's: these describe the
                # VIEW's columns, and the plan they were bound in is discarded
                # here. Reusing an identity from a throwaway plan would hand the
                # catalog a handle onto columns that no longer exist.
                identity=mint_column_identity(node.view_name, name),
            )
        )

    return RelationSchema(name=node.view_name, columns=columns)


def _view_store(view_name: str, context: BindingContext):
    """The connector that stores `view_name`'s definition.

    A view is catalog text, not storage, so it lives in the opteryx catalog
    entry whatever the workspace's DATA is bound to - see
    `view_store_connector` for the ruling. For a workspace with no external
    binding this is the same object `connector_factory` returns.
    """
    from opteryx.connectors import view_store_connector
    from opteryx.connectors.capabilities import Eidetic
    from opteryx.exceptions import ReadOnlyConnectorError

    store = view_store_connector(view_name, telemetry=context.telemetry)
    if not isinstance(store, Eidetic):
        raise ReadOnlyConnectorError(
            f"the catalog serving {md_table(view_name)} cannot store views"
        )
    if "variables" in dir(store):
        store.variables = context.execution_context.variables
    return store


def _assert_name_free_in_source(view_name: str, context: BindingContext) -> None:
    """Refuse a view name that a TABLE in the workspace's DATA binding holds.

    The view store and the data binding can be two different catalogs sharing
    one namespace, and neither can see the other's names - so this is the only
    place the collision can be caught. Left uncaught, the view would shadow the
    table (the resolver resolves views first) and make it unreachable.

    Only a TABLE refuses. A VIEW found here is either the very view being
    replaced - the store and the data binding are frequently the same catalog
    reached through two cache entries, and `is` does not tell you that - or a
    view in the data binding, and either way whether it may be replaced is
    `update_if_exists`'s question, not this one.
    """
    from opteryx.connectors import TableType
    from opteryx.connectors import connector_factory
    from opteryx.exceptions import SqlError
    from opteryx.exceptions import compose

    source = connector_factory(view_name, telemetry=context.telemetry)
    existing_type, _ = source.locate_object(view_name)
    if existing_type != TableType.Table:
        return

    raise SqlError(
        compose(
            f"{md_table(view_name)} already names a table",
            "A view and a table share one namespace, so a name identifies exactly one "
            "of them",
        )
    )


def visit_create_view(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the CREATE VIEW node to determine which connector should handle
    storing the view definition.

    NOT the same connector visit_scan resolves: the definition goes to the
    workspace's VIEW STORE (`_view_store`), which for an externally-bound
    workspace is the opteryx catalog entry rather than the data source.
    """
    from opteryx.managers.permissions import can_perform_action

    # The VIEW STORE, not the data binding: a view is catalog text and is held
    # in the opteryx catalog entry even when the workspace's data lives
    # elsewhere.
    node.connector = _view_store(node.view_name, context)

    # Ensure this user can write to the view location
    if not can_perform_action(context.execution_context, node.view_name, action="WRITE"):
        raise PermissionError(f"User does not have permission to create view {node.view_name}")

    _assert_name_free_in_source(node.view_name, context)

    # Rendered HERE, not in the operator, so the text that is bound below and the
    # text that is stored are the same string by construction rather than by two
    # calls that happen to agree.
    from opteryx.third_party import sqloxide

    if node.query is None:
        raise SqlError("**CREATE VIEW** requires a defining query.")
    node.view_sql = sqloxide.ast_to_sql([{"Query": node.query}])[0]
    node.view_schema = _view_output_schema(node, context)

    node.columns = []
    return node, context


def visit_alter_view(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the ALTER VIEW node to determine which connector should handle
    updating the view definition.

    The VIEW STORE, as CREATE VIEW uses - see `_view_store`.
    """
    from opteryx.managers.permissions import can_perform_action

    # The VIEW STORE - see visit_create_view.
    node.connector = _view_store(node.view_name, context)

    # Ensure this user can write to the view location
    if not can_perform_action(context.execution_context, node.view_name, action="WRITE"):
        raise PermissionError(f"User does not have permission to alter view {node.view_name}")

    # Rendered HERE, not in the operator, so the text that is bound below and the
    # text that is stored are the same string by construction rather than by two
    # calls that happen to agree.
    from opteryx.third_party import sqloxide

    if node.query is None:
        raise SqlError("**ALTER VIEW** requires a defining query.")
    node.view_sql = sqloxide.ast_to_sql([{"Query": node.query}])[0]
    node.view_schema = _view_output_schema(node, context)

    node.columns = []
    return node, context


def visit_drop_view(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the DROP VIEW node to determine which connector should handle
    removing the view definition(s).

    Since DROP VIEW can operate on multiple views, we need to check permissions
    and determine connectors for each view.
    """
    from opteryx.managers.permissions import can_perform_action

    # Store connectors for each view to be dropped
    node.connectors = {}

    for view_name in node.view_names:
        # The VIEW STORE the definition was written to - see visit_create_view.
        connector = _view_store(view_name, context)

        # WRITE, matching CREATE VIEW and ALTER VIEW: a view is text, and dropping
        # one destroys nothing that cannot be recreated from it, which is the
        # reason DROP is owner-only for tables and not a reason here. A writer
        # who may replace a view's definition may remove it.
        if not can_perform_action(context.execution_context, view_name, action="WRITE"):
            raise PermissionError(
                f"User does not have permission to drop view {view_name} (write required)"
            )

        node.connectors[view_name] = connector

    node.columns = []
    return node, context
