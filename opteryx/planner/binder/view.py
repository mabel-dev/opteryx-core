# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.expression import NodeType
from opteryx.models import LogicalColumn
from opteryx.models import Node
from opteryx.planner.binder.binding_context import BindingContext


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
    """Bind SHOW SNAPSHOTS FOR: consume the commit history the Scan below
    already fetched (visit_scan populates context.snapshots for a
    `for_snapshots_only` Scan, gated at READ — see dataset.py) and fix the
    output to the history's own schema, never the scanned relation's column
    schema, which is unrelated.
    """
    from opteryx.exceptions import UnsupportedSyntaxError
    from opteryx.models.snapshot_history import snapshots_output_schema

    if context.schema_only:
        # The history IS this statement's result, and a schema-only bind
        # deliberately did not read one — the same reasoning as SHOW MANIFEST:
        # the output schema is fixed, so the statement checks clean and only the
        # rows (and with them the row-count estimate) are left unknown.
        node.schema = snapshots_output_schema(node.relation)
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
        node.schema = snapshots_output_schema(node.relation)
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


# What SHOW CREATE asks for, per object type - see `visit_show`. TABLE is
# absent deliberately and falls through to READ.
_SHOW_CREATE_ACTIONS = {
    "VIEW": "WRITE",
    "MATERIALIZED VIEW": "WRITE",
    "TASK": "AUTOMATE",
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


def visit_create_view(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the CREATE VIEW node to determine which connector should handle
    storing the view definition.

    This uses the same logic as visit_scan to determine the appropriate connector
    based on the view name.
    """
    from opteryx.connectors import connector_factory
    from opteryx.managers.permissions import can_perform_action

    # Get connector gateway (cached by prefix)
    node.connector = connector_factory(node.view_name, telemetry=context.telemetry)

    # Ensure this user can write to the view location
    if not can_perform_action(context.execution_context, node.view_name, action="WRITE"):
        raise PermissionError(f"User does not have permission to create view {node.view_name}")

    if "variables" in dir(node.connector):
        node.connector.variables = context.execution_context.variables

    node.columns = []
    return node, context


def visit_alter_view(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the ALTER VIEW node to determine which connector should handle
    updating the view definition.

    This uses the same logic as visit_scan to determine the appropriate connector
    based on the view name.
    """
    from opteryx.connectors import connector_factory
    from opteryx.managers.permissions import can_perform_action

    # Get connector gateway (cached by prefix)
    node.connector = connector_factory(node.view_name, telemetry=context.telemetry)

    # Ensure this user can write to the view location
    if not can_perform_action(context.execution_context, node.view_name, action="WRITE"):
        raise PermissionError(f"User does not have permission to alter view {node.view_name}")

    if "variables" in dir(node.connector):
        node.connector.variables = context.execution_context.variables

    node.columns = []
    return node, context


def visit_drop_view(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the DROP VIEW node to determine which connector should handle
    removing the view definition(s).

    Since DROP VIEW can operate on multiple views, we need to check permissions
    and determine connectors for each view.
    """
    from opteryx.connectors import connector_factory
    from opteryx.managers.permissions import can_perform_action

    # Store connectors for each view to be dropped
    node.connectors = {}

    for view_name in node.view_names:
        # Get connector gateway (cached by prefix)
        connector = connector_factory(view_name, telemetry=context.telemetry)

        # WRITE, matching CREATE VIEW and ALTER VIEW: a view is text, and dropping
        # one destroys nothing that cannot be recreated from it, which is the
        # reason DROP is owner-only for tables and not a reason here. A writer
        # who may replace a view's definition may remove it.
        if not can_perform_action(context.execution_context, view_name, action="WRITE"):
            raise PermissionError(
                f"User does not have permission to drop view {view_name} (write required)"
            )

        if "variables" in dir(connector):
            connector.variables = context.execution_context.variables

        node.connectors[view_name] = connector

    node.columns = []
    return node, context
