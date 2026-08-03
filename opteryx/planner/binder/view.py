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


def visit_show(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind SHOW CREATE VIEW.

    A view's body names the relations it reads and the shape of the query over
    them, so showing it is a read of the view - gated at READ, the same tier as
    selecting from it. Without this the statement reached its operator with no
    authorization at all, because a node type with no visitor was silently
    passed through (see BinderVisitor.visit_node).
    """
    from opteryx.connectors import connector_factory
    from opteryx.managers.permissions import can_perform_action

    if not can_perform_action(context.execution_context, node.object_name, action="READ"):
        raise PermissionError(
            f"User does not have permission to read view {node.object_name}"
        )

    node.connector = connector_factory(node.object_name, telemetry=context.telemetry)

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

        # Ensure this user can drop the view - DROP is owner-only, a writer cannot
        if not can_perform_action(context.execution_context, view_name, action="DROP"):
            raise PermissionError(f"User does not have permission to drop view {view_name}")

        if "variables" in dir(connector):
            connector.variables = context.execution_context.variables

        node.connectors[view_name] = connector

    node.columns = []
    return node, context
