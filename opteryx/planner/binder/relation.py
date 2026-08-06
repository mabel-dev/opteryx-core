# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.models import Node
from opteryx.planner.binder.binding_context import BindingContext


def visit_create_relation(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the CREATE TABLE node to determine which connector should handle
    storing the table.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support CREATE TABLE"
        )

    # Same tier as CTAS's fresh-create branch in visit_insert - creating a
    # brand-new relation requires writer or owner, checked here rather than
    # left to the connector (some connectors, e.g. the catalog, auto-vivify
    # the workspace/collection on first write with no gate of their own).
    if not can_perform_action(context.execution_context, node.relation_name, action="CREATE"):
        raise PermissionError(
            f"User does not have permission to create table {node.relation_name}"
        )

    node.columns = []
    return node, context


def visit_drop_relation(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the DROP TABLE node to determine which connectors should handle
    removing the tables.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connectors = {}
    for relation_name in node.relation_names:
        connector = connector_factory(relation_name, telemetry=context.telemetry)
        if not isinstance(connector, Writable):
            raise ReadOnlyConnectorError(
                f"connector for {relation_name} does not support DROP TABLE"
            )

        # Ensure this user can drop the table - DROP is owner-only, a writer cannot
        if not can_perform_action(context.execution_context, relation_name, action="DROP"):
            raise PermissionError(f"User does not have permission to drop table {relation_name}")

        node.connectors[relation_name] = connector

    node.columns = []
    return node, context


def visit_create_collection(
    self, node: Node, context: BindingContext
) -> Tuple[Node, BindingContext]:
    """
    Bind the CREATE COLLECTION node to determine which connector should handle
    creating the collection.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.collection_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.collection_name} does not support CREATE COLLECTION"
        )

    # Creating a collection risks nothing existing, so this is the fresh-create
    # tier a writer holds - NOT the owner tier DROP COLLECTION requires, which
    # exists because dropping destroys. An owner policy covering the workspace
    # (e.g. "workspace.*") matches a 2-part collection name via fnmatch.
    if not can_perform_action(context.execution_context, node.collection_name, action="CREATE"):
        raise PermissionError(
            f"User does not have permission to create collection {node.collection_name}"
        )

    node.columns = []
    return node, context


def visit_drop_collection(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the DROP COLLECTION node to determine which connectors should handle
    removing the collections.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connectors = {}
    for collection_name in node.collection_names:
        connector = connector_factory(collection_name, telemetry=context.telemetry)
        if not isinstance(connector, Writable):
            raise ReadOnlyConnectorError(
                f"connector for {collection_name} does not support DROP COLLECTION"
            )

        # Same tier as DROP TABLE/VIEW - a writer's per-relation grant does not
        # extend to removing the collection itself, only an owner may. An
        # owner policy pattern covering the whole workspace (e.g. "workspace.*")
        # already matches a 2-part collection name via fnmatch, so no new
        # permission mechanism is needed for "owner of the workspace."
        if not can_perform_action(context.execution_context, collection_name, action="DROP"):
            raise PermissionError(
                f"User does not have permission to drop collection {collection_name}"
            )

        node.connectors[collection_name] = connector

    node.columns = []
    return node, context


def visit_alter_relation(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the ALTER TABLE ... CLUSTER BY node to determine which connector
    should handle persisting the new sort order.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support ALTER TABLE"
        )

    # Same tier as DROP - ALTER changes the relation's physical layout, not
    # just its contents, so a writer cannot do it, only an owner can.
    if not can_perform_action(context.execution_context, node.relation_name, action="ALTER"):
        raise PermissionError(
            f"User does not have permission to alter table {node.relation_name}"
        )

    node.columns = []
    return node, context


def visit_rename_relation(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the ALTER TABLE ... RENAME TO node to determine which connector should
    handle moving the relation.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support ALTER TABLE"
        )

    # A rename destroys the source name and creates the target one, so it is
    # gated at both ends: owner on the source (same tier as DROP - the old
    # relation stops existing under that name) and the fresh-create tier on the
    # target, so a writer cannot move a relation into a collection they have no
    # grant on. The workspace is guaranteed unchanged by the logical planner, so
    # both names resolve through the same connector.
    if not can_perform_action(context.execution_context, node.relation_name, action="ALTER"):
        raise PermissionError(
            f"User does not have permission to rename table {node.relation_name}"
        )
    if not can_perform_action(context.execution_context, node.new_relation_name, action="CREATE"):
        raise PermissionError(
            f"User does not have permission to rename table to {node.new_relation_name}"
        )

    node.columns = []
    return node, context


def visit_analyze(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the ANALYZE TABLE / DROP STATISTICS node.

    Both statements share the Analyze node, dispatching on `action`, so they
    share this binder and its permission tier.

    Gated at ALTER (owner), the same tier as ALTER TABLE ... CLUSTER BY: neither
    changes the relation's rows, both rewrite the metadata the optimizer plans
    from, and DROP STATISTICS destroys it outright. Until this existed neither
    statement was authorized at all - the Analyze node had no visitor, and a
    node type with no visitor is silently passed through by
    BinderVisitor.visit_node.
    """
    from opteryx.connectors import connector_factory
    from opteryx.managers.permissions import can_perform_action

    verb = "analyze" if node.action == "analyze_table" else "drop statistics on"

    if not can_perform_action(context.execution_context, node.table_name, action="ALTER"):
        raise PermissionError(f"User does not have permission to {verb} table {node.table_name}")

    node.connector = connector_factory(node.table_name, telemetry=context.telemetry)

    node.columns = []
    return node, context


def visit_alter_workspace(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the ALTER WORKSPACE ... SET node to determine which connector should
    handle persisting the workspace property.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_workspace_action

    node.connector = connector_factory(node.workspace_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.workspace_name} does not support ALTER WORKSPACE"
        )

    # Owner of the whole workspace - a grant covering only part of it is not
    # enough, since these properties govern the workspace entire (see
    # can_perform_workspace_action for why this is not can_perform_action).
    if not can_perform_workspace_action(
        context.execution_context, node.workspace_name, action="ALTER"
    ):
        raise PermissionError(
            f"User does not have permission to alter workspace {node.workspace_name}"
        )

    node.columns = []
    return node, context


def visit_truncate_relation(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the TRUNCATE TABLE node to determine which connector should handle
    truncating the table.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support TRUNCATE TABLE"
        )

    # TRUNCATE is a bulk row delete - same tier as DELETE.
    if not can_perform_action(context.execution_context, node.relation_name, action="DELETE"):
        raise PermissionError(
            f"User does not have permission to truncate table {node.relation_name}"
        )

    node.columns = []
    return node, context


def _types_compatible(src, tgt) -> bool:
    """Permitted source→target type relationships for INSERT.

    Strict: exact match, NULL into anything, or INTEGER → DOUBLE widening.
    Unresolved literal types (None) are permitted at bind time —
    runtime catches real mismatches.
    """
    from opteryx.types.logical_type import LogicalCategory, ColumnType

    # Normalize ColumnType → LogicalCategory for comparison.
    src_lc = src.category if isinstance(src, ColumnType) else src
    tgt_lc = tgt.category if isinstance(tgt, ColumnType) else tgt

    if src_lc == tgt_lc:
        return True
    if src_lc == LogicalCategory.NULL:
        return True
    if src_lc is None:
        return True
    if src_lc == LogicalCategory.INTEGER and tgt_lc == LogicalCategory.FLOAT:
        return True
    return False


def visit_insert(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the INSERT node:
    - resolve target connector, must be Writable
    - read target schema from connector
    - resolve source columns (VALUES feeder or bound SELECT tail)
    - resolve target column order (schema order or explicit list)
    - validate column count and per-column type compatibility
    - record column_mapping for the InsertNode to permute morsels at write time
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import (
        ReadOnlyConnectorError,
        DatasetNotFoundError,
        UnsupportedSyntaxError,
        InvalidInternalStateError,
    )
    from opteryx.expression import NodeType
    from opteryx.managers.permissions import can_perform_action
    from opteryx.models import LogicalColumn
    from opteryx.types.schema import RelationSchema

    from opteryx.types.logical_type import LogicalCategory
    from opteryx.types.schema import SchemaColumn

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support INSERT"
        )

    create_target = getattr(node, "create_target", False)
    if_not_exists = getattr(node, "if_not_exists", False)
    or_replace = getattr(node, "or_replace", False)

    if create_target:
        node.is_replace = False
        existing_column_names = None
        if node.connector.relation_exists(node.relation_name):
            if if_not_exists:
                node.is_noop = True
                node.columns = []
                node.target_schema = None
                node.column_mapping = None
                node.target_column_names = None
                return node, context
            if not or_replace:
                raise ValueError(
                    f"relation already exists: {node.relation_name} "
                    "(CTAS does not append to existing relations; use INSERT)"
                )
            # CREATE OR REPLACE on an existing relation has the same blast
            # radius as DROP (the old relation's data/history is gone) - reuse
            # that tier rather than inventing a new one.
            if not can_perform_action(context.execution_context, node.relation_name, action="DROP"):
                raise PermissionError(
                    f"User does not have permission to replace table {node.relation_name}"
                )
            node.is_replace = True
            existing_column_names = node.connector.relation_column_names(node.relation_name)
        else:
            if not can_perform_action(context.execution_context, node.relation_name, action="CREATE"):
                raise PermissionError(
                    f"User does not have permission to create table {node.relation_name}"
                )

        if getattr(self, "graph", None) is None or node.source_tail_id is None:
            raise InvalidInternalStateError(
                "visit_insert: CTAS requires graph and source_tail_id"
            )
        feeder = self.graph[node.source_tail_id]
        if not getattr(feeder, "columns", None):
            raise InvalidInternalStateError(
                "visit_insert: CTAS source feeder has no bound columns"
            )

        target_columns = []
        seen_names = {}
        for src_col in feeder.columns:
            sc = src_col.schema_column
            target_name = (
                src_col.alias
                or (sc.name if sc is not None else None)
                or src_col.source_column
            )
            if not target_name:
                raise InvalidInternalStateError(
                    "CTAS source column has no resolvable name"
                )
            # Disambiguate duplicates: SELECT 1, 1 → 1, 1_1
            if target_name in seen_names:
                seen_names[target_name] += 1
                target_name = f"{target_name}_{seen_names[target_name]}"
            else:
                seen_names[target_name] = 0
            if sc.category is None or sc.category == LogicalCategory.NULL:
                raise UnsupportedSyntaxError(
                    f"CTAS column '{target_name}' has unresolved type; "
                    "specify the SELECT's column types explicitly"
                )
            from opteryx.types.schema import mint_column_identity
            flat = SchemaColumn(
                name=target_name,
                column_type=sc.column_type,
                nullable=getattr(sc, "nullable", True),
                identity=mint_column_identity(getattr(node, "relation_name", None), target_name),
            )
            target_columns.append(flat)

        if existing_column_names is not None and not getattr(
            node.connector, "supports_schema_evolution_on_replace", False
        ):
            # Schema-preserving REPLACE only for connectors that can't evolve
            # schema (e.g. the catalog connector has no public primitive to
            # write a new schema version outside create_dataset's internal
            # path) - a changed column set fails loud here rather than
            # silently committing data the declared schema can't describe.
            # Column-name comparison only, not full type fidelity - the
            # catalog's stored schema and Opteryx's RelationSchema are two
            # independent type representations with no bridge between them yet.
            new_names = [c.name for c in target_columns]
            if set(new_names) != set(existing_column_names) or len(new_names) != len(
                existing_column_names
            ):
                raise UnsupportedSyntaxError(
                    f"CREATE OR REPLACE {node.relation_name}: the SELECT's columns "
                    f"({new_names}) differ from the existing relation's columns "
                    f"({existing_column_names}) - schema-changing REPLACE is not yet supported"
                )

        target_schema = RelationSchema(
            name=node.relation_name,
            columns=target_columns,
        )
        node.target_schema = target_schema
        node.column_mapping = list(range(len(target_columns)))
        node.target_column_names = [c.name for c in target_columns]
        node.columns = []
        return node, context

    if not node.connector.relation_exists(node.relation_name):
        raise DatasetNotFoundError(connector=node.connector, dataset=node.relation_name)

    # Appending rows to an existing relation - same tier as CREATE VIEW/COMMENT.
    if not can_perform_action(context.execution_context, node.relation_name, action="WRITE"):
        raise PermissionError(
            f"User does not have permission to insert into {node.relation_name}"
        )

    # Read the existing relation's schema through the connector-agnostic table
    # engine - the same mechanism the SELECT binder uses (see dataset.py) -
    # not _relation_dir/_read_dataset_json, which are LocalStoreConnector-only
    # private filesystem helpers. Catalog-backed connectors (e.g.
    # OpteryxConnector) have no such attributes, so calling them here crashed
    # unconditionally on every non-local deployment: AttributeError:
    # 'OpteryxConnector' object has no attribute '_relation_dir'.
    table = node.connector.table_engine(node.relation_name, telemetry=context.telemetry)
    if getattr(table, "get_dataset_metadata", None) is not None:
        target_schema, _manifest = table.get_dataset_metadata()
    else:
        target_schema = table.get_dataset_schema()  # RelationSchema with SchemaColumn list

    node.target_schema = target_schema
    node.columns = []  # binder convention; INSERT produces no output columns

    # ---- 1. Source column count and types ----
    values_node = node.values_feeder  # set for VALUES path; None for SELECT
    if values_node is not None:
        if not values_node.values:
            raise UnsupportedSyntaxError("INSERT VALUES requires at least one row")
        source_column_count = len(values_node.values[0])
        # Probe types from the first row (parser-resolved literal types).
        source_types = [values_node.values[0][i].type for i in range(source_column_count)]
        # Validate all rows have the same column count.
        for row in values_node.values:
            if len(row) != source_column_count:
                raise UnsupportedSyntaxError(
                    f"INSERT row has {len(row)} values, expected {source_column_count}"
                )
    else:
        if getattr(self, "graph", None) is None or node.source_tail_id is None:
            raise InvalidInternalStateError(
                "visit_insert: SELECT path requires graph and source_tail_id"
            )
        feeder = self.graph[node.source_tail_id]
        if not getattr(feeder, "columns", None):
            raise InvalidInternalStateError(
                "visit_insert: source feeder has no bound columns"
            )
        source_column_count = len(feeder.columns)
        source_types = [c.schema_column.category for c in feeder.columns]

    # ---- 2. Target column order (schema order, or explicit list order) ----
    explicit_columns = getattr(node, "explicit_columns", None)
    if explicit_columns is None:
        target_columns_in_order = list(target_schema.columns)
    else:
        schema_by_name = {c.name: c for c in target_schema.columns}
        target_columns_in_order = []
        for cname in explicit_columns:
            if cname not in schema_by_name:
                raise UnsupportedSyntaxError(
                    f"INSERT column '{cname}' does not exist in {node.relation_name}"
                )
            target_columns_in_order.append(schema_by_name[cname])
        if len(target_columns_in_order) != len(target_schema.columns):
            raise UnsupportedSyntaxError(
                f"INSERT explicit column list must list all target columns "
                f"(target has {len(target_schema.columns)}, got {len(target_columns_in_order)}). "
                "Partial column inserts are not yet supported."
            )

    # ---- 3. Validate count and per-column types ----
    if source_column_count != len(target_columns_in_order):
        raise UnsupportedSyntaxError(
            f"INSERT row has {source_column_count} values, "
            f"expected {len(target_columns_in_order)} (target table: {node.relation_name})"
        )

    for src_idx, target_col in enumerate(target_columns_in_order):
        src_type = source_types[src_idx]
        if not _types_compatible(src_type, target_col.category):
            raise UnsupportedSyntaxError(
                f"INSERT type mismatch on column '{target_col.name}': "
                f"source {src_type} is not compatible with target {target_col.category}"
            )

    # ---- 4. Build column mapping (source idx → target schema idx) ----
    schema_index_by_name = {c.name: i for i, c in enumerate(target_schema.columns)}
    column_mapping = [
        schema_index_by_name[target_columns_in_order[src_idx].name]
        for src_idx in range(source_column_count)
    ]
    node.column_mapping = column_mapping
    node.target_column_names = [c.name for c in target_schema.columns]

    # ---- 5. VALUES feeder mutation: replace placeholder columns ----
    # The downstream FunctionDataset has been bound with placeholder column
    # names (`$col0`, ...). Replace those with LogicalColumns matching the
    # user-listed target order so the source pipeline carries meaningful
    # names; the InsertNode will permute to schema order at write time.
    if values_node is not None:
        target_relation_name = values_node.alias
        columns = [
            LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=col.name,
                source=target_relation_name,
                schema_column=col,
            )
            for col in target_columns_in_order
        ]
        values_node.columns = columns
        schema = RelationSchema(
            name=target_relation_name,
            columns=[c.schema_column for c in columns],
        )
        context.schemas[target_relation_name] = schema

    return node, context
