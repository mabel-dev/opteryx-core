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

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support CREATE TABLE"
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

    node.connectors = {}
    for relation_name in node.relation_names:
        connector = connector_factory(relation_name, telemetry=context.telemetry)
        if not isinstance(connector, Writable):
            raise ReadOnlyConnectorError(
                f"connector for {relation_name} does not support DROP TABLE"
            )
        node.connectors[relation_name] = connector

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

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support TRUNCATE TABLE"
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

    if create_target:
        if node.connector.relation_exists(node.relation_name):
            if if_not_exists:
                node.is_noop = True
                node.columns = []
                node.target_schema = None
                node.column_mapping = None
                node.target_column_names = None
                return node, context
            raise ValueError(
                f"relation already exists: {node.relation_name} "
                "(CTAS does not append to existing relations; use INSERT)"
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

    # Read schema from dataset.json.
    relation_dir = node.connector._relation_dir(node.relation_name)
    descriptor = node.connector._read_dataset_json(relation_dir)
    target_schema = descriptor.schema  # RelationSchema with SchemaColumn list

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
        columns = tuple(
            LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=col.name,
                source=target_relation_name,
                schema_column=col,
            )
            for col in target_columns_in_order
        )
        values_node.columns = columns
        schema = RelationSchema(
            name=target_relation_name,
            columns=[c.schema_column for c in columns],
        )
        context.schemas[target_relation_name] = schema

    return node, context
