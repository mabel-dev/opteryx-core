# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
information_schema
-------------------

Minimum information_schema surface. Backed by the real Opteryx catalog
(opteryx_catalog) via list_collections()/list_datasets()/list_views() -
NOT a static/generated snapshot. Currently implements `tables` and `columns`.

information_schema is a reserved nested schema inside a catalog workspace,
addressed as `<workspace>.information_schema.<table>` - e.g.
`opteryx.information_schema.tables` for a workspace registered under the
`opteryx` prefix:

    opteryx.register_workspace("opteryx", OpteryxConnector, catalog=<catalog>, ...)

OpteryxConnector.table_engine() dispatches here when the relative
identifier's first segment is `information_schema`; there is no separate
top-level `information_schema` connector prefix.

`columns` reuses OpteryxTable._normalize_schema/_normalize_type for the
catalog-schema -> RelationSchema conversion (the same code path every
ordinary catalog table scan already goes through) rather than re-deriving
type mapping here. It does one `load_dataset` + `schema()` catalog round
trip per table found - there is no pruning/pushdown in this cut, so an
unfiltered `SELECT * FROM information_schema.columns` costs one round trip
per table in the catalog. Views are not included - their columns are only
knowable by binding the view body, which is out of scope here.

Row-level permissions: listing the catalog is not itself a bypass of
per-table READ policy. Every (collection, name) pair is checked with
can_perform_action(execution_context, "<workspace>.<collection>.<name>",
action="READ") before it is emitted - a user only sees tables/columns they
could otherwise SELECT FROM. A missing execution_context denies everything
(secure by default) rather than falling back to showing all rows.
"""

from typing import Iterable

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel

from opteryx.connectors.base.base_connector import BaseTable
from opteryx.exceptions import DatasetNotFoundError
from opteryx.managers.permissions import can_perform_action
from opteryx.types import logical_type as _lt
from opteryx.types.schema import RelationSchema
from opteryx.types.schema import SchemaColumn
from opteryx.types.schema import mint_column_identity


def build_information_schema_table(
    table_name: str, *, catalog, workspace: str, telemetry=None, execution_context=None, **kwargs
):
    """Factory called by OpteryxConnector.table_engine() for the `information_schema.<table_name>` relative id."""
    table_class = _TABLE_CLASSES.get(table_name)
    if table_class is None:
        raise DatasetNotFoundError(
            connector="InformationSchema",
            dataset=f"{workspace}.information_schema.{table_name}",
        )
    return table_class(
        dataset=f"information_schema.{table_name}",
        catalog=catalog,
        workspace=workspace,
        telemetry=telemetry,
        execution_context=execution_context,
    )


def _readable(execution_context, workspace: str, collection: str, name: str) -> bool:
    """Secure-by-default READ check for one (collection, name) catalog entry."""
    if execution_context is None:
        return False
    return can_perform_action(execution_context, f"{workspace}.{collection}.{name}", action="READ")


class InformationSchemaTablesTable(BaseTable):
    """Reads `information_schema.tables` from the catalog's collection/dataset/view listings."""

    __mode__ = "Internal"
    interal_only = True  # routes through the generic "Reader" physical node, like $planets/$no_table
    self_governs_permissions = True  # read_dataset() filters rows by READ access itself - see module docstring

    _COLUMNS = ("table_catalog", "table_schema", "table_name", "table_type")

    def __init__(self, *, dataset, catalog, workspace, telemetry, execution_context=None, **kwargs):
        super().__init__(dataset=dataset, telemetry=telemetry, **kwargs)
        self.catalog = catalog
        self.workspace = workspace
        self.execution_context = execution_context

    def get_dataset_schema(self) -> RelationSchema:
        self.schema = RelationSchema(
            name="information_schema.tables",
            columns=[
                SchemaColumn(
                    name=column_name,
                    column_type=_lt.VARCHAR,
                    identity=mint_column_identity("information_schema.tables", column_name),
                )
                for column_name in self._COLUMNS
            ],
        )
        return self.schema

    def read_dataset(self, **kwargs) -> Iterable[Morsel]:
        table_catalog = []
        table_schema = []
        table_name = []
        table_type = []

        for collection in self.catalog.list_collections():
            for name in self.catalog.list_datasets(collection):
                if not _readable(self.execution_context, self.workspace, collection, name):
                    continue
                table_catalog.append(self.workspace)
                table_schema.append(collection)
                table_name.append(name)
                table_type.append("BASE TABLE")
            for name in self.catalog.list_views(collection):
                if not _readable(self.execution_context, self.workspace, collection, name):
                    continue
                table_catalog.append(self.workspace)
                table_schema.append(collection)
                table_name.append(name)
                table_type.append("VIEW")

        vectors = [
            vector_from_sequence(table_catalog, dtype=DrakenType.VARCHAR),
            vector_from_sequence(table_schema, dtype=DrakenType.VARCHAR),
            vector_from_sequence(table_name, dtype=DrakenType.VARCHAR),
            vector_from_sequence(table_type, dtype=DrakenType.VARCHAR),
        ]
        yield Morsel.from_vectors(list(self._COLUMNS), vectors)


class InformationSchemaColumnsTable(BaseTable):
    """Reads `information_schema.columns` by fetching each dataset's schema from the catalog."""

    __mode__ = "Internal"
    interal_only = True  # routes through the generic "Reader" physical node, like $planets/$no_table
    self_governs_permissions = True  # read_dataset() filters rows by READ access itself - see module docstring

    _COLUMNS = (
        "table_catalog",
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "data_type",
        "is_nullable",
    )

    def __init__(self, *, dataset, catalog, workspace, telemetry, execution_context=None, **kwargs):
        super().__init__(dataset=dataset, telemetry=telemetry, **kwargs)
        self.catalog = catalog
        self.workspace = workspace
        self.execution_context = execution_context

    def get_dataset_schema(self) -> RelationSchema:
        column_types = {
            "table_catalog": _lt.VARCHAR,
            "table_schema": _lt.VARCHAR,
            "table_name": _lt.VARCHAR,
            "column_name": _lt.VARCHAR,
            "ordinal_position": _lt.INT32,
            "data_type": _lt.VARCHAR,
            "is_nullable": _lt.VARCHAR,
        }
        self.schema = RelationSchema(
            name="information_schema.columns",
            columns=[
                SchemaColumn(
                    name=column_name,
                    column_type=column_types[column_name],
                    identity=mint_column_identity("information_schema.columns", column_name),
                )
                for column_name in self._COLUMNS
            ],
        )
        return self.schema

    def read_dataset(self, **kwargs) -> Iterable[Morsel]:
        from opteryx.connectors.opteryx_connector import OpteryxTable

        table_catalog = []
        table_schema = []
        table_name = []
        column_name = []
        ordinal_position = []
        data_type = []
        is_nullable = []

        for collection in self.catalog.list_collections():
            for name in self.catalog.list_datasets(collection):
                if not _readable(self.execution_context, self.workspace, collection, name):
                    continue
                identifier = f"{collection}.{name}"
                dataset = self.catalog.load_dataset(identifier)
                snapshot = dataset.snapshot()
                if snapshot is None:
                    # No data committed yet - the dataset has no known columns.
                    continue
                raw_schema = dataset.schema(snapshot.schema_id)
                relation_schema = OpteryxTable._normalize_schema(raw_schema, relation_name=identifier)

                for position, column in enumerate(relation_schema.columns, start=1):
                    table_catalog.append(self.workspace)
                    table_schema.append(collection)
                    table_name.append(name)
                    column_name.append(column.name)
                    ordinal_position.append(position)
                    data_type.append(str(column.column_type) if column.column_type is not None else "UNKNOWN")
                    is_nullable.append("YES" if column.nullable else "NO")

        vectors = [
            vector_from_sequence(table_catalog, dtype=DrakenType.VARCHAR),
            vector_from_sequence(table_schema, dtype=DrakenType.VARCHAR),
            vector_from_sequence(table_name, dtype=DrakenType.VARCHAR),
            vector_from_sequence(column_name, dtype=DrakenType.VARCHAR),
            vector_from_sequence(ordinal_position, dtype=DrakenType.INT32),
            vector_from_sequence(data_type, dtype=DrakenType.VARCHAR),
            vector_from_sequence(is_nullable, dtype=DrakenType.VARCHAR),
        ]
        yield Morsel.from_vectors(list(self._COLUMNS), vectors)


_TABLE_CLASSES = {
    "tables": InformationSchemaTablesTable,
    "columns": InformationSchemaColumnsTable,
}
