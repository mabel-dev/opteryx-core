# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
information_schema
-------------------

Minimum information_schema surface. Backed by the real Opteryx catalog
(opteryx_catalog) via list_collections()/list_datasets()/list_views() -
NOT a static/generated snapshot. Currently implements `tables`, `columns`,
`views`, and `schemata`.

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

`tables` also does one `load_dataset` + `snapshot()` round trip per table
(same cost profile as `columns`, above) to surface per-table statistics
(snapshot id/sequence, update time, file/byte/record counts, sort order).
Views have no dataset/snapshot, so they report NULL for all of these.
A table with no committed snapshot yet also reports NULL for all of them.

`views` does one `load_view` catalog round trip per view found to surface its
SQL text and metadata (owner, last-updated, last-execution stats). It is a
separate table from `tables` (which lists views too, but only name/type) -
`tables` never opens a view's document, only `list_views()`.

`schemata` is deliberately thin: with information_schema scoped per-workspace
(catalog_name is constant), the only real column is schema_name, one row per
list_collections() entry. The catalog has no public accessor for
per-collection metadata (owner/description are written at create_collection()
time but not readable back), so there is nothing more to add without a new
opteryx_catalog API. A schema is only listed if the caller can READ at least
one table or view inside it (see _collection_has_readable_entry) - otherwise
schemata would leak the existence of collections the caller has zero access
to, which the per-row READ check on `tables`/`columns`/`views` doesn't
otherwise prevent (that check is per-table, not per-collection).

Predicate pushdown: all four tables declare PredicatePushable, restricted to
Eq/NotEq/InList/NotInList on the plain catalog-enumeration key columns
(table_catalog/table_schema/table_name, plus table_type for `tables`;
catalog_name/schema_name for `schemata`) - see `_KeyColumnPredicatePushable`.
These columns are known before any catalog round trip, so pushing them lets
read_dataset() skip list_datasets()/list_views()/load_dataset()/load_view()
calls for excluded collections/tables entirely, rather than merely pruning
rows after the fact. Every other predicate shape is declined and left as an
ordinary Filter node downstream.

Row-level permissions: listing the catalog is not itself a bypass of
per-table READ policy. Every (collection, name) pair is checked with
can_perform_action(execution_context, "<workspace>.<collection>.<name>",
action="READ") before it is emitted - a user only sees tables/columns/views
they could otherwise SELECT FROM. A missing execution_context denies
everything (secure by default) rather than falling back to showing all rows.
"""

import datetime
from typing import Iterable
from typing import Optional

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel

from opteryx.connectors.base.base_connector import BaseTable
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.expression import NodeType
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


def _collection_has_readable_entry(catalog, execution_context, workspace: str, collection: str) -> bool:
    """Whether the caller can READ at least one table or view inside `collection`.

    Used to gate `schemata` rows - READ is checked per-table everywhere else
    in this module, but a collection has no table of its own to check, so
    "is this collection visible at all" is defined as "is anything in it
    visible".
    """
    for name in catalog.list_datasets(collection):
        if _readable(execution_context, workspace, collection, name):
            return True
    for name in catalog.list_views(collection):
        if _readable(execution_context, workspace, collection, name):
            return True
    return False


def _ms_to_datetime(ms) -> Optional[datetime.datetime]:
    """Convert a catalog epoch-millisecond timestamp to a UTC datetime, or
    None when the catalog has no value recorded (e.g. a view never executed).
    """
    if ms is None:
        return None
    return datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc)


def _normalize_sort_order(sort_orders) -> Optional[dict]:
    """Reduce a stored `dataset.metadata.sort_orders` value to its primary sort
    key in canonical form: {"name", "field_id", "index", "ascending"}.

    `sort_orders` has been written in three incompatible shapes over time: a
    positional int index into the schema's columns, a bare column-name
    string, or an Iceberg-style {"fields": [{"name", "direction"}]} dict.
    This mirrors opteryx_catalog.catalog.compaction.normalize_sort_order (the
    write side owns the authoritative logic) rather than importing it - the
    currently-installed opteryx_catalog wheel predates that helper.
    Resolution precedence downstream is field_id -> name -> index.
    """
    if not sort_orders:
        return None
    entry = sort_orders[0]

    if isinstance(entry, bool):
        return None  # bool is an int subclass; never a valid column index
    if isinstance(entry, int):
        return {"name": None, "field_id": None, "index": entry, "ascending": True}
    if isinstance(entry, str):
        return {"name": entry, "field_id": None, "index": None, "ascending": True}

    if isinstance(entry, dict):
        field = entry
        fields = entry.get("fields")
        if isinstance(fields, (list, tuple)) and fields:
            field = fields[0]
        if not isinstance(field, dict):
            return None

        name = field.get("name")
        field_id = field.get("source-id")
        if field_id is None:
            field_id = field.get("field-id")
        ascending = str(field.get("direction", "asc")).lower() != "desc"

        if name is None and field_id is None:
            return None
        return {"name": name, "field_id": field_id, "index": None, "ascending": ascending}

    return None


def _render_sort_order(sort_orders, relation_schema) -> Optional[str]:
    """Render the primary sort key as "<column> ASC|DESC", resolving a
    field_id/index against `relation_schema` (the dataset's raw catalog
    schema) to a column name. None when there is no sort key configured or
    it can't be resolved to a column name.
    """
    normalized = _normalize_sort_order(sort_orders)
    if normalized is None:
        return None

    name = normalized["name"]
    columns = relation_schema.columns if relation_schema is not None else []
    if name is None and normalized["field_id"] is not None:
        for column in columns:
            if getattr(column, "id", None) == normalized["field_id"]:
                name = column.name
                break
    if name is None and normalized["index"] is not None:
        index = normalized["index"]
        if 0 <= index < len(columns):
            name = columns[index].name
    if name is None:
        return None

    return f"{name} {'ASC' if normalized['ascending'] else 'DESC'}"


_KEY_PUSHABLE_OPS = ("Eq", "NotEq", "InList", "NotInList")


def _extract_key_predicate(condition, allowed_columns: frozenset):
    """Return (col_name, op, value) for a `column OP literal` predicate on one
    of `allowed_columns`, or None if the shape isn't supported.

    Only Eq/NotEq (scalar literal, either side) and InList/NotInList
    (identifier on the left, a literal list/tuple/set of strings on the
    right) are recognised - everything else (ranges, LIKE, functions, OR,
    BETWEEN, ...) returns None so the caller (can_push) declines it.
    """
    if condition.node_type != NodeType.COMPARISON_OPERATOR:
        return None
    op = condition.value
    if op not in _KEY_PUSHABLE_OPS:
        return None

    left, right = condition.left, condition.right
    if left is None or right is None:
        return None

    if op in ("InList", "NotInList"):
        if left.node_type != NodeType.IDENTIFIER or right.node_type != NodeType.LITERAL:
            return None
        ident, literal = left, right
    elif left.node_type == NodeType.IDENTIFIER and right.node_type == NodeType.LITERAL:
        ident, literal = left, right
    elif right.node_type == NodeType.IDENTIFIER and left.node_type == NodeType.LITERAL:
        ident, literal = right, left
    else:
        return None

    column = getattr(ident, "schema_column", None)
    col_name = getattr(column, "name", None)
    if col_name not in allowed_columns:
        return None

    value = literal.value
    if op in ("InList", "NotInList"):
        if not isinstance(value, (list, tuple, set)) or not all(isinstance(v, str) for v in value):
            return None
        value = list(value)
    elif not isinstance(value, str):
        return None

    return (col_name, op, value)


def _compile_key_predicates(predicates, allowed_columns: frozenset):
    """Parse pushed-down predicate Nodes into (col_name, op, value) triples.

    Every entry here already passed `can_push`, which used this same
    extractor - a predicate that fails to parse now is an internal
    inconsistency between can_push and read_dataset, not a query error.
    """
    compiled = []
    for condition in predicates or []:
        parsed = _extract_key_predicate(condition, allowed_columns)
        if parsed is None:
            raise InvalidInternalStateError(
                "information_schema received a pushed-down predicate its own "
                "can_push() should have declined"
            )
        compiled.append(parsed)
    return compiled


def _key_predicates_allow(compiled, values: dict) -> bool:
    """Whether `values` (a partial {col_name: value} row) satisfies every
    compiled predicate that mentions a column present in `values`. Columns
    not yet known (e.g. table_type before it's decided) are treated as
    unconstrained by this check - callers evaluate them separately once known.
    """
    for col_name, op, value in compiled:
        if col_name not in values:
            continue
        actual = values[col_name]
        if op == "Eq" and actual != value:
            return False
        if op == "NotEq" and actual == value:
            return False
        if op == "InList" and actual not in value:
            return False
        if op == "NotInList" and actual in value:
            return False
    return True


class _KeyColumnPredicatePushable(PredicatePushable):
    """Predicate pushdown restricted to Eq/NotEq/InList/NotInList on the plain
    catalog-enumeration key columns declared by `_pushable_columns`. These are
    known before any catalog round trip, so read_dataset() can use a pushed
    predicate to skip list_datasets()/list_views()/load_dataset() calls for
    excluded collections/tables entirely - a real cost saving, not just a
    post-hoc row filter. Every other predicate shape is declined here and
    left as an ordinary Filter node downstream: a missed optimization, never
    a dropped predicate.
    """

    supports_predicate_pushdown = True
    PUSHABLE_OPS = {op: True for op in _KEY_PUSHABLE_OPS}
    _pushable_columns: frozenset = frozenset()

    def can_push(self, operator, types=None) -> bool:
        return _extract_key_predicate(operator.condition, self._pushable_columns) is not None


class InformationSchemaTablesTable(BaseTable, _KeyColumnPredicatePushable):
    """Reads `information_schema.tables` from the catalog's collection/dataset/view listings."""

    __mode__ = "Internal"
    interal_only = True  # routes through the generic "Reader" physical node, like $planets/$no_table
    self_governs_permissions = True  # read_dataset() filters rows by READ access itself - see module docstring
    # BaseTable also declares this (False); it comes first in the MRO, so it
    # would otherwise shadow _KeyColumnPredicatePushable's True.
    supports_predicate_pushdown = True

    _COLUMNS = (
        "table_catalog",
        "table_schema",
        "table_name",
        "table_type",
        "table_sort_order",
        "snapshot_id",
        "snapshot_sequence_id",
        "table_updated_at",
        "table_file_count",
        "table_bytes",
        "table_record_count",
    )

    _pushable_columns = frozenset({"table_catalog", "table_schema", "table_name", "table_type"})

    def __init__(self, *, dataset, catalog, workspace, telemetry, execution_context=None, **kwargs):
        BaseTable.__init__(self, dataset=dataset, telemetry=telemetry, **kwargs)
        PredicatePushable.__init__(self, **kwargs)
        self.catalog = catalog
        self.workspace = workspace
        self.execution_context = execution_context

    def get_dataset_schema(self) -> RelationSchema:
        column_types = {
            "table_catalog": _lt.VARCHAR,
            "table_schema": _lt.VARCHAR,
            "table_name": _lt.VARCHAR,
            "table_type": _lt.VARCHAR,
            "table_sort_order": _lt.VARCHAR,
            "snapshot_id": _lt.INT64,
            "snapshot_sequence_id": _lt.INT64,
            "table_updated_at": _lt.TIMESTAMP(),
            "table_file_count": _lt.INT64,
            "table_bytes": _lt.INT64,
            "table_record_count": _lt.INT64,
        }
        self.schema = RelationSchema(
            name="information_schema.tables",
            columns=[
                SchemaColumn(
                    name=column_name,
                    column_type=column_types[column_name],
                    identity=mint_column_identity("information_schema.tables", column_name),
                )
                for column_name in self._COLUMNS
            ],
        )
        return self.schema

    def read_dataset(self, predicates=None, **kwargs) -> Iterable[Morsel]:
        compiled = _compile_key_predicates(predicates, self._pushable_columns)

        table_catalog = []
        table_schema = []
        table_name = []
        table_type = []
        table_sort_order = []
        snapshot_id = []
        snapshot_sequence_id = []
        table_updated_at = []
        table_file_count = []
        table_bytes = []
        table_record_count = []

        def _append_stats_row(identifier: str):
            dataset = self.catalog.load_dataset(identifier)
            snapshot = dataset.snapshot()
            if snapshot is None:
                # No data committed yet - the dataset has no known snapshot stats.
                table_sort_order.append(None)
                snapshot_id.append(None)
                snapshot_sequence_id.append(None)
                table_updated_at.append(None)
                table_file_count.append(None)
                table_bytes.append(None)
                table_record_count.append(None)
                return

            relation_schema = dataset.schema()
            table_sort_order.append(_render_sort_order(dataset.metadata.sort_orders, relation_schema))
            snapshot_id.append(snapshot.snapshot_id)
            snapshot_sequence_id.append(snapshot.sequence_number)
            table_updated_at.append(_ms_to_datetime(snapshot.timestamp_ms))
            summary = snapshot.summary or {}
            table_file_count.append(summary.get("total-data-files", 0))
            table_bytes.append(summary.get("total-files-size", 0))
            table_record_count.append(summary.get("total-records", 0))

        # table_catalog is constant (== self.workspace) for every row this reader
        # can ever emit; a predicate excluding it excludes the whole result, so
        # skip enumerating the catalog entirely rather than filtering it out row
        # by row. Likewise, a table_type predicate that rules out BOTH values
        # would exclude everything - falling back to no enumeration is correct
        # there too, not just an optimization.
        want_catalog = _key_predicates_allow(compiled, {"table_catalog": self.workspace})
        want_tables = _key_predicates_allow(compiled, {"table_type": "BASE TABLE"})
        want_views = _key_predicates_allow(compiled, {"table_type": "VIEW"})

        if want_catalog and (want_tables or want_views):
            for collection in self.catalog.list_collections():
                if not _key_predicates_allow(compiled, {"table_schema": collection}):
                    continue
                if want_tables:
                    for name in self.catalog.list_datasets(collection):
                        if not _key_predicates_allow(compiled, {"table_name": name}):
                            continue
                        if not _readable(self.execution_context, self.workspace, collection, name):
                            continue
                        table_catalog.append(self.workspace)
                        table_schema.append(collection)
                        table_name.append(name)
                        table_type.append("BASE TABLE")
                        _append_stats_row(f"{collection}.{name}")
                if want_views:
                    for name in self.catalog.list_views(collection):
                        if not _key_predicates_allow(compiled, {"table_name": name}):
                            continue
                        if not _readable(self.execution_context, self.workspace, collection, name):
                            continue
                        table_catalog.append(self.workspace)
                        table_schema.append(collection)
                        table_name.append(name)
                        table_type.append("VIEW")
                        table_sort_order.append(None)
                        snapshot_id.append(None)
                        snapshot_sequence_id.append(None)
                        table_updated_at.append(None)
                        table_file_count.append(None)
                        table_bytes.append(None)
                        table_record_count.append(None)

        vectors = [
            vector_from_sequence(table_catalog, dtype=DrakenType.VARCHAR),
            vector_from_sequence(table_schema, dtype=DrakenType.VARCHAR),
            vector_from_sequence(table_name, dtype=DrakenType.VARCHAR),
            vector_from_sequence(table_type, dtype=DrakenType.VARCHAR),
            vector_from_sequence(table_sort_order, dtype=DrakenType.VARCHAR),
            vector_from_sequence(snapshot_id, dtype=DrakenType.INT64),
            vector_from_sequence(snapshot_sequence_id, dtype=DrakenType.INT64),
            vector_from_sequence(table_updated_at, dtype=DrakenType.TIMESTAMP64),
            vector_from_sequence(table_file_count, dtype=DrakenType.INT64),
            vector_from_sequence(table_bytes, dtype=DrakenType.INT64),
            vector_from_sequence(table_record_count, dtype=DrakenType.INT64),
        ]
        yield Morsel.from_vectors(list(self._COLUMNS), vectors)


class InformationSchemaColumnsTable(BaseTable, _KeyColumnPredicatePushable):
    """Reads `information_schema.columns` by fetching each dataset's schema from the catalog."""

    __mode__ = "Internal"
    interal_only = True  # routes through the generic "Reader" physical node, like $planets/$no_table
    self_governs_permissions = True  # read_dataset() filters rows by READ access itself - see module docstring
    # BaseTable also declares this (False); it comes first in the MRO, so it
    # would otherwise shadow _KeyColumnPredicatePushable's True.
    supports_predicate_pushdown = True

    _COLUMNS = (
        "table_catalog",
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "data_type",
        "is_nullable",
    )

    _pushable_columns = frozenset({"table_catalog", "table_schema", "table_name"})

    def __init__(self, *, dataset, catalog, workspace, telemetry, execution_context=None, **kwargs):
        BaseTable.__init__(self, dataset=dataset, telemetry=telemetry, **kwargs)
        PredicatePushable.__init__(self, **kwargs)
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

    def read_dataset(self, predicates=None, **kwargs) -> Iterable[Morsel]:
        from opteryx.connectors.opteryx_connector import OpteryxTable

        compiled = _compile_key_predicates(predicates, self._pushable_columns)

        table_catalog = []
        table_schema = []
        table_name = []
        column_name = []
        ordinal_position = []
        data_type = []
        is_nullable = []

        # table_catalog is constant (== self.workspace) for every row this reader
        # can ever emit - see InformationSchemaTablesTable.read_dataset - so an
        # excluding predicate skips enumeration entirely rather than filtering
        # row by row.
        if _key_predicates_allow(compiled, {"table_catalog": self.workspace}):
            for collection in self.catalog.list_collections():
                if not _key_predicates_allow(compiled, {"table_schema": collection}):
                    continue
                for name in self.catalog.list_datasets(collection):
                    if not _key_predicates_allow(compiled, {"table_name": name}):
                        continue
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


class InformationSchemaViewsTable(BaseTable, _KeyColumnPredicatePushable):
    """Reads `information_schema.views` by fetching each view's definition from the catalog.

    Unlike `tables` (which lists a view's name/type from `list_views()` alone),
    this opens every view's document via `load_view()` to surface its SQL text
    and metadata - one catalog round trip per view found.
    """

    __mode__ = "Internal"
    interal_only = True  # routes through the generic "Reader" physical node, like $planets/$no_table
    self_governs_permissions = True  # read_dataset() filters rows by READ access itself - see module docstring
    # BaseTable also declares this (False); it comes first in the MRO, so it
    # would otherwise shadow _KeyColumnPredicatePushable's True.
    supports_predicate_pushdown = True

    _COLUMNS = (
        "table_catalog",
        "table_schema",
        "table_name",
        "view_definition",
        "view_owner",
        "view_updated_at",
    )

    _pushable_columns = frozenset({"table_catalog", "table_schema", "table_name"})

    def __init__(self, *, dataset, catalog, workspace, telemetry, execution_context=None, **kwargs):
        BaseTable.__init__(self, dataset=dataset, telemetry=telemetry, **kwargs)
        PredicatePushable.__init__(self, **kwargs)
        self.catalog = catalog
        self.workspace = workspace
        self.execution_context = execution_context

    def get_dataset_schema(self) -> RelationSchema:
        column_types = {
            "table_catalog": _lt.VARCHAR,
            "table_schema": _lt.VARCHAR,
            "table_name": _lt.VARCHAR,
            "view_definition": _lt.VARCHAR,
            "view_owner": _lt.VARCHAR,
            "view_updated_at": _lt.TIMESTAMP(),
        }
        self.schema = RelationSchema(
            name="information_schema.views",
            columns=[
                SchemaColumn(
                    name=column_name,
                    column_type=column_types[column_name],
                    identity=mint_column_identity("information_schema.views", column_name),
                )
                for column_name in self._COLUMNS
            ],
        )
        return self.schema

    def read_dataset(self, predicates=None, **kwargs) -> Iterable[Morsel]:
        compiled = _compile_key_predicates(predicates, self._pushable_columns)

        table_catalog = []
        table_schema = []
        table_name = []
        view_definition = []
        view_owner = []
        view_updated_at = []

        # See InformationSchemaTablesTable.read_dataset - table_catalog is
        # constant per reader, so an excluding predicate skips enumeration
        # entirely rather than filtering row by row.
        if _key_predicates_allow(compiled, {"table_catalog": self.workspace}):
            for collection in self.catalog.list_collections():
                if not _key_predicates_allow(compiled, {"table_schema": collection}):
                    continue
                for name in self.catalog.list_views(collection):
                    if not _key_predicates_allow(compiled, {"table_name": name}):
                        continue
                    if not _readable(self.execution_context, self.workspace, collection, name):
                        continue
                    view = self.catalog.load_view(f"{collection}.{name}")
                    metadata = view.metadata

                    table_catalog.append(self.workspace)
                    table_schema.append(collection)
                    table_name.append(name)
                    view_definition.append(getattr(view, "definition", None) or getattr(view, "sql", None))
                    view_owner.append(getattr(metadata, "author", None))
                    view_updated_at.append(_ms_to_datetime(getattr(metadata, "timestamp_ms", None)))

        vectors = [
            vector_from_sequence(table_catalog, dtype=DrakenType.VARCHAR),
            vector_from_sequence(table_schema, dtype=DrakenType.VARCHAR),
            vector_from_sequence(table_name, dtype=DrakenType.VARCHAR),
            vector_from_sequence(view_definition, dtype=DrakenType.VARCHAR),
            vector_from_sequence(view_owner, dtype=DrakenType.VARCHAR),
            vector_from_sequence(view_updated_at, dtype=DrakenType.TIMESTAMP64),
        ]
        yield Morsel.from_vectors(list(self._COLUMNS), vectors)


class InformationSchemaSchemataTable(BaseTable, _KeyColumnPredicatePushable):
    """Reads `information_schema.schemata` from the catalog's collection listing.

    Deliberately thin - see the module docstring for why catalog_name is the
    only constant column and schema_name is the only other one.
    """

    __mode__ = "Internal"
    interal_only = True  # routes through the generic "Reader" physical node, like $planets/$no_table
    self_governs_permissions = True  # read_dataset() filters rows by READ access itself - see module docstring
    # BaseTable also declares this (False); it comes first in the MRO, so it
    # would otherwise shadow _KeyColumnPredicatePushable's True.
    supports_predicate_pushdown = True

    _COLUMNS = ("catalog_name", "schema_name")

    _pushable_columns = frozenset({"catalog_name", "schema_name"})

    def __init__(self, *, dataset, catalog, workspace, telemetry, execution_context=None, **kwargs):
        BaseTable.__init__(self, dataset=dataset, telemetry=telemetry, **kwargs)
        PredicatePushable.__init__(self, **kwargs)
        self.catalog = catalog
        self.workspace = workspace
        self.execution_context = execution_context

    def get_dataset_schema(self) -> RelationSchema:
        self.schema = RelationSchema(
            name="information_schema.schemata",
            columns=[
                SchemaColumn(
                    name=column_name,
                    column_type=_lt.VARCHAR,
                    identity=mint_column_identity("information_schema.schemata", column_name),
                )
                for column_name in self._COLUMNS
            ],
        )
        return self.schema

    def read_dataset(self, predicates=None, **kwargs) -> Iterable[Morsel]:
        compiled = _compile_key_predicates(predicates, self._pushable_columns)

        catalog_name = []
        schema_name = []

        # See InformationSchemaTablesTable.read_dataset - catalog_name is
        # constant per reader, so an excluding predicate skips enumeration
        # entirely rather than filtering row by row.
        if _key_predicates_allow(compiled, {"catalog_name": self.workspace}):
            for collection in self.catalog.list_collections():
                if not _key_predicates_allow(compiled, {"schema_name": collection}):
                    continue
                if not _collection_has_readable_entry(
                    self.catalog, self.execution_context, self.workspace, collection
                ):
                    continue
                catalog_name.append(self.workspace)
                schema_name.append(collection)

        vectors = [
            vector_from_sequence(catalog_name, dtype=DrakenType.VARCHAR),
            vector_from_sequence(schema_name, dtype=DrakenType.VARCHAR),
        ]
        yield Morsel.from_vectors(list(self._COLUMNS), vectors)


_TABLE_CLASSES = {
    "tables": InformationSchemaTablesTable,
    "columns": InformationSchemaColumnsTable,
    "views": InformationSchemaViewsTable,
    "schemata": InformationSchemaSchemataTable,
}
