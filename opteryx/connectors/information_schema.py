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
`views`, `schemata`, `triggers`, `tasks`, `column_relationships`, `grants` and
`listeners`.

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
per-table policy. Every row is checked with can_perform_action(
execution_context, "<workspace>.<collection>.<name>", action=...) before it is
emitted, and the ACTION is the tier of whoever could act on what the row
describes - never a per-table gate, because roles are held per pattern and one
caller is owner of some datasets and reader of others in the same workspace:

  tables, columns, schemata      READ      existence and shape are what a
                                           reader needs to write a query
  column_relationships           READ      on BOTH ends - see the class
  views                          READ      for the row; `view_definition` is
                                           NULL unless WRITE holds, because the
                                           SQL names relations the reader may
                                           hold no grant on, and a writer is
                                           who authors it
  triggers, tasks                AUTOMATE  automation: only an owner could have
                                           made one or can act on it

`SHOW CREATE` gates its text at the same tiers (see the binder's `visit_show`),
so nothing withheld here is one statement away. A missing execution_context
denies everything (secure by default) rather than falling back to showing all
rows.

`grants` is the exception to the READ rule, because it is not about data: a
row there is gated on the authority to ADMINISTER the object it describes -
owner authority covering it, the gate `SHOW GRANTS ON` holds - decided by the
registered permissions capability, which also owns the covering test. See
`InformationSchemaGrantsTable`.
"""

import datetime
import json
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
from opteryx.managers.permissions import effective_grants_in
from opteryx.models.sort_order import _resolve_name
from opteryx.models.sort_order import normalize_sort_order
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


def _permitted(execution_context, workspace: str, collection: str, name: str, action: str) -> bool:
    """Secure-by-default check of `action` for one (collection, name) catalog entry."""
    if execution_context is None:
        return False
    return can_perform_action(
        execution_context, f"{workspace}.{collection}.{name}", action=action
    )


def _readable(execution_context, workspace: str, collection: str, name: str) -> bool:
    """READ on one catalog entry - the gate for existence and shape."""
    return _permitted(execution_context, workspace, collection, name, "READ")


def _automatable(execution_context, workspace: str, collection: str, name: str) -> bool:
    """AUTOMATE on one catalog entry - the gate for a task or trigger row."""
    return _permitted(execution_context, workspace, collection, name, "AUTOMATE")


def _writable(execution_context, workspace: str, collection: str, name: str) -> bool:
    """WRITE on one catalog entry - the gate for a view's definition text."""
    return _permitted(execution_context, workspace, collection, name, "WRITE")


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


def _evidence_text(evidence) -> Optional[str]:
    """One inference proposal's evidence, as text for a VARCHAR column.

    The store holds it as a map - overlap ratio, how many values were compared,
    the two cardinalities - and this is the only place that has to flatten it.
    JSON rather than prose: the projection is read by the Studio and by people
    writing SQL against it, and a machine-readable shape lets the Studio show
    "94% of 1,685 values" while a person reading the column still sees what was
    measured. Keys are sorted so two runs that observed the same thing produce
    the same text and a diff of the graph shows only real changes.

    None for anything a person asserted, which is most rows: there was no
    measurement, and an empty object would suggest there had been one.
    """
    if not evidence:
        return None
    if isinstance(evidence, str):
        return evidence
    try:
        return json.dumps(evidence, sort_keys=True, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        # An evidence map is written by the inference job and read back here;
        # a shape json cannot render is a bug in that job, not a reason to fail
        # the whole projection for every other row.
        return str(evidence)


def _render_sort_order(sort_orders, relation_schema) -> Optional[str]:
    """Render the primary sort key as "<column> ASC|DESC", resolving a
    field_id/index against `relation_schema` (the dataset's raw catalog
    schema) to a column name. None when there is no sort key configured or
    it can't be resolved to a column name.

    The three stored shapes are read by opteryx.models.sort_order, which owns
    that knowledge for every reader - see `sort_order_column_names` for the
    whole-key form SHOW CREATE TABLE needs.
    """
    normalized = normalize_sort_order(sort_orders)
    if normalized is None:
        return None

    name = _resolve_name(normalized, relation_schema.columns if relation_schema is not None else [])
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

    # VARCHAR literals reach the pushdown layer as bytes; the catalog keys they
    # are compared against are str, so normalize here or nothing ever matches.
    value = literal.value
    if isinstance(value, bytes):
        value = value.decode()
    if op in ("InList", "NotInList"):
        if not isinstance(value, (list, tuple, set)) or not all(
            isinstance(v, (str, bytes)) for v in value
        ):
            return None
        value = [v.decode() if isinstance(v, bytes) else v for v in value]
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


def _pinned_equality(compiled, col_name: str):
    """The value `col_name` is pinned to by an Eq predicate, or None.

    Distinct from `_key_predicates_allow`, which asks whether a known value
    survives the predicates. This asks the other question - "is there exactly
    one value this column can take?" - which is what lets a read address one
    catalog key directly instead of scanning for it. Only Eq pins; NotEq and
    the list forms narrow without naming a single value, and they are still
    applied to every row afterwards.
    """
    for candidate, op, value in compiled:
        if candidate == col_name and op == "Eq":
            return value
    return None


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
                    # A dataset with nothing committed has no snapshot to read a
                    # schema AS OF, but it does have a registered one - and that
                    # is what `SELECT` and `SHOW COLUMNS` now serve it as (see
                    # OpteryxTable._resolve_snapshot). Skipping it here left this
                    # view saying a readable relation has no columns.
                    raw_schema = (
                        dataset.schema() if snapshot is None else dataset.schema(snapshot.schema_id)
                    )
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

    The ROW is at READ, the DEFINITION at WRITE. `view_definition` is the one
    column here that is not about the view but about what it reads: its SQL
    names relations the caller may hold no grant on, and the binder will refuse
    to run the view for them if so. A reader gets the row with the definition
    NULL - the shape Postgres and the SQL standard give a non-owner - and a
    writer, who may replace the definition, gets the text.
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
                    if _writable(self.execution_context, self.workspace, collection, name):
                        view_definition.append(
                            getattr(view, "definition", None) or getattr(view, "sql", None)
                        )
                    else:
                        view_definition.append(None)
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


class InformationSchemaTriggersTable(BaseTable, _KeyColumnPredicatePushable):
    """Reads `information_schema.triggers` from the catalog's per-holder
    trigger listings - list_collections() -> list_datasets() -> list_triggers()
    for the triggers datasets hold, then list_tasks() -> list_triggers(...,
    holder_kind="task") for the ones tasks hold; one round trip per holder.

    A commit trigger hangs off the dataset whose commits fire it (the SOURCE
    table), not off its target view. A schedule or signal trigger has no source
    dataset and hangs off the TASK it fires. Either way the per-row check is on
    that holder - and it is AUTOMATE, matching CREATE/DROP/ALTER TRIGGER's
    gate, which is also on the holder. A row here names the identity
    unattended runs carry and what they fire; only an owner could have made
    that arrangement or can change it, so only an owner sees it. An MV whose
    refresh trigger has been dropped simply has no row here.

    A SUSPENDED trigger is a row, and says so: `suspended_at`/`suspended_by`.
    That is the difference between a trigger somebody paused and one that was
    dropped or never made, and it is the whole reason `ALTER TRIGGER ...
    SUSPEND` is not just `DROP TRIGGER`.
    """

    __mode__ = "Internal"
    interal_only = True  # routes through the generic "Reader" physical node, like $planets/$no_table
    self_governs_permissions = True  # read_dataset() filters rows by AUTOMATE access itself - see module docstring
    # BaseTable also declares this (False); it comes first in the MRO, so it
    # would otherwise shadow _KeyColumnPredicatePushable's True.
    supports_predicate_pushdown = True

    _COLUMNS = (
        "trigger_catalog",
        "trigger_collection",
        "trigger_name",
        # What the trigger HANGS OFF, as `collection.name`: the dataset whose
        # commits fire a commit trigger, or the task a schedule or signal
        # trigger fires. `SHOW TRIGGERS FOR <name>` filters on this, so it
        # answers for a table and for a task alike.
        "trigger_holder",
        # The dataset the fired run's window is bound from: the holder for a
        # commit trigger (its window is the commit), the OVER table for a
        # schedule or signal trigger, null when such a trigger has none.
        "event_object_table",
        # WHAT FIRES IT - commit, schedule or signal - and for a schedule, when.
        # A record written before events were told apart has no `event-kind`
        # and is a commit trigger, the only kind there was.
        "event_kind",
        "schedule",
        "time_zone",
        "next_due_at",
        "action_kind",
        "target",
        # WHOSE AUTHORITY the trigger's unattended runs carry - the trigger's
        # `runs-as`, and the single most important thing about a row here. A
        # trigger fires with nobody present, so this is the only place the
        # identity behind that work is visible; without it, `created_by` reads
        # like the answer and is not one. Populated for EVERY kind: a view's
        # refresh trigger carries its own, as a task's does. The view itself
        # has none - it is stored SQL, and a person running REFRESH runs it as
        # themselves - so the refresh row here is where its unattended
        # identity lives, not somewhere on the view.
        "runs_as",
        "created_by",
        "created_at",
        "last_fired_at",
        "last_fired_status",
        # DELIBERATELY OFF, not stored anywhere: suspension only ever became
        # observable AFTER a write to the source, when the refused fire stamped
        # `last_fired_status: suspended`. On a quiet source a paused trigger
        # read as a healthy one. These two say so with nobody writing.
        #
        # Two columns and not a third: `suspended` as a boolean is
        # `suspended_at IS NOT NULL` and nothing else, and a derived column that
        # can disagree with the column it derives from is a bug waiting for a
        # partial write.
        "suspended_at",
        "suspended_by",
        # The floor between two firings, in seconds. Null for a trigger that
        # predates the field - which fires on every commit - and 0 for one
        # whose floor was removed; the two read differently on purpose.
        "minimum_interval_seconds",
    )

    # trigger_catalog/trigger_collection/trigger_holder are known before the
    # per-holder list_triggers() round trip, so pushing them skips those calls
    # entirely; so is event_object_table for a dataset's triggers, where it is
    # the holder. For a task's triggers it is read off the record, so it is
    # applied after the listing there, as trigger_name is everywhere.
    #
    # suspended_at/suspended_by are NOT here. They are known only once the
    # listing has been read, so pushing them would skip no round trip, and
    # `_extract_key_predicate` takes string literals only - a timestamp or a
    # null-ness test is declined and left as an ordinary Filter, which is the
    # right answer for both.
    _pushable_columns = frozenset(
        {
            "trigger_catalog",
            "trigger_collection",
            "trigger_holder",
            "event_object_table",
            "trigger_name",
        }
    )

    def __init__(self, *, dataset, catalog, workspace, telemetry, execution_context=None, **kwargs):
        BaseTable.__init__(self, dataset=dataset, telemetry=telemetry, **kwargs)
        PredicatePushable.__init__(self, **kwargs)
        self.catalog = catalog
        self.workspace = workspace
        self.execution_context = execution_context

    def get_dataset_schema(self) -> RelationSchema:
        column_types = {
            "trigger_catalog": _lt.VARCHAR,
            "trigger_collection": _lt.VARCHAR,
            "trigger_name": _lt.VARCHAR,
            "trigger_holder": _lt.VARCHAR,
            "event_object_table": _lt.VARCHAR,
            "event_kind": _lt.VARCHAR,
            "schedule": _lt.VARCHAR,
            "time_zone": _lt.VARCHAR,
            "next_due_at": _lt.TIMESTAMP(),
            "action_kind": _lt.VARCHAR,
            "target": _lt.VARCHAR,
            "runs_as": _lt.VARCHAR,
            "created_by": _lt.VARCHAR,
            "created_at": _lt.TIMESTAMP(),
            "last_fired_at": _lt.TIMESTAMP(),
            "last_fired_status": _lt.VARCHAR,
            "suspended_at": _lt.TIMESTAMP(),
            "suspended_by": _lt.VARCHAR,
            "minimum_interval_seconds": _lt.INT64,
        }
        self.schema = RelationSchema(
            name="information_schema.triggers",
            columns=[
                SchemaColumn(
                    name=column_name,
                    column_type=column_types[column_name],
                    identity=mint_column_identity("information_schema.triggers", column_name),
                )
                for column_name in self._COLUMNS
            ],
        )
        return self.schema

    def read_dataset(self, predicates=None, **kwargs) -> Iterable[Morsel]:
        compiled = _compile_key_predicates(predicates, self._pushable_columns)

        trigger_catalog = []
        trigger_collection = []
        trigger_name = []
        trigger_holder = []
        event_object_table = []
        event_kind = []
        schedule = []
        time_zone = []
        next_due_at = []
        action_kind = []
        # What the trigger RUNS, whichever kind it is. A trigger has exactly one
        # target, so this is one column rather than a `target_view`/`target_task`
        # pair where one is always null - which is what the column used to be,
        # and why a task trigger showed a NULL target and no way to see what it
        # fired.
        target = []
        runs_as = []
        created_by = []
        created_at = []
        last_fired_at = []
        last_fired_status = []
        suspended_at = []
        suspended_by = []
        minimum_interval_seconds = []

        def emit(collection: str, holder: str, window_from, trigger: dict) -> None:
            """One row, from a trigger record and the holder it was read off."""
            name_value = trigger.get("name")
            if not _key_predicates_allow(compiled, {"trigger_name": name_value}):
                return
            trigger_catalog.append(self.workspace)
            trigger_collection.append(collection)
            trigger_name.append(name_value)
            trigger_holder.append(holder)
            event_object_table.append(window_from)
            event_kind.append(trigger.get("event-kind") or "commit")
            schedule.append(trigger.get("schedule"))
            time_zone.append(trigger.get("time-zone"))
            next_due_at.append(_ms_to_datetime(trigger.get("next-due-at-ms")))
            action_kind.append(trigger.get("kind"))
            target.append(trigger.get("target-view") or trigger.get("target-task"))
            runs_as.append(trigger.get("runs-as"))
            created_by.append(trigger.get("created-by"))
            created_at.append(_ms_to_datetime(trigger.get("created-at-ms")))
            last_fired_at.append(_ms_to_datetime(trigger.get("last-fired-at-ms")))
            last_fired_status.append(trigger.get("last-fired-status"))
            # Both are cleared to None on RESUME, so a resumed trigger reports
            # nulls rather than the stamp of the suspension it came out of.
            suspended_at.append(_ms_to_datetime(trigger.get("suspended-at-ms")))
            suspended_by.append(trigger.get("suspended-by"))
            minimum_interval_seconds.append(trigger.get("minimum-interval-seconds"))

        # See InformationSchemaTablesTable.read_dataset - trigger_catalog is
        # constant per reader, so an excluding predicate skips enumeration
        # entirely rather than filtering row by row.
        if _key_predicates_allow(compiled, {"trigger_catalog": self.workspace}):
            # Task-held triggers exist only where the catalog has tasks at all.
            # An older library, or a test double, has no `list_tasks` and so
            # nothing a task could hold - the guard `is_task` applies in the
            # connector, for the same reason.
            list_tasks = getattr(self.catalog, "list_tasks", None)
            for collection in self.catalog.list_collections():
                if not _key_predicates_allow(compiled, {"trigger_collection": collection}):
                    continue
                for name in self.catalog.list_datasets(collection):
                    source_table = f"{collection}.{name}"
                    # A commit trigger's holder IS its event_object_table, so a
                    # predicate on either skips the round trip.
                    if not _key_predicates_allow(
                        compiled,
                        {"trigger_holder": source_table, "event_object_table": source_table},
                    ):
                        continue
                    if not _automatable(self.execution_context, self.workspace, collection, name):
                        continue
                    for trigger in self.catalog.list_triggers(source_table):
                        emit(collection, source_table, source_table, trigger)
                if list_tasks is None:
                    continue
                for name in list_tasks(collection):
                    holder = f"{collection}.{name}"
                    if not _key_predicates_allow(compiled, {"trigger_holder": holder}):
                        continue
                    if not _automatable(self.execution_context, self.workspace, collection, name):
                        continue
                    for trigger in self.catalog.list_triggers(holder, holder_kind="task"):
                        # The window source is on the record, so a pushed
                        # event_object_table predicate is honoured here, after
                        # the round trip it could not skip.
                        window_from = trigger.get("window-source")
                        if not _key_predicates_allow(compiled, {"event_object_table": window_from}):
                            continue
                        emit(collection, holder, window_from, trigger)

        vectors = [
            vector_from_sequence(trigger_catalog, dtype=DrakenType.VARCHAR),
            vector_from_sequence(trigger_collection, dtype=DrakenType.VARCHAR),
            vector_from_sequence(trigger_name, dtype=DrakenType.VARCHAR),
            vector_from_sequence(trigger_holder, dtype=DrakenType.VARCHAR),
            vector_from_sequence(event_object_table, dtype=DrakenType.VARCHAR),
            vector_from_sequence(event_kind, dtype=DrakenType.VARCHAR),
            vector_from_sequence(schedule, dtype=DrakenType.VARCHAR),
            vector_from_sequence(time_zone, dtype=DrakenType.VARCHAR),
            vector_from_sequence(next_due_at, dtype=DrakenType.TIMESTAMP64),
            vector_from_sequence(action_kind, dtype=DrakenType.VARCHAR),
            vector_from_sequence(target, dtype=DrakenType.VARCHAR),
            vector_from_sequence(runs_as, dtype=DrakenType.VARCHAR),
            vector_from_sequence(created_by, dtype=DrakenType.VARCHAR),
            vector_from_sequence(created_at, dtype=DrakenType.TIMESTAMP64),
            vector_from_sequence(last_fired_at, dtype=DrakenType.TIMESTAMP64),
            vector_from_sequence(last_fired_status, dtype=DrakenType.VARCHAR),
            vector_from_sequence(suspended_at, dtype=DrakenType.TIMESTAMP64),
            vector_from_sequence(suspended_by, dtype=DrakenType.VARCHAR),
            vector_from_sequence(minimum_interval_seconds, dtype=DrakenType.INT64),
        ]
        yield Morsel.from_vectors(list(self._COLUMNS), vectors)


class InformationSchemaTasksTable(BaseTable, _KeyColumnPredicatePushable):
    """Reads `information_schema.tasks` - every task registered in the workspace.

    A task is a statement the platform runs on its own. Until this table there
    was no way to LIST them: a task read only as the single row of its own
    definition, so seeing the workspace's tasks cost one request each and
    knowing their names in advance.

    `writes` is why this exists in the shape it does. A trigger records which
    dataset FIRES a task; nothing recorded which dataset it FEEDS, so a pipeline
    of `raw -> task -> curated -> task -> marts` read as disconnected fragments.
    It is derived from the task's own statement at registration - never
    declared - so it cannot disagree with what the task will actually do.

    Cost, stated plainly: one catalog listing per collection, plus one
    `get_task` per task (which itself reads the task document and its current
    statement document). This collapses N client requests into one query; it
    does not collapse the N catalog reads behind them.

    AUTOMATE is checked on the task's own name. A task shares one namespace with
    tables and views - a name identifies exactly one of them - so the grant that
    governs the name governs the row; and the action is AUTOMATE, not READ,
    because nobody SELECTs from a task. Its row is its statement, what that
    statement writes, and who runs it - automation, which only an owner may
    create, drop, or alter, and so only an owner may list.
    """

    __mode__ = "Internal"
    interal_only = True  # routes through the generic "Reader" physical node, like $planets/$no_table
    self_governs_permissions = True  # read_dataset() filters rows by AUTOMATE access itself
    supports_predicate_pushdown = True

    _COLUMNS = (
        "task_catalog",
        "task_collection",
        "task_name",
        "description",
        # The task's CURRENT statement, resolved through `statement-id` from the
        # subcollection that versions it - the same storage a view's definition
        # uses, and the reason a row here costs a second read.
        "statement",
        "statement_id",
        # WHAT THE STATEMENT WRITES, derived from its own AST at registration.
        # Comma-separated, which is unambiguous because a relation name cannot
        # contain a comma; empty for a task that writes no relation contents, and
        # for one registered before the field existed - a record that was never
        # asked the question answers "nothing", which is the honest reading.
        "writes",
        "created_by",
        "created_at",
        # Who last changed the STATEMENT, which is a different question from who
        # created the task and often a different person.
        "last_updated_by",
        "last_updated_at",
        "suspended_at",
        "suspended_by",
        "last_fired_at",
        "last_fired_status",
        # The `current_version` the last SUCCESSFUL run consumed to - a version
        # number, not a timestamp, and stamped only on success, so a gap behind a
        # failed run stays visible.
        "last_window_to",
        # YOUR OWN subscription to this task, or null - "ERROR", "SUCCESS" or
        # "EVERYTHING". Deliberately NOT a count of subscribers: that would tell
        # everyone who can read this table how many people watch a task, and on
        # a small team that is the subscriber list. Reading it costs no extra
        # round trip - one collection-group query answers for every row.
        "listening",
    )

    # task_catalog/task_collection are known before the per-collection listing,
    # so pushing them skips it entirely; task_name prunes before the `get_task`
    # round trip, which is the expensive one.
    _pushable_columns = frozenset({"task_catalog", "task_collection", "task_name"})

    def __init__(self, *, dataset, catalog, workspace, telemetry, execution_context=None, **kwargs):
        BaseTable.__init__(self, dataset=dataset, telemetry=telemetry, **kwargs)
        PredicatePushable.__init__(self, **kwargs)
        self.catalog = catalog
        self.workspace = workspace
        self.execution_context = execution_context

    def get_dataset_schema(self) -> RelationSchema:
        column_types = {
            "task_catalog": _lt.VARCHAR,
            "task_collection": _lt.VARCHAR,
            "task_name": _lt.VARCHAR,
            "description": _lt.VARCHAR,
            "statement": _lt.VARCHAR,
            "statement_id": _lt.VARCHAR,
            "writes": _lt.VARCHAR,
            "created_by": _lt.VARCHAR,
            "created_at": _lt.TIMESTAMP(),
            "last_updated_by": _lt.VARCHAR,
            "last_updated_at": _lt.TIMESTAMP(),
            "suspended_at": _lt.TIMESTAMP(),
            "suspended_by": _lt.VARCHAR,
            "last_fired_at": _lt.TIMESTAMP(),
            "last_fired_status": _lt.VARCHAR,
            "last_window_to": _lt.INT64,
            "listening": _lt.VARCHAR,
        }
        self.schema = RelationSchema(
            name="information_schema.tasks",
            columns=[
                SchemaColumn(
                    name=column_name,
                    column_type=column_types[column_name],
                    identity=mint_column_identity("information_schema.tasks", column_name),
                )
                for column_name in self._COLUMNS
            ],
        )
        return self.schema

    def read_dataset(self, predicates=None, **kwargs) -> Iterable[Morsel]:
        compiled = _compile_key_predicates(predicates, self._pushable_columns)

        task_catalog = []
        task_collection = []
        task_name = []
        description = []
        statement = []
        statement_id = []
        writes = []
        created_by = []
        created_at = []
        last_updated_by = []
        last_updated_at = []
        suspended_at = []
        suspended_by = []
        last_fired_at = []
        last_fired_status = []
        last_window_to = []
        listening = []

        # The caller's OWN subscriptions, read once for the whole listing rather
        # than per task: one collection-group query answers every row, so the
        # column costs no round trip per task. Keyed by (collection, task) -
        # the workspace is constant for this reader.
        user = self.execution_context.user if self.execution_context else None
        lister = getattr(self.catalog, "list_listeners_for_user", None)
        subscriptions = {}
        if user and lister is not None:
            subscriptions = {
                (row.get("collection"), row.get("task")): row.get("outcome")
                for row in lister(user)
            }

        # See InformationSchemaTablesTable.read_dataset - task_catalog is
        # constant per reader, so an excluding predicate skips enumeration
        # entirely rather than filtering row by row.
        if _key_predicates_allow(compiled, {"task_catalog": self.workspace}):
            for collection in self.catalog.list_collections():
                if not _key_predicates_allow(compiled, {"task_collection": collection}):
                    continue
                for name in self.catalog.list_tasks(collection):
                    if not _key_predicates_allow(compiled, {"task_name": name}):
                        continue
                    # Checked BEFORE `get_task`, so a task the caller may not
                    # see costs no round trip and discloses nothing.
                    if not _automatable(self.execution_context, self.workspace, collection, name):
                        continue
                    record = self.catalog.get_task(f"{collection}.{name}")
                    task_catalog.append(self.workspace)
                    task_collection.append(collection)
                    task_name.append(name)
                    description.append(record.get("description"))
                    statement.append(record.get("sql"))
                    statement_id.append(record.get("statement-id"))
                    writes.append(",".join(record.get("writes") or []))
                    created_by.append(record.get("created-by"))
                    created_at.append(_ms_to_datetime(record.get("created-at-ms")))
                    last_updated_by.append(record.get("last-updated-by"))
                    last_updated_at.append(_ms_to_datetime(record.get("last-updated-at-ms")))
                    suspended_at.append(_ms_to_datetime(record.get("suspended-at-ms")))
                    suspended_by.append(record.get("suspended-by"))
                    last_fired_at.append(_ms_to_datetime(record.get("last-fired-at-ms")))
                    last_fired_status.append(record.get("last-fired-status"))
                    last_window_to.append(record.get("last-window-to"))
                    # Null where this caller does not listen. NOT a subscriber
                    # count - see the column comment.
                    listening.append(subscriptions.get((collection, name)))

        vectors = [
            vector_from_sequence(task_catalog, dtype=DrakenType.VARCHAR),
            vector_from_sequence(task_collection, dtype=DrakenType.VARCHAR),
            vector_from_sequence(task_name, dtype=DrakenType.VARCHAR),
            vector_from_sequence(description, dtype=DrakenType.VARCHAR),
            vector_from_sequence(statement, dtype=DrakenType.VARCHAR),
            vector_from_sequence(statement_id, dtype=DrakenType.VARCHAR),
            vector_from_sequence(writes, dtype=DrakenType.VARCHAR),
            vector_from_sequence(created_by, dtype=DrakenType.VARCHAR),
            vector_from_sequence(created_at, dtype=DrakenType.TIMESTAMP64),
            vector_from_sequence(last_updated_by, dtype=DrakenType.VARCHAR),
            vector_from_sequence(last_updated_at, dtype=DrakenType.TIMESTAMP64),
            vector_from_sequence(suspended_at, dtype=DrakenType.TIMESTAMP64),
            vector_from_sequence(suspended_by, dtype=DrakenType.VARCHAR),
            vector_from_sequence(last_fired_at, dtype=DrakenType.TIMESTAMP64),
            vector_from_sequence(last_fired_status, dtype=DrakenType.VARCHAR),
            vector_from_sequence(last_window_to, dtype=DrakenType.INT64),
            vector_from_sequence(listening, dtype=DrakenType.VARCHAR),
        ]
        yield Morsel.from_vectors(list(self._COLUMNS), vectors)


class InformationSchemaColumnRelationshipsTable(BaseTable, _KeyColumnPredicatePushable):
    """Reads `information_schema.column_relationships` from the catalog's
    declared relationships - one read, never a walk over the workspace's
    datasets. See `_declared_relationships` for the two shapes that read takes
    and why enumerating datasets was both slow and redundant.

    A relationship is a DECLARATION that two columns hold corresponding values.
    Nothing enforces it: a write that breaks it succeeds, and no query plan
    consults it. This projection is the only way to read one back.

    TWO datasets, so two READ checks, and that is what makes this table
    different from every other one here. A trigger row is about the dataset it
    hangs off; a relationship row NAMES A SECOND DATASET - its collection,
    dataset and column - so showing it to someone who can read only the near
    side discloses the existence and shape of data they hold no grant on. Rows
    are therefore built only where both ends are readable, rather than built and
    then filtered: a row the caller may not see is never constructed. Copying
    the single-check shape from the triggers table would fail open, quietly, and
    only for the people it should protect.

    Only the near side is enumerated. "What points AT this dataset" is
    `find_relationships_to` on the catalog, a collection group query - not
    something this walk answers, and deliberately not a mirrored row.
    """

    __mode__ = "Internal"
    interal_only = True  # routes through the generic "Reader" physical node, like $planets/$no_table
    self_governs_permissions = True  # read_dataset() filters rows by READ access itself - see module docstring
    # BaseTable also declares this (False); it comes first in the MRO, so it
    # would otherwise shadow _KeyColumnPredicatePushable's True.
    supports_predicate_pushdown = True

    _COLUMNS = (
        "constraint_catalog",
        "constraint_collection",
        "constraint_name",
        "table_name",
        "column_name",
        "referenced_table_name",
        "referenced_column_name",
        "relationship_kind",
        "cardinality",
        "origin",
        "status",
        # Inferred rows only, NULL on anything a person asserted. `confidence`
        # is the job's own score and `evidence` is what it observed - the
        # overlap ratio and how many values were compared. Both are here
        # because a proposal is shown to an owner to be judged, and a bare
        # score is not something anyone can judge.
        "confidence",
        "evidence",
        "asserted_by",
        "asserted_at",
        "verified_at",
    )

    # constraint_catalog decides whether this reader has anything to say at
    # all, and table_name picks the read: pinned, it addresses one dataset's
    # subcollection directly, which is what makes the `$metadata` read ("what
    # relates to THIS dataset") a single keyed read. constraint_collection and
    # constraint_name prune rows after the read rather than round trips - still
    # worth pushing, since it settles them here instead of in a Filter node.
    _pushable_columns = frozenset(
        {"constraint_catalog", "constraint_collection", "table_name", "constraint_name"}
    )

    def __init__(self, *, dataset, catalog, workspace, telemetry, execution_context=None, **kwargs):
        BaseTable.__init__(self, dataset=dataset, telemetry=telemetry, **kwargs)
        PredicatePushable.__init__(self, **kwargs)
        self.catalog = catalog
        self.workspace = workspace
        self.execution_context = execution_context

    def get_dataset_schema(self) -> RelationSchema:
        column_types = {
            "constraint_catalog": _lt.VARCHAR,
            "constraint_collection": _lt.VARCHAR,
            "constraint_name": _lt.VARCHAR,
            "table_name": _lt.VARCHAR,
            "column_name": _lt.VARCHAR,
            "referenced_table_name": _lt.VARCHAR,
            "referenced_column_name": _lt.VARCHAR,
            "relationship_kind": _lt.VARCHAR,
            "cardinality": _lt.VARCHAR,
            "origin": _lt.VARCHAR,
            "status": _lt.VARCHAR,
            "confidence": _lt.FLOAT64,
            "evidence": _lt.VARCHAR,
            "asserted_by": _lt.VARCHAR,
            "asserted_at": _lt.TIMESTAMP(),
            "verified_at": _lt.TIMESTAMP(),
        }
        self.schema = RelationSchema(
            name="information_schema.column_relationships",
            columns=[
                SchemaColumn(
                    name=column_name,
                    column_type=column_types[column_name],
                    identity=mint_column_identity(
                        "information_schema.column_relationships", column_name
                    ),
                )
                for column_name in self._COLUMNS
            ],
        )
        return self.schema

    def _declared_relationships(self, compiled):
        """(collection, dataset, relationship) for every relationship declared
        in this workspace. Two shapes, and NEITHER enumerates datasets.

        The first implementation walked list_collections() -> list_datasets()
        -> list_relationships(), one round trip per dataset. That costs
        `1 + collections + datasets` sequential round trips to produce rows
        that number in the tens, and it scales with the size of the workspace
        rather than with the number of relationships in it - measured at 31
        round trips and 10.1s on a 25-dataset workspace, against 150ms here.

        Pinned `table_name` addresses that dataset's subcollection directly.
        Otherwise the catalog answers the whole workspace in one collection
        group query, and each row's denormalised near address says which
        dataset declared it, so the enumeration it replaced was redundant
        rather than merely slow.

        Rows arrive UNAUTHORIZED in both shapes, and both READ checks are owed
        by the caller below. What changes from the walk is only WHEN the near
        check happens: it used to skip an unreadable dataset before reading its
        subcollection, and now it discards that dataset's rows after the read.
        No row the caller may not see is ever constructed either way - the
        documents are read by the engine, never handed out.
        """
        pinned_table = _pinned_equality(compiled, "table_name")
        if pinned_table is not None:
            # Split as the catalog does - left-anchored, one split. A collection
            # name may not contain a dot and a dataset name may, so `a.b.c` is
            # dataset `b.c` in collection `a`.
            if "." not in pinned_table:
                return
            collection, dataset = pinned_table.split(".", 1)
            for relationship in self.catalog.list_relationships(pinned_table):
                yield collection, dataset, relationship
            return

        for relationship in self.catalog.list_workspace_relationships():
            yield relationship.get("collection"), relationship.get("dataset"), relationship

    def read_dataset(self, predicates=None, **kwargs) -> Iterable[Morsel]:
        compiled = _compile_key_predicates(predicates, self._pushable_columns)

        rows = {column_name: [] for column_name in self._COLUMNS}

        # See InformationSchemaTablesTable.read_dataset - constraint_catalog is
        # constant per reader, so an excluding predicate skips the catalog
        # entirely rather than filtering row by row.
        if _key_predicates_allow(compiled, {"constraint_catalog": self.workspace}):
            for collection, name, relationship in self._declared_relationships(compiled):
                table_name = f"{collection}.{name}"
                if not _key_predicates_allow(
                    compiled,
                    {"constraint_collection": collection, "table_name": table_name},
                ):
                    continue
                if not _readable(self.execution_context, self.workspace, collection, name):
                    continue

                constraint_name = relationship.get("name")
                if not _key_predicates_allow(compiled, {"constraint_name": constraint_name}):
                    continue

                # The far end, and the check the triggers table has no
                # equivalent of. A relationship the caller may only half
                # see is not shown at all: the alternative - blanking the
                # far columns - still discloses that SOMETHING over there
                # is related, which is most of what was worth hiding.
                far_collection = relationship.get("references-collection")
                far_dataset = relationship.get("references-dataset")
                if not _readable(
                    self.execution_context, self.workspace, far_collection, far_dataset
                ):
                    continue

                rows["constraint_catalog"].append(self.workspace)
                rows["constraint_collection"].append(collection)
                rows["constraint_name"].append(constraint_name)
                rows["table_name"].append(table_name)
                rows["column_name"].append(relationship.get("column"))
                rows["referenced_table_name"].append(f"{far_collection}.{far_dataset}")
                rows["referenced_column_name"].append(relationship.get("references-column"))
                rows["relationship_kind"].append(relationship.get("kind"))
                rows["cardinality"].append(relationship.get("cardinality"))
                rows["origin"].append(relationship.get("origin"))
                rows["status"].append(relationship.get("status"))
                rows["confidence"].append(relationship.get("confidence"))
                rows["evidence"].append(_evidence_text(relationship.get("evidence")))
                rows["asserted_by"].append(relationship.get("asserted-by"))
                rows["asserted_at"].append(_ms_to_datetime(relationship.get("asserted-at-ms")))
                rows["verified_at"].append(_ms_to_datetime(relationship.get("verified-at-ms")))

        _TIMESTAMP_COLUMNS = {"asserted_at", "verified_at"}
        _DTYPES = {"confidence": DrakenType.FLOAT64}
        vectors = [
            vector_from_sequence(
                rows[column_name],
                dtype=_DTYPES.get(
                    column_name,
                    DrakenType.TIMESTAMP64
                    if column_name in _TIMESTAMP_COLUMNS
                    else DrakenType.VARCHAR,
                ),
            )
            for column_name in self._COLUMNS
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



_GRANT_OBJECT_WORKSPACE = "workspace"
_GRANT_OBJECT_COLLECTION = "collection"
_GRANT_OBJECT_DATASET = "dataset"

_GRANT_ORIGIN_EXPLICIT = "explicit"
_GRANT_ORIGIN_INHERITED = "inherited"


def _grant_object_pattern(kind: str, name: str) -> str:
    """The pattern the grant statements issue for an object - the planner's
    own mapping: `WORKSPACE w` -> `w.*`, `COLLECTION w.c` -> `w.c.*`,
    `DATASET w.c.d` -> `w.c.d`."""
    if kind == _GRANT_OBJECT_DATASET:
        return name
    return f"{name}.*"


def _grant_object_from_pattern(workspace: str, pattern: str):
    """The (collection, kind, name) a pattern addresses - the inverse of
    `_grant_object_pattern`, for the patterns the capability reports that the
    engine did not ask about (a stored policy on something the catalog no
    longer holds). Also reads a bare object name (`w`, `w.c`, `w.c.d`) the
    way the statements do. None for a pattern that names no single object: a
    wildcard mid-pattern, or a subtree below a dataset.

    A dataset name may contain dots - the catalog splits `a.b.c` as dataset
    `b.c` in collection `a` - so three or more literal segments are a dataset,
    not an error.
    """
    if not pattern:
        return None
    segments = pattern.split(".")
    if segments[0] != workspace or any(not segment for segment in segments):
        return None
    if len(segments) == 1 or (len(segments) == 2 and segments[1] == "*"):
        return (None, _GRANT_OBJECT_WORKSPACE, workspace)
    if "*" in segments[1:-1] or segments[1] == "*":
        return None
    collection = segments[1]
    if segments[-1] == "*":
        if len(segments) == 3:
            return (collection, _GRANT_OBJECT_COLLECTION, f"{workspace}.{collection}")
        return None
    if len(segments) == 2:
        return (collection, _GRANT_OBJECT_COLLECTION, f"{workspace}.{collection}")
    return (collection, _GRANT_OBJECT_DATASET, pattern)


class InformationSchemaGrantsTable(BaseTable, _KeyColumnPredicatePushable):
    """Reads `information_schema.grants`: for every object in the workspace,
    every stored policy that reaches it, and whether that policy is stored AT
    the object or covers it from above.

    `SHOW GRANTS ON` and `SHOW EFFECTIVE GRANTS ON` answer one object at a
    time; this is both answers for the whole workspace, as a relation, so an
    interface can read access live through a filtered SELECT instead of
    queueing a statement per object. One row per (object, covering policy) -
    the statements' one-row-per-policy shape, and for the same reason: which
    policy grants the access is what an administrator has to act on, so a
    collapse to one role per user would hide the thing to change.

    `origin` is the column the table exists for. `explicit` means the policy's
    pattern IS the object - what a GRANT or REVOKE there acts on, the rows
    `SHOW GRANTS ON` reports. `inherited` means it covers the object from the
    collection or workspace above - the rows `SHOW EFFECTIVE GRANTS ON` adds.
    Every stored policy appears as `explicit` at its own pattern exactly once,
    whether or not the catalog still holds what it names, so a grant on a
    dropped dataset is listed rather than lost. That makes `WHERE origin =
    'explicit'` the workspace's stored policies and `WHERE object_name = ...`
    an object's effective ones - the two screens, one table.

    Objects are the three things the grant statements can name, named the way
    the statements name them: the workspace (`w`), each collection (`w.c`) and
    each table and view (`w.c.d`). The workspace row lists the policies that
    cover the workspace AS AN OBJECT (`w.*`), not every policy in it - the
    latter is the `explicit` rows.

    The catalog is walked once and the policy store read once, however many
    objects there are (`effective_grants_in`). Coverage, the explicit test
    and the gate all belong to the registered permissions capability: the
    matcher is the one that decides real queries, and the gate is the one the
    statements hold - owner authority covering the object. An object the
    caller may not administer has no rows, as every table here shows only
    what the caller may see; nothing is refused. A missing execution context
    shows nothing.
    """

    __mode__ = "Internal"
    interal_only = True  # routes through the generic "Reader" physical node, like $planets/$no_table
    self_governs_permissions = True  # the capability gates every row on administer authority - see class docstring
    # BaseTable also declares this (False); it comes first in the MRO, so it
    # would otherwise shadow _KeyColumnPredicatePushable's True.
    supports_predicate_pushdown = True

    _COLUMNS = (
        "grant_catalog",
        # The object's collection; NULL on the workspace row. The enumeration
        # key, as `trigger_collection` is for triggers.
        "grant_collection",
        # workspace | collection | dataset - the grant statements' own kinds.
        "object_kind",
        # Fully qualified (`w`, `w.c`, `w.c.d`), so it reads beside `pattern`
        # and is what `GRANT ... ON <kind> <object_name>` takes verbatim.
        "object_name",
        "grantee",
        "role",
        # The covering policy, as the statements report it: its pattern and
        # the level that pattern addresses.
        "pattern",
        "level",
        # explicit | inherited - see the class docstring.
        "origin",
    )

    # All four are known before the policy store is read. grant_catalog
    # decides whether there is anything to say at all; grant_collection and
    # object_kind skip catalog listings; object_name pinned by equality skips
    # the catalog entirely and asks about that one object. grantee, pattern,
    # level and origin are known only once the store has answered, so pushing
    # them would skip nothing and they stay ordinary Filters downstream.
    _pushable_columns = frozenset(
        {"grant_catalog", "grant_collection", "object_kind", "object_name"}
    )

    def __init__(self, *, dataset, catalog, workspace, telemetry, execution_context=None, **kwargs):
        BaseTable.__init__(self, dataset=dataset, telemetry=telemetry, **kwargs)
        PredicatePushable.__init__(self, **kwargs)
        self.catalog = catalog
        self.workspace = workspace
        self.execution_context = execution_context

    def get_dataset_schema(self) -> RelationSchema:
        self.schema = RelationSchema(
            name="information_schema.grants",
            columns=[
                SchemaColumn(
                    name=column_name,
                    column_type=_lt.VARCHAR,
                    identity=mint_column_identity("information_schema.grants", column_name),
                )
                for column_name in self._COLUMNS
            ],
        )
        return self.schema

    def _objects(self, compiled):
        """(collection, kind, name) for every object to ask about, in row order.

        A pinned `object_name` is the dataset page's read: no catalog round
        trip at all, one object, one policy read. The statements do not check
        the object exists either - the answer is about the name.
        """
        pinned = _pinned_equality(compiled, "object_name")
        if pinned is not None:
            described = _grant_object_from_pattern(self.workspace, pinned)
            return [described] if described else []

        objects = []
        if _key_predicates_allow(
            compiled,
            {
                "grant_collection": None,
                "object_kind": _GRANT_OBJECT_WORKSPACE,
                "object_name": self.workspace,
            },
        ):
            objects.append((None, _GRANT_OBJECT_WORKSPACE, self.workspace))

        for collection in self.catalog.list_collections():
            if not _key_predicates_allow(compiled, {"grant_collection": collection}):
                continue
            collection_name = f"{self.workspace}.{collection}"
            if _key_predicates_allow(
                compiled,
                {"object_kind": _GRANT_OBJECT_COLLECTION, "object_name": collection_name},
            ):
                objects.append((collection, _GRANT_OBJECT_COLLECTION, collection_name))
            if not _key_predicates_allow(compiled, {"object_kind": _GRANT_OBJECT_DATASET}):
                continue
            # Tables and views alike: both are `DATASET` to the grant
            # statements, and `tables` lists both.
            for name in [
                *self.catalog.list_datasets(collection),
                *self.catalog.list_views(collection),
            ]:
                dataset_name = f"{collection_name}.{name}"
                if _key_predicates_allow(compiled, {"object_name": dataset_name}):
                    objects.append((collection, _GRANT_OBJECT_DATASET, dataset_name))
        return objects

    def read_dataset(self, predicates=None, **kwargs) -> Iterable[Morsel]:
        compiled = _compile_key_predicates(predicates, self._pushable_columns)

        rows = {column_name: [] for column_name in self._COLUMNS}

        # See InformationSchemaTablesTable.read_dataset - grant_catalog is
        # constant per reader, so an excluding predicate skips everything.
        # No execution context means no caller to hold authority: nothing.
        if self.execution_context is not None and _key_predicates_allow(
            compiled, {"grant_catalog": self.workspace}
        ):
            by_pattern = {}
            for collection, kind, name in self._objects(compiled):
                by_pattern[_grant_object_pattern(kind, name)] = (collection, kind, name)

            reported = effective_grants_in(
                self.execution_context, self.workspace, list(by_pattern)
            )

            for row in reported:
                object_pattern = row.get("object")
                # What was asked maps straight back; a pattern the capability
                # added (a stored policy the catalog has no object for) is
                # read the way the statements read an object name.
                described = by_pattern.get(object_pattern) or _grant_object_from_pattern(
                    self.workspace, object_pattern
                )
                if described is None:
                    continue
                collection, kind, name = described
                # Asked objects already passed these; the capability's extras
                # have not, and a pushed predicate is a promise to the planner.
                if not _key_predicates_allow(
                    compiled,
                    {"grant_collection": collection, "object_kind": kind, "object_name": name},
                ):
                    continue
                rows["grant_catalog"].append(self.workspace)
                rows["grant_collection"].append(collection)
                rows["object_kind"].append(kind)
                rows["object_name"].append(name)
                rows["grantee"].append(row.get("user"))
                rows["role"].append(row.get("role"))
                rows["pattern"].append(row.get("pattern"))
                rows["level"].append(row.get("level"))
                rows["origin"].append(
                    _GRANT_ORIGIN_EXPLICIT if row.get("explicit") else _GRANT_ORIGIN_INHERITED
                )

        vectors = [
            vector_from_sequence(rows[column_name], dtype=DrakenType.VARCHAR)
            for column_name in self._COLUMNS
        ]
        yield Morsel.from_vectors(list(self._COLUMNS), vectors)


class InformationSchemaListenersTable(BaseTable):
    """Reads `information_schema.listeners` - the tasks the CALLER listens to.

    Self-scoped, and that is the whole of its authority model. It returns the
    session user's own subscriptions and nobody else's: there is no form that
    lists another user's, and none that lists a task's subscribers, which would
    tell whoever asked who else is watching it - the same leak that keeps
    listeners out of SHOW CREATE TASK.

    So it needs no permission check. Every row it can return was authorized by
    the LISTEN that wrote it, against what the task writes, at the moment it was
    written.

    This is the PRIMARY surface for a subscriber, not a convenience.
    `information_schema.tasks` carries the same answer in its `listening`
    column, but that table is AUTOMATE-gated and readable only by a task's
    OWNER - while LISTEN is gated on READ over what the task writes, so a
    subscriber who does not own the task cannot read it at all. `SHOW LISTENERS`
    is planned as a wildcard read of this table.

    One catalog query, not a walk: subscriptions live under the tasks they
    belong to, and `list_listeners_for_user` reaches them with a single
    collection-group read rather than a `get_task` per task.
    """

    __mode__ = "Internal"
    interal_only = True  # routes through the generic "Reader" physical node, like $planets/$no_table
    self_governs_permissions = True  # rows are the caller's own; there is nothing else to filter

    _COLUMNS = (
        "task_catalog",
        "task_collection",
        "task_name",
        # "ERROR" | "SUCCESS" | "EVERYTHING" - which outcomes this subscription
        # asked to hear about. Never null: a LISTEN with no FOR clause is
        # recorded as EVERYTHING rather than as an absence.
        "outcome",
        "created_at",
    )

    def __init__(self, *, dataset, catalog, workspace, telemetry, execution_context=None, **kwargs):
        BaseTable.__init__(self, dataset=dataset, telemetry=telemetry, **kwargs)
        self.catalog = catalog
        self.workspace = workspace
        self.execution_context = execution_context

    def get_dataset_schema(self) -> RelationSchema:
        column_types = {
            "task_catalog": _lt.VARCHAR,
            "task_collection": _lt.VARCHAR,
            "task_name": _lt.VARCHAR,
            "outcome": _lt.VARCHAR,
            "created_at": _lt.TIMESTAMP(),
        }
        self.schema = RelationSchema(
            name="information_schema.listeners",
            columns=[
                SchemaColumn(
                    name=column_name,
                    column_type=column_types[column_name],
                    identity=mint_column_identity("information_schema.listeners", column_name),
                )
                for column_name in self._COLUMNS
            ],
        )
        return self.schema

    def read_dataset(self, **kwargs):
        from draken.draken_native import DrakenType
        from draken.interop.vector_sequence import vector_from_sequence
        from draken.morsels.morsel import Morsel

        user = self.execution_context.user if self.execution_context else None

        task_catalog = []
        task_collection = []
        task_name = []
        outcome = []
        created_at = []

        # No user, no subscriptions. An unauthenticated session holds none -
        # LISTEN refuses to record one - so this is an empty answer, not a
        # missing filter.
        if user:
            # A catalog that predates listeners has none: the same skew
            # tolerance `InformationSchemaTriggersTable` applies to `list_tasks`.
            lister = getattr(self.catalog, "list_listeners_for_user", None)
            for row in lister(user) if lister is not None else []:
                task_catalog.append(row.get("workspace"))
                task_collection.append(row.get("collection"))
                task_name.append(row.get("task"))
                outcome.append(row.get("outcome"))
                created_at.append(_ms_to_datetime(row.get("created-at-ms")))

        vectors = [
            vector_from_sequence(task_catalog, dtype=DrakenType.VARCHAR),
            vector_from_sequence(task_collection, dtype=DrakenType.VARCHAR),
            vector_from_sequence(task_name, dtype=DrakenType.VARCHAR),
            vector_from_sequence(outcome, dtype=DrakenType.VARCHAR),
            vector_from_sequence(created_at, dtype=DrakenType.TIMESTAMP64),
        ]
        yield Morsel.from_vectors(list(self._COLUMNS), vectors)


_TABLE_CLASSES = {
    "tables": InformationSchemaTablesTable,
    "columns": InformationSchemaColumnsTable,
    "views": InformationSchemaViewsTable,
    "schemata": InformationSchemaSchemataTable,
    "triggers": InformationSchemaTriggersTable,
    "tasks": InformationSchemaTasksTable,
    "column_relationships": InformationSchemaColumnRelationshipsTable,
    "grants": InformationSchemaGrantsTable,
    "listeners": InformationSchemaListenersTable,
}
