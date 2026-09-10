# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Output shape for `SHOW SNAPSHOTS FOR <table>`.

One row per LIVE snapshot in a catalog-backed relation's commit history,
newest first. Every column is a field of the catalog's own `Snapshot` record
(opteryx_catalog.catalog.metadata.Snapshot), with the nine `summary` counters
unpacked into columns of their own and `is_current` derived from the dataset's
head pointer. The pointer is called `current`, not `latest`: a rollback moves it
BACKWARDS, so the snapshot it names is not necessarily the most recent one
committed, and `latest` claimed a recency nothing here guarantees.

The connector normalizes its history into these key names before it reaches
here (see OpteryxConnector.get_snapshots), so this module depends only on
draken — it never imports opteryx_catalog, and a second connector that grows a
commit log answers the same statement by producing the same dicts.

Expired snapshots are NOT rows here. The catalog's loader tombstones them out
of `metadata.snapshots`, so this is the history that can still be read, not
every commit that ever happened.
"""

import datetime
from typing import Dict, List, Optional

# Column order IS the output column order. Values are the dtype tag handed to
# `vector_from_sequence`. All but `tags` are flat scalars; `tags` is an ARRAY of
# the tag names on that snapshot, which `vector_from_sequence` builds from a list
# of lists without any special-casing here.
_SNAPSHOT_COLUMNS = {
    "snapshot_id": "INTEGER",
    "committed_at": "TIMESTAMP",
    # True for the head - the snapshot an unqualified read returns. Exactly one
    # row has it, and after a rollback that row is NOT the newest one. That case
    # is precisely why the column is `is_current` rather than `is_latest`.
    "is_current": "BOOLEAN",
    # The tags naming this snapshot, or an empty list. This is what makes a tag
    # visible: a tag pins its snapshot's storage indefinitely and that storage is
    # charged, so tags accumulating unseen is a bill nobody can account for.
    # Includes the virtual tags `current` (on the head) and `previous` (on the
    # previous version of the DATA), which are names that resolve rather than
    # pins - see OpteryxConnector.get_snapshots.
    "tags": "ARRAY",
    "operation_type": "VARCHAR",
    "author": "VARCHAR",
    "user_created": "BOOLEAN",
    "sequence_number": "INTEGER",
    "parent_snapshot_id": "INTEGER",
    "schema_id": "VARCHAR",
    "commit_message": "VARCHAR",
    "added_records": "INTEGER",
    "added_data_files": "INTEGER",
    "added_files_size_in_bytes": "INTEGER",
    "deleted_records": "INTEGER",
    "deleted_data_files": "INTEGER",
    "deleted_files_size_in_bytes": "INTEGER",
    "total_records": "INTEGER",
    "total_data_files": "INTEGER",
    "total_files_size_in_bytes": "INTEGER",
}

# The two columns `SHOW ALL SNAPSHOTS FOR` adds, and only that statement: plain
# `SHOW SNAPSHOTS FOR` returns the shape above unchanged, because a column that
# is null on every row it can ever return is not an answer.
#
# `is_queryable` is stated rather than left to be inferred from `expired_at`. An
# expired snapshot cannot be queried - `VERSION AS OF <expired id>` resolves to
# nothing, by design, because the files behind it are in quarantine or GCS
# soft-delete - and a reader who has only a timestamp has to know that rule to
# work it out. The column says it.
#
# "Queryable", not "readable": the RECORD is very much alive, and readable in
# the plain sense - it is the row this statement just returned. What has
# stopped being possible is querying the data behind it.
_EXPIRY_COLUMNS = {
    "expired_at": "TIMESTAMP",
    "is_queryable": "BOOLEAN",
}

_ALL_SNAPSHOT_COLUMNS = {**_SNAPSHOT_COLUMNS, **_EXPIRY_COLUMNS}


class SnapshotRows(list):
    """The rows of one `SHOW [ALL] SNAPSHOTS FOR`, carrying which shape they are.

    A plain list would say it in the rows themselves — the ALL form writes the
    expiry keys and the plain form does not — but an EMPTY result has no rows to
    say it with, and the ALL form must still emit the columns its binder put in
    the schema. So the loader that built them states it once, here, and
    `snapshots_to_morsel` reads it rather than guessing.
    """

    __slots__ = ("include_expiry",)

    def __init__(self, rows=(), include_expiry: bool = False):
        super().__init__(rows)
        self.include_expiry = include_expiry

# `summary` key on the catalog's Snapshot -> our column name. The catalog spells
# these hyphenated; SQL identifiers cannot be, and `-` would have to be quoted
# at every use site. Kept as an explicit map rather than a mechanical
# `replace("-", "_")` so a catalog-side rename breaks a lookup here instead of
# silently producing an all-null column.
_SUMMARY_COLUMNS = {
    "added-records": "added_records",
    "added-data-files": "added_data_files",
    "added-files-size": "added_files_size_in_bytes",
    "deleted-records": "deleted_records",
    "deleted-data-files": "deleted_data_files",
    "deleted-files-size": "deleted_files_size_in_bytes",
    "total-records": "total_records",
    "total-data-files": "total_data_files",
    "total-files-size": "total_files_size_in_bytes",
}


def _snapshot_column_types():
    from opteryx.types import logical_type as _lt

    integer_columns = {
        name for name, dtype in _ALL_SNAPSHOT_COLUMNS.items() if dtype == "INTEGER"
    }
    types = {name: _lt.INT64 for name in integer_columns}
    types["committed_at"] = _lt.TIMESTAMP()
    types["expired_at"] = _lt.TIMESTAMP()
    types["is_current"] = _lt.BOOLEAN
    types["is_queryable"] = _lt.BOOLEAN
    types["user_created"] = _lt.BOOLEAN
    types["operation_type"] = _lt.VARCHAR
    types["author"] = _lt.VARCHAR
    types["schema_id"] = _lt.VARCHAR
    types["commit_message"] = _lt.VARCHAR
    # The element type is stated rather than inferred: an ARRAY whose element
    # type is unknown is a column downstream cannot compare, and the names in it
    # are always strings.
    types["tags"] = _lt.ARRAY(_lt.VARCHAR)
    return types


def snapshots_output_schema(
    relation_name: str = "$snapshots", include_expiry: bool = False
):
    """The fixed RelationSchema `SHOW [ALL] SNAPSHOTS FOR <table>` returns.

    Every _SNAPSHOT_COLUMNS column, never trimmed or projected — SHOW SNAPSHOTS
    FOR has no WHERE/column-list grammar to do so with. row_count_estimate is
    left unset for the same reason manifest_output_schema leaves it: the caller
    (visit_show_snapshots) holds the real history and knows its length.

    `include_expiry` is the ALL form, which adds _EXPIRY_COLUMNS. The binder
    passes it from the same `history_view` the connector chooses its loader
    from, so the schema this returns and the rows that arrive cannot disagree
    about which shape the statement is.
    """
    from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity

    column_types = _snapshot_column_types()
    return RelationSchema(
        name=relation_name,
        columns=[
            SchemaColumn(
                name=name,
                column_type=column_types[name],
                identity=mint_column_identity(relation_name, name),
            )
            for name in (_ALL_SNAPSHOT_COLUMNS if include_expiry else _SNAPSHOT_COLUMNS)
        ],
    )


def normalize_snapshot(
    snapshot,
    current_snapshot_id: Optional[int] = None,
    tags: Optional[List[str]] = None,
    include_expiry: bool = False,
) -> Dict[str, object]:
    """Flatten one catalog `Snapshot` record into the _SNAPSHOT_COLUMNS shape.

    Reads by attribute so the caller can pass the catalog's dataclass straight
    in. `summary` is a plain dict on that dataclass and a snapshot written by
    an older catalog may be missing keys entirely — a missing counter is None
    (unknown), NOT zero, which would claim the commit added nothing.

    `tags` is the names bound to THIS snapshot, which the caller has already
    grouped (a tag points at a snapshot; a snapshot does not carry its names).
    An untagged snapshot gets an empty list, not None: nothing is pinning it,
    which is a fact rather than an unknown.

    `include_expiry` adds the two `SHOW ALL SNAPSHOTS FOR` columns, read off
    the catalog record's `expired_at_ms` — which is None for a live snapshot,
    so the same call shapes a live row and a tombstoned one.
    """
    summary = snapshot.summary or {}
    row = {
        "snapshot_id": snapshot.snapshot_id,
        "committed_at": _ms_to_datetime(snapshot.timestamp_ms),
        # A dataset with no head recorded has no current row, rather than every
        # row being current — `None == None` must not read as a match.
        "is_current": (
            current_snapshot_id is not None
            and snapshot.snapshot_id == current_snapshot_id
        ),
        "tags": list(tags or []),
        "operation_type": snapshot.operation_type,
        "author": snapshot.author,
        "user_created": snapshot.user_created,
        "sequence_number": snapshot.sequence_number,
        "parent_snapshot_id": snapshot.parent_snapshot_id,
        "schema_id": snapshot.schema_id,
        "commit_message": snapshot.commit_message,
    }
    for summary_key, column in _SUMMARY_COLUMNS.items():
        row[column] = summary.get(summary_key)
    if include_expiry:
        expired_at_ms = getattr(snapshot, "expired_at_ms", None)
        row["expired_at"] = _ms_to_datetime(expired_at_ms)
        # An expired snapshot is a restore-window record, not a version: every
        # reader of a snapshot by id refuses a tombstone, so `VERSION AS OF` on
        # this row's id resolves to nothing.
        row["is_queryable"] = expired_at_ms is None
    return row


def _ms_to_datetime(ms) -> Optional[datetime.datetime]:
    """Epoch milliseconds to UTC datetime; None stays None (unrecorded)."""
    if ms is None:
        return None
    return datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc)


def snapshots_to_morsel(rows: List[Dict[str, object]]):
    """Build the single `SHOW [ALL] SNAPSHOTS FOR` Morsel from normalized rows.

    Rows arrive in the order they will be emitted — the connector sorts them
    newest-first; this does not re-sort, so there is one place that decides
    the order.

    WHICH shape is read off the rows rather than passed in, so the ShowSnapshots
    operator (Cython) carries no flag it would only be forwarding: `SnapshotRows`
    states it, and rows built by hand are read by their keys.
    """
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel

    include_expiry = getattr(rows, "include_expiry", None)
    if include_expiry is None:
        include_expiry = any("expired_at" in row for row in rows)
    columns = _ALL_SNAPSHOT_COLUMNS if include_expiry else _SNAPSHOT_COLUMNS

    morsel = Morsel()
    for name, dtype in columns.items():
        morsel.append_vector(
            name, vector_from_sequence([row.get(name) for row in rows], dtype=dtype)
        )
    return morsel
