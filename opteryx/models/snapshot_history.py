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
        name for name, dtype in _SNAPSHOT_COLUMNS.items() if dtype == "INTEGER"
    }
    types = {name: _lt.INT64 for name in integer_columns}
    types["committed_at"] = _lt.TIMESTAMP()
    types["is_current"] = _lt.BOOLEAN
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


def snapshots_output_schema(relation_name: str = "$snapshots"):
    """The fixed RelationSchema `SHOW SNAPSHOTS FOR <table>` always returns.

    Every _SNAPSHOT_COLUMNS column, never trimmed or projected — SHOW SNAPSHOTS
    FOR has no WHERE/column-list grammar to do so with. row_count_estimate is
    left unset for the same reason manifest_output_schema leaves it: the caller
    (visit_show_snapshots) holds the real history and knows its length.
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
            for name in _SNAPSHOT_COLUMNS
        ],
    )


def normalize_snapshot(
    snapshot,
    current_snapshot_id: Optional[int] = None,
    tags: Optional[List[str]] = None,
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
    return row


def _ms_to_datetime(ms) -> Optional[datetime.datetime]:
    """Epoch milliseconds to UTC datetime; None stays None (unrecorded)."""
    if ms is None:
        return None
    return datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc)


def snapshots_to_morsel(rows: List[Dict[str, object]]):
    """Build the single `SHOW SNAPSHOTS FOR` Morsel from normalized rows.

    Rows arrive in the order they will be emitted — the connector sorts them
    newest-first; this does not re-sort, so there is one place that decides
    the order.
    """
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel

    morsel = Morsel()
    for name, dtype in _SNAPSHOT_COLUMNS.items():
        morsel.append_vector(
            name, vector_from_sequence([row.get(name) for row in rows], dtype=dtype)
        )
    return morsel
