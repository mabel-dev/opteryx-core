# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Output shape for `SHOW LINEAGE FOR <table>` - the receipts.

One row per (snapshot, read-source) in a catalog-backed relation's commit
history, newest snapshot first and then by `source_dataset`. The receipt is
the snapshot's `read-sources` (opteryx-catalog PROVENANCE_DESIGN.md S2.1): the
catalog relations the statement that produced the commit read, each with the
version it read and how that version was chosen.

A receipt has THREE states and this shape keeps all three apart, because two
of them look alike once flattened into rows:

- not recorded (`read_sources` is None): the writer did not report - the
  pre-feature history, or a commit through an engine older than the catalog.
  ONE marker row, `recorded` false, every source column null.
- read nothing (`[]`): `INSERT ... VALUES`, an upload, a CTAS over a literal.
  ONE row, `recorded` true, every source column null.
- read these: one row per entry, `recorded` true.

Collapsing the first two to "no rows" would make a table whose provenance was
never recorded look like one built from nothing, which is the one confusion
the receipt exists to prevent (S2.4).

As with `snapshot_history`, the connector normalizes the catalog's `Snapshot`
into these dicts (see OpteryxConnector.get_lineage), so this module depends
only on draken and never imports opteryx_catalog.

EVERY SOURCE IS NAMED, including one in a workspace the caller holds no grant
on (decision 2026-09-09; this reverses the elision the design first called
for). Lineage is a CITATION: knowing that this table was built from
`ops.raw.events` is not being able to read `ops.raw.events`, which still
needs a grant of its own. Blanking the name does not protect the upstream, it
just moves the work to a human who has to go and find out anyway - and it
makes impact analysis useless, since "something you cannot see depends on
this" is not an answer anybody can act on. Where a name is itself the
sensitive thing, the fix is upstream of here: do not publish data under a
name that leaks what it is.
"""

from typing import Dict, List, Optional

from opteryx.models.snapshot_history import _ms_to_datetime

# Column order IS the output column order. Values are the dtype tag handed to
# `vector_from_sequence`. Every column is a flat scalar.
_LINEAGE_COLUMNS = {
    "snapshot_id": "INTEGER",
    "committed_at": "TIMESTAMP",
    # True for the head, exactly as SHOW SNAPSHOTS reports it - and for the
    # same reason it is `is_current` rather than `is_latest`: a rollback moves
    # the head backwards, and the row it lands on is not the newest one.
    "is_current": "BOOLEAN",
    # `task:<name>` / `view:<name>` for a commit made by a task run or a
    # materialized-view refresh; null for a hand-run statement, where absent is
    # the true answer (S2.5). Named whatever the caller may read, as every
    # name here is.
    "produced_by": "VARCHAR",
    # ONE PHRASE for "how did this version come about", composed from the two
    # fields that answer it between them - `operation-type` (what happened to
    # the data) and `produced_by` (what made it happen). Derived here rather
    # than stored: a third field restating the other two is a third field that
    # can contradict them, and the reason for a column at all is that reading
    # the answer should not require joining two of them in your head.
    "how": "VARCHAR",
    # Null only on the marker row of the two empty states - a receipt that
    # was never reported, and one that read no catalog relation.
    "source_dataset": "VARCHAR",
    # Null for a source that had a schema and no commits when it was read: the
    # statement did read it, and read nothing, which is a fact about the commit
    # rather than an unknown.
    "source_snapshot_id": "INTEGER",
    # The S2.1 vocabulary: current / version / previous / tag / date.
    "resolved_by": "VARCHAR",
    # Whether the named source snapshot can still be read. A receipt does not
    # pin what it names (S2.1), so this is what tells a reader that the version
    # a table was built from has since expired. Null when the connector could
    # not look, which is not the same as False.
    "source_exists": "BOOLEAN",
    # False on the one marker row of a snapshot with no receipt. True on every
    # other row, INCLUDING the null-sourced row of a snapshot that read nothing:
    # that is a recorded fact, not a missing one.
    "recorded": "BOOLEAN",
}

# Columns that describe ONE read-source entry, and are therefore null on a
# marker row. `recorded` is the row's state and is never in this set.
_SOURCE_COLUMNS = ("source_dataset", "source_snapshot_id", "resolved_by", "source_exists")


def _lineage_column_types():
    from opteryx.types import logical_type as _lt

    return {
        "snapshot_id": _lt.INT64,
        "committed_at": _lt.TIMESTAMP(),
        "is_current": _lt.BOOLEAN,
        "produced_by": _lt.VARCHAR,
        "how": _lt.VARCHAR,
        "source_dataset": _lt.VARCHAR,
        "source_snapshot_id": _lt.INT64,
        "resolved_by": _lt.VARCHAR,
        "source_exists": _lt.BOOLEAN,
        "recorded": _lt.BOOLEAN,
    }


def lineage_output_schema(relation_name: str = "$lineage"):
    """The fixed RelationSchema `SHOW LINEAGE FOR <table>` always returns.

    Every _LINEAGE_COLUMNS column, never trimmed or projected - SHOW LINEAGE
    FOR has no WHERE/column-list grammar to do so with. row_count_estimate is
    left unset for the same reason snapshots_output_schema leaves it: the
    caller (visit_show_lineage) holds the real rows and knows their number.
    """
    from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity

    column_types = _lineage_column_types()
    return RelationSchema(
        name=relation_name,
        columns=[
            SchemaColumn(
                name=name,
                column_type=column_types[name],
                identity=mint_column_identity(relation_name, name),
            )
            for name in _LINEAGE_COLUMNS
        ],
    )


# What `operation-type` means in a sentence. The catalog's own vocabulary
# (opteryx_catalog.catalog.dataset), mapped once so the phrasing does not have
# to be reinvented at each surface. An operation absent from this map is shown
# as itself: the vocabulary can grow, and a word we have not met yet is more
# useful than "unknown".
_OPERATION_VERB = {
    "append": "appended",
    "add-files": "appended",
    "overwrite": "overwritten",
    "truncate-and-add-files": "replaced",
    "truncate": "truncated",
    "merge": "merged",
    "update": "updated",
    "delete": "rows deleted",
    "delete-files": "rows deleted",
    "compact": "compacted",
    "statistics-refresh": "statistics refreshed",
}

# Maintenance the catalog performs on itself. These carry no producer because
# nothing registered made them and nobody ran them, so they get the verb alone
# - saying "by hand" of a compaction would be a plain lie about who did it.
_MAINTENANCE = frozenset({"compact", "statistics-refresh", "expire"})


def describe_how(operation_type: Optional[str], produced_by: Optional[str]) -> str:
    """The `how` column: one phrase from the operation and its producer.

    The two are genuinely different facts and both are wanted - "merged" does
    not say who, and "task X" does not say what it did to the data - so this
    reads them together and neither is stored twice.

    A producer's kind decides how its second half reads: `task` and `view`
    name a catalog object, `upload` names the channel data arrived through.
    Absent means nobody registered made it, which is what a statement someone
    ran by hand looks like, and is said plainly rather than left blank.
    """
    verb = _OPERATION_VERB.get(operation_type or "", operation_type or "committed")
    if operation_type in _MAINTENANCE:
        return verb
    if not produced_by:
        return f"{verb}, by hand"
    kind, _, name = str(produced_by).partition(":")
    if kind == "upload":
        # The channel, not an object - there is no catalog name to give.
        return f"{verb}, uploaded via {name}" if name else f"{verb}, uploaded"
    if kind and name:
        return f"{verb}, by {kind} {name}"
    return f"{verb}, by {produced_by}"


def normalize_lineage(
    snapshot,
    current_snapshot_id: Optional[int] = None,
) -> List[Dict[str, object]]:
    """Expand one catalog `Snapshot` record into its _LINEAGE_COLUMNS rows.

    Reads by attribute so the caller can pass the catalog's dataclass straight
    in, and by `getattr` with a None default for the receipt fields: a
    `Snapshot` from a catalog older than the feature has no `read_sources`
    attribute at all, and that is the "not recorded" state, not an error.

    `source_exists` is left None on every row. It needs a catalog lookup per
    distinct source, which is the connector's to make (and to cache); this
    function only knows the receipt.

    Entries are emitted in (dataset, snapshot-id) order. The catalog writes the
    receipt sorted that way already (S2.1), but the ORDER of this statement's
    output is a promise of the statement and not of the store, so it is made
    here rather than trusted.
    """
    base = {
        "snapshot_id": snapshot.snapshot_id,
        "committed_at": _ms_to_datetime(snapshot.timestamp_ms),
        # A dataset with no head recorded has no current row, rather than every
        # row being current - `None == None` must not read as a match.
        "is_current": (
            current_snapshot_id is not None
            and snapshot.snapshot_id == current_snapshot_id
        ),
        "produced_by": getattr(snapshot, "produced_by", None),
        "how": describe_how(
            getattr(snapshot, "operation_type", None),
            getattr(snapshot, "produced_by", None),
        ),
    }
    read_sources = getattr(snapshot, "read_sources", None)

    if read_sources is None:
        # Not recorded. One marker row so the snapshot is still LISTED - a
        # history with gaps must show the gaps, not close over them.
        return [{**base, **dict.fromkeys(_SOURCE_COLUMNS), "recorded": False}]
    if not read_sources:
        # Read nothing. Also one row, and also all-null sources; `recorded` is
        # the only thing telling the two apart, which is why it is a column.
        return [{**base, **dict.fromkeys(_SOURCE_COLUMNS), "recorded": True}]

    def _entry_key(entry):
        # None sorts first within a name: a source read before its first commit
        # precedes every version of it that has one.
        snapshot_id = entry.get("snapshot-id")
        return (entry.get("dataset") or "", snapshot_id is not None, snapshot_id or 0)

    return [
        {
            **base,
            "source_dataset": entry.get("dataset"),
            "source_snapshot_id": entry.get("snapshot-id"),
            "resolved_by": entry.get("resolved-by"),
            "source_exists": None,
            "recorded": True,
        }
        for entry in sorted(read_sources, key=_entry_key)
    ]


def lineage_to_morsel(rows: List[Dict[str, object]]):
    """Build the single `SHOW LINEAGE FOR` Morsel from normalized rows.

    Rows arrive in the order they will be emitted - the connector orders the
    snapshots and normalize_lineage orders the entries within one; this does
    not re-sort.
    """
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel

    morsel = Morsel()
    for name, dtype in _LINEAGE_COLUMNS.items():
        morsel.append_vector(
            name, vector_from_sequence([row.get(name) for row in rows], dtype=dtype)
        )
    return morsel
