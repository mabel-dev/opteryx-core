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
only on draken and never imports opteryx_catalog. Elision (S4.4) is applied
here too, by `elide_lineage`, but against a predicate the binder supplies:
what a name means to the caller is the permissions capability's business, and
this module only knows which columns carry a name.
"""

from typing import Callable, Dict, List, Optional

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
    # the true answer (S2.5). Elided whole when the caller cannot READ the named
    # object - a task's name is as much a name as a dataset's.
    "produced_by": "VARCHAR",
    # Null when elided, and null on the marker row of the two empty states.
    "source_dataset": "VARCHAR",
    # Null for a source that had a schema and no commits when it was read: the
    # statement did read it, and read nothing, which is a fact about the commit
    # rather than an unknown.
    "source_snapshot_id": "INTEGER",
    # The S2.1 vocabulary: current / version / previous / tag / date.
    "resolved_by": "VARCHAR",
    # Whether the named source snapshot can still be read. A receipt does not
    # pin what it names (S2.1), so this is what tells a reader that the version
    # a table was built from has since expired. Null when the source is elided
    # - existence must not leak what the name did not - and null when the
    # connector could not look.
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


def elide_lineage(
    rows: List[Dict[str, object]], can_read: Callable[[str], bool]
) -> List[Dict[str, object]]:
    """Apply S4.4 to normalized rows, in place: null every NAME the caller may
    not READ and keep the row.

    The row stays because the existence of an upstream is not the secret - its
    name is - and "built from something you cannot see" is an answer the
    reader is owed. `source_exists` goes with the name: whether a snapshot of
    a dataset the caller cannot name still exists is a fact about that dataset,
    and would leak through a column that looks like a boolean about the row.
    `source_snapshot_id` and `resolved_by` stay, for the same reason the row
    does - they describe the read, not the relation.

    `produced_by` carries a name too, after its `task:`/`view:` prefix, and the
    whole value goes when that name is refused: the prefix alone says which
    kind of automation wrote the commit, and knowing there IS a task is the
    half of the fact the design keeps.

    `can_read` is asked once per distinct name. A hundred rows naming the same
    upstream are one question, and the capability behind the predicate may be
    a network call.
    """
    verdicts: Dict[str, bool] = {}

    def _visible(name: str) -> bool:
        if name not in verdicts:
            verdicts[name] = bool(can_read(name))
        return verdicts[name]

    for row in rows:
        source = row.get("source_dataset")
        if source is not None and not _visible(source):
            row["source_dataset"] = None
            row["source_exists"] = None
        producer = row.get("produced_by")
        if producer:
            _, _, name = str(producer).partition(":")
            if name and not _visible(name):
                row["produced_by"] = None
    return rows


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
