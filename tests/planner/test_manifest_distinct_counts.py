"""The manifest's `distinct_counts` column: a per-column NDV for a producer
whose source publishes it as a NUMBER rather than as something mergeable.

The engine's own NDV travels as a min-k SKETCH, which merges exactly across
files. A PostgreSQL or CockroachDB server publishes a count instead (`pg_stats`'
n_distinct, `SHOW STATISTICS`' distinct_count) and a count cannot be merged, so
it needs a channel of its own.

Two properties this file exists to hold:

  * the column is OPTIONAL on read - manifests written before it existed carry
    no such column and must keep reading, because none of them are rewritten;
  * everything read out of it is an ESTIMATE. The exactness flag is not
    persisted, so a count read back here can never reach
    `_exact_cardinality_from_footers`, whose answer consumers are entitled to
    treat as a BOUND (it prunes, and it answers DISTINCT without reading).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.models.manifest import Manifest
from opteryx.compiled.planner.native_manifest import NativeManifestBuilder
from opteryx.models.file_entry import FileEntry
from opteryx.models.manifest_io import read_manifest_file_entries
from opteryx.types import logical_type as _lt
from opteryx.types.schema import RelationSchema
from opteryx.planner.plan_context import PlanContext

# Bound columns are minted by a query's ColumnTable; these tests share one.
_PLAN_CONTEXT = PlanContext()


def _schema(*names):
    return RelationSchema(
        name="t",
        columns=[
            _PLAN_CONTEXT.columns.relation_column(
                "t",
                name,
                column_type=_lt.INT64,
            )
            for name in names
        ],
    )



def _manifest_bytes(schema, path, file_format, record_count, file_size, distinct=None, null_counts=None):
    """One file's manifest, written by the manifest writer (NativeManifest.to_parquet):
    `distinct` is {position: (count, is_exact)}, `null_counts` positional."""
    names = tuple(c.name for c in schema.columns)
    physical = tuple(c.column_type.physical for c in schema.columns)
    builder = NativeManifestBuilder(names, physical, True, True)
    row = builder.add_file(path, file_format, record_count, file_size)
    for position, (count, exact) in (distinct or {}).items():
        builder.set_distinct_count(row, position, count, exact)
    for position, nulls in enumerate(null_counts or []):
        if nulls is not None:
            builder.set_counts(row, position, null_count=nulls)
    return builder.build({}).to_parquet()

def test_distinct_counts_round_trip_positionally():
    """Position IS field id, the same convention `null_counts` and `min_values`
    use. A sparse dict must come back attached to the columns it was keyed to -
    keying from 1, or writing a gap, silently attaches every count to the NEXT
    column."""
    schema = _schema("a", "b", "c")
    data = _manifest_bytes(schema, "postgres://t", "POSTGRES", 1000, 0, distinct={0: (50, False), 2: (7, False)})
    back, _native = read_manifest_file_entries(data)
    assert back[0].distinct_value_counts == {0: (50, False), 2: (7, False)}


def test_a_producer_with_no_distinct_counts_writes_none():
    """The common case - the parquet path carries NDV inside `column_stats`
    instead. The column is written empty, the way min_k_hashes and
    histogram_counts are by producers that do not compute them."""
    schema = _schema("a")
    back, _native = read_manifest_file_entries(_manifest_bytes(schema, "a.parquet", "PARQUET", 10, 1))
    assert back[0].distinct_value_counts is None


def test_exactness_is_not_persisted_and_reads_back_as_an_estimate():
    """The safe direction. An exact count read back as an estimate loses an
    optimisation; an estimate read back as exact would lose ROWS."""
    schema = _schema("a")
    # written as EXACT ...
    data = _manifest_bytes(schema, "postgres://t", "POSTGRES", 100, 0, distinct={0: (9, True)})
    back, _native = read_manifest_file_entries(data)
    # ... and read back as an estimate, never the other way round.
    assert back[0].distinct_value_counts == {0: (9, False)}

    manifest = Manifest(back, schema, stats_are_authoritative=False, bounds_are_ordinal=True)
    # Not reachable as a BOUND ...
    assert manifest._exact_cardinality_from_footers("a") is None
    # ... but it is reachable for COSTING, which is the whole point.
    assert manifest.estimate_range_cardinality("a") == 9


def test_a_manifest_without_the_column_still_reads():
    """The backward-compatibility guarantee: `distinct_counts` was appended to
    the format, and no stored manifest is rewritten for it. A manifest that
    predates the column must read as "no counts", not raise."""
    schema = _schema("a")
    data = _manifest_bytes(schema, "a.parquet", "PARQUET", 10, 1, null_counts=[2])

    # The same manifest WITHOUT the column, the way an older writer produced
    # it, rather than asserting against a checked-in binary.
    import rugo.parquet as rugo_parquet

    with rugo_parquet.read_parquet(data) as reader:
        morsel = next(iter(reader))
    keep = [name for name in morsel.column_names if name not in (b"distinct_counts", "distinct_counts")]
    old_format = rugo_parquet.write_parquet(morsel.select(keep), compression="zstd")

    assert len(old_format) < len(data), "the old format really is missing a column"
    back, _native = read_manifest_file_entries(old_format)
    assert back[0].distinct_value_counts is None
    # everything else still reads
    assert back[0].record_count == 10
    assert back[0].null_counts == [2]


class _FakeDataFile:
    """A catalog DataFile: the bulk-scan path hands `from_datafile` an object
    carrying the manifest row on `.entry`."""

    def __init__(self, entry):
        self.entry = entry


def test_the_catalog_read_path_picks_up_distinct_counts():
    """The catalog is the manifest format's owner and writes its own manifests,
    so `from_datafile` - not manifest_io's reader - is what the engine uses for
    a catalog-backed relation. Without this the column could be written by the
    catalog and silently dropped on the way in."""
    entry = _FakeDataFile(
        {
            "file_path": "a.parquet",
            "record_count": 100,
            "file_size_in_bytes": 10,
            "field_ids": [0, 1],
            "min_values": [1, None],
            "max_values": [9, None],
            "distinct_counts": [9, None],
        }
    )
    file_entry = FileEntry.from_datafile(entry)
    # ESTIMATE-flagged, and a column with no count is absent rather than zero.
    assert file_entry.distinct_value_counts == {0: (9, False)}


def test_a_catalog_row_without_the_column_reads_as_not_computed():
    """Every manifest the catalog wrote before this column existed, and every
    one it writes for a relation it sketched itself. "Not computed" - never
    "no distinct values"."""
    file_entry = FileEntry.from_datafile(
        _FakeDataFile(
            {
                "file_path": "a.parquet",
                "record_count": 100,
                "file_size_in_bytes": 10,
                "field_ids": [0],
                "min_values": [1],
                "max_values": [9],
            }
        )
    )
    assert file_entry.distinct_value_counts is None


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
