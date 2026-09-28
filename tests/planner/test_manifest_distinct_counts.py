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
    `exact_cardinality_from_footers` (manifest_estimates.hpp, the first answer
    of `Manifest.estimate_cardinality`), whose answer consumers are entitled to
    treat as a BOUND (it prunes, and it answers DISTINCT without reading).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.models.manifest import Manifest
from opteryx.compiled.planner.native_manifest import decode_manifest_parquet
from opteryx.compiled.structures.plan_steps import ExitStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
from opteryx.types import logical_type as _lt
from opteryx.types.schema import RelationSchema
from opteryx.planner.plan_context import PlanContext
from tests.manifests import FileSpec
from tests.manifests import build_manifest

# Bound columns are minted by a query's ColumnTable; these tests share one.
_PLAN_CONTEXT = PlanContext()


def _schema(*names, field_ids=False):
    return RelationSchema(
        name="t",
        columns=[
            _PLAN_CONTEXT.columns.relation_column(
                "t",
                name,
                column_type=_lt.INT64,
                field_id=position if field_ids else None,
            )
            for position, name in enumerate(names)
        ],
    )


def _manifest_bytes(schema, path, file_format, record_count, file_size, distinct=None, null_counts=None):
    """One file's manifest, written by the manifest writer (NativeManifest.to_parquet):
    `distinct` is {position: (count, is_exact)}, `null_counts` positional."""
    spec = FileSpec(
        path,
        record_count=record_count,
        file_size_in_bytes=file_size,
        file_format=file_format,
        distinct_value_counts=dict(distinct or {}),
        null_value_counts={
            position: nulls for position, nulls in enumerate(null_counts or []) if nulls is not None
        },
    )
    return build_manifest(schema, [spec], bounds_are_ordinal=True).native.to_parquet()


def _read_back(schema, data, stats_are_authoritative=True):
    """The manifest bytes decoded by the engine's manifest reader."""
    return decode_manifest_parquet(
        data,
        tuple(c.name for c in schema.columns),
        tuple(c.column_type.physical for c in schema.columns),
        {},
        True,
        stats_are_authoritative,
    )


def _costing_distinct_count(manifest, name):
    """The NDV the planner COSTS with for column `name`: the base statistics the
    statistics refresh gives a Scan over `manifest` in _PLAN_CONTEXT (the
    estimate_cardinality answer, else the range/count-derived fallback)."""
    plan = LogicalPlan(_PLAN_CONTEXT)
    scan = plan.add_node(ScanStep(relation="t", schema=manifest.schema, manifest=manifest))
    plan.add_edge(scan, plan.add_node(ExitStep()))
    refresh_statistics(plan, _PLAN_CONTEXT)
    return _PLAN_CONTEXT.statistics.distinct_count(scan, manifest.schema.find_column(name).identity)


def test_distinct_counts_round_trip_positionally():
    """Position IS field id, the same convention `null_counts` and `min_values`
    use. A sparse dict must come back attached to the columns it was keyed to -
    keying from 1, or writing a gap, silently attaches every count to the NEXT
    column."""
    schema = _schema("a", "b", "c")
    data = _manifest_bytes(schema, "postgres://t", "POSTGRES", 1000, 0, distinct={0: (50, False), 2: (7, False)})
    back = _read_back(schema, data)
    assert [back.cell(0, position)["distinct_count"] for position in range(3)] == [
        (50, False),
        None,
        (7, False),
    ]


def test_a_producer_with_no_distinct_counts_writes_none():
    """The common case - the parquet path carries NDV inside `column_stats`
    instead. The column is written empty, the way min_k_hashes and
    histogram_counts are by producers that do not compute them."""
    schema = _schema("a")
    back = _read_back(schema, _manifest_bytes(schema, "a.parquet", "PARQUET", 10, 1))
    assert back.cell(0, 0)["distinct_count"] is None


def test_exactness_is_not_persisted_and_reads_back_as_an_estimate():
    """The safe direction. An exact count read back as an estimate loses an
    optimisation; an estimate read back as exact would lose ROWS."""
    schema = _schema("a")
    # written as EXACT ...
    data = _manifest_bytes(schema, "postgres://t", "POSTGRES", 100, 0, distinct={0: (9, True)})
    back = _read_back(schema, data, stats_are_authoritative=False)
    # ... and read back as an estimate, never the other way round.
    assert back.cell(0, 0)["distinct_count"] == (9, False)

    manifest = Manifest(back, schema)
    # Not reachable as a BOUND - estimate_cardinality answers from exact footer
    # counts or sketches only, and there are no sketches here ...
    assert manifest.estimate_cardinality("a") is None
    # ... but it is reachable for COSTING, which is the whole point.
    assert _costing_distinct_count(manifest, "a") == 9


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
    back = _read_back(schema, old_format)
    assert back.cell(0, 0)["distinct_count"] is None
    # everything else still reads
    assert back.record_counts() == [10]
    assert back.cell(0, 0)["null_count"] == 2


def test_the_catalog_read_path_picks_up_distinct_counts():
    """The catalog is the manifest format's owner and writes its own manifests,
    so the catalog connector's row reader (`_catalog_manifest`) - not the
    manifest parquet decoder - is what the engine uses for a catalog-backed
    relation. Without this the column could be written by the catalog and
    silently dropped on the way in."""
    from opteryx.connectors.opteryx_connector import _catalog_manifest

    entry = {
        "file_path": "a.parquet",
        "record_count": 100,
        "file_size_in_bytes": 10,
        "field_ids": [0, 1],
        "min_values": [1, None],
        "max_values": [9, None],
        "distinct_counts": [9, None],
    }
    manifest = _catalog_manifest(_schema("a", "b", field_ids=True), True, [entry], {}, {})
    # ESTIMATE-flagged, and a column with no count is absent rather than zero.
    assert manifest.native.cell(0, 0)["distinct_count"] == (9, False)
    assert manifest.native.cell(0, 1)["distinct_count"] is None


def test_a_catalog_row_without_the_column_reads_as_not_computed():
    """Every manifest the catalog wrote before this column existed, and every
    one it writes for a relation it sketched itself. "Not computed" - never
    "no distinct values"."""
    from opteryx.connectors.opteryx_connector import _catalog_manifest

    entry = {
        "file_path": "a.parquet",
        "record_count": 100,
        "file_size_in_bytes": 10,
        "field_ids": [0],
        "min_values": [1],
        "max_values": [9],
    }
    manifest = _catalog_manifest(_schema("a", field_ids=True), True, [entry], {}, {})
    assert manifest.native.cell(0, 0)["distinct_count"] is None


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
