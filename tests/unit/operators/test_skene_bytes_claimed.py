"""The skene scan's physical read-volume counter, `io_bytes_claimed`.

A v3 file is read by planned positional reads (the actual reads are counted
separately, as `io_skene_requests` / `io_skene_bytes_fetched`). This counter is
the PLANNED chunk bytes of the row groups the claim builder CLAIMED, for the
scan's read set — the ranges `skene::plan_fetch` returns once the read set's
directory blocks are attached. (For v2 files, which are mapped whole, it is -1:
not measured.)

Two properties define it, and both are asserted here because each is a way the
number could silently become useless:

  * it MOVES with row-group pruning. A counter that reported the whole dataset
    regardless of what was skipped would make a pruning regression invisible,
    which is the entire reason the benchmark series records it.
  * it FOLLOWS projection. v3 keeps per-column chunk extents in each column's
    directory block, which the claim builder attaches for the read set anyway,
    so the number is the bytes of the columns actually read. (Under v2 it was
    whole-row-group and blind to projection; that limit was pinned by this test
    and changed knowingly with the v3 bump, 2026-09-24.)

The fixture is the interleaved two-file packing `test_skene_reader_side_filter`
uses, for the same reason: consecutive row groups alternate between files, so
each FILE's bounds span nearly the whole range and plan-time FILE pruning cannot
stand in for the row-group skipping being measured.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pyarrow as pa  # test-only dep (allowed in tests/)
import pyarrow.parquet as pq
import pytest

import opteryx

N = 1200
ROWS_PER_GROUP = 100
ROW_GROUP_COUNT = N // ROWS_PER_GROUP  # 12
FILE_COUNT = 2


def _write_skene(dataset_dir):
    """Row groups alternating between two files — see the module docstring."""
    import skene
    from rugo.parquet import read_parquet

    os.makedirs(dataset_dir, exist_ok=True)
    parquet_path = os.path.join(dataset_dir, "_source.parquet")
    columns = {
        "k": pa.array(list(range(N)), type=pa.int64()),
        # CONSTANT within a row group, so `bucket = r` leaves exactly one row
        # group claimable and provably empties the other eleven.
        "bucket": pa.array([i // ROWS_PER_GROUP for i in range(N)], type=pa.int64()),
        "payload": pa.array(["payload-%d" % i for i in range(N)], type=pa.string()),
        "extra": pa.array(["extra-%d" % i for i in range(N)], type=pa.string()),
    }
    pq.write_table(pa.table(columns), parquet_path, row_group_size=ROWS_PER_GROUP)
    writers = [
        skene.SkeneWriter(read_acceleration=True, codec="none", zstd_level=0)
        for _ in range(FILE_COUNT)
    ]
    with read_parquet(parquet_path) as reader:
        for index, morsel in enumerate(reader):
            writers[index % FILE_COUNT].add_row_group(morsel)
    for index, writer in enumerate(writers):
        writer.write_to(os.path.join(dataset_dir, "part-%04d.skene" % index))
    # A dataset is single-format; leaving the parquet would make it MIXED.
    os.remove(parquet_path)
    return dataset_dir


@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    return _write_skene(str(tmp_path_factory.mktemp("skene_bytes_claimed")))


def _claimed(dataset, sql_tail):
    """`io_bytes_claimed` for the query, plus its (total, pruned) row-group counts.

    The counts come back too so a bytes assertion cannot pass vacuously against a
    scan that pruned nothing like the one it is being compared with.
    """
    sql = "SELECT " + sql_tail.format(DATASET="'%s'" % dataset)
    session = opteryx.session()
    for morsel in session.execute_to_morsels(sql):
        morsel.materialize()
    reading = session.telemetry
    facts = list((session._telemetry._reading.get("native_scan_facts") or {}).values())
    assert len(facts) == 1, "expected exactly one native scan, got %d" % len(facts)
    return reading.get("io_bytes_claimed"), facts[0]


def test_full_scan_reports_positive_bytes(dataset):
    """The counter exists and is a real measurement, not a 0 placeholder."""
    claimed, facts = _claimed(dataset, "k FROM {DATASET}")
    assert claimed is not None, (
        "io_bytes_claimed is absent for a skene scan that ran — the claim "
        "builder's write never reached telemetry"
    )
    assert claimed > 0, "a scan of %d rows claimed 0 bytes" % N
    assert facts["row_groups_pruned"] == 0, (
        "nothing should be pruned without a predicate; this test's baseline is "
        "supposed to be the whole dataset"
    )


def test_bytes_fall_when_row_groups_are_pruned(dataset):
    """The property the benchmark series actually depends on.

    `bucket` is constant per row group, so this prunes 11 of 12. The counter must
    fall roughly in step — a counter that reported the whole dataset regardless
    would make a pruning regression invisible, which is the failure this guards.
    """
    full, full_facts = _claimed(dataset, "k FROM {DATASET}")
    pruned, pruned_facts = _claimed(dataset, "k FROM {DATASET} WHERE bucket = 3")

    assert pruned_facts["row_groups_pruned"] == ROW_GROUP_COUNT - 1, (
        "expected %d of %d row groups pruned, got %d — the fixture is not "
        "exercising zone-map pruning, so the bytes assertion below would be "
        "measuring nothing"
        % (ROW_GROUP_COUNT - 1, ROW_GROUP_COUNT, pruned_facts["row_groups_pruned"])
    )
    assert full_facts["row_groups_pruned"] == 0
    assert pruned < full, (
        "pruning 11 of 12 row groups did not lower io_bytes_claimed "
        "(%d vs %d) — the counter is not tracking what the scan skipped"
        % (pruned, full)
    )
    # One row group of twelve. Bounded generously on both sides: row groups are
    # equal-sized here by construction, but per-group framing overhead is real
    # and the point of the assertion is the ORDER of magnitude, not an exact ratio.
    assert full / 4 > pruned > full / 40, (
        "claimed bytes for 1 of 12 row groups (%d) is not close to a twelfth of "
        "the full scan (%d)" % (pruned, full)
    )


def test_counter_follows_projection(dataset):
    """Widening the projection over the SAME row groups claims MORE bytes.

    v3 plans per column (FORMAT.md §5.5 directory blocks), so the claimed bytes
    are the read set's chunks, not the whole row group. If this ever reports
    equal bytes, the counter has fallen back to a whole-row-group measure.
    """
    narrow, narrow_facts = _claimed(dataset, "k FROM {DATASET} WHERE bucket = 3")
    wide, wide_facts = _claimed(
        dataset, "k, payload, extra FROM {DATASET} WHERE bucket = 3"
    )
    assert narrow_facts["row_groups_read"] == wide_facts["row_groups_read"], (
        "the two arms must claim the SAME row groups for this to be a statement "
        "about projection"
    )
    assert 0 < narrow < wide, (
        "io_bytes_claimed did not grow with the projection (%d vs %d) — it is "
        "documented as the read set's planned chunk bytes" % (narrow, wide)
    )
