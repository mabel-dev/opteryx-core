"""Bare-LIMIT row-group early-stop ("Lever 1").

For a scan with NO predicates, a kept row group's footer `num_rows` is its exact row
contribution — nothing downstream filters it out. `open_ipc_source(..., limit=N)` uses
that to stop enumerating row groups (and files) once the accumulated `num_rows` across
already-selected row groups covers `N`, instead of queueing every row group in the file.

This is unsound the moment a predicate is present: `_rg_passes_predicates_native` can
only prune a row group when its min/max stats *prove* exclusion — a kept row group can
still lose rows to per-row filtering downstream, so its footer row count no longer
equals what the LIMIT has actually satisfied. `open_ipc_source` must leave row-group
enumeration exactly as it was (predicate-pruned, not limit-truncated) whenever
`predicates` is non-empty, regardless of what `limit` is passed.

These tests drive `open_ipc_source` directly against a real local multi-row-group
file and count row groups actually enumerated (drained via `next_vectors()`), which
is the one observable proxy for `len(work_items)` from outside the Cython class.
"""

import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import opteryx.connectors.parquet_io.pool_reader as pool_reader

ROWS_PER_GROUP = 2_000
N_GROUPS = 10
TOTAL_ROWS = ROWS_PER_GROUP * N_GROUPS


@pytest.fixture
def multi_rg_file(tmp_path):
    table = pa.table(
        {
            "id": pa.array(range(TOTAL_ROWS), type=pa.int64()),
            "val": pa.array([f"row-{i}" for i in range(TOTAL_ROWS)]),
        }
    )
    path = os.path.join(tmp_path, "data.parquet")
    pq.write_table(table, path, row_group_size=ROWS_PER_GROUP)
    pf = pq.ParquetFile(path)
    assert pf.num_row_groups == N_GROUPS, "fixture must land on the row-group count these tests assume"
    return path


def _drain_row_group_count(src):
    """Pull every scheduled row group to completion; return how many were produced.

    This is the scan's real output, not `len(work_items)` (not exposed to Python) —
    draining is what proves the early-stop actually changed what gets fetched, not
    just what a static list would have contained."""
    n_row_groups = 0
    total_rows = 0
    try:
        while True:
            pulled = src.next_vectors()
            if pulled is None:
                break
            vectors = pulled[0]
            if vectors is None:
                continue
            n_row_groups += 1
            total_rows += len(vectors[0]) if vectors else 0
    finally:
        src.close()
    return n_row_groups, total_rows


def _open(path, limit=None, predicates=None):
    return pool_reader.open_ipc_source(
        None,
        [path],
        ["id", "val"],
        decode_workers=2,
        predicates=predicates,
        limit=limit,
    )


def test_no_limit_reads_every_row_group(multi_rg_file):
    n_row_groups, total_rows = _drain_row_group_count(_open(multi_rg_file))
    assert n_row_groups == N_GROUPS
    assert total_rows == TOTAL_ROWS


def test_limit_under_one_row_group_reads_only_that_row_group(multi_rg_file):
    n_row_groups, total_rows = _drain_row_group_count(_open(multi_rg_file, limit=50))
    assert n_row_groups == 1
    assert total_rows == ROWS_PER_GROUP


def test_limit_exactly_on_a_row_group_boundary_reads_exactly_that_many(multi_rg_file):
    n_row_groups, total_rows = _drain_row_group_count(
        _open(multi_rg_file, limit=ROWS_PER_GROUP * 3)
    )
    assert n_row_groups == 3
    assert total_rows == ROWS_PER_GROUP * 3


def test_limit_one_row_past_a_boundary_pulls_in_the_next_row_group(multi_rg_file):
    # ROWS_PER_GROUP*3 rows is not enough to cover ROWS_PER_GROUP*3 + 1 -> a 4th
    # row group must be scheduled, even though it will be under-consumed downstream.
    n_row_groups, total_rows = _drain_row_group_count(
        _open(multi_rg_file, limit=ROWS_PER_GROUP * 3 + 1)
    )
    assert n_row_groups == 4
    assert total_rows == ROWS_PER_GROUP * 4


def test_limit_covering_the_whole_file_reads_every_row_group(multi_rg_file):
    n_row_groups, total_rows = _drain_row_group_count(_open(multi_rg_file, limit=TOTAL_ROWS))
    assert n_row_groups == N_GROUPS
    assert total_rows == TOTAL_ROWS


def test_limit_beyond_the_whole_file_reads_every_row_group(multi_rg_file):
    n_row_groups, total_rows = _drain_row_group_count(
        _open(multi_rg_file, limit=TOTAL_ROWS * 10)
    )
    assert n_row_groups == N_GROUPS
    assert total_rows == TOTAL_ROWS


def test_predicate_present_disables_the_early_stop(multi_rg_file):
    # "InStr" (contains) has no min/max-based row-group pruning in
    # _rg_passes_predicates_native (unlike Eq/Gt/InList/...), so every row group
    # matches on stats alone regardless of `limit`. A small `limit` here must NOT
    # truncate scheduling — this predicate is unrelated to LimitFilesPruningStrategy's
    # `if node.predicates: return context` restriction, but exercises the same "any
    # predicate at all disables the optimization" contract inside open_ipc_source.
    predicates = [("val", "InStr", "row-")]

    n_no_limit, _ = _drain_row_group_count(_open(multi_rg_file, predicates=predicates))
    n_small_limit, _ = _drain_row_group_count(
        _open(multi_rg_file, limit=1, predicates=predicates)
    )

    assert n_no_limit == N_GROUPS
    assert n_small_limit == N_GROUPS, "a predicate must suppress the limit-based early-stop entirely"


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
