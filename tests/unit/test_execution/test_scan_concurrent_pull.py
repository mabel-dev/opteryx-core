"""Stage 5 (M4 prerequisite): thread-safe concurrent pull from ONE parquet scan.

The native single-pass scan path (IpcRowGroupSource) is the seam M4 workers pull
from concurrently. A Python generator is not reentrant; the cdef state machine is,
guarded by a native std::mutex over the cursor (advance the submit window + claim
one result), with submission and the blocking decode wait OUTSIDE the lock.

This test drives ONE source from N OS threads and asserts the union of pulled row
groups is byte-identical (as a row-count multiset) to a single-threaded reference —
no row group dropped, duplicated, or torn. A data race in the cursor would either
crash, miss/duplicate a row group, or diverge the totals. Many rounds shake out
timing.

pyarrow is used only to discover the file's columns/row count (test-side; banned in
the engine, allowed in tests per CLAUDE.md §4).
"""

import os
import sys
import threading

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

from opteryx.connectors.io_systems import create_filesystem
from opteryx.connectors.parquet_io.pool_reader import open_ipc_source

# A multi-row-group file so concurrent pull is actually exercised (10 row groups).
PARQUET = os.path.join(
    os.path.dirname(__file__), "../../..",
    "testdata/h2o/small/x_groupby/x_groupby.parquet",
)
THREADS = 8
ROUNDS = 6


def _columns_and_rows():
    import pyarrow.parquet as pq

    md = pq.ParquetFile(PARQUET).metadata
    names = pq.ParquetFile(PARQUET).schema_arrow.names[:3]  # a few columns is plenty
    return names, md.num_rows, md.num_row_groups


def _new_source(column_names):
    fs = create_filesystem("")  # local POSIX filesystem (no signed-URL rewrite)
    return open_ipc_source(fs, [os.path.abspath(PARQUET)], list(column_names), decode_workers=4)


def _drain_serial(column_names):
    """Single-threaded reference: per-row-group row counts + total."""
    src = _new_source(column_names)
    counts = []
    try:
        while True:
            pulled = src.next_vectors()
            if pulled is None:
                break
            vectors = pulled[0]
            counts.append(len(vectors[0]))
    finally:
        src.close()
    return sorted(counts), sum(counts)


@pytest.mark.skipif(not os.path.exists(PARQUET), reason="testdata parquet missing")
def test_concurrent_scan_pull_matches_serial():
    column_names, file_rows, n_rgs = _columns_and_rows()
    assert n_rgs >= 2, "need a multi-row-group file to exercise concurrency"

    ref_counts, ref_total = _drain_serial(column_names)
    assert ref_total == file_rows

    for _ in range(ROUNDS):
        src = _new_source(column_names)
        collected = []
        errors = []
        lock = threading.Lock()
        start = threading.Barrier(THREADS)

        def worker():
            try:
                start.wait()  # maximise concurrent re-entry into the cursor
                while True:
                    pulled = src.next_vectors()
                    if pulled is None:
                        break
                    n = len(pulled[0][0])
                    with lock:
                        collected.append(n)
            except Exception as exc:  # surface a race as a test failure, not a hang
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        src.close()

        assert not errors, f"worker raised under concurrent pull: {errors[0]!r}"
        # No row group dropped, duplicated, or torn: same multiset + total as serial.
        assert sorted(collected) == ref_counts
        assert sum(collected) == ref_total


if __name__ == "__main__":
    test_concurrent_scan_pull_matches_serial()
    print("✅ concurrent scan pull matches serial")
