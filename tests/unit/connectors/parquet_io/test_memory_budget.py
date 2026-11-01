"""Memory admission for parquet IO pipelines: one process-wide budget (`IO_BUDGET_PERCENT`,
docs/REMOTE_READ_FLOW_CONTROL_DESIGN.md §4.2).

Every pipeline charges the same budget for what it holds: decoded row groups from the
moment a worker claims one until the consumer pops its result, plus compressed bytes
from fetch to decode. A worker whose item would push the process total over budget
waits until a release in ANY pipeline makes room — but it waits only while its own
pipeline holds bytes its own consumer will release, so no scan can stall on its own
budget or on what another scan holds.

What is pinned here:
  * the import-time budget is IO_BUDGET_PERCENT of the container, and a pipeline
    reads back the budget it was built under (`memory_budget_bytes`), on both the
    trampoline and the native plan; 0 = off;
  * a budget far smaller than one row group still returns every row (progress);
  * the peak held stays at budget plus at most two items, and the budget is SEEN to
    bite (`admission_waits` > 0) under a slow consumer;
  * two scans running at once share ONE budget: the process peak stays bounded and
    both finish;
  * the ledger balances: nothing is left held once scans close.
"""

import json
import os
import sys
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx import config
from opteryx.compiled.platform import cgroup_memory_limit_bytes
from opteryx.compiled.platform import physical_memory_total_bytes
from opteryx.compiled.structures.memory_pool import configure_io_budget
from opteryx.compiled.structures.memory_pool import io_budget_stats
from opteryx.compiled.structures.memory_pool import reset_io_budget_peak

ROW_GROUP_SIZE = 5_000
N_ROW_GROUPS = 40
N = ROW_GROUP_SIZE * N_ROW_GROUPS

IMPORT_BUDGET = io_budget_stats()["budget"]


@pytest.fixture(autouse=True)
def restore_budget():
    yield
    configure_io_budget(IMPORT_BUDGET)


def _write(tmp, tag=""):
    path = os.path.join(tmp, f"budget_{os.getpid()}{tag}.parquet")
    table = pa.table({
        "x": pa.array(list(range(N)), type=pa.int64()),
        "y": pa.array([float(i) for i in range(N)], type=pa.float64()),
    })
    pq.write_table(table, path, row_group_size=ROW_GROUP_SIZE, column_encoding={"x": "PLAIN", "y": "PLAIN"}, use_dictionary=False)
    assert pq.ParquetFile(path).num_row_groups == N_ROW_GROUPS
    return path


def _rg_estimate(path):
    md = pq.ParquetFile(path).metadata
    rg = md.row_group(0)
    return sum(rg.column(i).total_uncompressed_size for i in range(rg.num_columns))


def _open(path, workers):
    from opteryx.connectors.parquet_io.pool_reader import iter_row_groups_ipc

    return iter_row_groups_ipc(None, [path], ["x", "y"], decode_workers=workers)


def _scan(path, workers=4, slow=False):
    fd, diag_path = tempfile.mkstemp(suffix=".jsonl")
    os.close(fd)
    os.environ["OPTERYX_IO_DIAG_JSON"] = diag_path
    seen = []
    try:
        gen = _open(path, workers)
        try:
            for _scan_rg, rg in gen:
                seen.extend(rg[b"x"].to_pylist())
                if slow:
                    time.sleep(0.01)   # a consumer slower than the decode workers
        finally:
            gen.close()
        with open(diag_path) as f:
            lines = f.read().strip().splitlines()
        assert len(lines) == 1, lines
        return seen, json.loads(lines[-1])
    finally:
        del os.environ["OPTERYX_IO_DIAG_JSON"]
        os.remove(diag_path)


def test_import_budget_is_the_configured_share_of_the_container():
    container = cgroup_memory_limit_bytes() or physical_memory_total_bytes()
    assert IMPORT_BUDGET == container * config.IO_BUDGET_PERCENT // 100


def test_off_and_on_read_back():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(tmp)
        configure_io_budget(0)
        rows_off, diag_off = _scan(path)
        configure_io_budget(1 << 30)
        rows_on, diag_on = _scan(path)
    assert diag_off["memory_budget_bytes"] == 0
    assert diag_off["admission_waits"] == 0
    assert diag_on["memory_budget_bytes"] == 1 << 30
    assert sorted(rows_off) == sorted(rows_on) == list(range(N))


def test_tiny_budget_still_completes_and_bounds_the_peak():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(tmp)
        est = _rg_estimate(path)
        # Smaller than ONE row group: only the "nothing held → admit" rule lets
        # anything through, so the scan runs one row group at a time.
        configure_io_budget(est // 2)
        rows, diag = _scan(path, workers=4, slow=True)
    assert sorted(rows) == list(range(N))
    assert diag["memory_budget_bytes"] == est // 2
    # Peak is bounded by the budget plus the two items the design admits
    # unconditionally: the one taken when nothing consumer-releasable is held
    # (the no-stall rule), and the one a blocked consumer decodes inline as the
    # drain. Neither waits, so neither can be counted against the budget.
    assert diag["memory_held_high_watermark"] <= est // 2 + 2 * est
    assert diag["admission_waits"] > 0
    assert diag["admission_blocked_ns"] > 0


def test_budget_of_a_few_row_groups_bites_only_under_a_slow_consumer():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(tmp)
        est = _rg_estimate(path)
        budget = est * 3
        configure_io_budget(budget)
        rows, diag = _scan(path, workers=4, slow=True)
    assert sorted(rows) == list(range(N))
    assert diag["memory_held_high_watermark"] <= budget + 2 * est
    assert diag["admission_waits"] > 0


def test_two_scans_share_one_budget():
    """Two pipelines open at once, consumed alternately by a slow consumer: their
    combined peak is bounded by ONE budget (plus each pipeline's two unconditional
    items), both return every row, and nothing is held once both close."""
    with tempfile.TemporaryDirectory() as tmp:
        a, b = _write(tmp, "_a"), _write(tmp, "_b")
        est = _rg_estimate(a)
        budget = est * 3
        configure_io_budget(budget)
        held_before = io_budget_stats()["held"]
        reset_io_budget_peak()
        gens = [_open(a, 4), _open(b, 4)]
        seen = [[], []]
        try:
            live = [True, True]
            while any(live):
                for k in (0, 1):
                    if not live[k]:
                        continue
                    item = next(gens[k], None)
                    if item is None:
                        live[k] = False
                        continue
                    seen[k].extend(item[1][b"x"].to_pylist())
                    time.sleep(0.005)
        finally:
            for g in gens:
                g.close()
        stats = io_budget_stats()
    assert sorted(seen[0]) == sorted(seen[1]) == list(range(N))
    assert stats["peak"] <= budget + 4 * est, (stats, est)
    assert stats["held"] == held_before, stats


def test_native_plan_reads_the_budget_back():
    from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan

    with tempfile.TemporaryDirectory() as tmp:
        # TWO DISTINCT files: a single cold local file hits the pre-existing
        # footer-cache poison-on-miss bug that enumerates zero row groups (see
        # test_fetch_ahead.py's xfail for the full diagnosis), which would make
        # this a test of that bug rather than of the budget.
        paths = [_write(tmp, "_a"), _write(tmp, "_b")]
        configure_io_budget(12345678)
        plan = open_native_scan_plan(paths, ["x"], decode_workers=2)
        try:
            assert plan.row_group_count == 2 * N_ROW_GROUPS
            assert plan.diagnostics()["memory_budget_bytes"] == 12345678
        finally:
            plan.close()


def test_negative_budget_is_refused():
    with pytest.raises(ValueError, match=">= 0"):
        configure_io_budget(-1)
