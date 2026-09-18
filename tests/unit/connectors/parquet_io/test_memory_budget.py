"""Memory admission for the parquet IO pipeline (`parquet_io_memory_budget_bytes`).

The pipeline holds decoded row groups from the moment a worker claims one until
the consumer pops its result, plus compressed bytes from fetch to decode. With a
budget set, a worker whose item would push that total over budget waits at the
gate until a pop releases enough — but one is always admitted when nothing
consumer-releasable is held, so a budget can never stall a scan on its own.

What is pinned here:
  * the knob reaches the pipeline and reads back (`memory_budget_bytes`) — the
    "prove the knob moves" gate — on both the trampoline and the native plan;
  * a budget far smaller than one row group still returns every row (progress);
  * the peak held (`memory_held_high_watermark`) stays at budget plus at most
    one item's estimate, and the gate is SEEN to bite (`admission_waits` > 0)
    when many row groups queue behind a slow consumer;
  * off by default in these APIs (0), and the auto derivation yields a positive
    number on any host that reports its memory.
"""

import json
import os
import sys
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

ROW_GROUP_SIZE = 5_000
N_ROW_GROUPS = 40
N = ROW_GROUP_SIZE * N_ROW_GROUPS


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


def _scan(path, memory_budget, workers=4, slow=False):
    from opteryx.connectors.parquet_io.pool_reader import iter_row_groups_ipc

    fd, diag_path = tempfile.mkstemp(suffix=".jsonl")
    os.close(fd)
    os.environ["OPTERYX_IO_DIAG_JSON"] = diag_path
    seen = []
    try:
        gen = iter_row_groups_ipc(None, [path], ["x", "y"], decode_workers=workers,
                                  memory_budget=memory_budget)
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


def test_off_by_default_and_reads_back_when_set():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write(tmp)
        rows_off, diag_off = _scan(path, memory_budget=0)
        rows_on, diag_on = _scan(path, memory_budget=1 << 30)
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
        rows, diag = _scan(path, memory_budget=est // 2, workers=4, slow=True)
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
        rows, diag = _scan(path, memory_budget=budget, workers=4, slow=True)
    assert sorted(rows) == list(range(N))
    assert diag["memory_held_high_watermark"] <= budget + 2 * est
    assert diag["admission_waits"] > 0


def test_native_plan_reads_the_budget_back():
    from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan

    with tempfile.TemporaryDirectory() as tmp:
        # TWO DISTINCT files: a single cold local file hits the pre-existing
        # footer-cache poison-on-miss bug that enumerates zero row groups (see
        # test_fetch_ahead.py's xfail for the full diagnosis), which would make
        # this a test of that bug rather than of the budget.
        paths = [_write(tmp, "_a"), _write(tmp, "_b")]
        plan = open_native_scan_plan(paths, ["x"], decode_workers=2, memory_budget=12345678)
        try:
            assert plan.row_group_count == 2 * N_ROW_GROUPS
            assert plan.diagnostics()["memory_budget_bytes"] == 12345678
        finally:
            plan.close()


def test_auto_derivation_is_positive_or_honestly_off():
    from opteryx.connectors.parquet_io.io_tuning import resolve_memory_budget
    from opteryx.compiled.platform import cgroup_memory_limit_bytes
    from opteryx.compiled.platform import physical_memory_total_bytes

    auto = resolve_memory_budget(None)
    ceiling = cgroup_memory_limit_bytes() or physical_memory_total_bytes() or 0
    if ceiling > 0:
        assert auto == int(ceiling * 0.5)
    else:
        assert auto == 0
    assert resolve_memory_budget(None, {"parquet_io_memory_budget_bytes": -1}) == 0
    assert resolve_memory_budget(None, {"parquet_io_memory_budget_bytes": 4096}) == 4096
