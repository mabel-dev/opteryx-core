"""M4 Stage 1: the concurrent-pull reentrancy capability + parallel-DISTINCT gate.

``BasePlanNode.is_concurrent_pull_safe()`` is the CORRECTNESS distinction that lets
a parallel strategy self-pull a scan locklessly ONLY when the source's
``next_morsel`` is reentrant — native single-pass parquet, whose cursor is guarded
by an internal std::mutex that hands each caller a distinct already-decoded row
group. Every other source (virtual datasets driven by a non-reentrant Python
generator, two-pass latmat, empty-manifest fallback) must report ``False`` so the
strategy SERIALISES the live pull. If it lied, N workers would re-enter one
generator and crash (``generator already executing``) or corrupt its state.

This is the regression for that crash: ``_distinct_stream`` previously self-pulled
LOCKLESS unconditionally, so a parallel DISTINCT over a virtual dataset (forced via
zero row-floor + W>1) crashed. It must now run serialised, not raise, and stay
result-identical to the serial engine.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import pytest

import opteryx
from opteryx import config
from opteryx.models import QueryTelemetry
from opteryx.planner import query_planner


def _plan(sql):
    return query_planner(
        operation=sql,
        parameters=None,
        visibility_filters=None,
        execution_context=opteryx.session().context,
        query_id="t",
        telemetry=QueryTelemetry.detached(),
    )


def _scan_node(plan):
    for nid in plan.nodes():
        node = plan[nid]
        if getattr(node, "is_scan", False):
            return node
    return None


def _rows(sql, *, workers):
    """Rows for `sql` at a pinned worker count, verifying the engine honoured it.

    `workers` is explicit and checked because it used to be neither: these tests
    set `config.MAX_EXECUTION_WORKERS` via monkeypatch, but the system-variable
    table froze that constant at import, so the setting never reached the engine.
    Both the "serial reference" and the "parallel" run executed at the same default
    width — the comparison was real work, but it was not serial-vs-parallel.
    """
    saved = config.MAX_EXECUTION_WORKERS
    try:
        config.MAX_EXECUTION_WORKERS = workers
        s = opteryx.session()
        out = []
        for m in s.execute_to_morsels(sql):
            for i in range(m.num_rows):
                out.append(tuple(m[i]))
        dop = s._telemetry._reading.get("native_engine_dop")
    finally:
        config.MAX_EXECUTION_WORKERS = saved
    assert dop == workers, (
        f"asked for {workers} workers but the engine ran at dop={dop} — this "
        f"comparison would not be serial-vs-parallel."
    )
    return sorted(out, key=repr)


PARQUET = os.path.join(
    os.path.dirname(__file__), "../../..", "testdata/clickbench_tiny"
)


# ── the capability itself ──────────────────────────────────────────────────────


def test_virtual_source_is_not_concurrent_pull_safe():
    """A virtual dataset is a non-reentrant generator → must report False."""
    scan = _scan_node(_plan("SELECT DISTINCT name FROM $planets"))
    assert scan is not None
    assert scan.is_concurrent_pull_safe() is False


@pytest.mark.skipif(not os.path.exists(PARQUET), reason="clickbench_tiny missing")
def test_parquet_single_pass_is_concurrent_pull_safe():
    """A plain projected parquet scan resolves to single-pass → reentrant → True."""
    scan = _scan_node(_plan('SELECT DISTINCT "UserID" FROM testdata.clickbench_tiny'))
    assert scan is not None
    assert scan.is_concurrent_pull_safe() is True


# ── the regression: parallel DISTINCT over a non-reentrant source ───────────────


def test_parallel_distinct_over_virtual_source_no_crash_matches_serial():
    """Force the parallel DISTINCT path (W>1) over a non-reentrant virtual source.
    Pre-fix this crashed; it must now serialise, not raise, and equal the serial
    answer. Many rounds shake out the timing."""
    sql = "SELECT DISTINCT name FROM $planets"
    reference = _rows(sql, workers=1)  # genuinely serial reference
    assert reference, "expected non-empty reference"

    for _ in range(12):
        assert _rows(sql, workers=4) == reference


def test_parallel_distinct_multi_column_over_virtual_no_crash():
    sql = "SELECT DISTINCT id, name FROM $planets"
    reference = _rows(sql, workers=1)
    for _ in range(12):
        assert _rows(sql, workers=4) == reference


if __name__ == "__main__":
    test_virtual_source_is_not_concurrent_pull_safe()
    if os.path.exists(PARQUET):
        test_parquet_single_pass_is_concurrent_pull_safe()
    print("✅ concurrent-pull capability + parallel-distinct regression")
