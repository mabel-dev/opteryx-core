"""Parallel DISTINCT over a non-reentrant source.

A parallel DISTINCT (forced via W>1) over a virtual dataset, which is driven by a
non-reentrant Python generator, used to crash (``generator already executing``)
because N workers re-entered one generator. It must run serialised, not raise, and
stay result-identical to the serial engine.

The `is_concurrent_pull_safe()` capability tests that lived here were deleted with
the Python scheduler they exercised; the native engine has no such method.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../.."))

import opteryx
from opteryx import config


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
    test_parallel_distinct_over_virtual_source_no_crash_matches_serial()
    test_parallel_distinct_multi_column_over_virtual_no_crash()
    print("✅ parallel-distinct regression")
