"""
Concurrency guard — fail LOUD if a parallel shape stops fanning out.

The small-data regression suite cannot see lost concurrency: below the row-floor
every shape runs serial, so a serial-vs-concurrent routing bug gives byte-identical
results in milliseconds (green suite, silent serialization — exactly the GROUP BY /
agg / DISTINCT regression this guard now catches).

Method (deterministic, not timing-based): force the row-floor to 0 so any data
fans out, instrument ``CppThreadPool`` to record the max worker count requested
during the query, and assert that each PARALLEL shape genuinely spawns > 1 worker.
A shape routed to the serial drive (``drive_scan_to_sink`` / ``CppThreadPool(1)``)
fails here.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
import opteryx.compiled.thread_pool as _tp
from opteryx import config


def _max_workers_for(sql, workers=4):
    """Run ``sql`` with the row-floor forced to 0 and DOP pinned, returning the max
    worker count any ``CppThreadPool`` was created with during execution."""
    real_pool = _tp.CppThreadPool
    seen = []

    def tracking_pool(w, name="guard"):
        seen.append(int(w))
        return real_pool(w, name)

    # `_operators` cimports CppThreadPool as a TYPE (for the native fan-out's typed
    # pool param + cdef submit_native); that bind resolves at _operators init by
    # looking up the type in the thread_pool module. Trigger the normal execution
    # import chain NOW (same module-load order a real query uses), so _operators binds
    # the real class before we shadow it with a tracking function below.
    from opteryx.managers.execution import execute  # noqa: F401

    old_workers = config.MAX_EXECUTION_WORKERS
    config.MAX_EXECUTION_WORKERS = workers
    _tp.CppThreadPool = tracking_pool
    try:
        session = opteryx.session()
        for _ in session.execute_to_morsels(sql):
            pass
        return max(seen) if seen else 0
    finally:
        _tp.CppThreadPool = real_pool
        config.MAX_EXECUTION_WORKERS = old_workers


def test_stateless_is_concurrent():
    # scan -> filter/projection -> exit  (was already native-concurrent)
    assert _max_workers_for("SELECT name FROM $planets WHERE id > 3") >= 2


def test_grouped_aggregate_is_concurrent():
    # The shape my single-scan gate accidentally serialized — this is the guard for it.
    assert _max_workers_for("SELECT name, COUNT(*) FROM $planets GROUP BY name") >= 2


def test_ungrouped_aggregate_is_concurrent():
    assert _max_workers_for("SELECT COUNT(*), SUM(id) FROM $planets") >= 2


def test_distinct_is_concurrent():
    assert _max_workers_for("SELECT DISTINCT name FROM $planets") >= 2


def test_inner_join_is_concurrent():
    assert (
        _max_workers_for(
            "SELECT p.name FROM $planets AS p "
            "INNER JOIN $planets AS q ON p.id = q.id"
        )
        >= 2
    )


if __name__ == "__main__":  # pragma: no cover
    for fn in (
        test_stateless_is_concurrent,
        test_grouped_aggregate_is_concurrent,
        test_ungrouped_aggregate_is_concurrent,
        test_distinct_is_concurrent,
        test_inner_join_is_concurrent,
    ):
        fn()
        print(f"✓ {fn.__name__}")
    print("✓ all parallel shapes fan out")
