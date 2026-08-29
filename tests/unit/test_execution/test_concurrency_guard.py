"""
Concurrency guard — fail LOUD if a parallel shape stops fanning out.

The small-data regression suite cannot see lost concurrency: below the row-floor
every shape runs serial, so a serial-vs-concurrent routing bug gives byte-identical
results in milliseconds (green suite, silent serialization — exactly the GROUP BY /
agg / DISTINCT regression this guard now catches).

Method (deterministic, not timing-based): pin the DOP, then read back the degree
each PIPELINE actually ran at from ``native_pipeline_stats``. That reading is the
engine's own ``dop_used`` (engine.hpp: ``pn->dop_used = pdop``), so a pipeline
forced to the serial drive — by a ``dop_override`` of 1, or by a routing bug —
reports 1 and fails here.

This guard previously instrumented ``CppThreadPool`` and asserted on the widest
pool CONSTRUCTED during the query. That sensor died when engine pools became
thread-local and reused across queries (``_acquire_engine_pool``): only the first
query in a process constructs a pool, so every later test saw no construction at
all and read 0. The result was a guard whose verdict depended on test ordering —
all five shapes passed in isolation and four failed when run together — rather
than on whether anything actually fanned out. ``dop_used`` is reported per query
and per pipeline, so it is immune to pool reuse.

The assertion is on the MINIMUM stage width, not the maximum: a shape whose scan
fans out but whose aggregate silently serialises is precisely the regression this
file exists to catch, and a maximum would hide it behind the healthy scan.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx import config


def _stage_dops(sql, workers=4):
    """Run ``sql`` with DOP pinned; return ``[(pipeline_label, dop_used), ...]``.

    The requested width is verified against ``native_engine_dop`` before anything
    else is asserted. ``config.MAX_EXECUTION_WORKERS`` used to be a setting that
    silently did nothing — the system-variable table froze it at import — and a
    guard that cannot tell "ran wide" from "never applied my setting" is not a
    guard. If the knob stops reaching the engine again, this fails first and says so.
    """
    saved = config.MAX_EXECUTION_WORKERS
    try:
        config.MAX_EXECUTION_WORKERS = workers
        session = opteryx.session()
        for _ in session.execute_to_morsels(sql):
            pass
        reading = session._telemetry._reading
    finally:
        config.MAX_EXECUTION_WORKERS = saved

    dop = reading.get("native_engine_dop")
    assert dop == workers, (
        f"asked for {workers} workers but the engine ran at dop={dop} — the worker "
        f"setting is not reaching the engine, so this guard is measuring nothing."
    )
    stages = reading.get("native_pipeline_stats") or []
    assert stages, (
        f"no native_pipeline_stats for: {sql} — the guard has lost its sensor and "
        f"would pass vacuously; it must fail instead."
    )
    return [(s["label"], s["dop"]) for s in stages]


def _assert_fans_out(sql, workers=4):
    stages = _stage_dops(sql, workers=workers)
    serial = [label for label, dop in stages if dop < 2]
    assert not serial, (
        f"pipeline(s) {serial} ran serial (dop<2) at DOP {workers} for: {sql}\n"
        f"stages={stages}"
    )


def test_stateless_is_concurrent():
    # scan -> filter/projection -> exit  (was already native-concurrent)
    _assert_fans_out("SELECT name FROM $planets WHERE id > 3")


def test_grouped_aggregate_is_concurrent():
    # The shape my single-scan gate accidentally serialized — this is the guard for it.
    _assert_fans_out("SELECT name, COUNT(*) FROM $planets GROUP BY name")


def test_ungrouped_aggregate_is_concurrent():
    _assert_fans_out("SELECT COUNT(*), SUM(id) FROM $planets")


def test_distinct_is_concurrent():
    _assert_fans_out("SELECT DISTINCT name FROM $planets")


def test_inner_join_is_concurrent():
    _assert_fans_out(
        "SELECT p.name FROM $planets AS p INNER JOIN $planets AS q ON p.id = q.id"
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
