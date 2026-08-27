"""
Engine LoopSpan (fixpoint) mechanics — hand-built NativePlan, no SQL.

Phase 1 gate for docs/RECURSIVE_CTE_DESIGN.md: proves the span-jump loop in
Engine::run() — anchor seeding through the control step, WORKING <- DELTA
swap, RESULT accumulation, convergence on empty delta, the iteration ceiling,
and the empty-anchor short-circuit — using only the builder edge the compiler
uses. The recursive "term" here is a passthrough (optionally behind a LIMIT
whose engine-side quota counter is cumulative across passes, which is what
makes the frontier run dry).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel


def _run_plan(build):
    """Build a NativePlan via `build(nplan, out_q)` and drive it to completion.
    Returns (morsels, terminal_exception)."""
    from opteryx.compiled.morsel_queue import MQ_FINISHED, PyMorselQueue
    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.operators._operators import (
        NativeErrorSlot,
        NativePlan,
        build_terminal_exc,
        native_plan_execute,
    )

    nplan = NativePlan()
    out_q = PyMorselQueue(4)
    build(nplan, out_q)

    pool = CppThreadPool(2, "engine")
    errslot = NativeErrorSlot()
    # The handle (and nplan/out_q/errslot/pool) must stay alive until the queue
    # reports finished — native_plan_execute's contract.
    handle = native_plan_execute(pool, nplan, 2, out_q, errslot)

    morsels = []
    while True:
        item = out_q.get()
        if item is None or item is MQ_FINISHED:
            break
        morsels.append(item)
    del handle
    return morsels, build_terminal_exc(nplan, errslot)


def _anchor_morsel(values):
    vec = vector_from_sequence(values, DrakenType.INT64)
    return Morsel.from_vectors(["n"], [vec])


def _build(nplan, out_q, anchor_values, max_iterations, cumulative_limit=None):
    """DELTA pre-seeded with the anchor; span = one passthrough pipeline
    WORKING -> [LIMIT] -> DELTA; consumer = RESULT -> select -> queue."""
    delta = nplan.new_scratch_buffer()
    working = nplan.new_scratch_buffer()
    result = nplan.new_buffer()

    if anchor_values:
        nplan.add_buffer_morsel(delta, _anchor_morsel(anchor_values))

    p0 = nplan.new_pipeline()
    nplan.set_buffer_source(p0, working)
    if cumulative_limit is not None:
        nplan.add_limit(p0, 0, cumulative_limit)
    nplan.set_buffer_append_sink(p0, delta)
    nplan.set_pipeline_dop(p0, 1)

    p1 = nplan.new_pipeline()
    nplan.set_buffer_source(p1, result)
    nplan.add_select(p1, [0], ["n"])
    nplan.set_queue_sink(p1, out_q)

    nplan.add_loop_span(p0, p0, working, delta, result, False, max_iterations, "loop")
    nplan.set_final_schema(["n"], [DrakenType.INT64.value], [None])


def test_fixpoint_converges_and_accumulates():
    # Anchor {1,2,3}; the LIMIT's cumulative quota (5) dries the frontier:
    # pass 1 emits 3 (quota 3/5), pass 2 emits 2 (quota 5/5, halt), pass 3
    # emits 0 -> convergence. RESULT = anchor 3 + 3 + 2 = 8 rows.
    morsels, exc = _run_plan(
        lambda nplan, q: _build(nplan, q, [1, 2, 3], 100, cumulative_limit=5)
    )
    assert exc is None, exc
    rows = sum(m.num_rows for m in morsels)
    assert rows == 8, f"expected 8 accumulated rows, got {rows}"


def test_fixpoint_ceiling_is_a_loud_error():
    # Pure passthrough never converges; the ceiling must stop it with the
    # user-facing (DataError-channel) message, never a truncated result.
    morsels, exc = _run_plan(lambda nplan, q: _build(nplan, q, [1, 2, 3], 4))
    assert exc is not None, "non-converging loop must error, not return rows"
    assert "did not converge within 4 iterations" in str(exc)


def test_fixpoint_empty_anchor_short_circuits():
    # Empty anchor -> the control step converges before the span ever runs;
    # the consumer sees zero rows (the courtesy morsel carries the schema).
    morsels, exc = _run_plan(lambda nplan, q: _build(nplan, q, [], 100))
    assert exc is None, exc
    assert sum(m.num_rows for m in morsels) == 0
    assert morsels, "courtesy empty morsel with the schema is still due"
    assert morsels[0].column_names == [b"n"]


if __name__ == "__main__":  # pragma: no cover
    test_fixpoint_converges_and_accumulates()
    test_fixpoint_ceiling_is_a_loud_error()
    test_fixpoint_empty_anchor_short_circuits()
    print("✅ okay")
