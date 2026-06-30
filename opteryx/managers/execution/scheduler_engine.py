# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Scheduler — routes each data pipeline to a NATIVE drive into a ``MorselQueue``.

Every data pipeline (SELECT and friends) runs through here; the dispatcher
(``managers/execution/__init__.py``) routes only the non-pipeline special ops
(EXPLAIN / SET / SHOW / INSERT / DDL) to ``serial_engine``.

There is no Python ``Executor``/``Event`` DAG and no ``queue.Queue`` hand-off any
more (native scheduler rewrite). ``execute`` resolves DOP, then dispatches by plan
shape to a native drive — each runs its work on a ``CppThreadPool`` and streams
morsels through a native ``MorselQueue`` to the consumer:

  * single-scan @ DOP=1 → ``_native_serial_execute`` (``drive_scan_to_sink``)
  * parallel stateless (``scan → stateless* → exit``) → ``_native_stateless_execute``
  * parallel inner-equi join → ``_native_join_execute``
  * multi-breaker chain (GROUP BY → DISTINCT) → ``_native_chain_execute`` (sequential)
  * everything else → ``_native_generic_execute`` (pumps the kept per-shape handler
    in ``parallel_engine.py`` — ``_run_breaker_segment`` / ``_serial_stream`` — into
    the ``MorselQueue``)

The per-shape drive machinery still lives in ``parallel_engine.py`` as the SUBSTRATE
(its worker push loops are native — ``drive_scan_to_sink`` / ``stateless_worker_drive``
/ ``accumulate_worker_drive``); this module owns the dispatch + the native hand-off.
DOP is the real ``resolve_worker_count``; worker count is degree-of-parallelism only —
it never selects a code path. DOP=1 stays byte-identical to serial (the prime
constraint).
"""

from typing import List

from opteryx import config
from opteryx.constants import ResultType
from opteryx.managers.execution.parallel_engine import Segment
from opteryx.managers.execution.parallel_engine import dispatch_data_pipeline
from opteryx.managers.execution.parallel_engine import identify_segments
from opteryx.managers.execution.parallel_engine import resolve_worker_count

# Bounded hand-off depth: backpressure so the worker cannot run unboundedly ahead
# of the consumer (matching the streaming memory profile of the serial path).
_QUEUE_DEPTH = 8


def _count_pipelines(segments: List[Segment]) -> int:
    """Number of pipeline segments (sources + post-breaker re-sources)."""
    return len(segments)


def resolve_dop(plan, segments: List[Segment], requested) -> int:
    """The scheduler's degree of parallelism — the real, floor-free worker count.

    Step 7 lifts the Stage-0 ``min(ceiling, 1)`` pin: now that the segments drive the
    kept skeleton (which row-routes / fans out per shape), the scheduler uses the full
    ``resolve_worker_count`` ceiling. Worker count is degree-of-parallelism only — it
    never selects a code path (the per-shape handlers own the W=1 / below-floor serial
    drive of the ORIGINAL breaker, the prime constraint).
    """
    return resolve_worker_count(requested)


def _count_scans(plan) -> int:
    return sum(1 for nid in plan.nodes() if getattr(plan[nid], "is_scan", False))


def _native_serial_execute(plan, telemetry=None):
    """Native serial drive (slice 3, option B). Drives every scan's chain through
    ``drive_scan_to_sink`` on ONE pool thread in DFS scan order (build legs before
    probe — the order ``compile_pipeline`` wires the push topology), the terminal
    Exit's output flowing straight into a ``MorselQueue``; the consumer generator
    drains it. Build legs drive with ``finish=False`` (they reach no Exit and must
    not signal end-of-data); a single ``sink.finish()`` after the last chain marks
    graceful end. No ``queue.Queue``, no ``Executor``/``threading``, no Python drive
    generator — this is the native replacement for ``_serial_stream``.
    """
    from opteryx.compiled.morsel_queue import MQ_FINISHED
    from opteryx.compiled.morsel_queue import PyMorselQueue
    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators._operators import native_serial_drive

    chains, exit_node, ctx = compile_pipeline(plan)
    sink = PyMorselQueue(_QUEUE_DEPTH)
    err_box: list = []
    pool = CppThreadPool(1, "m4-serial")

    def generator():
        # NATIVE producer: one native task drives the compiled chains into `sink`
        # (no Python task() closure, no Future). The handle keeps the native arg array
        # alive until the consumer below has drained the finish.
        _handle = native_serial_drive(pool, chains, exit_node, ctx, sink, err_box)
        try:
            while True:
                item = sink.get()
                if item is None or item is MQ_FINISHED:
                    break
                yield item
        finally:
            # Early abandon (LIMIT at the cursor): drop the remainder and unblock the
            # producer's backpressured put, then let the chain's finally run.
            sink.close()
            ctx.terminate()
            pool.shutdown(wait=True)
            if err_box:
                raise err_box[0]

    return generator(), ResultType.TABULAR


def _native_stateless_execute(plan, scan_id, op_ids, exit_id, workers, telemetry=None):
    """Native parallel STATELESS drive (slice 4): ``scan → stateless* → exit``, the
    CONCAT (no-merge) shape. Row-floor serial fallback, else W workers each pushing
    their cloned chain's output into a shared ``MorselQueue`` via the native
    ``stateless_worker_drive``. Replaces ``_stateless_stream``'s ``queue.Queue``
    fan-out + per-morsel ``push_one`` + the outer ``Executor``/``queue.Queue`` with
    a native push + queue. The shared-scan pull keeps a ``threading.Lock`` (it
    guards a Python buffer iterator) until the scan pull itself goes nogil.
    """
    import threading

    from opteryx import EOS as _EOS_SENTINEL
    from opteryx.compiled.morsel_queue import MQ_FINISHED
    from opteryx.compiled.morsel_queue import PyMorselQueue
    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators._operators import native_stateless_fanout
    from opteryx.operators._operators import pull_one
    from opteryx.operators._operators import push_one
    from opteryx.operators._operators import push_one_to_sink
    from opteryx.operators._operators import spawn_worker
    from opteryx.operators._operators import stateless_worker_drive

    chains, exit_node, ctx = compile_pipeline(plan)
    scan = plan[scan_id]
    # Bounded fan-out (the old out_q was unbounded): generous depth so workers rarely
    # block; the consumer drains steadily. Bounded only to cap memory.
    sink = PyMorselQueue(max(64, workers * 8))

    def generator():
        pool = None
        errors = [None] * workers
        try:
            # Row-floor: tiny inputs run serially through the ORIGINAL chain (this
            # path yields directly — no sink, no pool).
            buffer = []
            buffered_rows = 0
            exhausted = False
            while buffered_rows < config.PARALLEL_MIN_ROWS:
                morsel = pull_one(scan)
                if morsel is None:
                    exhausted = True
                    break
                buffer.append(morsel)
                buffered_rows += morsel.num_rows

            if exhausted and buffered_rows < config.PARALLEL_MIN_ROWS:
                head = plan[op_ids[0]]
                for morsel in buffer:
                    if ctx.is_terminated():
                        break
                    push_one(head, morsel)
                    while exit_node.has_pending():
                        yield exit_node.pop_pending()
                if not ctx.is_terminated():
                    push_one(head, _EOS_SENTINEL)
                    while exit_node.has_pending():
                        yield exit_node.pop_pending()
                return

            # Parallel: W workers push their cloned chain output into the sink.
            buf_iter = iter(buffer)
            pull_lock = threading.Lock()

            def next_input():
                with pull_lock:
                    if ctx.is_terminated():
                        return None
                    buffered = next(buf_iter, None)
                    if buffered is not None:
                        return buffered
                    return pull_one(scan)

            # Serial pre-clone of each worker's [op-clones → exit-clone] chain (coarse
            # PyObject setup), then a native STREAMING fan-out — no Python worker
            # closure, no Future. Each worker drives into the SHARED sink concurrently
            # while the consumer below drains; the handle keeps the native per-worker arg
            # array alive until the consumer has seen all W sink-finishes.
            heads = []
            exits = []
            for _k in range(workers):
                ops = [spawn_worker(plan[nid]) for nid in op_ids]
                exit_clone = spawn_worker(plan[exit_id])
                chain = ops + [exit_clone]
                for i, op in enumerate(chain):
                    op.set_context(ctx)
                    if i + 1 < len(chain):
                        op.set_downstream(chain[i + 1])
                heads.append(chain[0])
                exits.append(exit_clone)

            pool = CppThreadPool(workers, "m4-stateless")
            _fanout_handle = native_stateless_fanout(
                pool, heads, exits, next_input, ctx, sink, errors
            )

            done = 0
            yielded = False
            while done < workers:
                item = sink.get()
                if item is MQ_FINISHED:
                    done += 1
                    continue
                if item is None:  # consumer abandoned (we closed the sink)
                    break
                yielded = True
                yield item

            for exc in errors:
                if exc is not None:
                    raise exc

            # Empty result still needs the schema morsel the original Exit emits on EOS.
            if not yielded and not ctx.is_terminated():
                push_one(exit_node, _EOS_SENTINEL)
                while exit_node.has_pending():
                    yield exit_node.pop_pending()
        finally:
            # close() FIRST so any worker blocked on the bounded put unwinds before
            # we wait for the pool (else shutdown(wait=True) would deadlock).
            sink.close()
            if pool is not None:
                pool.shutdown(wait=True)
            ctx.terminate()
            scan.close_source()

    return generator(), ResultType.TABULAR


def _native_join_execute(plan, shape, workers, telemetry=None):
    """Native parallel INNER-EQUI-JOIN probe (slice 4): build once serially, then W
    workers probe DISJOINT slices of the probe scan through a private join clone
    (sharing the one read-only built ``left_hash``), streaming matches into a shared
    ``MorselQueue``. Replaces ``_join_probe_stream``'s ``queue.Queue`` fan-out +
    per-morsel ``push_one`` + outer ``Executor``/``queue.Queue`` with the native push
    + queue. Probe workers DO push EOS (the cloned join flushes its EOS path)."""
    import threading

    from opteryx import EOS as _EOS_SENTINEL
    from opteryx.compiled.morsel_queue import MQ_FINISHED
    from opteryx.compiled.morsel_queue import PyMorselQueue
    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators import JoinRightAdapter
    from opteryx.operators._operators import drive_scan
    from opteryx.operators._operators import native_stateless_fanout
    from opteryx.operators._operators import pull_one
    from opteryx.operators._operators import push_one
    from opteryx.operators._operators import push_one_to_sink
    from opteryx.operators._operators import spawn_worker
    from opteryx.operators._operators import stateless_worker_drive

    chains, exit_node, ctx = compile_pipeline(plan)
    build_scan = plan[shape.build_scan_id]
    probe_scan = plan[shape.probe_scan_id]
    j = plan[shape.join_id]

    build_head = probe_head = None
    for scan, head in chains:
        if scan is build_scan:
            build_head = head
        elif scan is probe_scan:
            probe_head = head
    if build_head is None or probe_head is None:
        from opteryx.managers.execution.parallel_engine import _serial_stream

        ctx.terminate()
        return _wrap_serial(_serial_stream(plan))

    sink = PyMorselQueue(max(64, workers * 8))

    def _drain_exit():
        while exit_node is not None and exit_node.has_pending():
            yield exit_node.pop_pending()

    def generator():
        pool = None
        errors = [None] * workers
        try:
            # ---- serial build prelude (build-before-probe): builds left_hash on j ----
            for _ in drive_scan(build_scan, build_head, exit_node, ctx):
                pass
            if ctx.is_terminated():
                return

            left_morsel = j.left_morsel
            left_columns = j.left_columns
            left_is_empty = j.left_is_empty
            columns = j.columns
            load_factor = j.carchar_probe_load_factor
            shared_left_hash = j.left_hash

            if left_is_empty:  # inner join, empty build → empty result (schema morsel)
                push_one(probe_head, _EOS_SENTINEL)
                yield from _drain_exit()
                return

            # ---- row-floor: tiny probe runs serially through the original chain ----
            buffer = []
            buffered_rows = 0
            exhausted = False
            while buffered_rows < config.PARALLEL_MIN_ROWS:
                morsel = pull_one(probe_scan)
                if morsel is None:
                    exhausted = True
                    break
                buffer.append(morsel)
                buffered_rows += morsel.num_rows

            if exhausted and buffered_rows < config.PARALLEL_MIN_ROWS:
                for morsel in buffer:
                    if ctx.is_terminated():
                        break
                    push_one(probe_head, morsel)
                    yield from _drain_exit()
                if not ctx.is_terminated():
                    push_one(probe_head, _EOS_SENTINEL)
                    yield from _drain_exit()
                return

            # ---- parallel probe ----
            buf_iter = iter(buffer)
            pull_lock = threading.Lock()
            concurrent_safe = probe_scan.is_concurrent_pull_safe()

            def next_input():
                if concurrent_safe:
                    with pull_lock:
                        m = next(buf_iter, None)
                    if m is not None:
                        return m
                    if ctx.is_terminated():
                        return None
                    return pull_one(probe_scan)
                with pull_lock:
                    if ctx.is_terminated():
                        return None
                    m = next(buf_iter, None)
                    if m is not None:
                        return m
                    return pull_one(probe_scan)

            # Serial pre-clone of each worker's join-probe chain (PRIVATE build state +
            # [probe-middle* → JoinRightAdapter(clone_join) → tail → exit]), then a native
            # STREAMING fan-out with an EOS flush — no Python worker closure, no Future.
            # The EOS (passed to native_stateless_fanout) makes each worker emit the
            # join's buffered probe results after draining its input.
            heads = []
            exits = []
            for _k in range(workers):
                clone_join = spawn_worker(j)
                clone_join.left_morsel = left_morsel
                clone_join.left_columns = left_columns
                clone_join.columns = columns
                clone_join.left_is_empty = left_is_empty
                clone_join.carchar_probe_load_factor = load_factor
                clone_join.left_hash = shared_left_hash
                clone_join._build_complete = True
                clone_join.set_context(ctx)

                tail = [spawn_worker(plan[nid]) for nid in shape.downstream_ids]
                exit_clone = spawn_worker(plan[shape.exit_id])
                tail = tail + [exit_clone]
                for i, op in enumerate(tail):
                    op.set_context(ctx)
                    if i + 1 < len(tail):
                        op.set_downstream(tail[i + 1])
                clone_join.set_downstream(tail[0])

                probe_ops = [spawn_worker(plan[nid]) for nid in shape.probe_middle_ids]
                adapter = JoinRightAdapter(clone_join)
                adapter.set_context(ctx)
                probe_chain = probe_ops + [adapter]
                for i, op in enumerate(probe_chain):
                    op.set_context(ctx)
                    if i + 1 < len(probe_chain):
                        op.set_downstream(probe_chain[i + 1])
                heads.append(probe_chain[0])
                exits.append(exit_clone)

            pool = CppThreadPool(workers, "m4-join-probe")
            _fanout_handle = native_stateless_fanout(
                pool, heads, exits, next_input, ctx, sink, errors, eos=_EOS_SENTINEL
            )

            done = 0
            yielded = False
            while done < workers:
                item = sink.get()
                if item is MQ_FINISHED:
                    done += 1
                    continue
                if item is None:
                    break
                yielded = True
                yield item

            for exc in errors:
                if exc is not None:
                    raise exc

            if not yielded and not ctx.is_terminated():
                push_one(probe_head, _EOS_SENTINEL)
                yield from _drain_exit()
        finally:
            sink.close()
            if pool is not None:
                pool.shutdown(wait=True)
            ctx.terminate()
            build_scan.close_source()
            probe_scan.close_source()

    return generator(), ResultType.TABULAR


def _wrap_serial(gen):
    """Adapt a bare morsel generator to the (generator, ResultType) contract."""

    def g():
        yield from gen

    return g(), ResultType.TABULAR


def _native_generic_execute(plan, dop, telemetry=None):
    """Native single-terminal drive for every NON-CHAIN plan (slice 4). When a plan
    is not a multi-breaker chain its drive is ONE terminal task, so the ``Executor``
    DAG (one real task + a no-op producer dependency) is pure overhead. Here that
    task runs on a ``CppThreadPool`` straight into a ``MorselQueue`` — no ``Executor``,
    no ``queue.Queue``. The per-shape handler (``dispatch_data_pipeline`` → breaker /
    serial / …) yields morsels; the driver pumps them into the sink, the consumer
    drains. (The fully-native push shapes — single-scan serial, stateless, join —
    are handled by their own paths before reaching here; this catches the breaker
    agg/distinct and any other non-chain shape.)"""
    from opteryx.compiled.morsel_queue import MQ_FINISHED
    from opteryx.compiled.morsel_queue import PyMorselQueue
    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.parallel_engine import dispatch_data_pipeline

    sink = PyMorselQueue(max(64, dop * 8))
    err_box: list = []
    pool = CppThreadPool(1, "m4-segment")

    def task():
        # PUSH-based (Level 1): the breaker skeleton drives straight into `sink` and
        # `finish()`es it on every exit (incl. its own teardown finally). No generator
        # coroutine, no pump loop, no gen_box.close().
        try:
            dispatch_data_pipeline(plan, dop, telemetry, out_q=sink)
        except BaseException as exc:  # noqa: BLE001 — surfaced on the consumer thread
            err_box.append(exc)
            sink.finish()  # defensive: a fault BEFORE the skeleton's finally must unblock

    def generator():
        pool.submit(task)
        try:
            while True:
                item = sink.get()
                if item is None or item is MQ_FINISHED:
                    break
                yield item
        finally:
            sink.close()  # unblock a producer blocked on the bounded put
            pool.shutdown(wait=True)  # joins the task → the skeleton's teardown ran
            if err_box:
                raise err_box[0]

    return generator(), ResultType.TABULAR


def _native_chain_execute(plan, chain, dop, telemetry=None):
    """Native drive for a single-scan LINEAR MULTI-BREAKER chain (e.g. GROUP BY →
    DISTINCT) — the last shape that used the Executor DAG. The chain is linear:
    producer segments materialise into buffers in order, then the terminal segment
    streams. Linear deps mean it drives SEQUENTIALLY (no DAG): each producer runs to
    completion before the next, the terminal streams into a ``MorselQueue``. No
    ``Executor``, no ``queue.Queue``."""
    from opteryx.compiled.morsel_queue import MQ_FINISHED
    from opteryx.compiled.morsel_queue import PyMorselQueue
    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.parallel_engine import _drive_passthrough_segment
    from opteryx.managers.execution.parallel_engine import _drive_segment
    from opteryx.managers.execution.pipeline_sink import recombination_class_for

    producers = [s for s in chain if s.tail_is_breaker]
    terminal_seg = chain[-1]
    bufs = [[] for _ in producers]  # producer i reads bufs[i-1], writes bufs[i]
    sink = PyMorselQueue(max(64, dop * 8))
    err_box: list = []
    pool = CppThreadPool(1, "m4-chain")

    def task():
        try:
            # Producers in dataflow order (linear deps → sequential, race-free bufs).
            for i in range(len(producers)):
                seg = producers[i]
                if i == 0:
                    source = ("scan", seg.nodes[0])
                    middle = list(seg.nodes[1:-1])
                else:
                    source = ("buffer", bufs[i - 1])
                    middle = list(seg.nodes[:-1])
                breaker_id = seg.nodes[-1]
                recomb = recombination_class_for(plan[breaker_id])
                # PUSH-based: producer captures into bufs[i] via collect_into (no out_q,
                # no generator). Drive it to completion.
                _drive_segment(
                    plan, source, middle, breaker_id, recomb, dop, telemetry,
                    collect_into=bufs[i],
                )

            # Terminal segment → drives straight into `sink` (push-based) and finishes it.
            last_buf = bufs[-1] if producers else []
            if terminal_seg.tail_is_breaker:
                breaker_id = terminal_seg.nodes[-1]
                recomb = recombination_class_for(plan[breaker_id])
                outs = list(plan.outgoing_edges(breaker_id))
                downstream_id = outs[0][1] if outs else None
                _drive_segment(
                    plan, ("buffer", last_buf), list(terminal_seg.nodes[:-1]),
                    breaker_id, recomb, dop, telemetry, downstream_id=downstream_id,
                    out_q=sink,
                )
            else:
                _drive_passthrough_segment(
                    plan, last_buf, terminal_seg.nodes[0], out_q=sink
                )
        except BaseException as exc:  # noqa: BLE001 — surfaced on the consumer thread
            err_box.append(exc)
            sink.finish()  # defensive: the terminal finishes sink, but a producer fault must too

    def generator():
        pool.submit(task)
        try:
            while True:
                item = sink.get()
                if item is None or item is MQ_FINISHED:
                    break
                yield item
        finally:
            sink.close()
            pool.shutdown(wait=True)  # joins the task → each segment's teardown ran
            if err_box:
                raise err_box[0]

    return generator(), ResultType.TABULAR


def execute(plan, head_node=None, telemetry=None):
    """Drive ``plan`` through the M4 Event-DAG scheduler — THE data executor.

    Returns the same ``(generator, ResultType)`` contract as the serial engine. The
    plan is cut into pipeline segments (one ``Event`` each); the terminal segment hosts
    the kept skeleton's streaming drive, and producer segments are dependency nodes
    (build-before-probe / breaker ordering as ``add_dependency`` edges). DOP is the real
    ``resolve_worker_count`` (the Stage-0 pin is lifted).
    """
    from opteryx.compiled.thread_pool import CppThreadPool

    segments = identify_segments(plan)
    dop = resolve_dop(plan, segments, config.MAX_EXECUTION_WORKERS)

    if telemetry is not None:
        telemetry._reading["scheduler_engaged"] = 1
        telemetry._reading["scheduler_pipelines"] = _count_pipelines(segments)
        telemetry._reading["scheduler_dop"] = dop

    # A single-SEGMENT single-scan plan at DOP=1 (no breaker — breakers cut segments)
    # is the byte-identical-serial prime constraint: the native push drive
    # `drive_scan_to_sink` straight into a `MorselQueue`. (A single-scan GROUP BY /
    # agg / DISTINCT is MULTI-segment and must reach the CONCURRENT breaker handler —
    # it is NOT caught here.)
    if dop == 1 and len(segments) == 1 and _count_scans(plan) == 1:
        return _native_serial_execute(plan, telemetry)

    # Native PARALLEL shapes first (so a single scan still parallelises): stateless
    # (scan → stateless* → exit) and inner-equi join run their own native drives.
    if dop >= 2:
        from opteryx.managers.execution.parallel_engine import _find_parallel_join
        from opteryx.managers.execution.parallel_engine import _find_parallel_stateless

        stateless = _find_parallel_stateless(plan)
        if stateless is not None:
            scan_id, op_ids, exit_id = stateless
            return _native_stateless_execute(
                plan, scan_id, op_ids, exit_id, dop, telemetry
            )

        join_shape = _find_parallel_join(plan)
        if join_shape is not None:
            return _native_join_execute(plan, join_shape, dop, telemetry)

    # A single-scan LINEAR MULTI-BREAKER chain (GROUP BY → DISTINCT) is the one shape
    # whose drive is genuinely multi-task (each breaker on its own DOP). Detected
    # BEFORE the single-scan serial gate so a chain still parallelises per-breaker.
    chain = None
    if dop >= 2:
        from opteryx.managers.execution.parallel_engine import (
            _find_linear_multibreaker_chain,
        )

        chain = _find_linear_multibreaker_chain(plan, dop)

    if chain is not None:
        if telemetry is not None:
            telemetry._reading["scheduler_multibreaker_chain"] = 1
            telemetry._reading["scheduler_chain_segments"] = len(chain)
        return _native_chain_execute(plan, chain, dop, telemetry)

    # Genuinely SERIAL plans (no parallel strategy covers them — multi-join, sort,
    # window, set-ops, limit-only, subqueries) drive natively, multi-chain, straight
    # into a `MorselQueue` via `_native_serial_execute` (the native replacement for
    # `_serial_stream` — no Python drive generator, no dispatch pump). CONCURRENT
    # breaker shapes (agg/distinct, join→agg) keep their worker fan-out via the
    # `_run_breaker_segment` handler pumped by `_native_generic_execute`.
    from opteryx.managers.execution.parallel_engine import _pipeline_is_serial

    if _pipeline_is_serial(plan, dop):
        return _native_serial_execute(plan, telemetry)
    return _native_generic_execute(plan, dop, telemetry)
