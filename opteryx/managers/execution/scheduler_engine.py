# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
M4 scheduler — THE data-pipeline executor (Step 7, generic pipeline parallelism).

This is the SOLE data executor. Every data pipeline (SELECT and friends) runs through
here; the dispatcher (``managers/execution/__init__.py``) routes only the non-pipeline
special ops (EXPLAIN / SET / SHOW / INSERT / DDL) to ``serial_engine``.

The fold (design §5 Step 7): the per-shape drive machinery — ``dispatch_data_pipeline``
and its handlers (``_run_breaker_segment``, ``_join_probe_stream``, ``_stateless_stream``,
``_serial_stream``), the ``_SharedSourceJoin`` build prelude — lives in
``parallel_engine.py`` as the kept SUBSTRATE (design §5 "Kept"). This module HOSTS that
substrate under the Event/Executor DAG:

  * ``identify_segments`` cuts the physical plan into pipeline segments — the scheduler's
    unit of work. Each ``Segment`` becomes ONE ``Event`` in the DAG.
  * Breaker ordering and build-before-probe become ``Event.add_dependency`` edges: a
    segment whose output feeds a breaker (or a join's build leg) is a dependency of the
    segment that consumes it, so the consuming Event only schedules once its producers
    finish. Multi-segment build subtrees (multi-join) wire every build segment ahead of
    the probe/terminal segment.
  * The TERMINAL segment (the one whose tail is the plan sink) hosts the streaming drive:
    its Event task pulls the kept skeleton's generator and hands each morsel across a
    bounded queue to the consumer. The non-terminal segment Events are the dependency
    nodes that express ordering; the actual cross-segment composition is the
    EMIT-into-cloned-downstream model (§4.1) the skeleton already performs — a breaker
    EMITs into its cloned downstream on EOS, it is not re-sourced — so the terminal drive
    runs the whole composed pipeline once, byte-identically to serial at DOP=1.

DOP is the real ``resolve_worker_count`` (the ``min(ceiling, 1)`` Stage-0 pin is lifted).
Worker count is degree-of-parallelism only — it never selects a code path; W=1 / below the
row-floor drives the ORIGINAL un-cloned breaker (the prime constraint, byte-identical to
serial — design §3).
"""

import queue
import threading
from typing import List

from opteryx import config
from opteryx.constants import ResultType
from opteryx.managers.execution.parallel_engine import Segment
from opteryx.managers.execution.parallel_engine import dispatch_data_pipeline
from opteryx.managers.execution.parallel_engine import identify_segments
from opteryx.managers.execution.parallel_engine import resolve_worker_count

# Sentinel pushed onto the hand-off queue when the drive task finishes (or dies).
_DONE = object()
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


class Event:
    """A node in the scheduling DAG and a batch of tasks, gated by two counters
    (DuckDB §6): upstream ``dependencies`` (runnable when all deps finish) and
    downstream ``tasks`` (complete when all tasks finish). Carried forward verbatim
    from the prototype (scratch/ddb_proto/scheduler.py) — proven shape.
    """

    def __init__(self, executor, name, schedule_tasks):
        self.executor = executor
        self.name = name
        self._schedule_tasks = schedule_tasks  # () -> [callable]
        self.total_deps = 0
        self.finished_deps = 0
        self.total_tasks = 0
        self.finished_tasks = 0
        self.parents = []

    def add_dependency(self, dep):
        self.total_deps += 1
        dep.parents.append(self)

    def maybe_schedule(self):
        if self.finished_deps == self.total_deps:
            self.schedule()

    def complete_dependency(self):
        with self.executor.lock:
            self.finished_deps += 1
            ready = self.finished_deps == self.total_deps
        if ready:
            self.schedule()

    def schedule(self):
        tasks = self._schedule_tasks()
        with self.executor.lock:
            self.total_tasks = len(tasks)
            empty = self.total_tasks == 0
        if empty:
            self.finish()
        else:
            for task in tasks:
                self.executor.submit(self, task)

    def finish_task(self):
        with self.executor.lock:
            self.finished_tasks += 1
            done = self.finished_tasks == self.total_tasks
        if done:
            self.finish()

    def finish(self):
        self.executor.event_completed()
        for parent in self.parents:
            parent.complete_dependency()


class Executor:
    """Per-query orchestrator: build events, wire deps, run the DAG to quiet on a
    CppThreadPool. The first error from any task is stashed and re-raised on the
    main thread; results stream out-of-band via the hand-off queue, not here.
    """

    def __init__(self, pool, telemetry=None):
        self.pool = pool
        self.lock = threading.Lock()
        self._all_done = threading.Event()
        self._error = None
        self._total_events = 0
        self._completed_events = 0
        self._events = []

    def add_event(self, name, schedule_tasks):
        ev = Event(self, name, schedule_tasks)
        self._events.append(ev)
        self._total_events += 1
        return ev

    def submit(self, event, task):
        future = self.pool.submit(task)
        future.add_done_callback(lambda f, e=event: self._task_done(e, f))

    def _task_done(self, event, future):
        exc = future.exception()
        if exc is not None:
            with self.lock:
                if self._error is None:
                    self._error = exc
            self._all_done.set()
            return
        event.finish_task()

    def stash_error(self, exc):
        """Stash the first error from any task (idempotent — keeps the first). Phase C
        producer tasks call this SYNCHRONOUSLY before unblocking the consumer, so the
        consumer sees the error after the ``_DONE`` it is woken with (the future
        callback ``_task_done`` would otherwise set it only after the task unwinds)."""
        with self.lock:
            if self._error is None:
                self._error = exc

    def event_completed(self):
        with self.lock:
            self._completed_events += 1
            done = self._completed_events == self._total_events
        if done:
            self._all_done.set()

    def start(self):
        for ev in self._events:
            if ev.total_deps == 0:
                ev.maybe_schedule()

    @property
    def error(self):
        return self._error


def _build_segment_dag(executor, plan, segments, drive_task):
    """Build one ``Event`` per pipeline ``Segment`` and wire the dependency edges.

    Each ``Segment`` from ``identify_segments`` is one Event. The dependency edges
    encode the dataflow ordering the design names: a segment whose tail feeds a breaker
    (or whose output is a join's build/probe input) is a DEPENDENCY of the segment that
    consumes it — so build-before-probe and breaker ordering fall out of
    ``add_dependency`` rather than inline sequencing. The TERMINAL segment (whose tail
    is the plan sink, ``not tail_is_breaker``) hosts the streaming drive: its Event task
    is ``drive_task``; all other (producer) segment Events are dependency nodes (no-op
    tasks — the actual cross-segment composition is the skeleton's
    EMIT-into-cloned-downstream model, §4.1, performed inside the terminal drive).

    Returns the terminal Event (the one that produces output). When the plan has a
    single segment (the common single-scan agg/stateless case) that segment IS the
    terminal one and there are no dependency edges.
    """
    # Map each segment to an Event. Producer segments (tail is a breaker, i.e. they feed
    # a downstream segment) get a no-op task; the terminal segment gets the real drive.
    seg_event = {}
    terminal = None
    for idx, seg in enumerate(segments):
        if seg.tail_is_breaker:
            ev = executor.add_event(f"segment[{idx}]:producer", lambda: [])
        else:
            ev = executor.add_event(
                f"segment[{idx}]:terminal", lambda dt=drive_task: [dt]
            )
            terminal = ev
        seg_event[seg.tail] = (seg, ev)

    if terminal is None:
        # Degenerate: no sink-tailed segment (every segment ends at a breaker). The
        # last segment in dataflow order is the producer of record; promote it to the
        # terminal drive so the pipeline still produces output.
        seg, _old = list(seg_event.values())[-1]
        terminal = executor.add_event("segment:terminal", lambda: [drive_task])

    # Wire dependency edges: a producer segment (tail = breaker) is a dependency of the
    # segment(s) downstream of that breaker. The push topology is single-output, so the
    # breaker's outgoing edge identifies the consuming segment's source. Every producer
    # segment is also, transitively, a dependency of the terminal drive — so the terminal
    # Event only schedules once all producers have completed (build-before-probe).
    for seg, ev in seg_event.values():
        if ev is terminal:
            continue
        terminal.add_dependency(ev)
    return terminal


def _build_multibreaker_chain_dag(
    executor, plan, chain, dop, telemetry, out_queue, gen_box
):
    """Phase C Event-DAG for a single-scan LINEAR MULTI-BREAKER chain.

    Each PRODUCER segment (every breaker segment, in dataflow order) gets a real drive
    Event that materialises its breaker's recombined output into a buffer; the next
    segment's Event DEPENDS on it (``add_dependency``) and is re-sourced from that
    buffer. The TERMINAL sink segment streams the last buffer to the hand-off queue.

    So every breaker parallelises on its own DOP (the agg AND the distinct, not just the
    first), and agg-before-readout / breaker-before-breaker ordering is genuine Event
    dependency — not the single terminal drive's EMIT-into-cloned-downstream tail.
    """
    from opteryx.managers.execution.parallel_engine import _drive_passthrough_segment
    from opteryx.managers.execution.parallel_engine import _drive_segment
    from opteryx.managers.execution.pipeline_sink import recombination_class_for

    producers = [s for s in chain if s.tail_is_breaker]
    terminal_seg = chain[-1]
    # One materialisation buffer per producer; producer i reads buf[i-1], writes buf[i].
    # Linear deps serialise the segment drives, so the buffers race-free.
    bufs = [[] for _ in producers]

    def _producer_task(i):
        seg = producers[i]
        if i == 0:
            source = ("scan", seg.nodes[0])
            middle = list(seg.nodes[1:-1])
        else:
            source = ("buffer", bufs[i - 1])
            middle = list(seg.nodes[:-1])  # buffer-sourced: nodes[0] is the first middle
        breaker_id = seg.nodes[-1]
        recomb = recombination_class_for(plan[breaker_id])

        def task():
            try:
                for _ in _drive_segment(
                    plan, source, middle, breaker_id, recomb, dop, telemetry,
                    collect_into=bufs[i],
                ):
                    pass
            except BaseException:  # noqa: BLE001 — unblock the consumer, then surface
                # The producer never feeds the hand-off queue (it materialises), so on
                # failure the terminal Event never runs and the consumer would block on
                # `out_queue.get()` forever. Stash + wake it.
                import sys

                executor.stash_error(sys.exc_info()[1])
                out_queue.put(_DONE)
                raise

        return task

    def _terminal_task():
        last_buf = bufs[-1] if producers else []
        if terminal_seg.tail_is_breaker:
            # Degenerate: the chain ends in a breaker with no sink segment — stream it.
            breaker_id = terminal_seg.nodes[-1]
            recomb = recombination_class_for(plan[breaker_id])
            outs = list(plan.outgoing_edges(breaker_id))
            downstream_id = outs[0][1] if outs else None
            gen = _drive_segment(
                plan, ("buffer", last_buf), list(terminal_seg.nodes[:-1]),
                breaker_id, recomb, dop, telemetry, downstream_id=downstream_id,
            )
        else:
            # The sink segment (e.g. a bare Exit, or stateless* → Exit): passthrough the
            # last materialised buffer through its head to the Exit.
            gen = _drive_passthrough_segment(plan, last_buf, terminal_seg.nodes[0])
        gen_box.append(gen)
        try:
            for morsel in gen:
                out_queue.put(morsel)
        finally:
            out_queue.put(_DONE)

    prod_events = []
    for i in range(len(producers)):
        ev = executor.add_event(
            f"chain.producer[{i}]", lambda i=i: [_producer_task(i)]
        )
        if i > 0:
            ev.add_dependency(prod_events[i - 1])
        prod_events.append(ev)
    term_ev = executor.add_event("chain.terminal", lambda: [_terminal_task()])
    if prod_events:
        term_ev.add_dependency(prod_events[-1])
    return term_ev


def _drive_pipeline(plan, workers, telemetry, out_queue, gen_box):
    """The terminal segment's task: pull the kept skeleton's drive generator (the shape
    router ``dispatch_data_pipeline``) and stream each morsel onto ``out_queue``. Runs on
    a pool thread. The generator is published into ``gen_box`` so the consumer can close
    it on early abandonment. Always terminates with a ``_DONE`` sentinel, exception or not.
    """
    gen = dispatch_data_pipeline(plan, workers, telemetry)
    gen_box.append(gen)
    try:
        for morsel in gen:
            out_queue.put(morsel)  # blocks on backpressure (bounded queue)
    finally:
        out_queue.put(_DONE)


def _stream(executor, out_queue, gen_box):
    """Consume the hand-off queue and yield morsels until the drive task signals
    ``_DONE``; then re-raise any stashed task error. On early consumer close, close the
    drive generator so its ``finally`` (ctx terminate / source close) runs and the worker
    stops at the next morsel.
    """
    try:
        while True:
            item = out_queue.get()
            if item is _DONE:
                break
            yield item
    finally:
        # Unblock the worker if the consumer abandoned us mid-stream (e.g. LIMIT): close
        # the drive generator (runs its finally: ctx terminate, source close) and drain
        # any in-flight morsel so its put() returns.
        for gen in gen_box:
            gen.close()
        while True:
            try:
                item = out_queue.get_nowait()
            except queue.Empty:
                break
            if item is _DONE:
                break
    if executor.error is not None:
        raise executor.error


def _count_scans(plan) -> int:
    return sum(1 for nid in plan.nodes() if getattr(plan[nid], "is_scan", False))


def _native_serial_execute(plan, telemetry=None):
    """Native single-scan serial drive (slice 3, option B). Pushes the scan's
    morsels through the compiled chain on a pool thread, the terminal Exit's output
    flowing straight into a ``MorselQueue``; the consumer generator drains it. No
    ``queue.Queue``, no ``Executor``/``threading``, no Python drive generator —
    byte-identical to the serial path at DOP=1.
    """
    from opteryx.compiled.morsel_queue import MQ_FINISHED
    from opteryx.compiled.morsel_queue import PyMorselQueue
    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators._operators import drive_scan_to_sink

    chains, exit_node, ctx = compile_pipeline(plan)
    scan, chain_head = chains[0]
    sink = PyMorselQueue(_QUEUE_DEPTH)
    err_box: list = []
    pool = CppThreadPool(1, "m4-serial")

    def task():
        # On any failure, stash and `finish()` so the consumer unblocks (it would
        # otherwise wait on `sink.get()` forever); the error is re-raised on the
        # consumer thread after the sentinel.
        try:
            drive_scan_to_sink(scan, chain_head, exit_node, ctx, sink)
        except BaseException as exc:  # noqa: BLE001 — surfaced on the consumer thread
            err_box.append(exc)
            sink.finish()

    def generator():
        pool.submit(task)
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

            def worker(index):
                ops = [spawn_worker(plan[nid]) for nid in op_ids]
                exit_clone = spawn_worker(plan[exit_id])
                chain = ops + [exit_clone]
                for i, op in enumerate(chain):
                    op.set_context(ctx)
                    if i + 1 < len(chain):
                        op.set_downstream(chain[i + 1])
                head = chain[0]
                try:
                    stateless_worker_drive(head, exit_clone, next_input, ctx, sink)
                except BaseException as exc:  # noqa: BLE001 — surfaced on main thread
                    errors[index] = exc
                finally:
                    sink.finish()  # this worker done (consumer counts W finishes)

            pool = CppThreadPool(workers, "m4-stateless")
            for k in range(workers):
                pool.submit(worker, k)

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

            def worker(index):
                try:
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
                    probe_head_clone = probe_chain[0]

                    stateless_worker_drive(probe_head_clone, exit_clone, next_input, ctx, sink)
                    if not ctx.is_terminated():
                        push_one_to_sink(probe_head_clone, exit_clone, _EOS_SENTINEL, sink)
                except BaseException as exc:  # noqa: BLE001 — surfaced on main thread
                    errors[index] = exc
                finally:
                    sink.finish()

            pool = CppThreadPool(workers, "m4-join-probe")
            for k in range(workers):
                pool.submit(worker, k)

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
    gen_box: list = []
    pool = CppThreadPool(1, "m4-segment")

    def task():
        try:
            gen = dispatch_data_pipeline(plan, dop, telemetry)
            gen_box.append(gen)
            for m in gen:
                if not sink.put(m):
                    break  # consumer abandoned (LIMIT / early close)
        except BaseException as exc:  # noqa: BLE001 — surfaced on the consumer thread
            err_box.append(exc)
        finally:
            sink.finish()

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
            pool.shutdown(wait=True)  # task no longer touches the handler generator
            for g in gen_box:
                g.close()  # run the handler's finally (ctx terminate, source close)
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
    gen_box: list = []
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
                for _ in _drive_segment(
                    plan, source, middle, breaker_id, recomb, dop, telemetry,
                    collect_into=bufs[i],
                ):
                    pass

            # Terminal segment → sink.
            last_buf = bufs[-1] if producers else []
            if terminal_seg.tail_is_breaker:
                breaker_id = terminal_seg.nodes[-1]
                recomb = recombination_class_for(plan[breaker_id])
                outs = list(plan.outgoing_edges(breaker_id))
                downstream_id = outs[0][1] if outs else None
                gen = _drive_segment(
                    plan, ("buffer", last_buf), list(terminal_seg.nodes[:-1]),
                    breaker_id, recomb, dop, telemetry, downstream_id=downstream_id,
                )
            else:
                gen = _drive_passthrough_segment(plan, last_buf, terminal_seg.nodes[0])
            gen_box.append(gen)
            for morsel in gen:
                if not sink.put(morsel):
                    break
        except BaseException as exc:  # noqa: BLE001 — surfaced on the consumer thread
            err_box.append(exc)
        finally:
            sink.finish()

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
            pool.shutdown(wait=True)
            for g in gen_box:
                g.close()
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

    # Slice 3 (native scheduler, option B): a single-scan plan at DOP=1 — the
    # byte-identical-serial prime constraint — runs the NATIVE push drive
    # (`drive_scan_to_sink`) straight into a `MorselQueue`. No Python `queue.Queue`,
    # no `Executor`/`threading`, no Python drive generator. The DOP>=2 parallel
    # handlers stay on the existing path until slice 4.
    if dop == 1 and len(segments) == 1 and _count_scans(plan) == 1:
        return _native_serial_execute(plan, telemetry)

    # Native parallel shapes (no queue.Queue / Executor for these).
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

    # A single-scan LINEAR MULTI-BREAKER chain (e.g. GROUP BY → DISTINCT) is the ONE
    # remaining plan shape whose drive is genuinely multi-task (each breaker
    # parallelises on its own DOP via materialise + re-source Event edges) — it keeps
    # the Executor DAG. EVERY OTHER plan has a single terminal drive, so the Executor
    # is pure overhead: it runs natively into a MorselQueue (no Executor/queue.Queue).
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

    return _native_generic_execute(plan, dop, telemetry)
