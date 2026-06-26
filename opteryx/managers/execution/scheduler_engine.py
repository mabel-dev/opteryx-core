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

    out_queue: "queue.Queue" = queue.Queue(maxsize=_QUEUE_DEPTH)
    gen_box: list = []

    # The scheduler's own pool runs the ONE terminal drive task (the per-shape handlers
    # spawn their OWN worker pools at the resolved DOP for the fan-out). min 1 so the pool
    # is always valid.
    pool = CppThreadPool(1, "m4-scheduler")
    executor = Executor(pool, telemetry)

    def drive_task():
        return _drive_pipeline(plan, dop, telemetry, out_queue, gen_box)

    _build_segment_dag(executor, plan, segments, drive_task)

    def generator():
        try:
            executor.start()
            yield from _stream(executor, out_queue, gen_box)
        finally:
            pool.shutdown(wait=True)

    return generator(), ResultType.TABULAR
