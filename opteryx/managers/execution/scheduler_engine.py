# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
M4 Stage 0 — event-DAG scheduler skeleton.

This is the morsel-driven scheduler's scaffold (docs/M4_SEGMENT_SCHEDULER_SHUFFLE_DESIGN.md).
At this stage it is, by design, **functionally a no-op**: it decomposes the plan
into pipeline segments, builds the event-DAG structure, computes a data-bounded
degree of parallelism, and drives the EXISTING push operators — producing output
byte-for-byte identical to the serial-inline path. No operator is rewritten here.

What is real at Stage 0:
  * pipeline decomposition via ``identify_segments`` (the scheduler's unit of work),
  * the two-counter ``Event`` / ``Executor`` scaffold running on ``CppThreadPool``,
  * a streaming cross-thread morsel hand-off (worker drives → bounded queue →
    consumer yields), the substrate parallel pipelines will use,
  * data-bounded DOP computation.

What is NOT yet real (Stage 1+ unlocks it): the operators are still a single push
chain that is not split at breakers into independently-scheduled pipelines, so the
scheduler runs the whole plan as ONE drive task. Effective DOP is therefore 1 until
the breaker contract lands. This is the honest no-op the gate demands — not a fake
parallel path.

Gated behind ``config.M4_USE_SCHEDULER`` (default off); the dispatcher
(``managers/execution/__init__.py``) routes here only when the flag is set, leaving
``parallel_engine`` untouched so the two can be compared at DOP=1 via ``make m4-sweep``.
"""

import queue
import threading
from typing import List

from opteryx.constants import ResultType
from opteryx.managers.execution.parallel_engine import Segment
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


def data_bounded_dop(plan, segments: List[Segment], requested) -> int:
    """DOP = min(source morsels available, scheduler threads) — the principled,
    floor-free degree from the design.

    Stage 0 caveat: the breaker contract is not implemented, so the plan drives as
    a single unit; the *effective* execution DOP is pinned to 1 here regardless of
    this computed ceiling. The computation is wired now so Stage 1+ can lift the pin
    without re-plumbing. We still resolve the worker ceiling so the value is honest.
    """
    ceiling = resolve_worker_count(requested)
    # Effective DOP at Stage 0 is 1 (no breaker-split pipelines yet).
    return min(ceiling, 1)


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


def _drive_whole_plan(plan, out_queue, ctx_box):
    """The Stage 0 task: drive the full push pipeline (identical to the serial-inline
    path) and stream each morsel onto ``out_queue``. Runs on a pool thread. The
    PipelineContext is published into ``ctx_box`` so the consumer can terminate it on
    early close. Always terminates with a ``_DONE`` sentinel, exception or not.
    """
    from opteryx.managers.execution.pipeline_compiler import compile_pipeline
    from opteryx.operators._operators import drive_scan

    chains, exit_node, ctx = compile_pipeline(plan)
    ctx_box.append(ctx)
    try:
        for scan, chain_head in chains:
            for morsel in drive_scan(scan, chain_head, exit_node, ctx):
                out_queue.put(morsel)  # blocks on backpressure (bounded queue)
                if ctx.is_terminated():
                    break
            if ctx.is_terminated():
                break
    finally:
        ctx.terminate()
        out_queue.put(_DONE)


def _stream(executor, out_queue, ctx_box):
    """Consume the hand-off queue and yield morsels until the drive task signals
    ``_DONE``; then re-raise any stashed task error. On early consumer close, the
    finally terminates the PipelineContext so the worker stops at the next morsel.
    """
    try:
        while True:
            item = out_queue.get()
            if item is _DONE:
                break
            yield item
    finally:
        # Unblock the worker if the consumer abandoned us mid-stream (e.g. LIMIT).
        for ctx in ctx_box:
            ctx.terminate()
        # Drain any in-flight morsels the worker already queued so its put() returns.
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
    """Drive ``plan`` through the Stage 0 event-DAG scaffold. Returns the same
    ``(generator, ResultType)`` contract as the parallel/serial engines.
    """
    from opteryx.compiled.thread_pool import CppThreadPool
    from opteryx import config

    segments = identify_segments(plan)
    dop = data_bounded_dop(plan, segments, config.MAX_EXECUTION_WORKERS)

    if telemetry is not None:
        telemetry._reading["scheduler_engaged"] = 1
        telemetry._reading["scheduler_pipelines"] = _count_pipelines(segments)
        telemetry._reading["scheduler_dop"] = dop

    out_queue: "queue.Queue" = queue.Queue(maxsize=_QUEUE_DEPTH)
    ctx_box: list = []

    # One worker thread for the single Stage 0 drive task; the pool is the real
    # substrate Stage 1+ fans out on. min 1 so the pool is always valid.
    pool = CppThreadPool(max(1, dop), "m4-scheduler")
    executor = Executor(pool, telemetry)
    executor.add_event(
        "plan",
        schedule_tasks=lambda: [lambda: _drive_whole_plan(plan, out_queue, ctx_box)],
    )

    def generator():
        try:
            executor.start()
            yield from _stream(executor, out_queue, ctx_box)
        finally:
            pool.shutdown(wait=True)

    return generator(), ResultType.TABULAR
