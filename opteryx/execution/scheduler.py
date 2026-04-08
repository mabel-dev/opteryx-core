# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Scheduler for the edge-based execution model.

The scheduler orchestrates work across threads, using edges to make scheduling
decisions. It:
- Schedules producers when there is queue capacity
- Schedules consumers when there is data
- Detects edge completion and calls finish() on downstream operators
- Handles special cases like join build-then-probe ordering
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from opteryx.execution.edge import Edge


class Scheduler:
    """
    Orchestrates parallel execution using edges for flow control.

    Manages producer and consumer work across threads, using edge state
    (queued, in_flight, open) to make scheduling decisions.

    The scheduler:
    - Detects when producers can produce (has queue capacity)
    - Detects when consumers can consume (has queued data)
    - Detects when edges are complete (completion invariant)
    - Calls finish() on operators when their input is drained
    - Handles join build→probe ordering via explicit synchronization
    """

    def __init__(
        self,
        max_workers: int = 4,
    ):
        """
        Initialize the scheduler.

        Args:
            max_workers: Maximum number of worker threads in the pool
        """
        self.max_workers = max_workers
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="opteryx-exec"
        )
        self._edges: Dict[str, Edge] = {}
        self._operators: Dict[str, Any] = {}

        # Mapping: (source_nid, sink_nid) → Edge
        self._edge_for_pair: Dict[Tuple[str, str], Edge] = {}

        # Producers: nid → callable(edge) -> List[morsel]
        self._producers: Dict[str, Callable] = {}

        # Consumers: nid → callable(morsel) -> List[morsel]
        self._consumers: Dict[str, Callable] = {}

        # Finish callbacks: nid → callable() -> List[morsel]
        self._finishers: Dict[str, Callable] = {}

        # Operator input/output edges
        self._inputs: Dict[str, List[Tuple[str, Edge]]] = {}  # nid → [(source_nid, edge), ...]
        self._outputs: Dict[str, List[Tuple[str, Edge]]] = {}  # nid → [(sink_nid, edge), ...]

        # Synchronization for joins
        self._join_build_complete: Dict[str, threading.Event] = {}  # join_nid → Event

        self._lock = threading.RLock()
        self._shutdown = False

    def add_edge(self, from_nid: str, to_nid: str, edge: Edge) -> None:
        """
        Register an edge in the execution graph.

        Args:
            from_nid: Producer node ID
            to_nid: Consumer node ID
            edge: Edge object connecting them
        """
        with self._lock:
            self._edges[edge.name] = edge
            self._edge_for_pair[(from_nid, to_nid)] = edge

            if to_nid not in self._inputs:
                self._inputs[to_nid] = []
            self._inputs[to_nid].append((from_nid, edge))

            if from_nid not in self._outputs:
                self._outputs[from_nid] = []
            self._outputs[from_nid].append((to_nid, edge))

    def add_producer(self, nid: str, operator: Any, producer_fn: Callable) -> None:
        """
        Register a producer (source) operator.

        The producer_fn should be callable as: producer_fn(edge) -> List[morsel]
        It produces one or more morsels and enqueues them to the edge.
        Returns empty list when exhausted, or None to signal close.

        Args:
            nid: Node ID
            operator: The operator instance
            producer_fn: Callable that produces morsels to an edge
        """
        with self._lock:
            self._producers[nid] = producer_fn
            self._operators[nid] = operator

    def add_consumer(
        self, nid: str, operator: Any, consumer_fn: Callable, finisher_fn: Optional[Callable] = None
    ) -> None:
        """
        Register a consumer (transform) operator.

        The consumer_fn should be callable as: consumer_fn(morsel) -> List[morsel]
        It processes one morsel and returns zero or more output morsels.

        The finisher_fn is called when all input is drained:
        finisher_fn() -> List[morsel]
        It emits final results (e.g., accumulated aggregates).

        Args:
            nid: Node ID
            operator: The operator instance
            consumer_fn: Callable that processes one morsel
            finisher_fn: Optional callable for finalization
        """
        with self._lock:
            self._consumers[nid] = consumer_fn
            self._finishers[nid] = finisher_fn
            self._operators[nid] = operator

    def add_join(
        self, join_nid: str, operator: Any, build_fn: Callable, probe_fn: Callable
    ) -> None:
        """
        Register a join operator with separate build and probe phases.

        The join has two input edges: build and probe.
        The build_fn processes morsels during build phase.
        The probe_fn processes morsels during probe phase.

        Probe execution is blocked until build is complete.

        Args:
            join_nid: Join operator node ID
            operator: The operator instance
            build_fn: Callable for build phase
            probe_fn: Callable for probe phase
        """
        with self._lock:
            self._consumers[join_nid] = (
                build_fn  # Will be replaced with probe_fn after build completes
            )
            self._operators[join_nid] = operator
            self._join_build_complete[join_nid] = threading.Event()

    def execute(self) -> None:
        """
        Run the execution loop.

        Orchestrates producers and consumers across threads until all edges
        are complete. Uses edge state to drive scheduling decisions.

        Returns when no more work can be scheduled.
        """
        futures = set()

        try:
            while not self._shutdown:
                work_scheduled = False

                # Schedule producers
                with self._lock:
                    for nid, producer_fn in list(self._producers.items()):
                        if nid not in self._outputs:
                            continue
                        output_edges = self._outputs[nid]
                        if not output_edges:
                            continue

                        edge = output_edges[0][1]
                        if edge.is_open() and edge.total_in_transit() < edge.target_queue_depth:
                            future = self._executor.submit(
                                self._run_producer, nid, producer_fn, edge
                            )
                            futures.add(future)
                            work_scheduled = True

                # Schedule consumers
                with self._lock:
                    for nid, consumer_fn in list(self._consumers.items()):
                        if nid not in self._inputs:
                            continue
                        input_edges = self._inputs[nid]

                        # Check if all inputs have data
                        has_data = any(edge.has_data() for _, edge in input_edges)
                        if has_data:
                            # For joins, check build phase is complete
                            if (
                                nid in self._join_build_complete
                                and not self._join_build_complete[nid].is_set()
                            ):
                                continue

                            for _, edge in input_edges:
                                morsel = edge.dequeue()
                                if morsel is not None:
                                    future = self._executor.submit(
                                        self._run_consumer, nid, consumer_fn, morsel, edge
                                    )
                                    futures.add(future)
                                    work_scheduled = True

                # Check for completion and call finish()
                with self._lock:
                    for nid, finisher_fn in list(self._finishers.items()):
                        if nid not in self._inputs or finisher_fn is None:
                            continue
                        input_edges = self._inputs[nid]
                        all_complete = all(edge.is_complete() for _, edge in input_edges)
                        if all_complete:
                            future = self._executor.submit(self._run_finisher, nid, finisher_fn)
                            futures.add(future)
                            work_scheduled = True
                            # Remove so we don't call finish() multiple times
                            self._finishers[nid] = None

                # Wait for one future to complete if work was scheduled
                if work_scheduled and futures:
                    done, futures = self._wait_for_one(futures, timeout=0.1)
                    for future in done:
                        try:
                            future.result()
                        except Exception as e:
                            print(f"Task failed: {e}")
                            self._shutdown = True
                elif not work_scheduled:
                    # No work scheduled; check if we're truly done
                    with self._lock:
                        all_complete = all(edge.is_complete() for edge in self._edges.values())
                    if all_complete:
                        break
                    time.sleep(0.01)

        finally:
            self._executor.shutdown(wait=True)

    def _run_producer(self, nid: str, producer_fn: Callable, edge: Edge) -> None:
        """Run a producer task: call producer_fn and enqueue morsels."""
        try:
            morsels = producer_fn(edge)
            if morsels is None:
                edge.close()
            elif isinstance(morsels, list):
                for morsel in morsels:
                    if morsel is not None:
                        edge.enqueue(morsel)
            else:
                edge.enqueue(morsels)
        except Exception as e:
            print(f"Producer {nid} failed: {e}")
            edge.close()

    def _run_consumer(self, nid: str, consumer_fn: Callable, morsel: Any, edge: Edge) -> None:
        """Run a consumer task: process one morsel and enqueue outputs."""
        try:
            output_edges = self._outputs.get(nid, [])
            if not output_edges:
                # No outputs; just mark complete
                edge.mark_complete()
                return

            results = consumer_fn(morsel)
            if results is not None:
                output_edge = output_edges[0][1]
                if isinstance(results, list):
                    for result in results:
                        if result is not None:
                            output_edge.enqueue(result)
                else:
                    output_edge.enqueue(results)
            edge.mark_complete()
        except Exception as e:
            print(f"Consumer {nid} failed: {e}")
            edge.mark_complete()

    def _run_finisher(self, nid: str, finisher_fn: Callable) -> None:
        """Run a finisher task: call finisher_fn and enqueue final results."""
        try:
            output_edges = self._outputs.get(nid, [])
            if not output_edges:
                return

            results = finisher_fn()
            if results is not None:
                output_edge = output_edges[0][1]
                if isinstance(results, list):
                    for result in results:
                        if result is not None:
                            output_edge.enqueue(result)
                else:
                    output_edge.enqueue(results)
                output_edge.close()
            else:
                output_edge.close()
        except Exception as e:
            print(f"Finisher {nid} failed: {e}")

    def _wait_for_one(self, futures, timeout=None):
        """Wait for at least one future to complete."""
        if not futures:
            return set(), futures
        done, pending = (), set(futures)
        for future in as_completed(futures, timeout=timeout):
            done = {future}
            pending = futures - {future}
            break
        return done, pending

    def shutdown(self) -> None:
        """Shutdown the scheduler and wait for all work to complete."""
        self._shutdown = True
        self._executor.shutdown(wait=True)
