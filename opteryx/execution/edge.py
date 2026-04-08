# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Edge class for the execution model.

An edge is a first-class queue object connecting two operators, tracking:
- queued: items in the queue
- in_flight: items being processed downstream
- open: whether upstream can still produce

State transitions happen via enqueue/dequeue/complete/close operations.
The completion invariant (open=False AND queued=0 AND in_flight=0) signals
that all data has drained and finish() should be called.
"""

import threading
from collections import deque
from typing import Any
from typing import Optional


class Edge:
    """
    Thread-safe queue edge in the execution DAG.

    Tracks three pieces of state:
    - queued: number of items in the queue (not yet claimed)
    - in_flight: number of items claimed by consumer but not yet complete
    - open: whether upstream can produce more items (True) or is exhausted (False)

    The completion invariant is: open=False AND queued=0 AND in_flight=0
    When true, all data has drained and finish() should be called.
    """

    def __init__(self, name: str, target_queue_depth: int = 10):
        """
        Initialize an edge.

        Args:
            name: Identifier for this edge (for debugging/tracing)
            target_queue_depth: Target maximum for (queued + in_flight)
        """
        self.name = name
        self.target_queue_depth = target_queue_depth

        self._lock = threading.RLock()
        self._queued = 0
        self._in_flight = 0
        self._open = True

        # FIFO queue of morsels
        self._queue: deque = deque()

        # Condition variables for synchronization
        self._state_changed = threading.Condition(self._lock)

    def enqueue(self, morsel: Any) -> None:
        """
        Add a morsel to the queue (producer side).

        Increments queued count. May block if backpressure is in effect
        (queued + in_flight >= target_queue_depth).

        Thread-safe.

        Args:
            morsel: The data to enqueue
        """
        with self._lock:
            # Backpressure: wait if at target depth
            while self._queued + self._in_flight >= self.target_queue_depth:
                self._state_changed.wait(timeout=0.1)

            self._queue.append(morsel)
            self._queued += 1
            self._state_changed.notify_all()

    def dequeue(self) -> Optional[Any]:
        """
        Remove a morsel from the queue and move to in_flight (consumer side).

        Transitions: queued -= 1, in_flight += 1
        Returns None if queue is empty (whether open or closed).

        Thread-safe.

        Returns:
            Morsel if available, None otherwise
        """
        with self._lock:
            if self._queued > 0:
                morsel = self._queue.popleft()
                self._queued -= 1
                self._in_flight += 1
                self._state_changed.notify_all()
                return morsel
            return None

    def mark_complete(self) -> None:
        """
        Mark an in-flight morsel as complete (consumer finished processing).

        Transitions: in_flight -= 1
        Signals state change so completion can be detected.

        Thread-safe.
        """
        with self._lock:
            if self._in_flight > 0:
                self._in_flight -= 1
                self._state_changed.notify_all()

    def close(self) -> None:
        """
        Signal that upstream is exhausted and no more morsels will arrive.

        Sets open=False. Signals state change so scheduler can detect
        when completion invariant is met.

        Thread-safe.
        """
        with self._lock:
            self._open = False
            self._state_changed.notify_all()

    def is_complete(self) -> bool:
        """
        Check if the completion invariant is satisfied.

        Completion means: open=False AND queued=0 AND in_flight=0

        When true, all data has drained through this edge and finish()
        should be called on downstream consumers.

        Thread-safe.

        Returns:
            True if completion invariant is met, False otherwise
        """
        with self._lock:
            return not self._open and self._queued == 0 and self._in_flight == 0

    def is_open(self) -> bool:
        """
        Check if upstream can still produce more data.

        Thread-safe.

        Returns:
            True if open, False if closed
        """
        with self._lock:
            return self._open

    def has_data(self) -> bool:
        """
        Check if there is data ready to dequeue.

        Thread-safe.

        Returns:
            True if queued > 0, False otherwise
        """
        with self._lock:
            return self._queued > 0

    def total_in_transit(self) -> int:
        """
        Get the total number of items in transit (queued + in_flight).

        Used for backpressure decisions: schedule producer if
        total_in_transit < target_queue_depth

        Thread-safe.

        Returns:
            Sum of queued and in_flight
        """
        with self._lock:
            return self._queued + self._in_flight

    def get_state(self) -> dict:
        """
        Get a snapshot of the edge's current state.

        Useful for debugging and monitoring.

        Thread-safe.

        Returns:
            Dict with keys: name, queued, in_flight, open, total_in_transit, complete
        """
        with self._lock:
            return {
                "name": self.name,
                "queued": self._queued,
                "in_flight": self._in_flight,
                "open": self._open,
                "total_in_transit": self._queued + self._in_flight,
                "complete": not self._open and self._queued == 0 and self._in_flight == 0,
            }

    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """
        Block until the completion invariant is met.

        Used by scheduler to detect when an edge (and its upstream operator)
        has fully drained.

        Thread-safe.

        Args:
            timeout: Maximum seconds to wait, None for indefinite

        Returns:
            True if completed, False if timed out
        """
        with self._lock:
            import time

            deadline = None
            if timeout is not None:
                deadline = time.monotonic() + timeout

            while not self.is_complete():
                remaining = None
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                self._state_changed.wait(timeout=remaining)

            return True
