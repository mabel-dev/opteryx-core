# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Thread-local circular ring buffer for low-overhead event recording.

The ring buffer is pre-allocated with fixed size to avoid allocations in hot paths.
When full, new events overwrite the oldest events. This is suitable for tracing
where we only care about recent/complete events.
"""


class RingBuffer:
    """
    A pre-allocated circular buffer that stores events without any dynamic allocations.

    Designed for use in multi-threaded scenarios where each thread has its own buffer,
    avoiding synchronization overhead.

    Example:
        >>> buffer = RingBuffer(max_events=1000)
        >>> buffer.push({"type": "test", "value": 42})
        >>> events = buffer.drain()
        >>> len(events)
        1
    """

    def __init__(self, max_events: int = 10000):
        """
        Initialize a ring buffer.

        Args:
            max_events: Maximum number of events to store before overwriting oldest.
                       Default 10,000 uses ~2MB of memory per thread.
        """
        self.max_events = max_events
        self.events = [None] * max_events
        self.head = 0
        self.tail = 0
        self.count = 0

    def push(self, event: dict) -> None:
        """
        Add an event to the buffer. O(1) operation, no allocations.

        If buffer is full, overwrites the oldest event.

        Args:
            event: Event dictionary to store. Can contain any keys/values.
        """
        self.events[self.tail] = event
        self.tail = (self.tail + 1) % self.max_events

        if self.count < self.max_events:
            self.count += 1
        else:
            # Buffer is full, discard oldest event
            self.head = (self.head + 1) % self.max_events

    def drain(self) -> list:
        """
        Extract all events from the buffer and reset it.

        Returns:
            List of all events currently in buffer, in order.
            Buffer is empty after this call.
        """
        if self.count == 0:
            return []

        # Handle wrap-around: events from head to end, then start to tail
        if self.head < self.tail:
            # No wrap
            result = self.events[self.head : self.tail]
        else:
            # Wrap around
            result = self.events[self.head :] + self.events[: self.tail]

        # Reset buffer
        self.head = 0
        self.tail = 0
        self.count = 0
        self.events = [None] * self.max_events

        return result

    def size(self) -> int:
        """Return the number of events currently in the buffer."""
        return self.count

    def is_full(self) -> bool:
        """Return True if buffer is at max capacity."""
        return self.count == self.max_events

    def clear(self) -> None:
        """Clear all events from the buffer."""
        self.head = 0
        self.tail = 0
        self.count = 0
        self.events = [None] * self.max_events
