# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Event recording system for IO tracing.

This module provides the public API for recording trace events. When tracing is enabled
(OPTERYX_TRACE=1), events are queued for asynchronous writing to disk. When disabled,
all calls are no-ops with zero overhead.
"""

import atexit
import threading
import time
from typing import Optional

from opteryx.tracing.ring_buffer import RingBuffer

# Thread-local storage for ring buffers - one per thread
_thread_local = threading.local()

# Global event store (always populated when tracing is enabled).
# This allows clients to inspect or persist events without writing to a file.
_global_events: list[dict] = []
_global_lock = threading.Lock()

# Global trace writer instance (initialized lazily)
_trace_writer: Optional["TraceWriter"] = None
_writer_lock = threading.Lock()

# When a session is active we remember its ID here so that events can
# be automatically tagged without callers needing to pass it explicitly.
# The value is pushed by :class:`opteryx.query_session.Session` and cleared
# on close; the simplest policy is globally scoped.  In the uncommon case
# of concurrent sessions in the same process the last-started session will
# win, which is acceptable for our current use cases.
_current_session_id: Optional[str] = None


def _get_thread_buffer() -> RingBuffer:
    """Get or create the thread-local ring buffer for this thread."""
    if not hasattr(_thread_local, "buffer"):
        _thread_local.buffer = RingBuffer(max_events=10000)
    return _thread_local.buffer


def _get_trace_writer() -> Optional["TraceWriter"]:
    """Return a writer if one has been explicitly installed.

    The default policy for the engine is **not** to write trace events to a
    file; they are collected in memory and surfaced via :meth:`Session.trace`.
    A custom writer could be installed programmatically if needed, but the
    core engine will never create one.
    """
    # always return None by default; keep the variable around for tests or
    # future user-installed writers
    return None


def record_event(event_type: str, **kwargs) -> None:
    """
    Record a trace event.

    When tracing is enabled (OPTERYX_TRACE=1), the event is queued for writing.
    When tracing is disabled, this is a no-op with minimal overhead.
    Thread-safe: writes directly to the writer's queue which is a thread.Queue.
    """
    # Import here to avoid circular dependency and check at call time
    import random

    from opteryx import config

    if not config.OPTERYX_TRACE:
        return

    # sampling logic: if the caller supplied a file_id and the random draw is
    # above the configured sample rate, skip the event entirely.  (We don't
    # want to pay the cost of building the event object or queueing it.)
    file_id = kwargs.get("file_id")
    if file_id and random.random() > config.OPTERYX_TRACE_SAMPLE_RATE:
        return

    # Create event with timestamp
    event = {"type": event_type, "timestamp": time.perf_counter(), **kwargs}

    # automatically decorate with session id if caller omitted it
    if "session_id" not in event and _current_session_id is not None:
        event["session_id"] = _current_session_id

    # record to global list for later retrieval
    with _global_lock:
        _global_events.append(event)

    # Write directly to the thread-safe writer queue if a file sink exists.
    writer = _get_trace_writer()
    if writer:
        writer.enqueue_events([event])


def _flush_thread_buffer() -> None:
    """Flush the current thread's ring buffer to the trace writer."""
    buffer = _get_thread_buffer()
    events = buffer.drain()

    if events:
        writer = _get_trace_writer()
        if writer:
            writer.enqueue_events(events)


def flush_all() -> list[dict]:
    """
    Flush all pending events and return the global event list.

    If a trace writer is active (i.e. file logging is enabled) the writer will
    be flushed and closed.  In all cases the current contents of the global
    event list are returned so callers can inspect or persist them.  The list
    is *not* cleared automatically; callers may call ``reset()`` if they wish to
    discard old events.
    """
    global _trace_writer
    writer = _trace_writer
    if writer and writer.running:
        writer.flush()
        writer.close()
        _trace_writer = None

    with _global_lock:
        return list(_global_events)


def reset() -> None:
    """Reset the tracing system (for testing)."""
    global _trace_writer

    # Clear thread-local buffer
    if hasattr(_thread_local, "buffer"):
        _thread_local.buffer.clear()

    # Clear global event list
    with _global_lock:
        _global_events.clear()

    # Writer is unused by default, but close if someone installed one
    if _trace_writer:
        try:
            _trace_writer.close()
        except Exception:
            pass
        _trace_writer = None


def _cleanup_on_exit() -> None:
    """Called when Python exits to ensure trace writer is closed."""
    global _trace_writer
    writer = _trace_writer
    if writer and writer.running:
        try:
            writer.flush()
            writer.close()
            _trace_writer = None
        except Exception:
            pass


# Register cleanup handler to ensure trace files are written even if sessions aren't explicitly closed
atexit.register(_cleanup_on_exit)
