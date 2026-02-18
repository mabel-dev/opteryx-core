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

# Global trace writer instance (initialized lazily)
_trace_writer: Optional["TraceWriter"] = None
_writer_lock = threading.Lock()


def _get_thread_buffer() -> RingBuffer:
    """Get or create the thread-local ring buffer for this thread."""
    if not hasattr(_thread_local, "buffer"):
        _thread_local.buffer = RingBuffer(max_events=10000)
    return _thread_local.buffer


def _get_trace_writer() -> Optional["TraceWriter"]:
    """Get the global trace writer instance, creating if needed."""
    global _trace_writer

    if _trace_writer is not None:
        return _trace_writer

    # Import here to avoid circular dependency
    from opteryx import config

    if not config.OPTERYX_TRACE or not config.OPTERYX_TRACE_FILE:
        return None

    # Create trace writer lazily on first event
    with _writer_lock:
        if _trace_writer is None:
            from opteryx.tracing.trace_writer import TraceWriter

            _trace_writer = TraceWriter(config.OPTERYX_TRACE_FILE)

    return _trace_writer


def record_event(event_type: str, **kwargs) -> None:
    """
    Record a trace event.

    When tracing is enabled (OPTERYX_TRACE=1), the event is queued for writing.
    When tracing is disabled, this is a no-op with minimal overhead.
    Thread-safe: writes directly to the writer's queue which is a thread.Queue.
    """
    # Import here to avoid circular dependency and check at call time
    from opteryx import config

    if not config.OPTERYX_TRACE:
        return

    # Create event with timestamp
    event = {"type": event_type, "timestamp": time.perf_counter(), **kwargs}

    # Write directly to the thread-safe writer queue.
    # This avoids thread-local ring buffers which are not visible between threads
    # (e.g. when record_event is called from a ThreadPoolExecutor worker).
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


def flush_all() -> None:
    """
    Flush all pending events to disk and wait for completion.

    Called automatically at the end of query execution. Safe to call multiple times.
    """
    global _trace_writer
    # Use the existing writer directly (don't call _get_trace_writer which would
    # create a new writer after the first flush_all sets _trace_writer = None)
    writer = _trace_writer
    if writer and writer.running:
        writer.flush()
        writer.close()
        _trace_writer = None


def reset() -> None:
    """Reset the tracing system (for testing)."""
    global _trace_writer

    # Clear thread-local buffer
    if hasattr(_thread_local, "buffer"):
        _thread_local.buffer.clear()

    # Close and reset writer
    if _trace_writer:
        _trace_writer.close()
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
