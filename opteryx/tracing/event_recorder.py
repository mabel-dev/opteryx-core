# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Event recording system for tracing.

This module provides the public API for recording trace events. When tracing is enabled
(OPTERYX_TRACE=1), events are queued for asynchronous writing to disk. When disabled,
all calls are no-ops with zero overhead.

The helpers in this module intentionally expose semantic trace phases rather than
implementation details so the trace stream can be used for profiling, bottleneck
analysis, and waterfall visualizations.
"""

from __future__ import annotations

import atexit
import threading
import time
from typing import Optional
from typing import TypedDict

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

_thread_local = threading.local()
_global_events: list[dict] = []
_global_lock = threading.Lock()
_trace_writer: Optional["TraceWriter"] = None
_writer_lock = threading.Lock()
_current_session_id: Optional[str] = None


class TraceEvent(TypedDict, total=False):
    type: str
    timestamp: float
    session_id: str
    file_id: str
    component: str
    rg_idx: int
    column: str
    operator_name: str
    operator_id: str
    phase: str
    rows_in: int
    rows_out: int
    bytes_received: int
    duration_ns: int
    produced_rows: bool
    columns: list[str]
    ranges: int
    rows_decoded: int
    bytes: int


def _get_thread_buffer():
    """Get or create the thread-local ring buffer for this thread."""
    from opteryx.tracing.ring_buffer import RingBuffer

    if not hasattr(_thread_local, "buffer"):
        _thread_local.buffer = RingBuffer(max_events=10000)
    return _thread_local.buffer


def _get_trace_writer() -> Optional["TraceWriter"]:
    """Return a writer if one has been explicitly installed.

    The default policy for the engine is not to write trace events to a file;
    they are collected in memory and surfaced via Session.trace().
    """
    return None


def record_event(event_type: str, **kwargs) -> None:
    """
    Record a trace event.

    When tracing is enabled (OPTERYX_TRACE=1), the event is stored in-memory for
    later inspection and optional export.
    """
    import random

    from opteryx import config

    if not config.OPTERYX_TRACE:
        return

    file_id = kwargs.get("file_id")
    if file_id and random.random() > config.OPTERYX_TRACE_SAMPLE_RATE:
        return

    event: TraceEvent = {"type": event_type, "timestamp": time.perf_counter(), **kwargs}

    if "session_id" not in event and _current_session_id is not None:
        event["session_id"] = _current_session_id

    with _global_lock:
        _global_events.append(event)

    writer = _get_trace_writer()
    if writer:
        writer.enqueue_events([event])


# ---------------------------------------------------------------------------
# Semantic trace helpers
# ---------------------------------------------------------------------------


def trace_io_started(**kwargs) -> None:
    record_event("download_start", **kwargs)


def trace_io_completed(**kwargs) -> None:
    record_event("download_complete", **kwargs)


def trace_buffer_started(**kwargs) -> None:
    record_event("buffer_start", **kwargs)


def trace_buffer_completed(**kwargs) -> None:
    record_event("buffer_complete", **kwargs)


def trace_row_group_buffered(**kwargs) -> None:
    record_event("buffer_complete", component="rowgroup", **kwargs)


def trace_column_buffered(**kwargs) -> None:
    record_event("buffer_complete", component="column", **kwargs)


def trace_decode_started(**kwargs) -> None:
    record_event("decode_start", **kwargs)


def trace_decode_completed(**kwargs) -> None:
    record_event("decode_complete", **kwargs)


def trace_operator_started(**kwargs) -> None:
    record_event("operator_execute", phase="start", **kwargs)


def trace_operator_completed(**kwargs) -> None:
    record_event("operator_execute", phase="finish", **kwargs)


# Backward-compatible aliases used by some call sites.
trace_operator_finished = trace_operator_completed


# ---------------------------------------------------------------------------
# Buffer / writer management
# ---------------------------------------------------------------------------


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

    If a trace writer is active, it will be flushed and closed first.
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
    """Reset the tracing system for testing."""
    global _trace_writer

    if hasattr(_thread_local, "buffer"):
        _thread_local.buffer.clear()

    with _global_lock:
        _global_events.clear()

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


atexit.register(_cleanup_on_exit)
