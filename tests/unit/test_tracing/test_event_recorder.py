# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Tests for event recorder."""

import threading
from unittest.mock import MagicMock, patch

import pytest

from opteryx import config
from opteryx.tracing import event_recorder
from opteryx.tracing.event_recorder import _global_events
from opteryx.query_session import Session


class TestEventRecorder:
    """Test event recording functionality."""
    
    def teardown_method(self):
        """Reset recorder state after each test."""
        event_recorder.reset()
    
    def test_record_event_disabled(self):
        """Test that events are not recorded when disabled."""
        with patch.object(config, 'OPTERYX_TRACE', False):
            # Should be a no-op
            event_recorder.record_event("test_event", key="value")
            # No assertion needed, just verify no exception
    
    def test_record_event_stores_global(self):
        """Test that events are always stored in the global list."""
        original_trace = config.OPTERYX_TRACE
        try:
            config.OPTERYX_TRACE = True
            event_recorder.reset()
            event_recorder.record_event("test_event", data=123)
            with event_recorder._global_lock:
                assert any(ev['type'] == 'test_event' for ev in _global_events)  # type: ignore
        finally:
            config.OPTERYX_TRACE = original_trace
    
    def test_record_event_fields(self):
        """Test that event fields are properly captured."""
        original_trace = config.OPTERYX_TRACE
        try:
            config.OPTERYX_TRACE = True
            
            # Record an event
            event_recorder.record_event(
                "download_complete",
                file_id="test.parquet",
                bytes_received=1024,
                duration_ms=100
            )
            
            # Get the thread-local buffer
            buffer = event_recorder._get_thread_buffer()
            events = buffer.drain()
            
            assert len(events) == 1
            
            event = events[0]
            assert event['type'] == "download_complete"
            assert event['file_id'] == "test.parquet"
            assert event['bytes_received'] == 1024
            assert event['duration_ms'] == 100
            assert 'timestamp' in event
        finally:
            config.OPTERYX_TRACE = original_trace
    
    def test_thread_buffer_isolation(self):
        """Test that thread-local buffers are isolated."""
        original_trace = config.OPTERYX_TRACE
        try:
            config.OPTERYX_TRACE = True
            results = []
            errors = []
            
            def record_in_thread(thread_id):
                try:
                    for i in range(3):
                        event_recorder.record_event(
                            "test",
                            thread_id=thread_id,
                            count=i
                        )
                    
                    buffer = event_recorder._get_thread_buffer()
                    buffer_size = buffer.size()
                    results.append((thread_id, buffer_size))
                except Exception as e:
                    errors.append(e)
            
            threads = [
                threading.Thread(target=record_in_thread, args=(1,)),
                threading.Thread(target=record_in_thread, args=(2,)),
            ]
            
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            
            # Check for errors
            assert not errors, f"Errors in threads: {errors}"
            
            # Each thread should have recorded independently
            assert len(results) == 2
            # Both should have 3 events
            for thread_id, count in results:
                assert count == 3
        finally:
            config.OPTERYX_TRACE = original_trace
    
    def test_flush_all_returns_events(self):
        """flush_all should return the global events regardless of writer."""
        original_trace = config.OPTERYX_TRACE
        try:
            config.OPTERYX_TRACE = True
            event_recorder.reset()
            event_recorder.record_event("e1")
            event_recorder.record_event("e2")
            events = event_recorder.flush_all()
            assert len(events) >= 2
        finally:
            config.OPTERYX_TRACE = original_trace

    def test_sampling_respected(self):
        """When sample rate is zero, subsequent events are not recorded."""
        original_trace = config.OPTERYX_TRACE
        original_rate = config.OPTERYX_TRACE_SAMPLE_RATE
        try:
            config.OPTERYX_TRACE = True
            config.OPTERYX_TRACE_SAMPLE_RATE = 0.0

            # record some events with a file_id
            for _ in range(5):
                event_recorder.record_event("download_start", file_id="foo")
            buffer = event_recorder._get_thread_buffer()
            events = buffer.drain()
            # should be empty because sampling filtered them
            assert not events
        finally:
            config.OPTERYX_TRACE = original_trace
            config.OPTERYX_TRACE_SAMPLE_RATE = original_rate


class TestEventRecorderIntegration:
    """Integration tests for event recorder."""
    
    def test_multiple_flushes(self):
        """Test multiple flush cycles do not crash and global events accumulate."""
        original_trace = config.OPTERYX_TRACE
        try:
            config.OPTERYX_TRACE = True
            event_recorder.reset()

            # Multiple record/flush cycles
            for cycle in range(3):
                event_recorder.record_event("cycle", num=cycle)
                event_recorder._flush_thread_buffer()

            # ensure events are available globally
            events = event_recorder.flush_all()
            assert any(e.get("type") == "cycle" for e in events)
        finally:
            config.OPTERYX_TRACE = original_trace


class TestSessionTrace:
    """Tests for Session.trace() API."""

    def test_session_trace_iterates(self, tmp_path):
        original_trace = config.OPTERYX_TRACE
        try:
            # enable global tracing
            config.OPTERYX_TRACE = True
            session = Session()
            # create a tiny parquet file and run a query to produce events
            import pandas as pd
            import pyarrow as pa, pyarrow.parquet as pq
            data_file = tmp_path / "data.parquet"
            pq.write_table(pa.Table.from_pandas(pd.DataFrame({"x": [1]})), str(data_file))
            list(session.execute(f"SELECT * FROM '{data_file}'"))
            events = list(session.trace())
            assert any(e.get("type") == "trace_session_start" for e in events)
        finally:
            config.OPTERYX_TRACE = original_trace

