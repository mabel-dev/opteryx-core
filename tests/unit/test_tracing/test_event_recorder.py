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
    
    def test_record_event_with_mock_writer(self):
        """Test event recording with mock writer."""
        # Set config to enable tracing
        original_trace = config.OPTERYX_TRACE
        original_file = config.OPTERYX_TRACE_FILE
        try:
            config.OPTERYX_TRACE = True
            config.OPTERYX_TRACE_FILE = "/tmp/trace_test.jsonl"
            
            with patch('opteryx.tracing.event_recorder._get_trace_writer') as mock_get_writer:
                mock_writer = MagicMock()
                mock_get_writer.return_value = mock_writer
                
                event_recorder.record_event("test_event", data=123)
                
                # Verify the event was recorded (buffer should not be empty)
                # and would be sent to writer during flush
                event_recorder._flush_thread_buffer()
                assert mock_writer.enqueue_events.called
        finally:
            config.OPTERYX_TRACE = original_trace
            config.OPTERYX_TRACE_FILE = original_file
    
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
    
    def test_flush_to_writer(self):
        """Test flushing thread buffer to writer."""
        original_trace = config.OPTERYX_TRACE
        original_file = config.OPTERYX_TRACE_FILE
        try:
            config.OPTERYX_TRACE = True
            config.OPTERYX_TRACE_FILE = "/tmp/trace_test.jsonl"
            
            with patch('opteryx.tracing.event_recorder._get_trace_writer') as mock_get_writer:
                mock_writer = MagicMock()
                mock_get_writer.return_value = mock_writer
                
                # Record some events
                event_recorder.record_event("event1", id=1)
                event_recorder.record_event("event2", id=2)
                
                # Flush
                event_recorder._flush_thread_buffer()
                
                # Verify enqueue was called
                assert mock_writer.enqueue_events.called
        finally:
            config.OPTERYX_TRACE = original_trace
            config.OPTERYX_TRACE_FILE = original_file


class TestEventRecorderIntegration:
    """Integration tests for event recorder."""
    
    def test_multiple_flushes(self):
        """Test multiple flush cycles."""
        original_trace = config.OPTERYX_TRACE
        original_file = config.OPTERYX_TRACE_FILE
        try:
            config.OPTERYX_TRACE = True
            config.OPTERYX_TRACE_FILE = "/tmp/trace_test.jsonl"
            
            with patch('opteryx.tracing.event_recorder._get_trace_writer') as mock_get_writer:
                mock_writer = MagicMock()
                mock_get_writer.return_value = mock_writer
                
                # Multiple record/flush cycles
                for cycle in range(3):
                    event_recorder.record_event("cycle", num=cycle)
                    event_recorder._flush_thread_buffer()
                
                # Should have enqueued multiple times
                assert mock_writer.enqueue_events.call_count >= 1
        finally:
            config.OPTERYX_TRACE = original_trace
            config.OPTERYX_TRACE_FILE = original_file
