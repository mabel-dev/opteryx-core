# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Tests for ring buffer implementation."""

import pytest
from opteryx.tracing.ring_buffer import RingBuffer


class TestRingBuffer:
    """Test ring buffer functionality."""
    
    def test_push_single_event(self):
        """Test pushing a single event."""
        buffer = RingBuffer(max_events=10)
        buffer.push({"type": "test", "value": 42})
        
        assert buffer.size() == 1
        assert not buffer.is_full()
    
    def test_push_multiple_events(self):
        """Test pushing multiple events."""
        buffer = RingBuffer(max_events=10)
        for i in range(5):
            buffer.push({"type": "test", "id": i})
        
        assert buffer.size() == 5
    
    def test_push_and_drain(self):
        """Test pushing and draining events."""
        buffer = RingBuffer(max_events=10)
        for i in range(3):
            buffer.push({"id": i})
        
        events = buffer.drain()
        
        assert len(events) == 3
        assert buffer.size() == 0
        assert [e['id'] for e in events] == [0, 1, 2]
    
    def test_overflow_overwrites_oldest(self):
        """Test that overflow overwrites oldest events."""
        buffer = RingBuffer(max_events=5)
        
        # Fill buffer
        for i in range(5):
            buffer.push({"id": i})
        
        assert buffer.is_full()
        
        # Add one more - should overwrite first
        buffer.push({"id": 5})
        
        events = buffer.drain()
        ids = [e['id'] for e in events]
        
        # Should have ids 1,2,3,4,5 (0 was overwritten)
        assert ids == [1, 2, 3, 4, 5]
    
    def test_clear(self):
        """Test clearing the buffer."""
        buffer = RingBuffer(max_events=10)
        buffer.push({"id": 1})
        buffer.push({"id": 2})
        
        buffer.clear()
        
        assert buffer.size() == 0
        assert len(buffer.drain()) == 0
    
    def test_empty_drain(self):
        """Test draining an empty buffer."""
        buffer = RingBuffer(max_events=10)
        events = buffer.drain()
        
        assert events == []
        assert buffer.size() == 0
    
    def test_large_buffer(self):
        """Test with larger number of events."""
        buffer = RingBuffer(max_events=1000)
        
        for i in range(500):
            buffer.push({"id": i})
        
        assert buffer.size() == 500
        assert not buffer.is_full()
        
        events = buffer.drain()
        assert len(events) == 500
        assert buffer.size() == 0
    
    def test_wrap_around(self):
        """Test buffer wrap-around behavior."""
        buffer = RingBuffer(max_events=3)
        
        # Fill and wrap around multiple times
        for i in range(10):
            buffer.push({"id": i})
        
        events = buffer.drain()
        # Should have last 3: 7, 8, 9
        assert [e['id'] for e in events] == [7, 8, 9]
