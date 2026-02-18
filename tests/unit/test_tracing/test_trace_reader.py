# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Tests for trace file reading and parsing."""

import json
import tempfile
from pathlib import Path

import pytest

from opteryx.tools.io_waterfall.reader import TraceReader


class TestTraceReader:
    """Test trace file reading and parsing."""
    
    def create_trace_file(self, events):
        """Helper to create a temporary trace file with given events."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            for event in events:
                f.write(json.dumps(event) + '\n')
            return Path(f.name)
    
    def test_read_events(self):
        """Test reading events from trace file."""
        events = [
            {"type": "trace_session_start", "timestamp": 1000, "session_id": "test-1"},
            {"type": "file_discovered", "timestamp": 1001, "file_id": "file1.parquet"},
            {"type": "download_start", "timestamp": 1002, "file_id": "file1.parquet"},
        ]
        
        trace_file = self.create_trace_file(events)
        try:
            reader = TraceReader(trace_file)
            read_events = list(reader.events())
            
            assert len(read_events) == 3
            assert read_events[0]["type"] == "trace_session_start"
            assert read_events[1]["type"] == "file_discovered"
            assert read_events[2]["type"] == "download_start"
        finally:
            trace_file.unlink()
    
    def test_read_metadata(self):
        """Test metadata extraction from session start."""
        events = [
            {
                "type": "trace_session_start",
                "timestamp": 1000,
                "session_id": "test-1",
                "query": "SELECT * FROM data"
            },
        ]
        
        trace_file = self.create_trace_file(events)
        try:
            reader = TraceReader(trace_file)
            metadata = reader.metadata()
            
            assert metadata["session_id"] == "test-1"
            assert metadata["query"] == "SELECT * FROM data"
        finally:
            trace_file.unlink()
    
    def test_file_timelines(self):
        """Test organizing events into file timelines."""
        events = [
            {"type": "trace_session_start", "timestamp": 0},
            {"type": "file_discovered", "timestamp": 100, "file_id": "file1.parquet"},
            {"type": "download_start", "timestamp": 101, "file_id": "file1.parquet"},
            {"type": "download_complete", "timestamp": 150, "file_id": "file1.parquet"},
            {"type": "decode_start", "timestamp": 151, "file_id": "file1.parquet"},
            {"type": "decode_complete", "timestamp": 200, "file_id": "file1.parquet"},
        ]
        
        trace_file = self.create_trace_file(events)
        try:
            reader = TraceReader(trace_file)
            timelines = reader.file_timelines()
            
            assert "file1.parquet" in timelines
            timeline = timelines["file1.parquet"]
            
            assert timeline["discovered"] == 100
            assert timeline["download_start"] == 101
            assert timeline["download_complete"] == 150
            assert timeline["decode_start"] == 151
            assert timeline["decode_complete"] == 200
        finally:
            trace_file.unlink()
    
    def test_statistics(self):
        """Test statistics computation."""
        events = [
            {"type": "trace_session_start", "timestamp": 0},
            {"type": "file_discovered", "timestamp": 100, "file_id": "file1.parquet", "bytes_total": 1000},
            {"type": "download_start", "timestamp": 101, "file_id": "file1.parquet"},
            {"type": "download_complete", "timestamp": 150, "file_id": "file1.parquet", "bytes_received": 1000},
            {"type": "decode_start", "timestamp": 151, "file_id": "file1.parquet"},
            {"type": "decode_complete", "timestamp": 200, "file_id": "file1.parquet", "rows_decoded": 100},
            {"type": "file_discovered", "timestamp": 102, "file_id": "file2.parquet", "bytes_total": 2000},
            {"type": "download_start", "timestamp": 110, "file_id": "file2.parquet"},
            {"type": "download_complete", "timestamp": 160, "file_id": "file2.parquet", "bytes_received": 2000},
        ]
        
        trace_file = self.create_trace_file(events)
        try:
            reader = TraceReader(trace_file)
            stats = reader.statistics()
            
            assert stats["total_files"] == 2
            assert stats["total_bytes"] >= 2000  # At least bytes from file2
            assert "download_phase_duration_ms" in stats
            assert "max_concurrent_downloads" in stats
        finally:
            trace_file.unlink()
    
    def test_malformed_line_strict(self):
        """Test strict mode with malformed JSON."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"type": "valid"}\n')
            f.write('not valid json\n')
            f.write('{"type": "valid2"}\n')
            trace_file = Path(f.name)
        
        try:
            reader = TraceReader(trace_file, strict=True)
            
            with pytest.raises(ValueError):
                list(reader.events())
        finally:
            trace_file.unlink()
    
    def test_malformed_line_lenient(self):
        """Test lenient mode with malformed JSON."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"type": "valid", "id": 1}\n')
            f.write('not valid json\n')
            f.write('{"type": "valid2", "id": 2}\n')
            trace_file = Path(f.name)
        
        try:
            reader = TraceReader(trace_file, strict=False)
            events = list(reader.events())
            
            # Should skip malformed line
            assert len(events) == 2
            assert events[0]["id"] == 1
            assert events[1]["id"] == 2
        finally:
            trace_file.unlink()
    
    def test_empty_file(self):
        """Test reading an empty trace file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            trace_file = Path(f.name)
        
        try:
            reader = TraceReader(trace_file)
            events = list(reader.events())
            
            assert events == []
        finally:
            trace_file.unlink()
    
    def test_missing_file(self):
        """Test error handling for missing trace file."""
        missing_file = Path("/tmp/nonexistent_trace_xyz.jsonl")
        
        reader = TraceReader(missing_file)
        
        with pytest.raises(FileNotFoundError):
            list(reader.events())
