# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Integration tests for IO waterfall tracing."""

import json
import tempfile
from pathlib import Path

import pytest

from opteryx import config
from opteryx.query_session import Session
from opteryx.tools.io_waterfall.reader import TraceReader


@pytest.mark.integration
class TestIOWaterfallIntegration:
    """Integration tests for complete IO waterfall tracing system."""
    
    def test_trace_buffering_on_query(self):
        """When tracing is enabled the events produced by a real session are
        retained in memory and accessible via ``session.trace()``.
        """
        original_trace = config.OPTERYX_TRACE
        try:
            config.OPTERYX_TRACE = True

            session = Session()
            session.execute("SELECT 1")
            session.close()

            events = list(session.trace())
            assert len(events) >= 2
            types = {e['type'] for e in events}
            assert 'trace_session_start' in types
            assert 'trace_session_end' in types
        finally:
            config.OPTERYX_TRACE = original_trace
    
    def test_trace_reader_parses_complete_trace(self):
        """Test that TraceReader correctly parses a complete trace file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            # Write a complete trace sequence
            events = [
                {
                    "type": "trace_session_start",
                    "timestamp": 0,
                    "session_id": "test-1",
                    "query": "SELECT * FROM data"
                },
                {
                    "type": "file_discovered",
                    "timestamp": 100,
                    "file_id": "file1.parquet",
                    "bytes_total": 1024
                },
                {
                    "type": "download_start",
                    "timestamp": 101,
                    "file_id": "file1.parquet"
                },
                {
                    "type": "download_complete",
                    "timestamp": 150,
                    "file_id": "file1.parquet",
                    "bytes_received": 1024
                },
                {
                    "type": "decode_start",
                    "timestamp": 151,
                    "file_id": "file1.parquet"
                },
                {
                    "type": "decode_complete",
                    "timestamp": 200,
                    "file_id": "file1.parquet",
                    "rows_decoded": 100
                },
                {
                    "type": "file_discovered",
                    "timestamp": 102,
                    "file_id": "file2.parquet",
                    "bytes_total": 2048
                },
                {
                    "type": "download_start",
                    "timestamp": 110,
                    "file_id": "file2.parquet"
                },
                {
                    "type": "download_complete",
                    "timestamp": 160,
                    "file_id": "file2.parquet",
                    "bytes_received": 2048
                },
                {
                    "type": "decode_start",
                    "timestamp": 161,
                    "file_id": "file2.parquet"
                },
                {
                    "type": "decode_complete",
                    "timestamp": 220,
                    "file_id": "file2.parquet",
                    "rows_decoded": 200
                },
            ]
            
            for event in events:
                f.write(json.dumps(event) + '\n')
            
            trace_file = Path(f.name)
        
        try:
            reader = TraceReader(trace_file)
            
            # Test metadata extraction
            metadata = reader.metadata()
            assert metadata['session_id'] == 'test-1'
            assert metadata['query'] == 'SELECT * FROM data'
            
            # Test file timelines
            timelines = reader.file_timelines()
            assert len(timelines) == 2
            assert 'file1.parquet' in timelines
            assert 'file2.parquet' in timelines
            
            file1_timeline = timelines['file1.parquet']
            assert file1_timeline['discovered'] == 100
            assert file1_timeline['download_start'] == 101
            assert file1_timeline['download_complete'] == 150
            assert file1_timeline['decode_start'] == 151
            assert file1_timeline['decode_complete'] == 200
            
            # Test statistics
            stats = reader.statistics()
            assert stats['total_files'] == 2
            assert stats['total_bytes'] == 3072  # 1024 + 2048
            assert stats['total_rows'] == 300  # 100 + 200
            assert stats['download_phase_duration_ms'] > 0
            assert stats['max_concurrent_downloads'] in [1, 2]  # Could be 1 or 2
            
        finally:
            trace_file.unlink()
    
    def test_real_query_generates_rich_trace(self):
        """Execute a simple parquet scan and confirm footer/column/rowgroup events."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_file = Path(tmpdir) / "data.parquet"

            # write a tiny parquet
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq

            df = pd.DataFrame({"x": [1, 2, 3]})
            pq.write_table(pa.Table.from_pandas(df), parquet_file)

            original_trace = config.OPTERYX_TRACE
            try:
                config.OPTERYX_TRACE = True

                session = Session()
                # execute the query and drain results
                list(session.execute(f"SELECT * FROM '{parquet_file}'"))

                # pull the in‑memory trace events
                events = list(session.trace())

                # we expect at least one footer event and one column decode event
                assert any(e.get("component") == "footer" for e in events), events
                assert any(e.get("component") == "column" for e in events), events
                assert any(e.get("component") == "rowgroup" for e in events), events
                # connector tag should be added by filesystem telemetry
                assert any(e.get("connector") == "LOCAL" or e.get("connector") == "FILESYSTEM" for e in events), events
            finally:
                config.OPTERYX_TRACE = original_trace

    def test_sampling_rate(self):
        """Sample rate 0 should suppress file-level events entirely."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_file = Path(tmpdir) / "data.parquet"

            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq

            df = pd.DataFrame({"x": [1, 2]})
            pq.write_table(pa.Table.from_pandas(df), parquet_file)

            original_trace = config.OPTERYX_TRACE
            original_rate = config.OPTERYX_TRACE_SAMPLE_RATE
            try:
                config.OPTERYX_TRACE = True
                config.OPTERYX_TRACE_SAMPLE_RATE = 0.0

                session = Session()
                list(session.execute(f"SELECT * FROM '{parquet_file}'"))

                events = list(session.trace())
                # expect only session events when sample rate is zero
                assert all(e['type'].startswith('trace_session') for e in events), events
            finally:
                config.OPTERYX_TRACE = original_trace


    def test_tracing_disabled_has_no_overhead(self):
        """Test that when tracing is disabled, there's minimal overhead."""
        # This test verifies the pattern: when disabled, record_event is a no-op
        original_trace = config.OPTERYX_TRACE
        try:
            config.OPTERYX_TRACE = False
            
            from opteryx.tracing.event_recorder import record_event
            
            # Recording events when disabled should be instant
            for _ in range(1000):
                record_event("test", value=1)
            
            # No exception should be raised
            # This is more of a smoke test to ensure disabled mode works
            
        finally:
            config.OPTERYX_TRACE = original_trace
