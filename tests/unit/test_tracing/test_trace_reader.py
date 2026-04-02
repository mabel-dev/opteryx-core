# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Tests for trace file reading and parsing."""

import json
import tempfile
from pathlib import Path

import pytest
from io_waterfall.reader import TraceReader


class TestTraceReader:
    """Test trace file reading and parsing."""

    def create_trace_file(self, events):
        """Helper to create a temporary trace file with given events."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            for event in events:
                f.write(json.dumps(event) + "\n")
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
                "query": "SELECT * FROM data",
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
            {
                "type": "file_discovered",
                "timestamp": 100,
                "file_id": "file1.parquet",
                "bytes_total": 1000,
            },
            {"type": "download_start", "timestamp": 101, "file_id": "file1.parquet"},
            {
                "type": "download_complete",
                "timestamp": 150,
                "file_id": "file1.parquet",
                "bytes_received": 1000,
            },
            {"type": "decode_start", "timestamp": 151, "file_id": "file1.parquet"},
            {
                "type": "decode_complete",
                "timestamp": 200,
                "file_id": "file1.parquet",
                "rows_decoded": 100,
            },
            {
                "type": "file_discovered",
                "timestamp": 102,
                "file_id": "file2.parquet",
                "bytes_total": 2000,
            },
            {"type": "download_start", "timestamp": 110, "file_id": "file2.parquet"},
            {
                "type": "download_complete",
                "timestamp": 160,
                "file_id": "file2.parquet",
                "bytes_received": 2000,
            },
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

    def test_operation_timelines_include_footer_and_rowgroup(self):
        """Operation timelines should preserve footer and rowgroup activity."""
        events = [
            {"type": "trace_session_start", "timestamp": 0},
            {"type": "file_discovered", "timestamp": 1, "file_id": "file1.parquet"},
            {
                "type": "download_start",
                "timestamp": 2,
                "file_id": "file1.parquet",
                "component": "footer",
            },
            {
                "type": "download_complete",
                "timestamp": 3,
                "file_id": "file1.parquet",
                "component": "footer",
                "bytes_received": 64,
            },
            {
                "type": "download_start",
                "timestamp": 4,
                "file_id": "file1.parquet",
                "component": "columns",
                "rg_idx": 0,
            },
            {
                "type": "download_complete",
                "timestamp": 6,
                "file_id": "file1.parquet",
                "component": "columns",
                "rg_idx": 0,
                "bytes_received": 1024,
            },
            {
                "type": "decode_start",
                "timestamp": 6.1,
                "file_id": "file1.parquet",
                "component": "rowgroup",
                "rg_idx": 0,
            },
            {
                "type": "decode_complete",
                "timestamp": 7,
                "file_id": "file1.parquet",
                "component": "rowgroup",
                "rg_idx": 0,
                "rows_decoded": 12,
            },
        ]
        trace_file = self.create_trace_file(events)
        try:
            reader = TraceReader(trace_file)
            operations = reader.operation_timelines()
            assert len(operations) == 2

            footer = [row for row in operations if row["component"] == "footer"]
            rowgroup = [row for row in operations if row["component"] == "rowgroup"]

            assert len(footer) == 1
            assert footer[0]["download_start"] == 2
            assert footer[0]["download_complete"] == 3

            assert len(rowgroup) == 1
            assert rowgroup[0]["download_start"] == 4
            assert rowgroup[0]["download_complete"] == 6
            assert rowgroup[0]["decode_start"] == 6.1
            assert rowgroup[0]["decode_complete"] == 7
            assert rowgroup[0]["rows_decoded"] == 12
        finally:
            trace_file.unlink()

    def test_statistics_include_component_ops(self):
        """Stats should include component-level operation counts and bytes."""
        events = [
            {"type": "trace_session_start", "timestamp": 0},
            {"type": "file_discovered", "timestamp": 1, "file_id": "file1.parquet"},
            {
                "type": "download_start",
                "timestamp": 2,
                "file_id": "file1.parquet",
                "component": "footer",
            },
            {
                "type": "download_complete",
                "timestamp": 2.5,
                "file_id": "file1.parquet",
                "component": "footer",
                "bytes_received": 64,
            },
            {
                "type": "download_start",
                "timestamp": 3,
                "file_id": "file1.parquet",
                "component": "columns",
                "rg_idx": 0,
            },
            {
                "type": "download_complete",
                "timestamp": 4,
                "file_id": "file1.parquet",
                "component": "columns",
                "rg_idx": 0,
                "bytes_received": 512,
            },
            {
                "type": "decode_start",
                "timestamp": 4.1,
                "file_id": "file1.parquet",
                "component": "rowgroup",
                "rg_idx": 0,
            },
            {
                "type": "decode_complete",
                "timestamp": 5,
                "file_id": "file1.parquet",
                "component": "rowgroup",
                "rg_idx": 0,
                "rows_decoded": 7,
            },
        ]
        trace_file = self.create_trace_file(events)
        try:
            reader = TraceReader(trace_file)
            stats = reader.statistics()
            assert stats["total_files"] == 1
            assert stats["total_bytes"] == 576
            assert stats["total_rows"] == 7
            assert stats["footer_download_ops"] == 1
            assert stats["rowgroup_download_ops"] == 1
            assert stats["rowgroup_decode_ops"] == 1
            assert stats["download_ops_by_component"]["footer"] == 1
            assert stats["download_ops_by_component"]["rowgroup"] == 1
            assert stats["download_bytes_by_component"]["footer"] == 64
            assert stats["download_bytes_by_component"]["rowgroup"] == 512
        finally:
            trace_file.unlink()

    def test_malformed_line_strict(self):
        """Test strict mode with malformed JSON."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"type": "valid"}\n')
            f.write("not valid json\n")
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
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"type": "valid", "id": 1}\n')
            f.write("not valid json\n")
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
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            trace_file = Path(f.name)

    def test_component_and_rowgroup_helpers(self):
        """Verify the new helper methods filter by component and row group."""
        events = [
            {"type": "download_start", "timestamp": 1, "file_id": "a", "component": "footer"},
            {"type": "download_complete", "timestamp": 2, "file_id": "a", "component": "footer"},
            {
                "type": "decode_start",
                "timestamp": 3,
                "file_id": "a",
                "component": "rowgroup",
                "rg_idx": 0,
            },
            {
                "type": "decode_complete",
                "timestamp": 4,
                "file_id": "a",
                "component": "rowgroup",
                "rg_idx": 0,
            },
            {
                "type": "decode_start",
                "timestamp": 5,
                "file_id": "a",
                "component": "column",
                "rg_idx": 0,
                "column": "x",
            },
            {
                "type": "decode_complete",
                "timestamp": 6,
                "file_id": "a",
                "component": "column",
                "rg_idx": 0,
                "column": "x",
            },
        ]
        trace_file = self.create_trace_file(events)
        try:
            reader = TraceReader(trace_file)
            assert len(reader.events_by_component("footer")) == 2
            assert len(reader.events_by_component("rowgroup")) == 2
            assert len(reader.events_by_component("column")) == 2
            assert len(reader.events_for_row_group("a", 0)) == 4
        finally:
            trace_file.unlink()

    def test_sampling_behavior(self):
        """Verify that OPTERYX_TRACE_SAMPLE_RATE controls event recording."""
        from opteryx import config

        original_rate = config.OPTERYX_TRACE_SAMPLE_RATE
        original_trace = config.OPTERYX_TRACE
        try:
            config.OPTERYX_TRACE = True
            config.OPTERYX_TRACE_SAMPLE_RATE = 0.0
            from opteryx.tracing.event_recorder import _global_events, flush_all, record_event

            _global_events.clear()

            record_event("download_start", file_id="foo")
            record_event("download_complete", file_id="foo")
            flush_all()
            # with 0% sampling we expect the global list to contain only session or
            # other non-file events
            assert all(e.get("file_id") is None for e in _global_events)
        finally:
            config.OPTERYX_TRACE_SAMPLE_RATE = original_rate
            config.OPTERYX_TRACE = original_trace

    def test_filesystem_table_connector_tag(self):
        """Create a FileSystemTable and confirm traced events include connector."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from opteryx import config
        from opteryx.connectors.filesystem_connector import FileSystemTable
        from opteryx.tracing.event_recorder import flush_all

        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = Path(tmpdir) / "data.parquet"
            # make a tiny parquet file
            import pandas as pd

            df = pa.Table.from_pandas(pd.DataFrame({"a": [1, 2, 3]}))
            pq.write_table(df, parquet_path)

            original_trace = config.OPTERYX_TRACE
            try:
                config.OPTERYX_TRACE = True
                from opteryx.tracing.event_recorder import _global_events, flush_all

                _global_events.clear()
                # create table and read
                fs = pa.fs.LocalFileSystem()
                table = FileSystemTable(
                    dataset=str(parquet_path), filesystem=fs, storage_type="LOCAL"
                )
                for _ in table.read_dataset():
                    pass
                flush_all()
                assert any(e.get("connector") == "LOCAL" for e in _global_events)
            finally:
                config.OPTERYX_TRACE = original_trace

    def test_missing_file(self):
        """Test error handling for missing trace file."""
        missing_file = Path("/tmp/nonexistent_trace_xyz.jsonl")

        reader = TraceReader(missing_file)

        with pytest.raises(FileNotFoundError):
            list(reader.events())

    def test_session_trace_method(self):
        """Session.trace() should iterate over the same events written to file."""
        from opteryx import config, session

        original_trace = config.OPTERYX_TRACE
        try:
            config.OPTERYX_TRACE = True
            from opteryx.tracing.event_recorder import _global_events, flush_all

            _global_events.clear()

            sess = session()
            sess.execute("SELECT 1")
            sess.close()

            events = list(sess.trace())
            assert any(e.get("type") == "trace_session_start" for e in events)
        finally:
            config.OPTERYX_TRACE = original_trace
