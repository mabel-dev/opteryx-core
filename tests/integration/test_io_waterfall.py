# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Integration tests for the execution-tracing waterfall (docs/EXECUTION_TRACING_DESIGN.md).

Session.trace() is the single trace contact surface: it returns the raw
``(blob, node_symbols, file_symbols)`` bundle for a query run with
OPTERYX_TRACE=1. It is NOT part of QueryTelemetry (bytes read, time executing,
etc. — always present; the trace bundle only exists when tracing is armed).
``opteryx.tracing.interpret_trace()`` turns the raw bundle into meaningful,
JSON-serializable span records. A prior version of both this test file and
Session.trace() covered a different, coarser mechanism (dataset/file-discovery
events keyed by session id) — that added no diagnostic value over the span
waterfall and has been removed.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../dev"))

import pytest

from io_waterfall.span_reader import SpanTraceReader

from opteryx import config
from opteryx.query_session import Session
from opteryx.tracing import interpret_trace


@pytest.mark.integration
class TestIOWaterfallIntegration:
    """Integration tests for the execution-tracing waterfall."""

    def test_trace_not_armed_raises(self):
        """Session.trace() raises when tracing was not enabled for the query,
        rather than returning an empty/misleading bundle."""
        original_trace = config.OPTERYX_TRACE
        try:
            config.OPTERYX_TRACE = False

            session = Session()
            for _ in session.execute_to_morsels("SELECT * FROM $planets"):
                pass

            with pytest.raises(RuntimeError):
                session.trace()
        finally:
            config.OPTERYX_TRACE = original_trace

    def test_trace_not_in_telemetry(self):
        """Trace data must never appear in QueryTelemetry — it is a
        different concern (an event stream that only exists when tracing is
        on) from telemetry's always-present aggregates."""
        original_trace = config.OPTERYX_TRACE
        try:
            config.OPTERYX_TRACE = True

            session = Session()
            for _ in session.execute_to_morsels("SELECT * FROM $planets"):
                pass

            reading = session._telemetry._reading
            assert "trace_spans" not in reading
            assert "trace_symbols" not in reading
            assert "trace_file_symbols" not in reading

            telemetry_dict = session.telemetry
            assert "trace_spans" not in telemetry_dict
        finally:
            config.OPTERYX_TRACE = original_trace

    def test_real_query_generates_span_waterfall(self):
        """Execute a real parquet scan with tracing on and confirm the native
        span waterfall carries IO and operator activity, correlated and
        resolvable back to plan-node identity / file path."""
        original_trace = config.OPTERYX_TRACE
        try:
            config.OPTERYX_TRACE = True

            session = Session()
            for _ in session.execute_to_morsels(
                "SELECT * FROM testdata.satellites WHERE planetId > 3"
            ):
                pass

            blob, node_symbols, file_symbols = session.trace()
            assert blob, "expected a non-empty span blob"

            # opteryx.tracing.interpret_trace() is the canonical binary ->
            # meaningful conversion — exercise it directly, not just through
            # the dev-tool reader built on top of it.
            resolved = interpret_trace(blob, node_symbols, file_symbols)
            assert resolved
            assert any(r["type"] == "op_exec" and r["operator_name"] for r in resolved)
            assert any(
                r["type"] == "decode" and r["file"] and r["file"].endswith("satellites.parquet")
                for r in resolved
            )

            reader = SpanTraceReader(blob, node_symbols, file_symbols)

            # IO waterfall rows: at least one row-group gather, resolvable to a
            # real file path and carrying decoded row/byte counts.
            operations = reader.operation_timelines()
            assert operations, "expected at least one row-group operation"
            op = operations[0]
            assert op["file_id"] and op["file_id"].endswith("satellites.parquet")
            assert op["rows_decoded"] > 0
            assert op["bytes_received"] > 0
            assert op["download_start"] is not None
            assert op["decode_start"] is not None

            # Operator execution waterfall: at least one operator span,
            # resolvable to a plan-node identity via node_symbols.
            exec_ops, _t0, total_duration = reader.exec_timelines()
            assert exec_ops, "expected at least one operator span"
            assert total_duration is not None and total_duration >= 0
            assert all(op["operator_name"] != "unknown" for op in exec_ops)

            profiles = reader.operator_profiles()
            assert profiles

            stats = reader.statistics()
            assert stats["total_files"] == 1
            assert stats["total_rows"] > 0
        finally:
            config.OPTERYX_TRACE = original_trace
