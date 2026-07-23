# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Execution Waterfall Visualization Tools

Reads and visualizes the native execution-tracing span stream
(docs/EXECUTION_TRACING_DESIGN.md). Includes:

- SpanTraceReader / dump_trace / load_trace: read the span blob + symbol
  tables (from QueryTelemetry, or a persisted .trace.json file)
- generate_waterfall_html: generate an interactive HTML waterfall chart
- CLI interface: command-line tool for trace analysis

Usage:
    # After running a query with OPTERYX_TRACE=1:
    from io_waterfall.span_reader import dump_trace
    blob, node_symbols, file_symbols, host_info, truncated = session.trace()
    dump_trace(blob, node_symbols, file_symbols, "q.trace.json", query_text=sql,
               host_info=host_info, truncated=truncated)

    # Generate chart from the dumped trace
    PYTHONPATH=dev python -m io_waterfall trace q.trace.json

    # View statistics
    PYTHONPATH=dev python -m io_waterfall stats q.trace.json

    # Programmatic access, no file round-trip
    from io_waterfall import SpanTraceReader, generate_waterfall_html_from_reader
    blob, node_symbols, file_symbols, host_info, truncated = session.trace()
    reader = SpanTraceReader(blob, node_symbols, file_symbols, host_info=host_info, truncated=truncated)
    html_path = generate_waterfall_html_from_reader(reader, "q.html")
"""

from .generator import generate_waterfall_html
from .generator import generate_waterfall_html_from_reader
from .span_reader import SpanTraceReader
from .span_reader import dump_trace
from .span_reader import load_trace

__all__ = [
    "SpanTraceReader",
    "dump_trace",
    "load_trace",
    "generate_waterfall_html",
    "generate_waterfall_html_from_reader",
]
