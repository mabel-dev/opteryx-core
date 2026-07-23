# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Chart-shaped views over the native execution-trace span stream
(docs/EXECUTION_TRACING_DESIGN.md).

The wire format, the "make it meaningful" interpreter, and the grouped/
chart-shaped views themselves all live in ``opteryx.tracing`` (production
code, importable by any consumer — not just this dev tool; e.g. a
job-results API endpoint needs the same row-group/operator grouping this
tool's ECharts builders do). ``SpanTraceReader`` here is a thin subclass of
``opteryx.tracing.TraceTimelines`` that adds only what's specific to this
dev tool: the .trace.json sidecar format and query-text/session-id/host-info
bookkeeping for the chart header.

Usage — after running a query with OPTERYX_TRACE=1:

    blob, node_symbols, file_symbols, host_info = session.trace()
    reader = SpanTraceReader(blob, node_symbols, file_symbols, host_info=host_info)

Or persist first and reload later — see dump_trace()/load_trace() below.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional

from opteryx.tracing import TraceTimelines


class SpanTraceReader(TraceTimelines):
    """
    Adds this dev tool's chart-header bookkeeping (query text, session id,
    host info) on top of ``opteryx.tracing.TraceTimelines``'s grouped views.
    """

    def __init__(
        self,
        blob: bytes,
        node_symbols: Optional[Dict[int, str]] = None,
        file_symbols: Optional[Dict[int, str]] = None,
        query_text: str = "",
        session_id: str = "",
        host_info: str = "",
    ):
        super().__init__(blob, node_symbols, file_symbols)
        self._query_text = query_text
        self._session_id = session_id
        self._host_info = host_info

    # ------------------------------------------------------------------
    def metadata(self) -> Dict[str, Any]:
        return {
            "query": self._query_text,
            "session_id": self._session_id,
            "host_info": self._host_info,
        }

    def events(self):
        """One resolved dict per span (opteryx.tracing.interpret_trace()),
        with a timestamp normalized to seconds relative to this trace's first
        span, and ``file_id`` aliased to the resolved path (generator.py
        groups drill-down events by ``ev.get("file_id")``)."""
        for ev in self._resolved:
            out = dict(ev)
            out["timestamp"] = self._seconds(ev["t_start_ns"])
            out["file_id"] = ev.get("file")
            yield out


# ---------------------------------------------------------------------------
# Persistence — a small JSON sidecar (NOT the old .jsonl event stream) so a
# trace captured in one process can be rendered later / elsewhere. The blob
# is base64'd; symbol tables are small dicts. This is the DEV-TOOL sidecar
# format — a production consumer that just wants to persist the trace (e.g.
# a worker service uploading it alongside a query's results) should store
# the blob as raw bytes and the symbol tables as a small JSON, not go through
# this base64 envelope; see docs/EXECUTION_TRACING_DESIGN.md.
# ---------------------------------------------------------------------------


def dump_trace(
    blob: bytes,
    node_symbols: Dict[int, str],
    file_symbols: Dict[int, str],
    path: str,
    query_text: str = "",
    session_id: str = "",
    host_info: str = "",
) -> str:
    """Persist a trace bundle (as returned by Session.trace()) to `path`
    (.trace.json). Call right after a traced query, e.g.:

        blob, node_symbols, file_symbols, host_info = session.trace()
        dump_trace(blob, node_symbols, file_symbols, "out.trace.json",
                    query_text=sql, host_info=host_info)
    """
    import base64

    payload = {
        "spans_b64": base64.b64encode(blob or b"").decode("ascii"),
        "node_symbols": node_symbols or {},
        "file_symbols": file_symbols or {},
        "query": query_text,
        "session_id": session_id,
        "host_info": host_info,
    }
    out_path = Path(path)
    out_path.write_text(json.dumps(payload), encoding="utf-8")
    return str(out_path)


def load_trace(path: str) -> SpanTraceReader:
    """Load a trace dumped by dump_trace() into a SpanTraceReader."""
    import base64

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    blob = base64.b64decode(payload.get("spans_b64", ""))
    return SpanTraceReader(
        blob,
        node_symbols=payload.get("node_symbols", {}),
        file_symbols=payload.get("file_symbols", {}),
        query_text=payload.get("query", ""),
        session_id=payload.get("session_id", ""),
        host_info=payload.get("host_info", ""),
    )
