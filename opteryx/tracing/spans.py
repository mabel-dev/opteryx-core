# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Interpreter for the native execution-trace span format.

See docs/EXECUTION_TRACING_DESIGN.md. ``Session.trace()`` hands back the RAW
payload — a packed binary span blob plus two small id->name symbol tables —
deliberately uninterpreted, so a query whose trace nobody looks at pays no
per-span Python object cost. ``interpret_trace()`` here is the one canonical
place that turns that binary payload into a meaningful, JSON-serializable
structure, for any consumer (a script, a notebook, a worker service persisting
the raw bundle now and inspecting it later) that wants to actually look.

This does NOT build chart-shaped views (grouped-by-row-group waterfalls,
operator profiles, etc.) — that is dev/io_waterfall's job, built on top of the
flat record list this module returns. This module's job stops at "meaningful",
not "visualized".
"""

from __future__ import annotations

import struct
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from urllib.parse import urlsplit
from urllib.parse import urlunsplit


def strip_signed_url_query(path: str) -> str:
    """Drop the query string from a file identity before it goes anywhere
    meant to be looked at (a rendered chart, a log line, ...).

    A GCS-backed scan's file_symbols value is often a signed URL
    (``...?X-Goog-Signature=...``) — that query string IS a live, time-boxed
    bearer credential for the object, not just cosmetic. interpret_trace()
    resolves file_id to a human-meaningful identity; a signature is neither
    human-meaningful nor something that should be echoed into a chart label
    or a stored trace. A plain local path (no ``?``) is returned unchanged.
    """
    if "?" not in path:
        return path
    parts = urlsplit(path)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

# Canonical span-category vocabulary — mirrors draken/core/trace_bridge_c.h's
# DrakenTraceCategory exactly (same integer values). That C header is the
# authoritative source; if it changes, this must change with it. There is no
# codegen link between them (the C enum is compiled into the extension, this
# is plain Python), so this is a manually-maintained mirror — check
# trace_bridge_c.h before editing this list.
TC_SOURCE_PULL = 1
TC_OP_EXEC = 2
TC_SINK = 3
TC_COMBINE = 4
TC_QUEUE_WAIT = 5
TC_IO_REQUEST = 6
TC_IO_WAIT = 7
TC_BUFFER_RESIDENT = 8
TC_DECODE = 9
TC_DECODE_PHASE = 10
TC_QUEUE_STALL = 11

CATEGORY_NAMES = {
    TC_SOURCE_PULL: "source_pull",
    TC_OP_EXEC: "op_exec",
    TC_SINK: "sink",
    TC_COMBINE: "combine",
    TC_QUEUE_WAIT: "queue_wait",
    TC_IO_REQUEST: "io_request",
    TC_IO_WAIT: "io_wait",
    TC_BUFFER_RESIDENT: "buffer_resident",
    TC_DECODE: "decode",
    TC_DECODE_PHASE: "decode_phase",
    TC_QUEUE_STALL: "queue_stall",
}

# Field order for struct.iter_unpack — kept alongside
# opteryx.operators._operators.TRACE_SPAN_STRUCT_FORMAT, the single source of
# truth for the wire format (that constant is imported below, not duplicated
# here; only the resulting field NAMES are listed, since struct doesn't carry
# them).
_SPAN_FIELDS = (
    "t_start_ns",
    "t_end_ns",
    "query_seq",
    "category",
    "worker_id",
    "node_id",
    "corr_id",
    "rg_idx",
    "rows",
    "bytes",
    "detail",
    "file_id",
    "_reserved0",
    "_reserved1",
)


def parse_spans(blob: bytes) -> List[Dict[str, Any]]:
    """Unpack the raw span blob into a list of raw field dicts — no id
    resolution, no category names. Usually you want interpret_trace() instead;
    this is exposed for callers that need the unresolved fields directly."""
    from opteryx.operators._operators import TRACE_SPAN_STRUCT_FORMAT

    if not blob:
        return []
    return [dict(zip(_SPAN_FIELDS, rec)) for rec in struct.iter_unpack(TRACE_SPAN_STRUCT_FORMAT, blob)]


def interpret_trace(
    blob: bytes,
    node_symbols: Optional[Dict[Any, str]] = None,
    file_symbols: Optional[Dict[Any, str]] = None,
) -> List[Dict[str, Any]]:
    """Turn a raw (blob, node_symbols, file_symbols) trace bundle — see
    opteryx.models.trace_bundle.TraceBundle / Session.trace() — into a flat
    list of resolved, JSON-serializable span dicts: one per recorded span,
    with category as a name, node_id/file_id resolved to strings where known,
    and timestamps still in nanoseconds (the caller picks its own time origin
    to normalize against — this function does not assume one).
    """
    node_symbols = {int(k): v for k, v in (node_symbols or {}).items()}
    file_symbols = {int(k): v for k, v in (file_symbols or {}).items()}

    out: List[Dict[str, Any]] = []
    for s in parse_spans(blob):
        node_id = s["node_id"] or None
        file_id = s["file_id"] or None
        rg_idx = s["rg_idx"] if s["rg_idx"] != 0xFFFFFFFF else None
        out.append(
            {
                "type": CATEGORY_NAMES.get(s["category"], str(s["category"])),
                "t_start_ns": s["t_start_ns"],
                "t_end_ns": s["t_end_ns"],
                "duration_ns": s["t_end_ns"] - s["t_start_ns"],
                "worker_id": s["worker_id"],
                "node_id": node_id,
                "operator_name": node_symbols.get(node_id) if node_id else None,
                "corr_id": s["corr_id"] or None,
                "rg_idx": rg_idx,
                "rows": s["rows"],
                "bytes": s["bytes"],
                "detail": s["detail"],
                "file_id": file_id,
                "file": strip_signed_url_query(file_symbols[file_id]) if file_id and file_id in file_symbols else None,
            }
        )
    return out
