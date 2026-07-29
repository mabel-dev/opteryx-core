# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Grouped, chart-shaped views over a native execution trace — the "meaningful
spans -> waterfall-ready rows" layer on top of ``opteryx.tracing.spans``'s
"binary -> meaningful" layer.

This lives in ``opteryx`` (packaged, importable by any consumer) rather than
in ``dev/io_waterfall`` because more than the dev tool needs it: a service
that wants to serve a trace's waterfall over an API (e.g. a job-results
endpoint) needs the same row-group/operator grouping the local HTML chart
renderer does, and that grouping logic must not be duplicated between the two
— see docs/EXECUTION_TRACING_DESIGN.md. dev/io_waterfall.SpanTraceReader
subclasses TraceTimelines and adds only dev-tool-specific concerns (the
.trace.json sidecar format, query-text/session-id bookkeeping for the chart
header).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from opteryx.tracing.spans import TC_DECODE
from opteryx.tracing.spans import TC_IO_REQUEST
from opteryx.tracing.spans import TC_IO_WAIT
from opteryx.tracing.spans import TC_OP_EXEC
from opteryx.tracing.spans import TC_QUEUE_STALL
from opteryx.tracing.spans import TC_QUEUE_WAIT
from opteryx.tracing.spans import TC_SINK
from opteryx.tracing.spans import TC_SOURCE_PULL
from opteryx.tracing.spans import interpret_trace
from opteryx.tracing.spans import parse_spans
from opteryx.tracing.spans import strip_signed_url_query


class TraceTimelines:
    """
    Reads a raw (blob, node_symbols, file_symbols) trace bundle — see
    ``opteryx.models.trace_bundle.TraceBundle`` / ``Session.trace()`` — and
    exposes grouped, JSON-serializable views: one row per row-group gather
    (``operation_timelines``), one row per pipeline-stage span
    (``exec_timelines``), per-node aggregates (``operator_profiles``), and
    summary counters (``statistics``).

    Spans arrive already start/end-paired (one record per unit of work, not a
    start event and a separate complete event to match up) — so this is
    mostly grouping and unit conversion, not an event-pairing state machine.
    """

    def __init__(
        self,
        blob: bytes,
        node_symbols: Optional[Dict[int, str]] = None,
        file_symbols: Optional[Dict[int, str]] = None,
    ):
        self.spans = parse_spans(blob or b"")
        self.node_symbols = {int(k): v for k, v in (node_symbols or {}).items()}
        self.file_symbols = {int(k): v for k, v in (file_symbols or {}).items()}
        self._t0_ns = min((s["t_start_ns"] for s in self.spans), default=0)
        # Resolved once here (not duplicated field-by-field) — a subclass's
        # events()-style helpers can build on top of this without re-parsing.
        self._resolved = interpret_trace(blob or b"", self.node_symbols, self.file_symbols)

    # ------------------------------------------------------------------
    def _seconds(self, ns: int) -> float:
        return (ns - self._t0_ns) / 1e9

    # ------------------------------------------------------------------
    def operation_timelines(self) -> List[Dict[str, Any]]:
        """One row per row-group gather (grouped by corr_id): queue-wait,
        download (TC_IO_REQUEST), and decode (TC_DECODE) phases. The
        "buffer" lane — free in the old event vocabulary — carries
        TC_QUEUE_WAIT here (a real, measured gap; the old "buffer" concept,
        bytes-arrived-but-not-yet-decoding, isn't emitted — see
        docs/EXECUTION_TRACING_DESIGN.md §9).
        """
        groups: Dict[int, Dict[str, Any]] = {}
        for s in self.spans:
            corr_id = s["corr_id"]
            if corr_id == 0 or s["category"] not in (TC_QUEUE_WAIT, TC_IO_REQUEST, TC_DECODE):
                continue
            row = groups.get(corr_id)
            if row is None:
                path = self.file_symbols.get(s["file_id"])
                # Strip a signed URL's query string (a live, time-boxed bearer
                # credential — see opteryx.tracing.spans.strip_signed_url_query) before
                # it becomes a chart label or a stored "file_id" field.
                path = strip_signed_url_query(path) if path else path
                base = Path(path).name if path else f"corr {corr_id}"
                rg_idx = s["rg_idx"] if s["rg_idx"] != 0xFFFFFFFF else None
                row = {
                    "id": corr_id,
                    "file_id": path,
                    "rg_idx": rg_idx,
                    "component": "rowgroup",
                    "label": f"{base} [rg {rg_idx}]" if rg_idx is not None else base,
                    "download_start": None,
                    "download_complete": None,
                    "buffer_start": None,
                    "buffer_complete": None,
                    "decode_start": None,
                    "decode_complete": None,
                    "bytes_received": 0,
                    "rows_decoded": 0,
                }
                groups[corr_id] = row

            start_s, end_s = self._seconds(s["t_start_ns"]), self._seconds(s["t_end_ns"])
            if s["category"] == TC_QUEUE_WAIT:
                row["buffer_start"], row["buffer_complete"] = start_s, end_s
            elif s["category"] == TC_IO_REQUEST:
                row["download_start"], row["download_complete"] = start_s, end_s
                row["bytes_received"] += int(s["bytes"])
            elif s["category"] == TC_DECODE:
                row["decode_start"], row["decode_complete"] = start_s, end_s
                row["rows_decoded"] += int(s["rows"])

        # Stable base order (by corr_id, i.e. gather-issue order) — NOT a
        # display order. A caller wanting queue/download/decode display
        # orderings computes them as permutations over this base list, so
        # every mode maps consistently back to the same rows.
        rows = list(groups.values())
        rows.sort(key=lambda r: r["id"])
        return rows

    # ------------------------------------------------------------------
    # Pipeline-stage roles this waterfall covers — every stage a morsel can
    # pass through, not just the Operator role. A plan with no Operator-role
    # nodes (e.g. a scan with its predicate baked in, feeding straight into a
    # Sort/TopN sink — see docs/EXECUTION_TRACING_DESIGN.md's "operator
    # waterfall goes blank" gap) still has Source/Sink activity to show.
    # TC_IO_WAIT ("io_wait") is a sub-span nested inside TC_SOURCE_PULL — the
    # portion of one get_morsel() call spent blocked in
    # ParquetIOPipeline::wait_and_get_result(), as distinct from the
    # column-materialization work around it (native_parquet_scan_source.hpp).
    # Rendered as its own row here so a stall can be pinned to "waiting on the
    # pipeline" vs. "everything else in the source pull" instead of being one
    # opaque TC_SOURCE_PULL duration.
    _EXEC_CATEGORIES = {
        TC_SOURCE_PULL: "source",
        TC_OP_EXEC: "operator",
        TC_SINK: "sink",
        TC_IO_WAIT: "io_wait",
    }

    def exec_timelines(self) -> tuple:
        """(ops, t0, total_duration) for the pipeline-stage execution
        waterfall. One row per TC_SOURCE_PULL/TC_OP_EXEC/TC_SINK span
        (already start/end-paired — no phase="start"/"end" matching
        needed). ``worker_id`` is the executing worker's index (stamped at
        every trace_begin() call site in executor.hpp) — a caller wanting a
        by-thread swimlane view (grouping spans by which worker ran them,
        rather than by which plan node) groups on this instead of
        operator_id. It's a small, config-bounded number (MAX_EXECUTION_WORKERS),
        unlike operator_id/node_id which can be in the hundreds for a large plan."""
        ops: List[Dict[str, Any]] = []
        t_max_ns = self._t0_ns
        for s in self.spans:
            role = self._EXEC_CATEGORIES.get(s["category"])
            if role is None:
                continue
            t_max_ns = max(t_max_ns, s["t_end_ns"])
            name = self.node_symbols.get(s["node_id"], "unknown")
            ops.append(
                {
                    "operator_id": str(s["node_id"]),
                    "operator_name": f"{name} [{role}]",
                    "worker_id": s["worker_id"],
                    "wall_start": self._seconds(s["t_start_ns"]),
                    "wall_end": self._seconds(s["t_end_ns"]),
                    "rows_out": s["rows"],
                    "duration_ns": s["t_end_ns"] - s["t_start_ns"],
                    "produced_rows": s["rows"] > 0,
                }
            )
        ops.sort(key=lambda row: (row["wall_start"], row["operator_name"]))
        total_duration = self._seconds(t_max_ns) if ops else None
        return ops, 0.0, total_duration

    def operator_profiles(self) -> List[Dict[str, Any]]:
        """Per-node_id aggregated stats, across all three pipeline-stage
        roles (source/operator/sink — see exec_timelines). rows_in is not
        tracked at span granularity (only rows OUT is recorded) — left at 0,
        which the caller already treats as "selectivity unknown"."""
        agg: Dict[int, Dict[str, Any]] = {}
        order: List[int] = []
        for s in self.spans:
            role = self._EXEC_CATEGORIES.get(s["category"])
            if role is None:
                continue
            nid = s["node_id"]
            if nid not in agg:
                agg[nid] = {
                    "role": None,
                    "total_duration_ns": 0,
                    "total_rows_in": 0,
                    "total_rows_out": 0,
                    "io_wait_ns": 0,
                    "call_count": 0,
                    "producing_calls": 0,
                }
                order.append(nid)
            # Spans are recorded at trace_end, so a NESTED io_wait lands before
            # the source pull enclosing it — taking the role from whichever span
            # arrived first would label the scan "[io_wait]". Only a real
            # pipeline stage names the node.
            if role != "io_wait" and agg[nid]["role"] is None:
                agg[nid]["role"] = role
            a = agg[nid]
            # io_wait is a sub-span NESTED inside this same node's TC_SOURCE_PULL
            # (see _EXEC_CATEGORIES), not a sibling stage — `agg` is keyed on
            # node_id alone, so folding it into total_duration_ns/call_count the
            # way a real role is folded would report a scan as having run for
            # source_pull + the wait already contained within it. Recorded on its
            # own axis instead: total_duration_ns stays the node's true wall
            # time, and io_wait_ns says how much of that was spent blocked, so
            # "this operator looks busy but was 89% waiting on the network" is
            # answerable from one row without joining anything.
            if role == "io_wait":
                a["io_wait_ns"] += s["t_end_ns"] - s["t_start_ns"]
                continue
            a["total_duration_ns"] += s["t_end_ns"] - s["t_start_ns"]
            a["total_rows_out"] += s["rows"]
            a["call_count"] += 1
            if s["rows"] > 0:
                a["producing_calls"] += 1

        result = []
        for nid in order:
            a = agg[nid]
            name = self.node_symbols.get(nid, "unknown")
            result.append(
                {
                    "operator_id": str(nid),
                    # io_wait only ever nests inside a source pull, so a node
                    # carrying nothing but io_wait spans is a source whose
                    # enclosing span was lost to arena truncation.
                    "operator_name": f"{name} [{a['role'] or 'source'}]",
                    "total_duration_ns": a["total_duration_ns"],
                    "total_rows_in": a["total_rows_in"],
                    "total_rows_out": a["total_rows_out"],
                    "io_wait_ns": a["io_wait_ns"],
                    "call_count": a["call_count"],
                    "producing_calls": a["producing_calls"],
                    "selectivity": None,
                }
            )
        return result

    # ------------------------------------------------------------------
    @staticmethod
    def _max_concurrent(spans: List[Dict[str, Any]]) -> int:
        """Sweep-line peak overlap count over a set of spans' [start, end)
        intervals — how many were simultaneously in flight at any instant.
        Works for any phase (queue-wait, download, decode); the caller picks
        which spans to pass in."""
        if not spans:
            return 0
        points = []
        for s in spans:
            points.append((s["t_start_ns"], 1))
            points.append((s["t_end_ns"], -1))
        # Ends applied before starts at the same timestamp — a span closing
        # and another opening at the identical instant aren't "overlapping".
        points.sort(key=lambda p: (p[0], p[1]))
        current = peak = 0
        for _, delta in points:
            current += delta
            peak = max(peak, current)
        return peak

    def statistics(self) -> Dict[str, Any]:
        queue_spans = [s for s in self.spans if s["category"] == TC_QUEUE_WAIT]
        io_spans = [s for s in self.spans if s["category"] == TC_IO_REQUEST]
        decode_spans = [s for s in self.spans if s["category"] == TC_DECODE]
        # TC_QUEUE_STALL: the consumer found rugo's pending_items_ AND
        # result_queue_ both empty and genuinely blocked — distinct from
        # queue_spans above (an item sitting claimable, not yet picked up).
        # See DrakenTraceCategory in draken/core/trace_bridge_c.h.
        stall_spans = [s for s in self.spans if s["category"] == TC_QUEUE_STALL]

        if not self.spans:
            return {
                "total_files": 0,
                "total_bytes": 0,
                "total_rows": 0,
                "total_operations": 0,
                "total_download_ops": 0,
                "total_decode_ops": 0,
                "footer_download_ops": 0,
                "rowgroup_download_ops": 0,
                "rowgroup_decode_ops": 0,
                "download_phase_duration_ms": 0,
                "decode_phase_duration_ms": 0,
                "query_duration_ms": 0,
                "max_concurrent_queued": 0,
                "max_concurrent_downloads": 0,
                "max_concurrent_decodes": 0,
                "avg_download_time_ms": 0,
                "avg_decode_time_ms": 0,
                "queue_stall_events": 0,
                "queue_stall_total_ms": 0,
                "avg_queue_stall_ms": 0,
                "max_queue_stall_ms": 0,
            }

        def phase_duration_ms(spans):
            if not spans:
                return 0
            start = min(s["t_start_ns"] for s in spans)
            end = max(s["t_end_ns"] for s in spans)
            return (end - start) / 1e6

        def avg_duration_ms(spans):
            if not spans:
                return 0
            return sum(s["t_end_ns"] - s["t_start_ns"] for s in spans) / len(spans) / 1e6

        def max_duration_ms(spans):
            if not spans:
                return 0
            return max(s["t_end_ns"] - s["t_start_ns"] for s in spans) / 1e6

        t_min = min(s["t_start_ns"] for s in self.spans)
        t_max = max(s["t_end_ns"] for s in self.spans)

        distinct_files = {s["file_id"] for s in self.spans if s["file_id"]}
        distinct_corr = {s["corr_id"] for s in self.spans if s["corr_id"]}

        return {
            "total_files": len(distinct_files),
            "total_bytes": sum(int(s["bytes"]) for s in io_spans),
            "total_rows": sum(int(s["rows"]) for s in decode_spans),
            "total_operations": len(distinct_corr),
            "total_download_ops": len(io_spans),
            "total_decode_ops": len(decode_spans),
            # Footer fetches aren't span-recorded (they land in
            # telemetry["time_engine_footer_fetch"] instead) — every
            # TC_IO_REQUEST span today is a row-group fetch.
            "footer_download_ops": 0,
            "rowgroup_download_ops": len(io_spans),
            "rowgroup_decode_ops": len(decode_spans),
            "download_phase_duration_ms": phase_duration_ms(io_spans),
            "decode_phase_duration_ms": phase_duration_ms(decode_spans),
            "query_duration_ms": (t_max - t_min) / 1e6,
            # Peak simultaneous count per phase — three independent sweeps,
            # since a row group can be queued while others are still
            # downloading/decoding, so these do not need to sum to anything.
            "max_concurrent_queued": self._max_concurrent(queue_spans),
            "max_concurrent_downloads": self._max_concurrent(io_spans),
            "max_concurrent_decodes": self._max_concurrent(decode_spans),
            "avg_download_time_ms": avg_duration_ms(io_spans),
            "avg_decode_time_ms": avg_duration_ms(decode_spans),
            # A stall is the consumer thread finding NOTHING claimable/ready and
            # genuinely blocking — zero events is the healthy case (the queue
            # never ran dry). Non-zero and growing means the decode/download
            # side can't keep the consumer fed.
            "queue_stall_events": len(stall_spans),
            "queue_stall_total_ms": sum(
                s["t_end_ns"] - s["t_start_ns"] for s in stall_spans
            ) / 1e6,
            "avg_queue_stall_ms": avg_duration_ms(stall_spans),
            "max_queue_stall_ms": max_duration_ms(stall_spans),
        }
