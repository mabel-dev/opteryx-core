# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parquet row-group transport over a shared-memory slot ring with a dedicated IO process.

This module implements the design in docs/io-process/rowgroup-draken-ring-design.md.
It is intentionally opt-in via a feature flag.
"""

from __future__ import annotations

import io
import math
import queue
import struct
import threading
import time
import traceback
import zlib
from collections import deque
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from dataclasses import dataclass
from multiprocessing import Event
from multiprocessing import Queue
from multiprocessing import get_context
from multiprocessing.shared_memory import SharedMemory
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional

from opteryx import config as _cfg
from opteryx.connectors.io_systems import create_filesystem
from opteryx.draken.morsels.morsel import Morsel
from opteryx.draken.storage import read_morsel
from opteryx.draken.storage import write_morsel
from opteryx.parquet_io.cache import InMemoryParquetCache
from opteryx.parquet_io.predicates import row_group_may_satisfy

# Slot states
FREE = 0
WRITING = 1
READY = 2
READING = 3
ERROR = 4

# Frame flags
FLAG_LAST_FRAGMENT = 1 << 0
FLAG_SLICED_ROWGROUP = 1 << 1
FLAG_ERROR = 1 << 2

# Header layout (packed at slot start). Keep compact; slot header budget is 256 bytes.
_SLOT_STATE_STRUCT = struct.Struct("<I")
_SLOT_FRAME_STRUCT = struct.Struct("<IIQQIIIIIIIQQ")

_EVENT_IO_READY = "io_ready"
_EVENT_FRAME_READY = "frame_ready"
_EVENT_TRANSFER_ERROR = "transfer_error"
_EVENT_SCAN_COMPLETE = "scan_complete"

_CMD_SCAN_START = "scan_start"
_CMD_SCAN_CANCEL = "scan_cancel"
_CMD_SHUTDOWN = "shutdown"


def _stable_u64(value: str) -> int:
    """Small stable hash for header fields."""
    return zlib.crc32(value.encode("utf8")) & 0xFFFFFFFF


def _percentile(values: List[int], q: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * q))))
    return int(ordered[idx])


def _decode_column_name(name: bytes | str) -> str:
    if isinstance(name, bytes):
        return name.decode("utf8")
    return str(name)


@dataclass
class _TransferAssembly:
    transfer_id: int
    fragment_count: int
    metadata: dict
    slice_index: int
    slice_count: int
    rows_in_slice: int
    fragments: Dict[int, bytes]
    created_ns: int


@dataclass
class _IOFileState:
    file_seq: int
    path: str
    total_rowgroups: int
    pending_rg_indices: deque[int]
    footer_bytes: int = 0
    footer_fetch_ns: int = 0
    next_rg_idx: int = 0
    active_rowgroups: int = 0


@dataclass
class _IOColumnWork:
    name: str
    stats: dict
    offset: int
    length: int


@dataclass
class _IORowGroupState:
    file_seq: int
    path: str
    rg_idx: int
    admitted_ns: int
    column_order: List[str]
    pending_columns: List[_IOColumnWork]
    footer_bytes: int = 0
    footer_fetch_ns: int = 0
    in_flight: int = 0
    in_flight_peak: int = 0
    first_dispatch_ns: Optional[int] = None
    completed_ns: Optional[int] = None
    decode_started: bool = False
    time_to_first_rowgroup_ns: int = 0
    ready_queue_depth_at_ready: int = 0
    queue_wait_ns: int = 0
    task_total_ns: int = 0
    read_ns: int = 0
    decode_ns: int = 0
    bytes_fetched: int = 0
    bytes_requested: int = 0
    range_request_count: int = 0
    columns: Dict[str, Any] = None

    def __post_init__(self):
        if self.columns is None:
            self.columns = {}


class _SharedMemoryRing:
    def __init__(
        self,
        slot_bytes: int,
        slot_count: int,
        *,
        name: Optional[str] = None,
        create: bool = False,
    ) -> None:
        if slot_bytes < 1024:
            raise ValueError(f"slot_bytes too small: {slot_bytes}")
        if slot_count <= 0:
            raise ValueError(f"slot_count must be > 0, got {slot_count}")

        self.slot_bytes = int(slot_bytes)
        self.slot_count = int(slot_count)
        self.header_bytes = 256
        self.payload_bytes = self.slot_bytes - self.header_bytes
        if self.payload_bytes <= 0:
            raise ValueError(
                f"slot_bytes ({self.slot_bytes}) must be greater than header_bytes ({self.header_bytes})"
            )

        total_bytes = self.slot_bytes * self.slot_count
        self.shm = SharedMemory(name=name, create=create, size=total_bytes)
        self.buf = self.shm.buf

    @property
    def name(self) -> str:
        return self.shm.name

    def close(self) -> None:
        self.shm.close()

    def unlink(self) -> None:
        try:
            self.shm.unlink()
        except FileNotFoundError:
            pass

    def initialize_free(self) -> None:
        for slot_id in range(self.slot_count):
            self.write_state(slot_id, FREE)

    def _slot_offset(self, slot_id: int) -> int:
        return slot_id * self.slot_bytes

    def write_state(self, slot_id: int, state: int) -> None:
        struct.pack_into(_SLOT_STATE_STRUCT.format, self.buf, self._slot_offset(slot_id), state)

    def read_state(self, slot_id: int) -> int:
        (state,) = struct.unpack_from(
            _SLOT_STATE_STRUCT.format, self.buf, self._slot_offset(slot_id)
        )
        return state

    def claim_free_slot(self, cancel_event: Event) -> tuple[int, int, int]:
        """Return (slot_id, waited_ns, wait_events)."""
        waited_ns = 0
        wait_events = 0
        while True:
            for slot_id in range(self.slot_count):
                if self.read_state(slot_id) == FREE:
                    self.write_state(slot_id, WRITING)
                    return slot_id, waited_ns, wait_events
            wait_events += 1
            block_start = time.monotonic_ns()
            if cancel_event.wait(timeout=0.001):
                raise RuntimeError("scan cancelled")
            waited_ns += time.monotonic_ns() - block_start

    def write_frame(
        self,
        slot_id: int,
        *,
        query_id_hash: int,
        transfer_id: int,
        file_id_hash: int,
        row_group_index: int,
        slice_index: int,
        fragment_index: int,
        fragment_count: int,
        rows_in_slice: int,
        flags: int,
        payload: bytes,
    ) -> None:
        payload_len = len(payload)
        if payload_len > self.payload_bytes:
            raise ValueError(
                f"frame payload {payload_len} exceeds slot payload capacity {self.payload_bytes}"
            )
        payload_crc = zlib.crc32(payload) & 0xFFFFFFFF

        base = self._slot_offset(slot_id)
        struct.pack_into(
            _SLOT_FRAME_STRUCT.format,
            self.buf,
            base,
            WRITING,  # state
            flags,
            transfer_id,
            file_id_hash,
            row_group_index,
            slice_index,
            fragment_index,
            fragment_count,
            rows_in_slice,
            payload_len,
            payload_crc,
            query_id_hash,
            0,
        )
        payload_off = base + self.header_bytes
        self.buf[payload_off : payload_off + payload_len] = payload
        self.write_state(slot_id, READY)

    def read_frame(self, slot_id: int) -> tuple[dict, bytes]:
        base = self._slot_offset(slot_id)
        fields = struct.unpack_from(_SLOT_FRAME_STRUCT.format, self.buf, base)
        (
            state,
            flags,
            transfer_id,
            file_id_hash,
            row_group_index,
            slice_index,
            fragment_index,
            fragment_count,
            rows_in_slice,
            payload_len,
            payload_crc,
            query_id_hash,
            _,
        ) = fields

        if state not in (READY, READING):
            raise RuntimeError(f"slot {slot_id} is not READY/READING (state={state})")

        payload_off = base + self.header_bytes
        payload = bytes(self.buf[payload_off : payload_off + payload_len])
        if (zlib.crc32(payload) & 0xFFFFFFFF) != payload_crc:
            raise RuntimeError(
                f"CRC mismatch for slot {slot_id} transfer={transfer_id} fragment={fragment_index}"
            )

        header = {
            "flags": flags,
            "transfer_id": transfer_id,
            "file_id_hash": file_id_hash,
            "row_group_index": row_group_index,
            "slice_index": slice_index,
            "fragment_index": fragment_index,
            "fragment_count": fragment_count,
            "rows_in_slice": rows_in_slice,
            "payload_bytes": payload_len,
            "payload_crc32": payload_crc,
            "query_id_hash": query_id_hash,
        }
        return header, payload


def _serialize_morsel(morsel: Morsel) -> tuple[bytes, int]:
    start_ns = time.monotonic_ns()
    sink = io.BytesIO()
    write_morsel(sink, morsel)
    payload = sink.getvalue()
    return payload, (time.monotonic_ns() - start_ns)


def _slice_and_serialize(
    morsel: Morsel,
    *,
    slot_payload_bytes: int,
    max_fragments_per_transfer: int,
    target_slice_bytes: int,
) -> tuple[List[dict], int]:
    """
    Return transfer payloads for this row group.

    Each entry:
      {
        "slice_index": int,
        "slice_count": int,
        "rows_in_slice": int,
        "payload": bytes,
        "fragment_count": int,
      }
    """
    payload, serialize_ns = _serialize_morsel(morsel)
    fragment_count = max(1, math.ceil(len(payload) / slot_payload_bytes))
    if fragment_count <= max_fragments_per_transfer:
        return (
            [
                {
                    "slice_index": 0,
                    "slice_count": 1,
                    "rows_in_slice": morsel.num_rows,
                    "payload": payload,
                    "fragment_count": fragment_count,
                }
            ],
            serialize_ns,
        )

    # Too fragmented: derive deterministic row slices and retry until each
    # slice transfer is under fragment cap (or one-row slices remain).
    rows_total = morsel.num_rows
    if rows_total <= 1:
        return (
            [
                {
                    "slice_index": 0,
                    "slice_count": 1,
                    "rows_in_slice": rows_total,
                    "payload": payload,
                    "fragment_count": fragment_count,
                }
            ],
            serialize_ns,
        )

    est_rows_per_slice = max(1, int((target_slice_bytes * rows_total) / max(1, len(payload))))
    rows_per_slice = min(rows_total, max(1, est_rows_per_slice))
    if rows_per_slice >= rows_total:
        rows_per_slice = max(1, rows_total // 2)

    while True:
        serialized: List[tuple[int, bytes, int]] = []
        total_serialize_ns = 0
        too_fragmented = False

        for start_row in range(0, rows_total, rows_per_slice):
            length = min(rows_per_slice, rows_total - start_row)
            slice_morsel = morsel.slice(start_row, length)
            slice_payload, slice_serialize_ns = _serialize_morsel(slice_morsel)
            total_serialize_ns += slice_serialize_ns
            slice_fragments = max(1, math.ceil(len(slice_payload) / slot_payload_bytes))
            serialized.append((length, slice_payload, slice_fragments))
            if slice_fragments > max_fragments_per_transfer and length > 1:
                too_fragmented = True
                break

        if not too_fragmented or rows_per_slice == 1:
            entries: List[dict] = []
            slice_count = len(serialized)
            for idx, (rows_in_slice, slice_payload, slice_fragments) in enumerate(serialized):
                entries.append(
                    {
                        "slice_index": idx,
                        "slice_count": slice_count,
                        "rows_in_slice": rows_in_slice,
                        "payload": slice_payload,
                        "fragment_count": slice_fragments,
                    }
                )
            return entries, total_serialize_ns

        rows_per_slice = max(1, rows_per_slice // 2)


def _connector_to_protocol(connector: Optional[str]) -> str:
    if not connector:
        return ""
    norm = connector.strip().lower()
    if norm in ("gcs", "gs"):
        return "gs"
    if norm in ("s3", "minio"):
        return "s3"
    if norm in ("file", "local", "filesystem"):
        return "file"
    return norm


def _resolve_protocol(paths: List[str], connector: Optional[str]) -> str:
    if paths and "://" in paths[0]:
        return paths[0].split("://", 1)[0].lower()
    return _connector_to_protocol(connector)


def _column_chunk_range(col_stats: dict) -> tuple[int, int]:
    dict_off = col_stats.get("dictionary_page_offset")
    data_off = col_stats["data_page_offset"]
    if dict_off is not None and dict_off >= 0 and dict_off < data_off:
        base_offset = dict_off
    else:
        base_offset = data_off
    return base_offset, int(col_stats["total_compressed_size"])


def _resolve_decoder() -> Any:
    try:
        from opteryx.rugo import parquet as rugo_parquet
    except ImportError:
        raise RuntimeError(
            "rugo.parquet is required but not available. "
            "Ensure rugo is compiled and in the Python path."
        )
    return rugo_parquet.decode_column_from_chunk


def _read_column_task(
    filesystem: Any,
    path: str,
    rg_idx: int,
    work: _IOColumnWork,
    submitted_ns: int,
    connector: Optional[str] = None,
) -> dict:
    from opteryx import config as _trace_cfg
    from opteryx.tracing import record_event

    task_start_ns = time.monotonic_ns()
    queue_wait_ns = task_start_ns - submitted_ns

    if _trace_cfg.OPTERYX_TRACE:
        kwargs = {"file_id": path, "component": "columns", "rg_idx": rg_idx, "column": work.name}
        if connector:
            kwargs["connector"] = connector
        record_event("download_start", **kwargs)

    read_start_ns = time.monotonic_ns()
    (raw_bytes,) = filesystem.read_ranges(path, [(work.offset, work.length)])
    read_ns = time.monotonic_ns() - read_start_ns
    bytes_fetched = len(raw_bytes)

    if _trace_cfg.OPTERYX_TRACE:
        kwargs = {
            "file_id": path,
            "component": "columns",
            "rg_idx": rg_idx,
            "column": work.name,
            "bytes_received": bytes_fetched,
        }
        if connector:
            kwargs["connector"] = connector
        record_event("download_complete", **kwargs)

    return {
        "name": work.name,
        "raw_bytes": raw_bytes,
        "bytes_fetched": bytes_fetched,
        "bytes_requested": work.length,
        "range_request_count": 1,
        "read_ns": read_ns,
        "queue_wait_ns": queue_wait_ns,
        "task_total_ns": time.monotonic_ns() - task_start_ns,
    }


def _decode_column_task(
    path: str,
    rg_idx: int,
    work: _IOColumnWork,
    raw_bytes: bytes,
    decoder: Any,
    submitted_ns: int,
    connector: Optional[str] = None,
) -> dict:
    from opteryx import config as _trace_cfg
    from opteryx.tracing import record_event

    task_start_ns = time.monotonic_ns()
    queue_wait_ns = task_start_ns - submitted_ns

    if _trace_cfg.OPTERYX_TRACE:
        kwargs = {
            "file_id": path,
            "component": "column",
            "rg_idx": rg_idx,
            "column": work.name,
        }
        if connector:
            kwargs["connector"] = connector
        record_event("decode_start", **kwargs)

    decode_start_ns = time.monotonic_ns()
    decoded = decoder(raw_bytes, work.stats)
    decode_ns = time.monotonic_ns() - decode_start_ns
    if decoded is None:
        raise RuntimeError(
            f"Decoder returned None for column '{path}:{rg_idx}:{work.name}' "
            f"(codec={work.stats.get('compression_codec')}, encodings={work.stats.get('encodings')})"
        )

    if _trace_cfg.OPTERYX_TRACE:
        kwargs = {
            "file_id": path,
            "component": "column",
            "rg_idx": rg_idx,
            "column": work.name,
            "rows_decoded": getattr(decoded, "num_rows", 0),
        }
        if connector:
            kwargs["connector"] = connector
        record_event("decode_complete", **kwargs)

    return {
        "name": work.name,
        "decoded": decoded,
        "decode_ns": decode_ns,
        "queue_wait_ns": queue_wait_ns,
        "task_total_ns": time.monotonic_ns() - task_start_ns,
    }


def _emit_loop(
    ready_queue: "queue.Queue[_IORowGroupState | None]",
    ring: _SharedMemoryRing,
    event_q: Queue,
    cancel_event: Event,
    *,
    query_id_hash: int,
    slot_payload_bytes: int,
    max_fragments: int,
    target_slice_bytes: int,
    metrics: dict,
    metrics_lock: threading.Lock,
    next_transfer_id: List[int],
) -> None:
    try:
        while True:
            state = ready_queue.get()
            if state is None:
                return
            if cancel_event.is_set():
                continue

            emit_start_ns = time.monotonic_ns()
            with metrics_lock:
                metrics["io_transfer_emit_wait_ns"] += max(
                    0, emit_start_ns - (state.completed_ns or emit_start_ns)
                )

            vectors = [state.columns[name] for name in state.column_order]
            morsel = Morsel.from_vectors(list(state.column_order), vectors)
            payload_entries, serialize_ns = _slice_and_serialize(
                morsel,
                slot_payload_bytes=slot_payload_bytes,
                max_fragments_per_transfer=max_fragments,
                target_slice_bytes=target_slice_bytes,
            )
            with metrics_lock:
                metrics["io_serialize_ns"] += serialize_ns
                metrics["io_rowgroup_slice_count"] += max(0, len(payload_entries) - 1)

            base_meta = {
                "__path__": state.path,
                "__row_group__": state.rg_idx,
                "__bytes_fetched__": state.bytes_fetched + int(getattr(state, "footer_bytes", 0)),
                "__footer_bytes__": int(getattr(state, "footer_bytes", 0)),
                "__footer_fetch_ns__": int(getattr(state, "footer_fetch_ns", 0)),
                "__range_request_count__": state.range_request_count,
                "__range_bytes_requested__": state.bytes_requested,
                "__time_read_ranges_ns__": state.read_ns,
                "__time_decode_columns_ns__": state.decode_ns,
                "__cache_column_hits__": 0,
                "__cache_column_misses__": len(state.column_order),
                "__task_queue_wait_ns__": state.queue_wait_ns,
                "__task_total_ns__": state.task_total_ns,
                "__scheduler_wait_ns__": max(
                    0, (state.first_dispatch_ns or emit_start_ns) - state.admitted_ns
                ),
                "__rowgroup_completion_latency_ns__": max(
                    0, (state.completed_ns or emit_start_ns) - state.admitted_ns
                ),
                "__rowgroup_peak_in_flight__": state.in_flight_peak,
                "__ranges_in_flight_peak__": int(metrics.get("ranges_in_flight_peak", 0)),
                "__active_files_peak__": int(metrics.get("active_files_peak", 0)),
                "__active_rowgroups_peak__": int(metrics.get("active_rowgroups_peak", 0)),
                "__rowgroups_in_flight_cap__": int(metrics.get("rowgroups_in_flight_cap", 0)),
                "__emit_wait_ns__": max(0, emit_start_ns - (state.completed_ns or emit_start_ns)),
                "__emit_queue_depth_at_ready__": state.ready_queue_depth_at_ready,
                "__scheduler_empty_wait_ns__": 0,
                "__scheduler_empty_wait_events__": 0,
                "__time_to_first_rowgroup_ns__": int(
                    getattr(state, "time_to_first_rowgroup_ns", 0)
                ),
                "__row_groups_pruned__": int(metrics.get("row_groups_pruned", 0)),
            }

            for payload_entry in payload_entries:
                with metrics_lock:
                    next_transfer_id[0] += 1
                    transfer_id = next_transfer_id[0]
                payload = payload_entry["payload"]
                fragment_count = payload_entry["fragment_count"]
                with metrics_lock:
                    metrics["transfer_fragment_counts"].append(fragment_count)
                    metrics["transfer_payload_sizes"].append(len(payload))
                file_id_hash = _stable_u64(state.path)

                transfer_meta = dict(base_meta)
                if payload_entry["slice_index"] > 0:
                    for key in (
                        "__bytes_fetched__",
                        "__footer_bytes__",
                        "__range_request_count__",
                        "__range_bytes_requested__",
                        "__time_read_ranges_ns__",
                        "__time_decode_columns_ns__",
                        "__cache_column_hits__",
                        "__cache_column_misses__",
                        "__task_queue_wait_ns__",
                        "__task_total_ns__",
                        "__scheduler_wait_ns__",
                        "__rowgroup_completion_latency_ns__",
                        "__emit_wait_ns__",
                        "__scheduler_empty_wait_ns__",
                        "__scheduler_empty_wait_events__",
                        "__time_to_first_rowgroup_ns__",
                    ):
                        transfer_meta[key] = 0

                for fragment_index in range(fragment_count):
                    if cancel_event.is_set():
                        break
                    start = fragment_index * slot_payload_bytes
                    end = start + slot_payload_bytes
                    fragment_payload = payload[start:end]
                    flags = 0
                    if payload_entry["slice_count"] > 1:
                        flags |= FLAG_SLICED_ROWGROUP
                    if fragment_index == (fragment_count - 1):
                        flags |= FLAG_LAST_FRAGMENT

                    slot_id, wait_ns, wait_events = ring.claim_free_slot(cancel_event)
                    with metrics_lock:
                        metrics["io_ring_producer_full_wait_ns"] += wait_ns
                        metrics["io_ring_producer_full_wait_events"] += wait_events
                    ring.write_frame(
                        slot_id,
                        query_id_hash=query_id_hash,
                        transfer_id=transfer_id,
                        file_id_hash=file_id_hash,
                        row_group_index=state.rg_idx,
                        slice_index=payload_entry["slice_index"],
                        fragment_index=fragment_index,
                        fragment_count=fragment_count,
                        rows_in_slice=payload_entry["rows_in_slice"],
                        flags=flags,
                        payload=fragment_payload,
                    )
                    event_q.put(
                        {
                            "type": _EVENT_FRAME_READY,
                            "slot_id": slot_id,
                            "transfer_id": transfer_id,
                            "fragment_index": fragment_index,
                            "fragment_count": fragment_count,
                            "rows_in_slice": payload_entry["rows_in_slice"],
                            "slice_index": payload_entry["slice_index"],
                            "slice_count": payload_entry["slice_count"],
                            "row_group_meta": transfer_meta,
                        }
                    )
    except Exception as err:
        event_q.put(
            {
                "type": _EVENT_TRANSFER_ERROR,
                "message": str(err),
                "traceback": traceback.format_exc(),
            }
        )
        cancel_event.set()


def _io_worker(
    shm_name: str,
    slot_bytes: int,
    slot_count: int,
    command_q: Queue,
    event_q: Queue,
    cancel_event: Event,
) -> None:
    ring = _SharedMemoryRing(
        slot_bytes=slot_bytes, slot_count=slot_count, name=shm_name, create=False
    )
    event_q.put({"type": _EVENT_IO_READY})

    try:
        while True:
            command = command_q.get()
            cmd_type = command.get("type")
            if cmd_type == _CMD_SHUTDOWN:
                return
            if cmd_type == _CMD_SCAN_CANCEL:
                cancel_event.set()
                continue
            if cmd_type != _CMD_SCAN_START:
                continue

            cancel_event.clear()
            metrics = {
                "io_ring_slot_bytes": slot_bytes,
                "io_ring_slot_count": slot_count,
                "io_ring_total_bytes": slot_bytes * slot_count,
                "io_ring_producer_full_wait_ns": 0,
                "io_ring_producer_full_wait_events": 0,
                "io_ring_consumer_empty_wait_ns": 0,
                "io_ring_consumer_empty_wait_events": 0,
                "io_transfer_ready_backlog_peak": 0,
                "io_transfer_emit_wait_ns": 0,
                "io_serialize_ns": 0,
                "io_rowgroup_slice_count": 0,
                "ranges_in_flight_peak": 0,
                "active_files_peak": 0,
                "active_rowgroups_peak": 0,
                "row_groups_pruned": 0,
                "rowgroups_in_flight_cap": 0,
                "transfer_fragment_counts": [],
                "transfer_payload_sizes": [],
            }

            ready_queue_cap = max(2, int(_cfg.PARQUET_READY_ROWGROUP_QUEUE_CAP))
            ready_queue: "queue.Queue[_IORowGroupState | None]" = queue.Queue(
                maxsize=ready_queue_cap
            )
            metrics_lock = threading.Lock()
            next_transfer_id = [0]
            emitter: Optional[threading.Thread] = None
            read_pool: Optional[ThreadPoolExecutor] = None
            decode_pool: Optional[ThreadPoolExecutor] = None
            read_futures: Dict[Future, tuple[tuple[int, int], _IOColumnWork]] = {}
            decode_futures: Dict[Future, tuple[tuple[int, int], _IOColumnWork]] = {}
            decode_pending: deque[tuple[tuple[int, int], _IOColumnWork, bytes]] = deque()

            try:
                from opteryx import config as _trace_cfg
                from opteryx.parquet_io.reader import _parse_footer_envelope
                from opteryx.parquet_io.reader import _read_footer_payload
                from opteryx.tracing import record_event

                paths = command["paths"]
                column_names = command["column_names"]
                predicates = command.get("predicates")
                file_sizes = command.get("file_sizes") or {}
                max_workers = int(command.get("max_workers", 16))
                connector = command.get("connector")
                query_id_hash = _stable_u64(str(command.get("query_id", "")))
                prefetched_footers = command.get("prefetched_footers") or {}

                global_ranges_cap = max(1, int(_cfg.PARQUET_GLOBAL_RANGE_READERS))
                per_rowgroup_cap = max(1, int(_cfg.PARQUET_RANGE_READERS_PER_ROWGROUP))
                rowgroups_in_flight_cap = max(1, int(_cfg.PARQUET_ROWGROUPS_IN_FLIGHT))
                rowgroups_per_file_cap = max(1, int(_cfg.PARQUET_ROWGROUPS_PER_FILE_IN_FLIGHT))
                decode_workers = max(1, int(_cfg.PARQUET_DECODE_WORKERS))
                decode_buffer_cap = max(global_ranges_cap, int(_cfg.PARQUET_READ_DECODE_BUFFER_CAP))

                active_target_default = max(1, int(_cfg.PARQUET_ACTIVE_ROWGROUPS_TARGET))
                warm_start_ops = max(0, int(_cfg.PARQUET_WARM_START_OPS))
                low_col_threshold = max(0, int(_cfg.PARQUET_LOW_COLUMN_THRESHOLD))
                low_col_active_target = max(1, int(_cfg.PARQUET_LOW_COLUMN_ACTIVE_ROWGROUPS_TARGET))
                low_col_per_rowgroup_cap = max(1, int(_cfg.PARQUET_LOW_COLUMN_PER_ROWGROUP_SLOTS))

                if low_col_threshold > 0 and len(column_names) < low_col_threshold:
                    active_target = min(rowgroups_in_flight_cap, low_col_active_target)
                    per_rowgroup_cap = min(per_rowgroup_cap, low_col_per_rowgroup_cap)
                else:
                    active_target = min(rowgroups_in_flight_cap, active_target_default)
                metrics["rowgroups_in_flight_cap"] = active_target
                ready_backlog_cap = max(
                    active_target,
                    int(_cfg.PARQUET_COMPLETED_ROWGROUP_BACKLOG_CAP),
                )

                slot_payload_bytes = slot_bytes - 256
                max_fragments = int(command["max_fragments_per_transfer"])
                target_slice_bytes = int(command["target_slice_bytes"])

                protocol = _resolve_protocol(paths, connector)
                filesystem = create_filesystem(protocol)
                decoder_fn = _resolve_decoder()

                unique_paths = list(dict.fromkeys(paths))
                footers: Dict[str, dict] = {}
                footer_fetch_ns: Dict[str, int] = {}

                for p in unique_paths:
                    prefetch_meta = prefetched_footers.get(p)
                    if prefetch_meta is not None:
                        footers[p] = prefetch_meta
                        footer_fetch_ns[p] = 0
                        continue
                    known_size = file_sizes.get(p)
                    if not isinstance(known_size, int) or known_size <= 0:
                        known_size = None
                    if known_size is None:
                        envelope, footer_bytes, fetch_ns = _read_footer_payload(
                            filesystem, p, connector=connector
                        )
                    else:
                        envelope, footer_bytes, fetch_ns = _read_footer_payload(
                            filesystem, p, known_size, connector
                        )
                    parse_start_ns = time.monotonic_ns()
                    meta = _parse_footer_envelope(p, envelope, footer_bytes)
                    parse_ns = time.monotonic_ns() - parse_start_ns
                    footers[p] = meta
                    footer_fetch_ns[p] = fetch_ns + parse_ns

                file_states: Dict[int, _IOFileState] = {}
                file_rr: deque[int] = deque()
                for file_seq, path in enumerate(paths):
                    meta = footers[path]
                    rg_meta_list = meta.get("row_groups", [])
                    pending_rg: deque[int] = deque()
                    for rg_idx, rg_meta in enumerate(rg_meta_list):
                        if predicates and not row_group_may_satisfy(rg_meta, predicates):
                            metrics["row_groups_pruned"] += 1
                            continue
                        pending_rg.append(rg_idx)
                    file_states[file_seq] = _IOFileState(
                        file_seq=file_seq,
                        path=path,
                        total_rowgroups=len(rg_meta_list),
                        pending_rg_indices=pending_rg,
                        footer_bytes=int(meta.get("__footer_bytes__", 0)),
                        footer_fetch_ns=int(footer_fetch_ns.get(path, 0)),
                    )
                    if pending_rg:
                        file_rr.append(file_seq)

                def _active_file_count(
                    active_states: Dict[tuple[int, int], _IORowGroupState],
                ) -> int:
                    return len({state.file_seq for state in active_states.values()})

                active_states: Dict[tuple[int, int], _IORowGroupState] = {}
                read_futures.clear()
                decode_futures.clear()
                decode_pending.clear()
                ready_backlog: deque[_IORowGroupState] = deque()
                reads_in_flight = 0
                scan_start_ns = time.monotonic_ns()
                first_completion_emitted = False
                warm_start_remaining = warm_start_ops
                first_rowgroup_key: Optional[tuple[int, int]] = None

                emitter = threading.Thread(
                    target=_emit_loop,
                    args=(ready_queue, ring, event_q, cancel_event),
                    kwargs={
                        "query_id_hash": query_id_hash,
                        "slot_payload_bytes": slot_payload_bytes,
                        "max_fragments": max_fragments,
                        "target_slice_bytes": target_slice_bytes,
                        "metrics": metrics,
                        "metrics_lock": metrics_lock,
                        "next_transfer_id": next_transfer_id,
                    },
                    daemon=True,
                )
                emitter.start()

                read_pool = ThreadPoolExecutor(max_workers=max(max_workers, global_ranges_cap))
                decode_pool = ThreadPoolExecutor(max_workers=max(decode_workers, 1))

                def _ready_buffer_depth() -> int:
                    return len(ready_backlog) + ready_queue.qsize()

                def _flush_ready_backlog() -> int:
                    moved = 0
                    while ready_backlog and not cancel_event.is_set():
                        state = ready_backlog[0]
                        try:
                            ready_queue.put_nowait(state)
                        except queue.Full:
                            break
                        ready_backlog.popleft()
                        moved += 1
                    if moved:
                        with metrics_lock:
                            metrics["io_transfer_ready_backlog_peak"] = max(
                                metrics["io_transfer_ready_backlog_peak"],
                                _ready_buffer_depth(),
                            )
                    return moved

                def _admit_rowgroups() -> None:
                    nonlocal first_rowgroup_key
                    if cancel_event.is_set():
                        return
                    while (
                        len(active_states) < active_target
                        and file_rr
                        and _ready_buffer_depth() < ready_backlog_cap
                    ):
                        cycle = len(file_rr)
                        admitted = False
                        for _ in range(cycle):
                            file_seq = file_rr.popleft()
                            fstate = file_states[file_seq]
                            if fstate.active_rowgroups >= rowgroups_per_file_cap:
                                if fstate.pending_rg_indices:
                                    file_rr.append(file_seq)
                                continue
                            if not fstate.pending_rg_indices:
                                continue

                            rg_idx = fstate.pending_rg_indices.popleft()
                            rg_meta = footers[fstate.path]["row_groups"][rg_idx]
                            name_to_stats: Dict[str, dict] = {
                                col["name"]: col for col in rg_meta["columns"]
                            }
                            column_work: List[_IOColumnWork] = []
                            for col_name in column_names:
                                if col_name not in name_to_stats:
                                    raise KeyError(
                                        f"Column '{col_name}' not found in row group {rg_idx}. "
                                        f"Available columns: {list(name_to_stats.keys())}"
                                    )
                                col_stats = name_to_stats[col_name]
                                offset, length = _column_chunk_range(col_stats)
                                column_work.append(
                                    _IOColumnWork(
                                        name=col_name,
                                        stats=col_stats,
                                        offset=offset,
                                        length=length,
                                    )
                                )
                            # Largest columns first to reduce tail.
                            column_work.sort(key=lambda item: item.length, reverse=True)

                            admitted_ns = time.monotonic_ns()
                            state = _IORowGroupState(
                                file_seq=file_seq,
                                path=fstate.path,
                                rg_idx=rg_idx,
                                admitted_ns=admitted_ns,
                                column_order=list(column_names),
                                pending_columns=column_work,
                                footer_bytes=fstate.footer_bytes if rg_idx == 0 else 0,
                                footer_fetch_ns=fstate.footer_fetch_ns if rg_idx == 0 else 0,
                            )
                            key = (file_seq, rg_idx)
                            active_states[key] = state
                            fstate.active_rowgroups += 1
                            if first_rowgroup_key is None:
                                first_rowgroup_key = key
                            admitted = True

                            if fstate.pending_rg_indices:
                                file_rr.append(file_seq)
                            if len(active_states) >= active_target:
                                break

                        if not admitted:
                            break

                        with metrics_lock:
                            metrics["active_rowgroups_peak"] = max(
                                metrics["active_rowgroups_peak"], len(active_states)
                            )
                            metrics["active_files_peak"] = max(
                                metrics["active_files_peak"], _active_file_count(active_states)
                            )

                def _pick_dispatch_state() -> Optional[tuple[tuple[int, int], _IORowGroupState]]:
                    nonlocal warm_start_remaining
                    if warm_start_remaining > 0 and first_rowgroup_key in active_states:
                        first_state = active_states[first_rowgroup_key]
                        if first_state.pending_columns and first_state.in_flight < per_rowgroup_cap:
                            warm_start_remaining -= 1
                            return first_rowgroup_key, first_state

                    candidates: List[tuple[int, int, tuple[int, int], _IORowGroupState]] = []
                    for key, state in active_states.items():
                        if not state.pending_columns:
                            continue
                        if state.in_flight >= per_rowgroup_cap:
                            continue
                        largest = state.pending_columns[0].length
                        candidates.append((largest, -state.admitted_ns, key, state))
                    if not candidates:
                        return None
                    candidates.sort(reverse=True, key=lambda item: (item[0], item[1]))
                    _, _, key, state = candidates[0]
                    return key, state

                def _dispatch_columns() -> int:
                    nonlocal reads_in_flight
                    dispatched = 0
                    while (
                        reads_in_flight < global_ranges_cap
                        and not cancel_event.is_set()
                        and (len(decode_pending) + len(decode_futures)) < decode_buffer_cap
                    ):
                        picked = _pick_dispatch_state()
                        if picked is None:
                            break
                        key, state = picked
                        if not state.pending_columns:
                            break
                        work = state.pending_columns.pop(0)
                        submit_ns = time.monotonic_ns()
                        if state.first_dispatch_ns is None:
                            state.first_dispatch_ns = submit_ns
                        fut = read_pool.submit(
                            _read_column_task,
                            filesystem,
                            state.path,
                            state.rg_idx,
                            work,
                            submit_ns,
                            connector,
                        )
                        read_futures[fut] = (key, work)
                        state.in_flight += 1
                        state.in_flight_peak = max(state.in_flight_peak, state.in_flight)
                        reads_in_flight += 1
                        with metrics_lock:
                            metrics["ranges_in_flight_peak"] = max(
                                metrics["ranges_in_flight_peak"], reads_in_flight
                            )
                        dispatched += 1
                    return dispatched

                def _dispatch_decodes() -> int:
                    dispatched = 0
                    while (
                        decode_pending
                        and len(decode_futures) < decode_workers
                        and not cancel_event.is_set()
                    ):
                        key, work, raw_bytes = decode_pending.popleft()
                        state = active_states.get(key)
                        if state is None:
                            continue
                        submit_ns = time.monotonic_ns()
                        if not state.decode_started and _trace_cfg.OPTERYX_TRACE:
                            kwargs = {
                                "file_id": state.path,
                                "component": "rowgroup",
                                "rg_idx": state.rg_idx,
                                "columns": state.column_order,
                            }
                            if connector:
                                kwargs["connector"] = connector
                            record_event("decode_start", **kwargs)
                            state.decode_started = True

                        fut = decode_pool.submit(
                            _decode_column_task,
                            state.path,
                            state.rg_idx,
                            work,
                            raw_bytes,
                            decoder_fn,
                            submit_ns,
                            connector,
                        )
                        decode_futures[fut] = (key, work)
                        dispatched += 1
                    return dispatched

                def _complete_rowgroup(key: tuple[int, int], state: _IORowGroupState) -> None:
                    nonlocal first_completion_emitted
                    now_ns = time.monotonic_ns()
                    state.completed_ns = now_ns
                    if not first_completion_emitted:
                        state.time_to_first_rowgroup_ns = max(0, now_ns - scan_start_ns)
                        first_completion_emitted = True
                    if _trace_cfg.OPTERYX_TRACE and state.decode_started:
                        kwargs = {
                            "file_id": state.path,
                            "component": "rowgroup",
                            "rg_idx": state.rg_idx,
                            "rows_decoded": (
                                len(next(iter(state.columns.values()))) if state.columns else 0
                            ),
                        }
                        if connector:
                            kwargs["connector"] = connector
                        record_event("decode_complete", **kwargs)

                    fstate = file_states[state.file_seq]
                    fstate.active_rowgroups = max(0, fstate.active_rowgroups - 1)
                    del active_states[key]

                    state.ready_queue_depth_at_ready = _ready_buffer_depth()
                    ready_backlog.append(state)
                    with metrics_lock:
                        metrics["io_transfer_ready_backlog_peak"] = max(
                            metrics["io_transfer_ready_backlog_peak"],
                            _ready_buffer_depth(),
                        )

                while not cancel_event.is_set():
                    _flush_ready_backlog()
                    _dispatch_decodes()
                    _admit_rowgroups()
                    dispatched_reads = _dispatch_columns()
                    dispatched_decodes = _dispatch_decodes()

                    if not read_futures and not decode_futures:
                        if (
                            not active_states
                            and not file_rr
                            and not decode_pending
                            and not ready_backlog
                            and ready_queue.empty()
                        ):
                            break
                        if dispatched_reads == 0 and dispatched_decodes == 0:
                            sleep_start = time.monotonic_ns()
                            time.sleep(0.001)
                            with metrics_lock:
                                metrics["io_ring_consumer_empty_wait_events"] += 1
                                metrics["io_ring_consumer_empty_wait_ns"] += (
                                    time.monotonic_ns() - sleep_start
                                )
                        continue

                    wait_start = time.monotonic_ns()
                    waiting = set(read_futures) | set(decode_futures)
                    done, _ = wait(
                        waiting,
                        timeout=0 if (dispatched_reads > 0 or dispatched_decodes > 0) else 0.02,
                        return_when=FIRST_COMPLETED,
                    )
                    if not done:
                        with metrics_lock:
                            metrics["io_ring_consumer_empty_wait_events"] += 1
                            metrics["io_ring_consumer_empty_wait_ns"] += (
                                time.monotonic_ns() - wait_start
                            )
                        continue

                    for fut in done:
                        read_entry = read_futures.pop(fut, None)
                        if read_entry is not None:
                            key, work = read_entry
                            reads_in_flight = max(0, reads_in_flight - 1)
                            state = active_states.get(key)
                            if state is None:
                                continue
                            result = fut.result()
                            state.bytes_fetched += result["bytes_fetched"]
                            state.bytes_requested += result["bytes_requested"]
                            state.range_request_count += result["range_request_count"]
                            state.read_ns += result["read_ns"]
                            state.queue_wait_ns += result["queue_wait_ns"]
                            state.task_total_ns += result["task_total_ns"]
                            decode_pending.append((key, work, result["raw_bytes"]))
                            continue

                        decode_entry = decode_futures.pop(fut, None)
                        if decode_entry is None:
                            continue
                        key, _work = decode_entry
                        state = active_states.get(key)
                        if state is None:
                            continue
                        result = fut.result()
                        state.in_flight = max(0, state.in_flight - 1)
                        state.columns[result["name"]] = result["decoded"]
                        state.decode_ns += result["decode_ns"]
                        state.queue_wait_ns += result["queue_wait_ns"]
                        state.task_total_ns += result["task_total_ns"]

                        if not state.pending_columns and state.in_flight == 0:
                            _complete_rowgroup(key, state)

                    _dispatch_decodes()
                    _flush_ready_backlog()

                while ready_backlog and not cancel_event.is_set():
                    moved = _flush_ready_backlog()
                    if moved == 0:
                        time.sleep(0.001)

                while True:
                    try:
                        ready_queue.put(None, timeout=0.1)
                        break
                    except queue.Full:
                        if cancel_event.is_set():
                            break
                if emitter is not None:
                    emitter.join(timeout=10)

                metrics["io_transfer_fragment_count_p50"] = _percentile(
                    metrics["transfer_fragment_counts"], 0.5
                )
                metrics["io_transfer_fragment_count_p95"] = _percentile(
                    metrics["transfer_fragment_counts"], 0.95
                )
                metrics["io_transfer_fragment_count_max"] = (
                    max(metrics["transfer_fragment_counts"])
                    if metrics["transfer_fragment_counts"]
                    else 0
                )
                metrics["io_transfer_payload_bytes_p50"] = _percentile(
                    metrics["transfer_payload_sizes"], 0.5
                )
                metrics["io_transfer_payload_bytes_p95"] = _percentile(
                    metrics["transfer_payload_sizes"], 0.95
                )
                metrics["io_transfer_payload_bytes_max"] = (
                    max(metrics["transfer_payload_sizes"])
                    if metrics["transfer_payload_sizes"]
                    else 0
                )
                metrics.pop("transfer_fragment_counts", None)
                metrics.pop("transfer_payload_sizes", None)

                event_q.put(
                    {
                        "type": _EVENT_SCAN_COMPLETE,
                        "cancelled": bool(cancel_event.is_set()),
                        "metrics": metrics,
                    }
                )
            except Exception as err:
                cancel_event.set()
                try:
                    ready_queue.put_nowait(None)
                except Exception:
                    pass
                if emitter is not None and emitter.is_alive():
                    emitter.join(timeout=2)
                event_q.put(
                    {
                        "type": _EVENT_TRANSFER_ERROR,
                        "message": str(err),
                        "traceback": traceback.format_exc(),
                    }
                )
                # Preserve partial metrics for debugging.
                metrics.pop("transfer_fragment_counts", None)
                metrics.pop("transfer_payload_sizes", None)
                event_q.put({"type": _EVENT_SCAN_COMPLETE, "cancelled": True, "metrics": metrics})
            finally:
                for fut in list(read_futures):
                    fut.cancel()
                for fut in list(decode_futures):
                    fut.cancel()
                if read_pool is not None:
                    read_pool.shutdown(wait=False, cancel_futures=True)
                if decode_pool is not None:
                    decode_pool.shutdown(wait=False, cancel_futures=True)
    finally:
        ring.close()


def _build_row_group_from_payload(payload: bytes, metadata: dict) -> tuple[Dict[str, Any], int]:
    start_ns = time.monotonic_ns()
    morsel = read_morsel(io.BytesIO(payload))
    deserialize_ns = time.monotonic_ns() - start_ns

    row_group: Dict[str, Any] = {}
    for col_name in morsel.column_names:
        key = _decode_column_name(col_name)
        raw_name = col_name if isinstance(col_name, bytes) else str(col_name).encode("utf8")
        row_group[key] = morsel.column(raw_name)
    row_group.update(metadata)
    return row_group, deserialize_ns


def iter_row_groups_io_process_v2(
    paths: List[str],
    column_names: List[str],
    *,
    max_workers: int = 16,
    predicates: Optional[List] = None,
    file_sizes: Optional[Dict[str, int]] = None,
    connector: Optional[str] = None,
    query_id: Optional[str] = None,
    prefetched_footers: Optional[Dict[str, dict]] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Process-isolated row-group iterator.

    Contract matches parquet_io.reader.iter_row_groups(): yields
    ``Dict[column_name -> DrakenVector]`` plus ``__*`` metadata.
    """
    slot_bytes = int(_cfg.IO_RING_SLOT_BYTES)
    slot_count = int(_cfg.IO_RING_SLOT_COUNT)
    max_fragments = int(_cfg.IO_MAX_FRAGMENTS_PER_TRANSFER)
    target_slice_bytes = int(_cfg.IO_TARGET_SLICE_BYTES)

    ring = _SharedMemoryRing(slot_bytes=slot_bytes, slot_count=slot_count, create=True)
    ring.initialize_free()

    ctx = get_context("spawn")
    command_q: Queue = ctx.Queue()
    event_q: Queue = ctx.Queue()
    cancel_event: Event = ctx.Event()

    worker = ctx.Process(
        target=_io_worker,
        args=(ring.name, slot_bytes, slot_count, command_q, event_q, cancel_event),
        daemon=True,
    )
    worker.start()

    worker_metrics = {}
    consumer_empty_wait_ns = 0
    consumer_empty_wait_events = 0
    consumer_empty_wait_ns_emitted = 0
    consumer_empty_wait_events_emitted = 0
    transfer_ready_backlog_peak = 0
    transfer_emit_wait_ns = 0
    transfer_emit_wait_ns_emitted = 0

    assemblies: Dict[int, _TransferAssembly] = {}
    pending_row_group: Optional[Dict[str, Any]] = None
    poll_timeout_s = 0.05

    try:
        # Wait for worker readiness.
        ready = event_q.get(timeout=30)
        if ready.get("type") != _EVENT_IO_READY:
            raise RuntimeError(f"IO worker did not become ready (msg={ready})")

        command_q.put(
            {
                "type": _CMD_SCAN_START,
                "query_id": query_id or "",
                "paths": paths,
                "column_names": column_names,
                "predicates": predicates,
                "file_sizes": file_sizes,
                "connector": connector,
                "max_workers": max_workers,
                "max_fragments_per_transfer": max_fragments,
                "target_slice_bytes": target_slice_bytes,
                "prefetched_footers": prefetched_footers,
            }
        )

        scan_complete = False
        while True:
            if scan_complete and not assemblies:
                if pending_row_group is not None:
                    # Attach scan-level transport metrics to the final emitted row group.
                    pending_row_group["__io_ring_consumer_empty_wait_ns__"] += (
                        consumer_empty_wait_ns - consumer_empty_wait_ns_emitted
                    )
                    pending_row_group["__io_ring_consumer_empty_wait_events__"] += (
                        consumer_empty_wait_events - consumer_empty_wait_events_emitted
                    )
                    pending_row_group["__io_transfer_emit_wait_ns__"] += (
                        transfer_emit_wait_ns - transfer_emit_wait_ns_emitted
                    )
                    pending_row_group["__io_transfer_ready_backlog_peak__"] = max(
                        pending_row_group.get("__io_transfer_ready_backlog_peak__", 0),
                        transfer_ready_backlog_peak,
                    )
                    for key, value in worker_metrics.items():
                        pending_row_group[f"__{key}__"] = value
                    yield pending_row_group
                    pending_row_group = None
                break

            try:
                event = event_q.get(timeout=poll_timeout_s)
            except queue.Empty:
                consumer_empty_wait_events += 1
                consumer_empty_wait_ns += int(poll_timeout_s * 1_000_000_000)
                if scan_complete and not assemblies:
                    break
                if not worker.is_alive() and event_q.empty():
                    break
                continue

            event_type = event.get("type")
            if event_type == _EVENT_TRANSFER_ERROR:
                details = event.get("message", "unknown transfer error")
                tb = event.get("traceback", "")
                raise RuntimeError(f"IO process transfer error: {details}\n{tb}")

            if event_type == _EVENT_SCAN_COMPLETE:
                worker_metrics = dict(event.get("metrics") or {})
                scan_complete = True
                continue

            if event_type != _EVENT_FRAME_READY:
                continue

            slot_id = int(event["slot_id"])
            ring.write_state(slot_id, READING)
            _header, payload = ring.read_frame(slot_id)
            ring.write_state(slot_id, FREE)

            transfer_id = int(event["transfer_id"])
            assembly = assemblies.get(transfer_id)
            if assembly is None:
                assembly = _TransferAssembly(
                    transfer_id=transfer_id,
                    fragment_count=int(event["fragment_count"]),
                    metadata=dict(event.get("row_group_meta") or {}),
                    slice_index=int(event.get("slice_index", 0)),
                    slice_count=int(event.get("slice_count", 1)),
                    rows_in_slice=int(event.get("rows_in_slice", 0)),
                    fragments={},
                    created_ns=time.monotonic_ns(),
                )
                assemblies[transfer_id] = assembly

            assembly.fragments[int(event["fragment_index"])] = payload
            transfer_ready_backlog_peak = max(transfer_ready_backlog_peak, len(assemblies))

            if len(assembly.fragments) < assembly.fragment_count:
                continue

            assembled = b"".join(assembly.fragments[i] for i in range(assembly.fragment_count))
            row_group, deserialize_ns = _build_row_group_from_payload(assembled, assembly.metadata)
            transfer_emit_wait_ns += max(0, time.monotonic_ns() - assembly.created_ns)

            row_group["__slice_index__"] = assembly.slice_index
            row_group["__slice_count__"] = assembly.slice_count
            row_group["__rows_in_slice__"] = assembly.rows_in_slice
            row_group["__io_deserialize_ns__"] = deserialize_ns
            row_group["__io_ring_consumer_empty_wait_ns__"] = (
                consumer_empty_wait_ns - consumer_empty_wait_ns_emitted
            )
            row_group["__io_ring_consumer_empty_wait_events__"] = (
                consumer_empty_wait_events - consumer_empty_wait_events_emitted
            )
            row_group["__io_transfer_emit_wait_ns__"] = (
                transfer_emit_wait_ns - transfer_emit_wait_ns_emitted
            )
            row_group["__io_transfer_ready_backlog_peak__"] = transfer_ready_backlog_peak
            consumer_empty_wait_ns_emitted = consumer_empty_wait_ns
            consumer_empty_wait_events_emitted = consumer_empty_wait_events
            transfer_emit_wait_ns_emitted = transfer_emit_wait_ns
            del assemblies[transfer_id]

            if pending_row_group is not None:
                yield pending_row_group
            pending_row_group = row_group

    finally:
        cancel_event.set()
        try:
            command_q.put_nowait({"type": _CMD_SCAN_CANCEL})
        except Exception:
            pass
        try:
            command_q.put_nowait({"type": _CMD_SHUTDOWN})
        except Exception:
            pass

        worker.join(timeout=5)
        if worker.is_alive():
            worker.terminate()
            worker.join(timeout=5)

        # Force-reset any non-free slots to avoid reattach confusion in tests/dev.
        for slot_id in range(ring.slot_count):
            state = ring.read_state(slot_id)
            if state != FREE:
                ring.write_state(slot_id, FREE)

        ring.close()
        ring.unlink()
