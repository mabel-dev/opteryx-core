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
import time
import traceback
import zlib
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
    transfer_id_seq = 0

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
                "io_serialize_ns": 0,
                "io_rowgroup_slice_count": 0,
            }
            fragment_counts: List[int] = []
            payload_sizes: List[int] = []

            try:
                from opteryx.parquet_io.reader import _iter_row_groups_v2

                paths = command["paths"]
                column_names = command["column_names"]
                predicates = command.get("predicates")
                file_sizes = command.get("file_sizes")
                max_workers = int(command.get("max_workers", 16))
                connector = command.get("connector")
                query_id_hash = _stable_u64(str(command.get("query_id", "")))
                prefetched_footers = command.get("prefetched_footers") or {}

                slot_payload_bytes = slot_bytes - 256
                max_fragments = int(command["max_fragments_per_transfer"])
                target_slice_bytes = int(command["target_slice_bytes"])

                protocol = _resolve_protocol(paths, connector)
                filesystem = create_filesystem(protocol)

                for row_group in _iter_row_groups_v2(
                    filesystem,
                    paths,
                    column_names,
                    cache=InMemoryParquetCache(),
                    max_workers=max_workers,
                    decoder=None,
                    predicates=predicates,
                    file_sizes=file_sizes,
                    connector=connector,
                    prefetched_footers=prefetched_footers,
                ):
                    if cancel_event.is_set():
                        break

                    row_group_meta = {k: v for k, v in row_group.items() if k.startswith("__")}
                    columns = {k: v for k, v in row_group.items() if not k.startswith("__")}
                    if not columns:
                        continue

                    morsel = Morsel.from_vectors(list(columns.keys()), list(columns.values()))
                    payload_entries, serialize_ns = _slice_and_serialize(
                        morsel,
                        slot_payload_bytes=slot_payload_bytes,
                        max_fragments_per_transfer=max_fragments,
                        target_slice_bytes=target_slice_bytes,
                    )
                    metrics["io_serialize_ns"] += serialize_ns
                    metrics["io_rowgroup_slice_count"] += max(0, len(payload_entries) - 1)

                    for payload_entry in payload_entries:
                        transfer_id_seq += 1
                        transfer_id = transfer_id_seq
                        payload = payload_entry["payload"]
                        fragment_count = payload_entry["fragment_count"]
                        payload_sizes.append(len(payload))
                        fragment_counts.append(fragment_count)

                        path = str(row_group_meta.get("__path__", ""))
                        file_id_hash = _stable_u64(path)
                        rg_idx = int(row_group_meta.get("__row_group__", 0))
                        transfer_meta = dict(row_group_meta)
                        if payload_entry["slice_index"] > 0:
                            # Attribute underlying file-read and decode costs only once
                            # (first slice) so downstream totals remain correct.
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
                            metrics["io_ring_producer_full_wait_ns"] += wait_ns
                            metrics["io_ring_producer_full_wait_events"] += wait_events
                            ring.write_frame(
                                slot_id,
                                query_id_hash=query_id_hash,
                                transfer_id=transfer_id,
                                file_id_hash=file_id_hash,
                                row_group_index=rg_idx,
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

                metrics["io_transfer_fragment_count_p50"] = _percentile(fragment_counts, 0.5)
                metrics["io_transfer_fragment_count_p95"] = _percentile(fragment_counts, 0.95)
                metrics["io_transfer_fragment_count_max"] = (
                    max(fragment_counts) if fragment_counts else 0
                )
                metrics["io_transfer_payload_bytes_p50"] = _percentile(payload_sizes, 0.5)
                metrics["io_transfer_payload_bytes_p95"] = _percentile(payload_sizes, 0.95)
                metrics["io_transfer_payload_bytes_max"] = (
                    max(payload_sizes) if payload_sizes else 0
                )

                event_q.put(
                    {
                        "type": _EVENT_SCAN_COMPLETE,
                        "cancelled": bool(cancel_event.is_set()),
                        "metrics": metrics,
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
                event_q.put({"type": _EVENT_SCAN_COMPLETE, "cancelled": True, "metrics": metrics})
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
