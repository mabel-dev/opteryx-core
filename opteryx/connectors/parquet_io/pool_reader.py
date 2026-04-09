# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parquet row-group transport over an in-process MemoryPool with threaded IO.

ARCHITECTURE
============
1. Producer (_emit_loop): Serializes Morsel objects, writes directly into a
   reserved MemoryPool segment (reserve-and-write, zero intermediate copy),
   then signals consumer via event queue with ref_id.
2. MemoryPool: In-process auto-resizable memory pool. Data lives here;
   control (ref_id + metadata) travels on the event queue.
3. Consumer (iter_row_groups_pool): Reads ref_id from event queue, zero-copy
   reads from MemoryPool, deserializes morsel, releases pool segment.

THREADING MODEL
===============
- IO Worker (_io_worker): Daemon thread. Persistent thread pools for IO+decode.
- Emitter Thread (_emit_loop): Per-scan daemon thread.
- Consumer: Main thread.
"""

from __future__ import annotations

import ctypes
import io
import math
import queue
import threading
import time
import traceback
import zlib
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass, field
from multiprocessing import Event, Queue, get_context
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from opteryx import config as _cfg
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.compiled.draken.storage import read_morsel, write_morsel
from opteryx.compiled.structures.memory_pool import MemoryPool
from opteryx.connectors.io_systems import create_filesystem
from opteryx.connectors.parquet_io.cache import InMemoryParquetCache
from opteryx.connectors.parquet_io.predicates import row_group_may_satisfy

_EVENT_IO_READY = "io_ready"
_EVENT_ROWGROUP_READY = "rowgroup_ready"
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


@dataclass
class _CodecMetrics:
    """Track average decode cost per compression codec."""

    codec_name: str
    samples: deque = field(default_factory=lambda: deque(maxlen=100))
    avg_ns_per_byte: float = 0.0


def _record_decode_cost(
    codec_metrics: Dict[str, _CodecMetrics],
    codec: str,
    raw_bytes: int,
    decode_ns: int,
) -> None:
    """Record actual decode cost for a codec."""
    if not codec:
        codec = "UNKNOWN"
    if codec not in codec_metrics:
        codec_metrics[codec] = _CodecMetrics(codec_name=codec)

    metrics = codec_metrics[codec]
    if raw_bytes > 0:
        ns_per_byte = decode_ns / raw_bytes
        metrics.samples.append(ns_per_byte)
        if len(metrics.samples) >= 10:
            metrics.avg_ns_per_byte = sum(metrics.samples) / len(metrics.samples)


def _estimate_decode_cost(
    codec_metrics: Dict[str, _CodecMetrics],
    codec: str,
    raw_bytes: int,
) -> int:
    """Estimate decode cost in nanoseconds based on historical codec performance."""
    codec_defaults = {
        "SNAPPY": 100,
        "GZIP": 1000,
        "LZ4": 50,
        "ZSTD": 200,
        "PLAIN": 10,
        "RLE": 20,
        "DELTA": 30,
    }

    if codec in codec_metrics and codec_metrics[codec].avg_ns_per_byte > 0:
        rate = codec_metrics[codec].avg_ns_per_byte
    else:
        rate = codec_defaults.get(codec, 100)

    return int(raw_bytes * rate)


def _decode_column_name(name: bytes | str) -> str:
    if isinstance(name, bytes):
        return name.decode("utf8")
    return str(name)


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
    columns: Dict[str, Any] = field(default_factory=dict)


def _connector_to_protocol(connector: Optional[str]) -> str:
    if not connector:
        return ""
    norm = connector.strip().lower()
    if norm in ("gcs", "gs"):
        return "gs"
    if norm in ("file", "local", "filesystem"):
        return "file"
    return norm


def _resolve_protocol(paths: List[str], connector: Optional[str]) -> str:
    if paths and "://" in paths[0]:
        return paths[0].split("://", 1)[0].lower()
    return _connector_to_protocol(connector)


def _column_chunk_range(col_stats: dict) -> Tuple[int, int]:
    dict_off = col_stats.get("dictionary_page_offset")
    data_off = col_stats["data_page_offset"]
    if dict_off is not None and dict_off >= 0 and dict_off < data_off:
        base_offset = dict_off
    else:
        base_offset = data_off
    return base_offset, int(col_stats["total_compressed_size"])


def _resolve_decoder() -> Any:
    try:
        from opteryx.compiled.rugo.parquet import decode_column_from_chunk  # type: ignore[import]
    except ImportError:
        raise RuntimeError(
            "rugo.parquet is required but not available. "
            "Ensure rugo is compiled and in the Python path."
        )
    return decode_column_from_chunk


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
    codec_metrics: Optional[Dict[str, _CodecMetrics]] = None,
    scan_codec_metrics: Optional[Dict[str, _CodecMetrics]] = None,
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

    # Record decode cost for cost-aware dispatch ordering
    if _cfg.OPTERYX_TRACK_CODEC_METRICS:
        codec = work.stats.get("compression_codec", "PLAIN")
        if codec_metrics is not None:
            _record_decode_cost(codec_metrics, codec, len(raw_bytes), decode_ns)
        if scan_codec_metrics is not None:
            _record_decode_cost(scan_codec_metrics, codec, len(raw_bytes), decode_ns)

    return {
        "name": work.name,
        "decoded": decoded,
        "decode_ns": decode_ns,
        "queue_wait_ns": queue_wait_ns,
        "task_total_ns": time.monotonic_ns() - task_start_ns,
    }


def _emit_loop(
    ready_queue: "queue.Queue",
    pool: MemoryPool,
    event_q: Queue,
    cancel_event: Event,
    *,
    query_id_hash: int,
    metrics: dict,
    metrics_lock: threading.Lock,
) -> None:
    """
    Emit serialized row groups into the MemoryPool and signal the consumer.

    For each completed row group attempts reserve-and-write (zero intermediate
    copy): reserves a pool segment, writes the Morsel directly into it via
    _PoolWriter, then finalizes.  Falls back to commit(bytes) if the size
    estimate is too small or the reservation fails.
    """
    # Rolling size estimator for the reserve-and-write fast path.
    _size_history: deque = deque(maxlen=16)
    _RESERVE_INITIAL_BYTES = 8 * 1024 * 1024  # 8 MB conservative start

    def _reserve_estimate() -> int:
        if not _size_history:
            return _RESERVE_INITIAL_BYTES
        return max(_RESERVE_INITIAL_BYTES, int(max(_size_history) * 1.5))

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

            serialize_start_ns = time.monotonic_ns()

            # Reserve-and-write: attempt to write the morsel directly into a pool
            # segment without an intermediate bytes allocation (zero extra copy).
            # Falls back to commit(bytes) if the reservation fails or the size
            # estimate is too small.
            ref_id = -1
            actual_bytes = 0
            rsv_ref_id, ptr_int, capacity = pool.reserve_for_write_ptr(_reserve_estimate())
            if rsv_ref_id != -1:
                try:
                    ctypes_buf = (ctypes.c_char * capacity).from_address(ptr_int)
                    mv = memoryview(ctypes_buf)
                    result = write_morsel(mv, morsel)
                    actual_bytes = result["bytes_output"]
                    pool.finalize_commit(rsv_ref_id, actual_bytes)
                    ref_id = rsv_ref_id
                except ValueError:
                    # _PoolWriter overflow: estimate too small — release reserved
                    # segment and fall through to the commit(bytes) path.
                    pool.release(rsv_ref_id)
                except Exception:
                    pool.release(rsv_ref_id)
                    raise

            if ref_id == -1:
                # Fallback: serialize to bytes then commit into pool (one extra copy).
                data = write_morsel(None, morsel)
                actual_bytes = len(data)
                ref_id = pool.commit(data)
                if ref_id == -1:
                    cancel_event.set()
                    raise RuntimeError("MemoryPool exhausted: cannot commit row group data")

            serialize_ns = time.monotonic_ns() - serialize_start_ns
            _size_history.append(actual_bytes)

            with metrics_lock:
                metrics["io_serialize_ns"] += serialize_ns
                metrics["transfer_payload_sizes"].append(actual_bytes)

            metadata = {
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

            event = {
                "type": _EVENT_ROWGROUP_READY,
                "ref_id": ref_id,
                "row_group_meta": metadata,
            }
            try:
                event_q.put(event, timeout=0.1)
            except queue.Full:
                pool.release(ref_id)
                cancel_event.set()
                raise RuntimeError("Event queue full: consumer not keeping up")

    except Exception as err:
        error_event = {
            "type": _EVENT_TRANSFER_ERROR,
            "message": str(err),
            "traceback": traceback.format_exc(),
        }
        try:
            event_q.put(error_event, timeout=0.1)
        except queue.Full:
            pass
        cancel_event.set()


def _io_worker(
    pool: MemoryPool,
    command_q: Queue,  # type: ignore[type-arg]
    event_q: Queue,  # type: ignore[type-arg]
    cancel_event: Event,
) -> None:
    # Create persistent thread pools once at subprocess startup and reuse them
    # across all scans.  Per-scan creation/destruction of 64+ threads adds
    # measurable latency for high-frequency short queries.
    _worker_read_pool_size = max(1, int(_cfg.PARQUET_GLOBAL_RANGE_READERS))
    _worker_decode_pool_size = max(1, int(_cfg.PARQUET_DECODE_WORKERS))
    _persistent_read_pool = ThreadPoolExecutor(
        max_workers=_worker_read_pool_size,
        thread_name_prefix="io-ring-read",
    )
    _persistent_decode_pool = ThreadPoolExecutor(
        max_workers=_worker_decode_pool_size,
        thread_name_prefix="io-ring-decode",
    )

    # Filesystem cache: reuse instances across scans so that HTTP sessions
    # (and their underlying TCP connections) survive between queries.
    # For GCS, this preserves keep-alive connections, saving ~RTT per scan.
    _filesystem_by_protocol: Dict[str, Any] = {}

    # Codec metrics: track decode cost per compression codec across all scans
    # to enable cost-aware dispatch ordering
    codec_metrics: Dict[str, _CodecMetrics] = {}

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
                "io_transfer_ready_backlog_peak": 0,
                "io_transfer_emit_wait_ns": 0,
                "io_serialize_ns": 0,
                "ranges_in_flight_peak": 0,
                "active_files_peak": 0,
                "active_rowgroups_peak": 0,
                "row_groups_pruned": 0,
                "rowgroups_in_flight_cap": 0,
                "transfer_payload_sizes": [],
                "io_pool_commits": 0,
                "io_pool_bytes_committed": 0,
                "io_scheduler_empty_wait_events": 0,
                "io_scheduler_empty_wait_ns": 0,
            }

            # Per-scan codec metrics for cost-aware dispatch
            scan_codec_metrics: Dict[str, _CodecMetrics] = {}

            ready_queue_cap = max(2, int(_cfg.PARQUET_READY_ROWGROUP_QUEUE_CAP))
            ready_queue: "queue.Queue[_IORowGroupState | None]" = queue.Queue(
                maxsize=ready_queue_cap
            )
            metrics_lock = threading.Lock()
            emitter: Optional[threading.Thread] = None
            read_pool: ThreadPoolExecutor = _persistent_read_pool
            decode_pool: ThreadPoolExecutor = _persistent_decode_pool
            read_futures: Dict[Future, tuple[tuple[int, int], _IOColumnWork]] = {}
            decode_futures: Dict[Future, tuple[tuple[int, int], _IOColumnWork]] = {}
            decode_pending: deque[tuple[tuple[int, int], _IOColumnWork, bytes]] = deque()

            try:
                from opteryx import config as _trace_cfg
                from opteryx.connectors.parquet_io.reader import (
                    _parse_footer_envelope,
                    _read_footer_payload,
                )
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
                read_queue_cap = max(1, int(_cfg.PARQUET_READ_QUEUE_CAP or global_ranges_cap))
                decode_queue_cap = max(
                    read_queue_cap * 2, int(_cfg.PARQUET_DECODE_QUEUE_CAP or (read_queue_cap * 2))
                )

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

                protocol = _resolve_protocol(paths, connector)
                if protocol not in _filesystem_by_protocol:
                    _filesystem_by_protocol[protocol] = create_filesystem(protocol)
                filesystem = _filesystem_by_protocol[protocol]
                decoder_fn = _resolve_decoder()

                unique_paths = list(dict.fromkeys(paths))
                footers: Dict[str, dict] = {}
                footer_fetch_ns: Dict[str, int] = {}

                # Fetch footer payloads in parallel (pure IO), then parse on
                # this thread (rugo C++ parse must not cross thread boundaries).
                _footer_io_futures: Dict[Future, str] = {}
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
                        fut = _persistent_read_pool.submit(
                            _read_footer_payload, filesystem, p, connector=connector
                        )
                    else:
                        fut = _persistent_read_pool.submit(
                            _read_footer_payload, filesystem, p, known_size, connector
                        )
                    _footer_io_futures[fut] = p

                for fut in as_completed(_footer_io_futures):
                    p = _footer_io_futures[fut]
                    envelope, footer_bytes, fetch_ns = fut.result()
                    parse_start_ns = time.monotonic_ns()
                    meta = _parse_footer_envelope(p, envelope, footer_bytes)
                    parse_ns = time.monotonic_ns() - parse_start_ns
                    footers[p] = meta
                    footer_fetch_ns[p] = fetch_ns + parse_ns

                    # Emit file_discovered event after footer is successfully parsed
                    if _trace_cfg.OPTERYX_TRACE:
                        file_kwargs = {"file_id": p}
                        if connector:
                            file_kwargs["connector"] = connector
                        # file_size is optional metadata
                        if file_sizes and p in file_sizes and file_sizes[p] > 0:
                            file_kwargs["size_bytes"] = file_sizes[p]
                        record_event("file_discovered", **file_kwargs)

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
                    args=(ready_queue, pool, event_q, cancel_event),
                    kwargs={
                        "query_id_hash": query_id_hash,
                        "metrics": metrics,
                        "metrics_lock": metrics_lock,
                    },
                    daemon=True,
                )
                emitter.start()

                # Pools are persistent (created at subprocess startup); no per-scan creation.

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
                    """Pick next row group to dispatch, using cost-aware ordering.

                    Prioritizes by estimated decode cost (cost = size * codec_rate),
                    then by size (tie-breaker), then by admission order (oldest first).

                    This reduces queue depth variance by processing hard problems early.
                    """
                    nonlocal warm_start_remaining

                    # Warm-start: prioritize first row group
                    if warm_start_remaining > 0 and first_rowgroup_key in active_states:
                        first_state = active_states[first_rowgroup_key]
                        if first_state.pending_columns and first_state.in_flight < per_rowgroup_cap:
                            warm_start_remaining -= 1
                            return first_rowgroup_key, first_state

                    # Build candidates with cost estimates
                    candidates = []
                    for key, state in active_states.items():
                        if not state.pending_columns or state.in_flight >= per_rowgroup_cap:
                            continue

                        col = state.pending_columns[0]
                        codec = col.stats.get("compression_codec", "PLAIN")
                        cost = _estimate_decode_cost(scan_codec_metrics, codec, col.length)

                        candidates.append((cost, col.length, -state.admitted_ns, key, state))

                    if not candidates:
                        return None

                    # Sort by cost (highest first to prioritize fast ones)
                    candidates.sort(reverse=True, key=lambda x: (x[0], x[1], x[2]))
                    _, _, _, key, state = candidates[0]
                    return key, state

                def _dispatch_columns() -> int:
                    nonlocal reads_in_flight
                    dispatched = 0
                    while reads_in_flight < read_queue_cap and not cancel_event.is_set():
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
                        len(decode_pending) + len(decode_futures) < decode_queue_cap
                        and decode_pending
                        and not cancel_event.is_set()
                    ):
                        key, work, raw_bytes = decode_pending.popleft()
                        state = active_states.get(key)
                        if state is None:
                            continue

                        # Emit buffer_complete event when buffered data is about to be decoded
                        if _trace_cfg.OPTERYX_TRACE:
                            buf_kwargs = {
                                "file_id": state.path,
                                "component": "column",
                                "rg_idx": state.rg_idx,
                                "column": work.name,
                            }
                            if connector:
                                buf_kwargs["connector"] = connector
                            record_event("buffer_complete", **buf_kwargs)

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
                            codec_metrics,
                            scan_codec_metrics,
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
                                metrics["io_scheduler_empty_wait_events"] += 1
                                metrics["io_scheduler_empty_wait_ns"] += (
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
                            metrics["io_scheduler_empty_wait_events"] += 1
                            metrics["io_scheduler_empty_wait_ns"] += (
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

                            # Emit buffer_start event when column is queued for decode
                            if _trace_cfg.OPTERYX_TRACE:
                                buf_kwargs = {
                                    "file_id": state.path,
                                    "component": "column",
                                    "rg_idx": state.rg_idx,
                                    "column": work.name,
                                    "bytes": len(result["raw_bytes"]),
                                }
                                if connector:
                                    buf_kwargs["connector"] = connector
                                record_event("buffer_start", **buf_kwargs)

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
                metrics.pop("transfer_payload_sizes", None)
                event_q.put({"type": _EVENT_SCAN_COMPLETE, "cancelled": True, "metrics": metrics})
            finally:
                # Cancel in-flight futures; do NOT shut down the persistent pools
                # since they are reused across all scans in this subprocess.
                for fut in list(read_futures):
                    fut.cancel()
                for fut in list(decode_futures):
                    fut.cancel()


def _build_row_group_from_payload(payload: bytes, metadata: dict) -> Tuple[Dict[str, Any], int]:
    start_ns = time.monotonic_ns()
    mv = memoryview(payload) if not isinstance(payload, memoryview) else payload
    morsel = read_morsel(mv)
    deserialize_ns = time.monotonic_ns() - start_ns

    row_group: Dict[str, Any] = {}
    for col_name in morsel.column_names:
        key = _decode_column_name(col_name)
        raw_name = col_name if isinstance(col_name, bytes) else str(col_name).encode("utf8")
        row_group[key] = morsel.column(raw_name)
    row_group.update(metadata)
    return row_group, deserialize_ns


def iter_row_groups_pool(
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
    Threaded pool-based row-group iterator.

    Runs a persistent IO worker thread with a MemoryPool transport. Yields
    ``Dict[column_name -> DrakenVector]`` plus ``__*`` metadata, matching the
    contract of parquet_io.reader.iter_row_groups().
    """
    pool_size = int(_cfg.IO_POOL_SLOT_BYTES) * int(_cfg.IO_POOL_SLOT_COUNT)
    pool = MemoryPool(size=pool_size, name="parquet-io-pool", auto_resize=True, alignment=8)

    # Run IO worker in-process using a thread (replace process-based worker)
    # Note: we keep the same queue/event semantics but use thread-safe primitives
    # so the rest of the logic (command_q.get(), event_q.put(), cancel_event.wait(), etc.)
    # continues to work with minimal changes.
    command_q: queue.Queue = queue.Queue()
    event_q: queue.Queue = queue.Queue()
    cancel_event: threading.Event = threading.Event()

    worker = threading.Thread(
        target=_io_worker,
        args=(pool, command_q, event_q, cancel_event),
        daemon=True,
        name="io-worker-thread",
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
                "prefetched_footers": prefetched_footers,
            }
        )

        scan_complete = False
        while True:
            if scan_complete:
                if pending_row_group is not None:
                    # Attach scan-level transport metrics to the final emitted row group.
                    pending_row_group["__io_consumer_empty_wait_ns__"] += (
                        consumer_empty_wait_ns - consumer_empty_wait_ns_emitted
                    )
                    pending_row_group["__io_consumer_empty_wait_events__"] += (
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
                # Queue empty: emitter backlog is drained by emit_loop in worker.
                consumer_empty_wait_events += 1
                consumer_empty_wait_ns += int(poll_timeout_s * 1_000_000_000)
                if scan_complete:
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

            if event_type != _EVENT_ROWGROUP_READY:
                continue

            ref_id = int(event["ref_id"])
            metadata = dict(event.get("row_group_meta") or {})

            deserialize_start_ns = time.monotonic_ns()
            mv = pool.read(ref_id, zero_copy=True, latch=True)
            morsel = read_morsel(mv)
            pool.unlatch(ref_id)
            pool.release(ref_id)
            deserialize_ns = time.monotonic_ns() - deserialize_start_ns

            row_group: Dict[str, Any] = {}
            for col_name in morsel.column_names:
                key = _decode_column_name(col_name)
                raw_name = col_name if isinstance(col_name, bytes) else str(col_name).encode("utf8")
                row_group[key] = morsel.column(raw_name)
            row_group.update(metadata)

            row_group["__io_deserialize_ns__"] = deserialize_ns
            row_group["__io_consumer_empty_wait_ns__"] = (
                consumer_empty_wait_ns - consumer_empty_wait_ns_emitted
            )
            row_group["__io_consumer_empty_wait_events__"] = (
                consumer_empty_wait_events - consumer_empty_wait_events_emitted
            )
            row_group["__io_transfer_emit_wait_ns__"] = (
                transfer_emit_wait_ns - transfer_emit_wait_ns_emitted
            )
            row_group["__io_transfer_ready_backlog_peak__"] = transfer_ready_backlog_peak
            consumer_empty_wait_ns_emitted = consumer_empty_wait_ns
            consumer_empty_wait_events_emitted = consumer_empty_wait_events
            transfer_emit_wait_ns_emitted = transfer_emit_wait_ns

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

        # Thread-based worker: join and best-effort stop. Threads cannot be
        # forcibly terminated; ensure the worker checks cancel_event periodically.
        worker.join(timeout=5)
        if worker.is_alive():
            # No graceful way to terminate a Python thread; log situation and continue.
            # The worker should exit after seeing cancel_event. If it doesn't, the
            # process will continue and resources will be reclaimed at process exit.
            pass
