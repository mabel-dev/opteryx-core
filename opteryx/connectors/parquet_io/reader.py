"""
Parquet column-chunk reader orchestration.

Stateless functions that coordinate:
1. filesystem.get_file_info() for file size (footer reads only)
2. filesystem.read_ranges() for raw bytes
3. rugo.parquet.read_metadata_from_bytes() for footer parsing
4. rugo.parquet.decode_column_from_chunk() for column decoding
5. Pluggable caching layer

No internal state — cache is passed in by caller.

Assembly model
--------------
The unit of work yielded to downstream operators is one row group:
    (path, rg_idx) -> Dict[column_name -> DrakenVector]

``iter_row_groups()`` parallelises across both files and row groups:
    Phase 1 — fetch footers for all paths concurrently (two range reads each).
    Phase 2 — fan out all (path, rg_idx) units to a thread pool; each unit
              batches all column-chunk ranges into one read_ranges() call,
              then decodes. Assembled row groups are yielded in completion
              order via concurrent.futures.as_completed().
"""

from __future__ import annotations

import heapq
import os
import struct
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import Future
from concurrent.futures import as_completed
from concurrent.futures import wait
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Deque
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

from opteryx.connectors.parquet_io.cache import InMemoryParquetCache
from opteryx.connectors.parquet_io.cache import ParquetCache
from opteryx.connectors.parquet_io.predicates import row_group_may_satisfy
from opteryx.connectors.parquet_io.thread_pool_manager import LazyPoolProxy
from opteryx.connectors.parquet_io.thread_pool_manager import get_decode_pool
from opteryx.connectors.parquet_io.thread_pool_manager import get_range_pool


class ListColumnError(ValueError):
    """Raised when a column's decoded length doesn't match the row group row count.

    This happens for LIST/REPEATED Parquet columns: ``decode_column_from_chunk``
    returns the flat leaf values (N elements) rather than the outer list structure
    (M rows where M < N).  Callers should fall back to whole-file reading.
    """


# Parquet magic constant.
_PARQUET_MAGIC = b"PAR1"
# Number of bytes at end of file that hold (footer_length: uint32 LE, magic: 4B).
_PARQUET_FOOTER_SUFFIX = 8
# Speculative tail read size: covers the footer for the vast majority of real-world Parquet
# files in a single read_ranges() call.  Files with footers larger than this fall back to a
# second targeted read.
_FOOTER_PREFETCH = 65536
# Coalesce small gaps between nearby column chunks into fewer range reads.
# This reduces request overhead on object storage with negligible over-read.
_RANGE_COALESCE_MAX_GAP_BYTES = 64
_RANGE_COALESCE_MAX_SPAN_BYTES = 32 * 1024 * 1024
_LOCAL_SERIAL_COMBINE_READ_RATIO = 0.5

# Module-level thread pools shared across all queries.
# These are lazy-initialized via thread_pool_manager to support both
# C++ and Python backends with automatic fallback.


def _get_range_pool():
    """Get the range read pool (32 workers for v1 scheduler)."""
    return get_range_pool(name="parquet-range", max_workers=32)


def _get_decode_pool():
    """Get the decode pool (cpu_count workers for column decoding).

    CPU-bound decode pool — one worker per physical core (no artificial cap).
    Each column decode calls into C++ (nogil), so threads run truly in parallel
    across all available cores.
    Kept separate from _RANGE_POOL to avoid deadlocks when row-group tasks
    (submitted to _RANGE_POOL) try to submit per-column decode tasks.
    """
    return get_decode_pool(max_workers=os.cpu_count() or 4)


def _get_range_pool_v2():
    """Get the large-capacity IO pool for v2 scheduler (64 workers).

    The v2 scheduler issues up to PARQUET_GLOBAL_RANGE_READERS (default 64)
    concurrent range reads. _RANGE_POOL (32 workers) is not enough; a separate
    pool avoids per-query thread creation for the common case.
    """
    return get_range_pool(name="parquet-range-v2", max_workers=64)


# Module-level thread pools: lazy proxies that always defer to thread_pool_manager cache.
# This ensures that even if pools are shut down (e.g., in tests), the proxies will
# get the fresh recreated pool from the cache on next access.
_RANGE_POOL = LazyPoolProxy(_get_range_pool)
_DECODE_POOL = LazyPoolProxy(_get_decode_pool)
_RANGE_POOL_V2 = LazyPoolProxy(_get_range_pool_v2)


def _read_footer_payload(
    filesystem: Any,
    path: str,
    file_size: Optional[int] = None,
    connector: Optional[str] = None,
) -> Tuple[bytes, int, int]:
    """Fetch footer bytes and build a parseable envelope.

    Issues a single speculative tail read of _FOOTER_PREFETCH bytes that covers
    both the 8-byte suffix and the footer data for the vast majority of Parquet
    files.  Falls back to a second dedicated read only when the footer is larger
    than the prefetch window.

    Returns:
        tuple(envelope, bytes_fetched, elapsed_ns)
    """
    # trace footer IO
    from opteryx.tracing import record_event

    from opteryx import config as _cfg

    if _cfg.OPTERYX_TRACE:
        kwargs = {"file_id": path, "component": "footer"}
        if connector:
            kwargs["connector"] = connector
        record_event("download_start", **kwargs)

    start_ns = time.monotonic_ns()

    # Step 1: resolve file size (from manifest when available, otherwise stat call).
    if file_size is None or file_size <= 0:
        file_info = filesystem.get_file_info(path)
        file_size = file_info.size
    if file_size < _PARQUET_FOOTER_SUFFIX:
        raise ValueError(f"File {path!r} is too small to be a valid Parquet file ({file_size} B)")

    # Step 2: speculative tail read — one call for both suffix and footer data.
    prefetch_size = min(_FOOTER_PREFETCH, file_size)
    prefetch_offset = file_size - prefetch_size
    (tail_bytes,) = filesystem.read_ranges(path, [(prefetch_offset, prefetch_size)])

    magic = tail_bytes[-4:]
    if magic != _PARQUET_MAGIC:
        raise ValueError(
            f"File {path!r} does not end with Parquet magic bytes "
            f"(got {magic!r}, expected {_PARQUET_MAGIC!r})"
        )
    (footer_length,) = struct.unpack_from(
        "<I", tail_bytes, len(tail_bytes) - _PARQUET_FOOTER_SUFFIX
    )

    if footer_length == 0 or footer_length > file_size - _PARQUET_FOOTER_SUFFIX:
        raise ValueError(
            f"Invalid footer length {footer_length} in {path!r} (file_size={file_size})"
        )

    total_footer_payload = footer_length + _PARQUET_FOOTER_SUFFIX
    if total_footer_payload <= prefetch_size:
        # Common path: footer fits within the prefetch buffer — slice it out directly.
        footer_start = len(tail_bytes) - total_footer_payload
        footer_bytes_data = tail_bytes[footer_start : footer_start + footer_length]
        bytes_fetched = prefetch_size
    else:
        # Fallback: footer exceeds the prefetch window; issue one more targeted read.
        footer_offset = file_size - _PARQUET_FOOTER_SUFFIX - footer_length
        (footer_bytes_data,) = filesystem.read_ranges(path, [(footer_offset, footer_length)])
        bytes_fetched = prefetch_size + footer_length

    # write completion event
    if _cfg.OPTERYX_TRACE:
        kwargs = {
            "file_id": path,
            "component": "footer",
            "bytes_received": bytes_fetched,
        }
        if connector:
            kwargs["connector"] = connector
        record_event("download_complete", **kwargs)

    # Step 3: wrap footer bytes in a minimal Parquet envelope.
    # read_metadata_from_bytes expects: PAR1 + thrift_footer + uint32_LE(len) + PAR1.
    envelope = (
        _PARQUET_MAGIC + footer_bytes_data + struct.pack("<I", footer_length) + _PARQUET_MAGIC
    )
    return envelope, bytes_fetched, (time.monotonic_ns() - start_ns)


def _parse_footer_envelope(path: str, envelope: bytes, footer_bytes: int) -> dict:
    """Parse a footer envelope with rugo and return metadata dict."""
    try:
        from opteryx.compiled.rugo import parquet as rugo_parquet
    except ImportError:
        raise RuntimeError(
            "rugo.parquet is required but not available. "
            "Ensure rugo is compiled and in the Python path."
        )

    try:
        meta = rugo_parquet.read_metadata_from_bytes(envelope)
    except Exception as exc:
        raise RuntimeError(f"Failed to parse Parquet footer from {path!r}: {exc}") from exc

    meta["__footer_bytes__"] = footer_bytes
    return meta


def fetch_footer(
    filesystem: Any,
    path: str,
    cache: Optional[ParquetCache] = None,
    file_size: Optional[int] = None,
    connector: Optional[str] = None,
) -> dict:
    """Fetch and parse the Parquet footer for *path*, optionally caching the result.

    Instrumentation notes
    ---------------------
    A simple download-start/complete pair is recorded for the footer read so
    that the visualization can show the two small range reads that precede
    any column I/O.
    """
    """Fetch and parse the Parquet footer for *path*, optionally caching the result.

    Uses two range reads:
      1. Last 8 bytes  -> footer length (uint32 LE) + magic ("PAR1").
      2. Footer bytes  -> thrift-encoded FileMetaData, parsed by rugo.

    File size is obtained from ``filesystem.get_file_info(path).size``.

    Args:
        filesystem: Any object with ``get_file_info(path)`` and
                    ``read_ranges(path, ranges) -> List[bytes]``.
        path: Parquet file path (interpreted by filesystem).
        cache: Optional ParquetCache instance.  If ``None``, no caching.

    Returns:
        Parsed footer dict with row group and column chunk metadata.

    Raises:
        ValueError: File is smaller than 8 bytes or magic bytes are wrong.
        RuntimeError: rugo.parquet unavailable or footer parse failure.
    """
    if cache is not None:
        cached = cache.get_footer(path)
        if cached is not None:
            return cached

    if file_size is None:
        envelope, footer_bytes, _ = _read_footer_payload(filesystem, path, connector=connector)
    else:
        envelope, footer_bytes, _ = _read_footer_payload(filesystem, path, file_size, connector)
    meta = _parse_footer_envelope(path, envelope, footer_bytes)

    if cache is not None:
        cache.set_footer(path, meta)
    return meta


def fetch_columns(
    filesystem: Any,
    path: str,
    rg_idx: int,
    column_names: List[str],
    cache: Optional[ParquetCache] = None,
    decoder: Optional[Any] = None,
    connector: Optional[str] = None,
    row_mask=None,
) -> Dict[str, Any]:
    """
    Fetch and decode specific column chunks from a row group.

    High-level orchestration:
    1. Fetch footer (cached).
    2. Resolve column stats from row group metadata.
    3. Check column cache for hits.
    4. For misses: compute byte ranges and batch read_ranges().
    5. Decode each missing column via decoder (default: rugo).
    6. Populate column cache.
    7. Return dict of decoded columns.

    Args:
        filesystem: Any object with `read_ranges(path, ranges) → List[bytes]`.
        path: Parquet file path.
        rg_idx: Row group index (0-based).
        column_names: List of column names to fetch.
        cache: Optional ParquetCache. If None, uses InMemoryParquetCache.
        decoder: Optional decoder (default: rugo.parquet.decode_column_from_chunk).
                 Must be callable(raw_bytes, col_stats_dict) → decoded_vector.

    Returns:
        Dict[column_name] → decoded vector (Draken or equivalent).

    Raises:
        KeyError: Column name not found in row group.
        RuntimeError: Decode failure or missing rugo.
    """
    if cache is None:
        cache = InMemoryParquetCache()

    if decoder is None:
        try:
            from opteryx.compiled.rugo import parquet as rugo_parquet

            decoder = rugo_parquet.decode_column_from_chunk
        except ImportError:
            raise RuntimeError(
                "rugo.parquet is required but not available. "
                "Ensure rugo is compiled and in the Python path."
            )

    # Fetch footer (may be cached)
    meta = fetch_footer(filesystem, path, cache=cache)

    # Validate row group index
    if rg_idx < 0 or rg_idx >= len(meta["row_groups"]):
        raise IndexError(f"Row group {rg_idx} out of range [0, {len(meta['row_groups'])})")

    rg_meta = meta["row_groups"][rg_idx]

    # Build column name → stats dict for fast lookup
    name_to_stats: Dict[str, dict] = {col["name"]: col for col in rg_meta["columns"]}

    # Separate cache hits from misses
    results: Dict[str, Any] = {}
    misses: List[str] = []
    bytes_fetched: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    range_request_count: int = 0
    range_bytes_requested: int = 0
    time_read_ranges_ns: int = 0
    time_decode_columns_ns: int = 0

    for col_name in column_names:
        if col_name not in name_to_stats:
            raise KeyError(
                f"Column '{col_name}' not found in row group {rg_idx}. "
                f"Available columns: {list(name_to_stats.keys())}"
            )

        # Check cache
        cached = cache.get_column(path, rg_idx, col_name)
        if cached is not None:
            results[col_name] = cached
            cache_hits += 1
        else:
            misses.append(col_name)
            cache_misses += 1

    # Snapshot rugo page counters before decode so we can compute per-call
    # deltas only (counters are cumulative across the process lifetime).
    _pages_skipped_before: int = 0
    _pages_decoded_before: int = 0
    if row_mask is not None and misses:
        from opteryx.compiled.rugo import parquet as _rugo_parquet

        _tel_before = _rugo_parquet.get_telemetry()
        _pages_skipped_before = _tel_before.get("parquet_pages_skipped", 0)
        _pages_decoded_before = _tel_before.get("parquet_pages_decoded", 0)

    # Batch-read missing column chunks
    if misses:
        # Compute byte ranges for each missed column
        ranges: List[Tuple[int, int]] = []
        for col_name in misses:
            col_stats = name_to_stats[col_name]

            # Column chunk may start with dictionary page (if present)
            dict_off = col_stats.get("dictionary_page_offset")
            data_off = col_stats["data_page_offset"]

            # Start from dictionary page if it exists and comes before data
            if dict_off is not None and dict_off >= 0 and dict_off < data_off:
                base_offset = dict_off
            else:
                base_offset = data_off

            compressed_size = col_stats["total_compressed_size"]
            range_bytes_requested += compressed_size
            ranges.append((base_offset, compressed_size))
        coalesced_ranges, coalesced_parts = _coalesce_ranges(ranges)
        range_request_count = len(coalesced_ranges)

        # Batch read all missing column chunks
        read_start_ns = time.monotonic_ns()
        # record download of column-batch
        from opteryx.tracing import record_event

        from opteryx import config as _cfg

        if _cfg.OPTERYX_TRACE:
            kwargs = {
                "file_id": path,
                "component": "columns",
                "rg_idx": rg_idx,
                "columns": misses,
                "ranges": len(coalesced_ranges),
            }
            if connector:
                kwargs["connector"] = connector
            record_event("download_start", **kwargs)

        merged_raw_buffers = filesystem.read_ranges(path, coalesced_ranges)
        time_read_ranges_ns += time.monotonic_ns() - read_start_ns
        bytes_fetched += sum(len(b) for b in merged_raw_buffers)
        raw_buffers = _split_coalesced_buffers(merged_raw_buffers, coalesced_parts, len(misses))

        if _cfg.OPTERYX_TRACE:
            kwargs = {
                "file_id": path,
                "component": "columns",
                "rg_idx": rg_idx,
                "columns": misses,
                "bytes_received": bytes_fetched,
            }
            if connector:
                kwargs["connector"] = connector
            record_event("download_complete", **kwargs)

        # Decode each chunk in parallel — columns are independent and the
        # C++ decoder releases the GIL, so true multi-core parallelism applies.
        def _decode_one(col_name: str, raw_bytes: bytes) -> tuple:
            _col_stats = name_to_stats[col_name]
            if _cfg.OPTERYX_TRACE:
                _kwargs = {
                    "file_id": path,
                    "component": "column",
                    "rg_idx": rg_idx,
                    "column": col_name,
                }
                if connector:
                    _kwargs["connector"] = connector
                record_event("decode_start", **_kwargs)
            decoded = (
                decoder(raw_bytes, _col_stats)
                if row_mask is None
                else decoder(raw_bytes, _col_stats, row_mask)
            )
            if decoded is None:
                raise RuntimeError(
                    f"Decoder returned None for column '{col_name}' "
                    f"(codec={_col_stats.get('compression_codec')}, "
                    f"encodings={_col_stats.get('encodings')})"
                )
            if _cfg.OPTERYX_TRACE:
                _kwargs = {
                    "file_id": path,
                    "component": "column",
                    "rg_idx": rg_idx,
                    "column": col_name,
                    "rows_decoded": getattr(decoded, "num_rows", None) or 0,
                }
                if connector:
                    _kwargs["connector"] = connector
                record_event("decode_complete", **_kwargs)
            return col_name, decoded

        decode_start_ns = time.monotonic_ns()
        if len(misses) == 1:
            # Single column: skip pool overhead.
            col_name = misses[0]
            try:
                col_name, decoded = _decode_one(col_name, raw_buffers[0])
            except RuntimeError:
                raise
            except Exception as e:
                raise RuntimeError(
                    f"Failed to decode column '{path}:{rg_idx}:{col_name}': {e}"
                ) from e
            cache.set_column(path, rg_idx, col_name, decoded)
            results[col_name] = decoded
        else:
            # Multiple columns: decode in parallel via dedicated CPU pool.
            decode_futures = {
                _DECODE_POOL.submit(_decode_one, cn, rb): cn for cn, rb in zip(misses, raw_buffers)
            }
            for fut in as_completed(decode_futures):
                cn = decode_futures[fut]
                try:
                    cn, decoded = fut.result()
                except RuntimeError:
                    raise
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to decode column '{path}:{rg_idx}:{cn}': {e}"
                    ) from e
                cache.set_column(path, rg_idx, cn, decoded)
                results[cn] = decoded
        time_decode_columns_ns = time.monotonic_ns() - decode_start_ns

    result_dict = {col_name: results[col_name] for col_name in column_names}
    result_dict["__bytes_fetched__"] = bytes_fetched
    result_dict["__range_request_count__"] = range_request_count
    result_dict["__range_bytes_requested__"] = range_bytes_requested
    result_dict["__time_read_ranges_ns__"] = time_read_ranges_ns
    result_dict["__time_decode_columns_ns__"] = time_decode_columns_ns
    result_dict["__cache_column_hits__"] = cache_hits
    result_dict["__cache_column_misses__"] = cache_misses
    if row_mask is not None:
        from opteryx.compiled.rugo import parquet as _rugo_parquet

        _tel_after = _rugo_parquet.get_telemetry()
        result_dict["__pages_skipped__"] = (
            _tel_after.get("parquet_pages_skipped", 0) - _pages_skipped_before
        )
        result_dict["__pages_decoded__"] = (
            _tel_after.get("parquet_pages_decoded", 0) - _pages_decoded_before
        )
    return result_dict


def _fetch_columns_task(
    submitted_ns: int,
    filesystem: Any,
    path: str,
    rg_idx: int,
    column_names: List[str],
    cache: ParquetCache,
    decoder: Optional[Any] = None,
    connector: Optional[str] = None,
) -> Dict[str, Any]:
    """Wrapper around ``fetch_columns`` to capture pool queue and task timings.

    Also emits high‑level row-group decode events for tracing so that the
    waterfall tool can collapse per-column activity if desired.
    """
    from opteryx.tracing import record_event

    from opteryx import config as _cfg

    start_ns = time.monotonic_ns()

    if _cfg.OPTERYX_TRACE:
        kwargs = {
            "file_id": path,
            "component": "rowgroup",
            "rg_idx": rg_idx,
            "columns": column_names,
        }
        if connector:
            kwargs["connector"] = connector
        record_event("decode_start", **kwargs)

    row_group = fetch_columns(
        filesystem, path, rg_idx, column_names, cache, decoder=decoder, connector=connector
    )

    if _cfg.OPTERYX_TRACE:
        kwargs = {
            "file_id": path,
            "component": "rowgroup",
            "rg_idx": rg_idx,
            "rows_decoded": sum(
                (getattr(v, "num_rows", 0) for k, v in row_group.items() if not k.startswith("__"))
            ),
        }
        if connector:
            kwargs["connector"] = connector
        record_event("decode_complete", **kwargs)

    row_group["__task_queue_wait_ns__"] = start_ns - submitted_ns
    row_group["__task_total_ns__"] = time.monotonic_ns() - start_ns
    return row_group


@dataclass
class _ColumnWorkItem:
    name: str
    stats: dict
    offset: int
    length: int


@dataclass
class _FileState:
    file_seq: int
    path: str
    total_rowgroups: int = 0
    footer_ready: bool = False
    next_rg_idx: int = 0
    active_rowgroups: int = 0


@dataclass
class _RowGroupState:
    file_seq: int
    path: str
    rg_idx: int
    admission_seq: int
    admitted_ns: int
    column_order: List[str]
    # All column work items for this row group — dispatched as one batched read.
    column_work: List[_ColumnWorkItem] = field(default_factory=list)
    columns: Dict[str, Any] = field(default_factory=dict)
    dispatched: bool = False
    first_dispatch_ns: Optional[int] = None
    completed_ns: Optional[int] = None
    queued_for_emit: bool = False
    bytes_fetched: int = 0
    range_request_count: int = 0
    range_bytes_requested: int = 0
    time_read_ranges_ns: int = 0
    time_decode_columns_ns: int = 0
    cache_misses: int = 0
    task_queue_wait_ns: int = 0
    task_total_ns: int = 0
    ready_queue_depth_at_ready: int = 0
    pending_cols: int = 0
    decode_started: bool = False

    @property
    def key(self) -> Tuple[int, int]:
        return (self.file_seq, self.rg_idx)


def _resolve_decoder(decoder: Optional[Any]) -> Any:
    if decoder is not None:
        return decoder
    try:
        from opteryx.compiled.rugo import parquet as rugo_parquet
    except ImportError:
        raise RuntimeError(
            "rugo.parquet is required but not available. "
            "Ensure rugo is compiled and in the Python path."
        )
    return rugo_parquet.decode_column_from_chunk


def _column_chunk_range(col_stats: dict) -> Tuple[int, int]:
    dict_off = col_stats.get("dictionary_page_offset")
    data_off = col_stats["data_page_offset"]
    if dict_off is not None and dict_off >= 0 and dict_off < data_off:
        base_offset = dict_off
    else:
        base_offset = data_off
    return base_offset, col_stats["total_compressed_size"]


def _coalesce_ranges(
    ranges: List[Tuple[int, int]],
) -> Tuple[List[Tuple[int, int]], List[List[Tuple[int, int, int]]]]:
    """Merge nearby byte ranges and return slicing maps back to original order."""
    if not ranges:
        return [], []

    indexed = sorted(enumerate(ranges), key=lambda item: item[1][0])
    merged: List[Dict[str, Any]] = []

    for original_idx, (offset, length) in indexed:
        if not merged:
            merged.append(
                {
                    "offset": offset,
                    "length": length,
                    "parts": [(original_idx, 0, length)],
                }
            )
            continue

        last = merged[-1]
        last_offset = last["offset"]
        last_end = last_offset + last["length"]
        this_end = offset + length
        gap = offset - last_end
        next_span = max(last_end, this_end) - last_offset

        if (
            gap >= 0
            and gap <= _RANGE_COALESCE_MAX_GAP_BYTES
            and next_span <= _RANGE_COALESCE_MAX_SPAN_BYTES
        ):
            last["parts"].append((original_idx, offset - last_offset, length))
            last["length"] = next_span
            continue

        merged.append(
            {
                "offset": offset,
                "length": length,
                "parts": [(original_idx, 0, length)],
            }
        )

    merged_ranges = [(entry["offset"], entry["length"]) for entry in merged]
    merged_parts = [entry["parts"] for entry in merged]
    return merged_ranges, merged_parts


def _split_coalesced_buffers(
    merged_buffers: List[bytes],
    merged_parts: List[List[Tuple[int, int, int]]],
    expected_parts: int,
) -> List[memoryview]:
    """Expand coalesced range buffers back into the original range order.

    Returns memoryview slices rather than bytes slices — the slice is zero-copy
    because it references the merged buffer directly. The merged buffer stays
    alive as long as any slice view holds a reference to it.
    """
    expanded: List[memoryview] = [memoryview(b"")] * expected_parts
    for buffer, parts in zip(merged_buffers, merged_parts):
        mv = memoryview(buffer)
        for original_idx, rel_offset, length in parts:
            expanded[original_idx] = mv[rel_offset : rel_offset + length]
    return expanded


def _read_rowgroup_task(
    filesystem: Any,
    path: str,
    rg_idx: int,
    column_work: List[_ColumnWorkItem],
    submitted_ns: int,
    connector: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetch all column chunks of one row group in a single batched read_ranges() call."""
    task_start_ns = time.monotonic_ns()
    queue_wait_ns = task_start_ns - submitted_ns

    # Batch all column-chunk ranges into one vectored I/O call.
    ranges = [(work.offset, work.length) for work in column_work]

    # tracing: download start for this row group
    from opteryx.tracing import record_event

    from opteryx import config as _cfg

    if _cfg.OPTERYX_TRACE:
        kwargs = {"file_id": path, "component": "columns", "rg_idx": rg_idx}
        if connector:
            kwargs["connector"] = connector
        record_event("download_start", **kwargs)
    coalesced_ranges, coalesced_parts = _coalesce_ranges(ranges)
    read_start_ns = time.monotonic_ns()
    merged_raw_buffers = filesystem.read_ranges(path, coalesced_ranges)
    read_ns = time.monotonic_ns() - read_start_ns

    raw_buffers = _split_coalesced_buffers(merged_raw_buffers, coalesced_parts, len(column_work))

    bytes_fetched = sum(len(b) for b in merged_raw_buffers)
    bytes_requested = sum(work.length for work in column_work)

    # tracing: download complete
    if _cfg.OPTERYX_TRACE:
        kwargs = {
            "file_id": path,
            "component": "columns",
            "rg_idx": rg_idx,
            "bytes_received": bytes_fetched,
        }
        if connector:
            kwargs["connector"] = connector
        record_event("download_complete", **kwargs)

    task_total_ns = time.monotonic_ns() - task_start_ns
    return {
        "raw_buffers": raw_buffers,
        "bytes_fetched": bytes_fetched,
        "bytes_requested": bytes_requested,
        "range_request_count": len(coalesced_ranges),
        "read_ns": read_ns,
        "queue_wait_ns": queue_wait_ns,
        "task_total_ns": task_total_ns,
    }


def _decode_column_task(
    path: str,
    rg_idx: int,
    work: _ColumnWorkItem,
    raw_bytes: bytes,
    decoder: Any,
    submitted_ns: int,
    connector: Optional[str] = None,
) -> Dict[str, Any]:
    from opteryx.tracing import record_event

    from opteryx import config as _cfg

    task_start_ns = time.monotonic_ns()
    queue_wait_ns = task_start_ns - submitted_ns

    decode_start_ns = time.monotonic_ns()
    decoded = decoder(raw_bytes, work.stats)
    if decoded is None:
        raise RuntimeError(
            f"Decoder returned None for column '{path}:{rg_idx}:{work.name}' "
            f"(codec={work.stats.get('compression_codec')}, encodings={work.stats.get('encodings')})"
        )
    decode_ns = time.monotonic_ns() - decode_start_ns

    if _cfg.OPTERYX_TRACE:
        kwargs = {
            "file_id": path,
            "component": "column",
            "rg_idx": rg_idx,
            "column": work.name,
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


def _connector_name(filesystem: Any, connector: Optional[str]) -> Optional[str]:
    if connector:
        return str(connector).upper()

    try:
        from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem

        if isinstance(filesystem, OpteryxLocalFileSystem):
            return "LOCAL"
    except Exception:
        pass

    return None


def _serial_reader_selected(
    filesystem: Any,
    connector: Optional[str],
    serial_targets: frozenset[str],
) -> bool:
    if not serial_targets:
        return False
    if "ALL" in serial_targets:
        return True
    resolved = _connector_name(filesystem, connector)
    return resolved in serial_targets if resolved is not None else False


def _yield_with_scan_strategy(
    row_groups: Iterator[Dict[str, Any]],
    strategy: str,
) -> Iterator[Dict[str, Any]]:
    for row_group in row_groups:
        row_group["__parquet_scan_strategy__"] = strategy
        yield row_group


def _iter_row_groups_local_serial(
    filesystem: Any,
    paths: List[str],
    column_names: List[str],
    cache: Optional[ParquetCache] = None,
    max_workers: int = 16,
    decoder: Optional[Any] = None,
    predicates: Optional[List] = None,
    file_sizes: Optional[Dict[str, int]] = None,
    connector: Optional[str] = None,
    prefetched_footers: Optional[Dict[str, dict]] = None,
) -> Iterator[Dict[str, Any]]:
    """Synchronous local-storage fast path.

    This bypasses the process-ring/thread-pool schedulers entirely for local
    storage when the feature flag is enabled. It still reads only projected
    columns, processes one row group at a time, and issues one range read per
    column chunk to avoid pulling unnecessary data.
    """
    if cache is None:
        cache = InMemoryParquetCache()

    from opteryx import config as _cfg

    decoder_fn = _resolve_decoder(decoder)
    prefetched_footers = prefetched_footers or {}
    file_sizes = file_sizes or {}

    footers: Dict[str, dict] = {}
    footer_fetch_ns: Dict[str, int] = {}
    unique_paths = list(dict.fromkeys(paths))

    for path in unique_paths:
        prefetch_meta = prefetched_footers.get(path)
        if prefetch_meta is not None:
            cache.set_footer(path, prefetch_meta)
            footers[path] = prefetch_meta
            footer_fetch_ns[path] = 0
            continue

        cached = cache.get_footer(path)
        if cached is not None:
            footers[path] = cached
            footer_fetch_ns[path] = 0
            continue

        known_size = file_sizes.get(path)
        if not isinstance(known_size, int) or known_size <= 0:
            known_size = None

        envelope, footer_bytes, fetch_ns = _read_footer_payload(
            filesystem,
            path,
            known_size,
            connector,
        )
        parse_start_ns = time.monotonic_ns()
        meta = _parse_footer_envelope(path, envelope, footer_bytes)
        parse_ns = time.monotonic_ns() - parse_start_ns
        cache.set_footer(path, meta)
        footers[path] = meta
        footer_fetch_ns[path] = fetch_ns + parse_ns

    scan_start_ns = time.monotonic_ns()
    first_rowgroup_emit_ns: Optional[int] = None
    rg_pruned_total = 0

    trace_enabled = bool(_cfg.OPTERYX_TRACE)
    record_event = None
    if trace_enabled:
        from opteryx.tracing import record_event as _record_event

        record_event = _record_event

    for path in paths:
        meta = footers[path]
        row_groups = meta.get("row_groups", [])

        # Pre-compute column name to index mapping once per file
        # to avoid rebuilding the dict for every row group
        column_name_to_idx: Dict[str, int] = {}
        if row_groups:
            first_rg_cols = row_groups[0]["columns"]
            for idx, col in enumerate(first_rg_cols):
                column_name_to_idx[col["name"]] = idx

        for rg_idx, rg_meta in enumerate(row_groups):
            if predicates and not row_group_may_satisfy(rg_meta, predicates):
                rg_pruned_total += 1
                continue

            rowgroup_start_ns = time.monotonic_ns()
            rg_columns = rg_meta["columns"]
            row_group: Dict[str, Any] = {}
            bytes_fetched = 0
            range_request_count = 0
            range_bytes_requested = 0
            time_read_ranges_ns = 0
            time_decode_columns_ns = 0
            cache_hits = 0
            cache_misses = 0
            miss_work: List[Tuple[str, dict, int, int]] = []
            projected_bytes = 0
            rowgroup_bytes = sum(
                int(col.get("total_compressed_size") or 0) for col in rg_meta.get("columns", [])
            )

            if trace_enabled:
                kwargs = {
                    "file_id": path,
                    "component": "rowgroup",
                    "rg_idx": rg_idx,
                }
                if connector:
                    kwargs["connector"] = connector
                record_event("decode_start", **kwargs)

            for col_name in column_names:
                col_idx = column_name_to_idx.get(col_name)
                if col_idx is None:
                    raise KeyError(
                        f"Column '{col_name}' not found in row group {rg_idx}. "
                        f"Available columns: {list(column_name_to_idx.keys())}"
                    )
                col_stats = rg_columns[col_idx]

                cached = cache.get_column(path, rg_idx, col_name)
                if cached is not None:
                    row_group[col_name] = cached
                    cache_hits += 1
                    continue

                cache_misses += 1
                offset, length = _column_chunk_range(col_stats)
                projected_bytes += length
                range_bytes_requested += length
                miss_work.append((col_name, col_stats, offset, length))

            combine_reads = (
                bool(miss_work)
                and rowgroup_bytes > 0
                and projected_bytes >= (rowgroup_bytes * _LOCAL_SERIAL_COMBINE_READ_RATIO)
            )

            if combine_reads:
                span_start = min(offset for _, _, offset, _ in miss_work)
                span_end = max(offset + length for _, _, offset, length in miss_work)
                span_length = span_end - span_start

                if trace_enabled:
                    kwargs = {
                        "file_id": path,
                        "component": "columns",
                        "rg_idx": rg_idx,
                        "columns": [col_name for col_name, *_ in miss_work],
                        "ranges": 1,
                    }
                    if connector:
                        kwargs["connector"] = connector
                    record_event("download_start", **kwargs)

                read_start_ns = time.monotonic_ns()
                (span_buffer,) = filesystem.read_ranges(path, [(span_start, span_length)])
                read_elapsed_ns = time.monotonic_ns() - read_start_ns
                time_read_ranges_ns += read_elapsed_ns
                bytes_fetched += len(span_buffer)
                range_request_count += 1

                if trace_enabled:
                    kwargs = {
                        "file_id": path,
                        "component": "columns",
                        "rg_idx": rg_idx,
                        "columns": [col_name for col_name, *_ in miss_work],
                        "bytes_received": len(span_buffer),
                    }
                    if connector:
                        kwargs["connector"] = connector
                    record_event("download_complete", **kwargs)

                decoded_inputs = [
                    (
                        col_name,
                        col_stats,
                        span_buffer[offset - span_start : offset - span_start + length],
                    )
                    for col_name, col_stats, offset, length in miss_work
                ]
            else:
                decoded_inputs = []
                for col_name, col_stats, offset, length in miss_work:
                    range_request_count += 1
                    if trace_enabled:
                        kwargs = {
                            "file_id": path,
                            "component": "column",
                            "rg_idx": rg_idx,
                            "column": col_name,
                        }
                        if connector:
                            kwargs["connector"] = connector
                        record_event("download_start", **kwargs)

                    read_start_ns = time.monotonic_ns()
                    (raw_bytes,) = filesystem.read_ranges(path, [(offset, length)])
                    read_elapsed_ns = time.monotonic_ns() - read_start_ns
                    time_read_ranges_ns += read_elapsed_ns
                    bytes_fetched += len(raw_bytes)

                    if trace_enabled:
                        kwargs = {
                            "file_id": path,
                            "component": "column",
                            "rg_idx": rg_idx,
                            "column": col_name,
                            "bytes_received": len(raw_bytes),
                        }
                        if connector:
                            kwargs["connector"] = connector
                        record_event("download_complete", **kwargs)

                    decoded_inputs.append((col_name, col_stats, raw_bytes))

            decode_start_ns = time.monotonic_ns()
            if len(decoded_inputs) == 1:
                # Single column: skip pool overhead.
                col_name, col_stats, raw_bytes = decoded_inputs[0]
                if trace_enabled:
                    kwargs = {
                        "file_id": path,
                        "component": "column",
                        "rg_idx": rg_idx,
                        "column": col_name,
                    }
                    if connector:
                        kwargs["connector"] = connector
                    record_event("decode_start", **kwargs)

                decoded = decoder_fn(raw_bytes, col_stats)
                if decoded is None:
                    raise RuntimeError(
                        f"Decoder returned None for column '{path}:{rg_idx}:{col_name}' "
                        f"(codec={col_stats.get('compression_codec')}, encodings={col_stats.get('encodings')})"
                    )
                if trace_enabled:
                    kwargs = {
                        "file_id": path,
                        "component": "column",
                        "rg_idx": rg_idx,
                        "column": col_name,
                    }
                    if connector:
                        kwargs["connector"] = connector
                    record_event("decode_complete", **kwargs)
                cache.set_column(path, rg_idx, col_name, decoded)
                row_group[col_name] = decoded
            else:
                # Multiple columns: decode in parallel — each decode is nogil.
                def _decode_serial_one(col_name: str, col_stats: dict, raw_bytes: bytes) -> tuple:
                    if trace_enabled:
                        _kw = {
                            "file_id": path,
                            "component": "column",
                            "rg_idx": rg_idx,
                            "column": col_name,
                        }
                        if connector:
                            _kw["connector"] = connector
                        record_event("decode_start", **_kw)
                    result = decoder_fn(raw_bytes, col_stats)
                    if result is None:
                        raise RuntimeError(
                            f"Decoder returned None for column '{path}:{rg_idx}:{col_name}' "
                            f"(codec={col_stats.get('compression_codec')}, encodings={col_stats.get('encodings')})"
                        )
                    if trace_enabled:
                        _kw = {
                            "file_id": path,
                            "component": "column",
                            "rg_idx": rg_idx,
                            "column": col_name,
                        }
                        if connector:
                            _kw["connector"] = connector
                        record_event("decode_complete", **_kw)
                    return col_name, result

                decode_futs = {
                    _DECODE_POOL.submit(_decode_serial_one, cn, cs, rb): cn
                    for cn, cs, rb in decoded_inputs
                }
                for fut in as_completed(decode_futs):
                    cn, decoded = fut.result()
                    cache.set_column(path, rg_idx, cn, decoded)
                    row_group[cn] = decoded
            time_decode_columns_ns = time.monotonic_ns() - decode_start_ns

            completed_ns = time.monotonic_ns()
            if first_rowgroup_emit_ns is None:
                first_rowgroup_emit_ns = completed_ns

            if trace_enabled:
                rows_decoded = sum(getattr(v, "num_rows", 0) for v in row_group.values())
                kwargs = {
                    "file_id": path,
                    "component": "rowgroup",
                    "rg_idx": rg_idx,
                    "rows_decoded": rows_decoded,
                }
                if connector:
                    kwargs["connector"] = connector
                record_event("decode_complete", **kwargs)

            footer_bytes = meta.get("__footer_bytes__", 0) if rg_idx == 0 else 0
            footer_time_ns = footer_fetch_ns.get(path, 0) if rg_idx == 0 else 0

            row_group["__path__"] = path
            row_group["__row_group__"] = rg_idx
            row_group["__bytes_fetched__"] = bytes_fetched + footer_bytes
            row_group["__footer_bytes__"] = footer_bytes
            row_group["__footer_fetch_ns__"] = footer_time_ns
            row_group["__range_request_count__"] = range_request_count
            row_group["__range_bytes_requested__"] = range_bytes_requested
            row_group["__time_read_ranges_ns__"] = time_read_ranges_ns
            row_group["__time_decode_columns_ns__"] = time_decode_columns_ns
            row_group["__cache_column_hits__"] = cache_hits
            row_group["__cache_column_misses__"] = cache_misses
            row_group["__task_queue_wait_ns__"] = 0
            row_group["__task_total_ns__"] = completed_ns - rowgroup_start_ns
            row_group["__scheduler_wait_ns__"] = 0
            row_group["__rowgroup_completion_latency_ns__"] = completed_ns - rowgroup_start_ns
            row_group["__rowgroup_peak_in_flight__"] = 1
            row_group["__ranges_in_flight_peak__"] = 1 if range_request_count else 0
            row_group["__active_files_peak__"] = 1
            row_group["__active_rowgroups_peak__"] = 1
            row_group["__rowgroups_in_flight_cap__"] = 1
            row_group["__emit_wait_ns__"] = 0
            row_group["__emit_queue_depth_at_ready__"] = 0
            row_group["__scheduler_empty_wait_ns__"] = 0
            row_group["__scheduler_empty_wait_events__"] = 0
            row_group["__time_to_first_rowgroup_ns__"] = (
                completed_ns - scan_start_ns if first_rowgroup_emit_ns == completed_ns else 0
            )
            row_group["__row_groups_pruned__"] = rg_pruned_total

            yield row_group


def iter_row_groups(
    filesystem: Any,
    paths: List[str],
    column_names: List[str],
    cache: Optional[ParquetCache] = None,
    max_workers: int = 16,
    decoder: Optional[Any] = None,
    predicates: Optional[List] = None,
    file_sizes: Optional[Dict[str, int]] = None,
    connector: Optional[str] = None,
    query_id: Optional[str] = None,
    prefetched_footers: Optional[Dict[str, dict]] = None,
) -> Iterator[Dict[str, Any]]:
    """Yield assembled row groups using the configured scheduler implementation."""
    from opteryx.config import features

    if _serial_reader_selected(filesystem, connector, features.use_serial_reader):
        yield from _yield_with_scan_strategy(
            _iter_row_groups_local_serial(
                filesystem,
                paths,
                column_names,
                cache=cache,
                max_workers=max_workers,
                decoder=decoder,
                predicates=predicates,
                file_sizes=file_sizes,
                connector=connector,
                prefetched_footers=prefetched_footers,
            ),
            "local_serial",
        )
        return

    # io_process_ring is the default scheduler for parallel/network IO
    from opteryx.connectors.parquet_io.io_process_ring import iter_row_groups_io_process_v2

    yield from _yield_with_scan_strategy(
        iter_row_groups_io_process_v2(
            paths,
            column_names,
            max_workers=max_workers,
            predicates=predicates,
            file_sizes=file_sizes,
            connector=connector,
            query_id=query_id,
            prefetched_footers=prefetched_footers,
        ),
        "io_process_ring",
    )
