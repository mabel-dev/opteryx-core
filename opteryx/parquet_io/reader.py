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
import struct
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
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

from opteryx.parquet_io.cache import InMemoryParquetCache
from opteryx.parquet_io.cache import ParquetCache
from opteryx.parquet_io.predicates import row_group_may_satisfy


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

# Module-level thread pool shared across all queries (I/O bound; GIL released
# on socket/disk reads so real parallelism is achieved).
_RANGE_POOL: ThreadPoolExecutor = ThreadPoolExecutor(
    max_workers=32,
    thread_name_prefix="parquet-io",
)


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
    from opteryx import config as _cfg
    from opteryx.tracing import record_event

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
        from opteryx.rugo import parquet as rugo_parquet
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
            from opteryx.rugo import parquet as rugo_parquet

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
        from opteryx import config as _cfg
        from opteryx.tracing import record_event

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

        # Decode each chunk and populate cache
        for col_name, raw_bytes in zip(misses, raw_buffers):
            col_stats = name_to_stats[col_name]

            # record per-column decode events
            from opteryx import config as _cfg
            from opteryx.tracing import record_event

            if _cfg.OPTERYX_TRACE:
                kwargs = {
                    "file_id": path,
                    "component": "column",
                    "rg_idx": rg_idx,
                    "column": col_name,
                }
                if connector:
                    kwargs["connector"] = connector
                record_event("decode_start", **kwargs)

            try:
                decode_start_ns = time.monotonic_ns()
                decoded = decoder(raw_bytes, col_stats)
                time_decode_columns_ns += time.monotonic_ns() - decode_start_ns
                if decoded is None:
                    raise RuntimeError(
                        f"Decoder returned None for column '{col_name}' "
                        f"(codec={col_stats.get('compression_codec')}, "
                        f"encodings={col_stats.get('encodings')})"
                    )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to decode column '{path}:{rg_idx}:{col_name}': {e}"
                ) from e

            if _cfg.OPTERYX_TRACE:
                kwargs = {
                    "file_id": path,
                    "component": "column",
                    "rg_idx": rg_idx,
                    "column": col_name,
                    "rows_decoded": getattr(decoded, "num_rows", None) or 0,
                }
                if connector:
                    kwargs["connector"] = connector
                record_event("decode_complete", **kwargs)

            # Cache and add to results
            cache.set_column(path, rg_idx, col_name, decoded)
            results[col_name] = decoded

    result_dict = {col_name: results[col_name] for col_name in column_names}
    result_dict["__bytes_fetched__"] = bytes_fetched
    result_dict["__range_request_count__"] = range_request_count
    result_dict["__range_bytes_requested__"] = range_bytes_requested
    result_dict["__time_read_ranges_ns__"] = time_read_ranges_ns
    result_dict["__time_decode_columns_ns__"] = time_decode_columns_ns
    result_dict["__cache_column_hits__"] = cache_hits
    result_dict["__cache_column_misses__"] = cache_misses
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
    from opteryx import config as _cfg
    from opteryx.tracing import record_event

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


def _iter_row_groups_v1(
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
    """Yield assembled row groups across multiple Parquet files.

    Each yielded value is a complete row group for the requested columns,
    ready to hand directly to the next operator stage:
        ``Dict[column_name -> DrakenVector]``

    Parallelism
    -----------
    Phase 1 (footer reads):
        One task per unique path is submitted to the thread pool.  Each task
        performs two small range reads (last 8 bytes + footer bytes) then
        parses with rugo.  Footers land in *cache* so subsequent row-group
        tasks skip the stat + range reads.

    Phase 2 (column-chunk reads):
        One task per ``(path, rg_idx)`` is submitted.  Each task calls
        ``filesystem.read_ranges()`` once for all requested columns in that
        row group (batched), then decodes each chunk.

    Completion order:
        Row groups are yielded as they finish, not in submission order.
        Downstream operators must tolerate out-of-order row groups.

    Args:
        filesystem: Any object with ``get_file_info`` and ``read_ranges``.
        paths: Parquet files to scan.  Duplicates are allowed — footers are
               fetched once per unique path.
        column_names: Columns to project.
        cache: Shared cache across all files and row groups.  If ``None``,
               a fresh ``InMemoryParquetCache`` is allocated (footers cached
               within this call; pass an explicit instance to reuse across
               multiple ``iter_row_groups`` calls).
        max_workers: Thread pool size.  Defaults to the module-level 16-thread
                     pool.  Pass a different value to use a call-local pool
                     (e.g. for isolated tests).

    Yields:
        One ``Dict[column_name -> DrakenVector]`` per row group in completion
        order.  Two metadata keys are injected:
          - ``"__path__"`` (str): source file path.
          - ``"__row_group__"`` (int): row group index within that file.

    Raises:
        RuntimeError: Footer parse or column decode failure.  Per-task errors
                      are re-raised immediately on the generator side.
    """
    if cache is None:
        cache = InMemoryParquetCache()

    if max_workers == 16:
        pool: ThreadPoolExecutor = _RANGE_POOL
        local_pool = False
    else:
        pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="parquet-io-local")
        local_pool = True

    try:
        unique_paths = list(dict.fromkeys(paths))  # deduplicate, preserve order

        # ── Phase 1: fetch footer bytes in parallel, parse in caller thread ───
        # We parallelize pure I/O (file stat + range reads) but keep rugo parse
        # on the generator thread. This avoids known exception-conversion risk
        # when C++ errors cross threadpool boundaries.
        footers: Dict[str, dict] = {}
        footer_fetch_ns: Dict[str, int] = {}
        footer_futures: Dict[Future, str] = {}

        def _known_file_size(p: str) -> Optional[int]:
            if not file_sizes:
                return None
            size = file_sizes.get(p)
            return size if isinstance(size, int) and size > 0 else None

        prefetched_footers = prefetched_footers or {}

        for p in unique_paths:
            prefetch_meta = prefetched_footers.get(p)
            if prefetch_meta is not None:
                footers[p] = prefetch_meta
                footer_fetch_ns[p] = 0
                cache.set_footer(p, prefetch_meta)
                continue
            cached = cache.get_footer(p)
            if cached is not None:
                footers[p] = cached
                footer_fetch_ns[p] = 0
                continue
            known_size = _known_file_size(p)
            if known_size is None:
                fut = pool.submit(_read_footer_payload, filesystem, p, connector=connector)
            else:
                fut = pool.submit(_read_footer_payload, filesystem, p, known_size, connector)
            footer_futures[fut] = p

        # Helper: fan out row-group tasks as soon as a file's footer is known.
        rg_futures: Dict[Future, Tuple[str, int]] = {}
        rg_pruned_total: int = 0

        def _pipeline_rowgroups(p: str) -> None:
            """Immediately submit (path, rg_idx) tasks once the footer for p is known."""
            nonlocal rg_pruned_total
            for rg_idx in range(len(footers[p]["row_groups"])):
                rg_meta = footers[p]["row_groups"][rg_idx]
                if predicates and not row_group_may_satisfy(rg_meta, predicates):
                    rg_pruned_total += 1
                    continue
                submitted_ns = time.monotonic_ns()
                fut = pool.submit(
                    _fetch_columns_task,
                    submitted_ns,
                    filesystem,
                    p,
                    rg_idx,
                    column_names,
                    cache,
                    decoder,
                    connector,
                )
                rg_futures[fut] = (p, rg_idx)

        # Paths whose footers were already cached can be dispatched immediately.
        for p in unique_paths:
            if p in footers:
                _pipeline_rowgroups(p)

        # Parse arriving footer payloads and immediately pipeline their row-group reads.
        for fut in as_completed(footer_futures):
            p = footer_futures[fut]
            envelope, footer_bytes, fetch_ns = fut.result()
            parse_start_ns = time.monotonic_ns()
            meta = _parse_footer_envelope(p, envelope, footer_bytes)
            parse_ns = time.monotonic_ns() - parse_start_ns
            cache.set_footer(p, meta)
            footers[p] = meta
            footer_fetch_ns[p] = fetch_ns + parse_ns
            _pipeline_rowgroups(p)

        # Yield row groups as they complete — no head-of-line blocking.
        for fut in as_completed(rg_futures):
            p, rg_idx = rg_futures[fut]
            row_group = fut.result()  # propagates per-task exceptions
            # Attribute footer bytes to the first row group of each file so
            # total bytes_fetched across all row groups == actual I/O bytes.
            col_bytes = row_group.pop("__bytes_fetched__", 0)
            range_request_count = row_group.pop("__range_request_count__", 0)
            range_bytes_requested = row_group.pop("__range_bytes_requested__", 0)
            time_read_ranges_ns = row_group.pop("__time_read_ranges_ns__", 0)
            time_decode_columns_ns = row_group.pop("__time_decode_columns_ns__", 0)
            cache_hits = row_group.pop("__cache_column_hits__", 0)
            cache_misses = row_group.pop("__cache_column_misses__", 0)
            task_queue_wait_ns = row_group.pop("__task_queue_wait_ns__", 0)
            task_total_ns = row_group.pop("__task_total_ns__", 0)
            footer_bytes = footers[p].get("__footer_bytes__", 0) if rg_idx == 0 else 0
            footer_time_ns = footer_fetch_ns[p] if rg_idx == 0 else 0
            row_group["__path__"] = p
            row_group["__row_group__"] = rg_idx
            row_group["__bytes_fetched__"] = col_bytes + footer_bytes
            row_group["__footer_bytes__"] = footer_bytes
            row_group["__footer_fetch_ns__"] = footer_time_ns
            row_group["__range_request_count__"] = range_request_count
            row_group["__range_bytes_requested__"] = range_bytes_requested
            row_group["__time_read_ranges_ns__"] = time_read_ranges_ns
            row_group["__time_decode_columns_ns__"] = time_decode_columns_ns
            row_group["__cache_column_hits__"] = cache_hits
            row_group["__cache_column_misses__"] = cache_misses
            row_group["__task_queue_wait_ns__"] = task_queue_wait_ns
            row_group["__task_total_ns__"] = task_total_ns
            row_group["__row_groups_pruned__"] = rg_pruned_total
            yield row_group

    finally:
        if local_pool:
            pool.shutdown(wait=False)


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
        from opteryx.rugo import parquet as rugo_parquet
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
) -> List[bytes]:
    """Expand coalesced range buffers back into the original range order."""
    expanded: List[bytes] = [b""] * expected_parts
    for buffer, parts in zip(merged_buffers, merged_parts):
        for original_idx, rel_offset, length in parts:
            expanded[original_idx] = buffer[rel_offset : rel_offset + length]
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
    from opteryx import config as _cfg
    from opteryx.tracing import record_event

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
    from opteryx import config as _cfg
    from opteryx.tracing import record_event

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


def _iter_row_groups_v2(
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
    if cache is None:
        cache = InMemoryParquetCache()

    from opteryx import config as _cfg

    files_in_flight = max(1, int(_cfg.PARQUET_FILES_IN_FLIGHT))
    rowgroups_per_file = max(1, int(_cfg.PARQUET_ROWGROUPS_PER_FILE_IN_FLIGHT))
    rowgroups_in_flight_cap = max(1, int(_cfg.PARQUET_ROWGROUPS_IN_FLIGHT))
    global_ranges_cap = max(1, int(_cfg.PARQUET_GLOBAL_RANGE_READERS))
    decode_workers = max(1, int(_cfg.PARQUET_DECODE_WORKERS))
    raw_ring_cap = max(global_ranges_cap, int(_cfg.PARQUET_RAW_RING_CAP))

    decoder_fn = _resolve_decoder(decoder)
    unique_paths = list(dict.fromkeys(paths))
    file_states = [_FileState(file_seq=i, path=p) for i, p in enumerate(paths)]
    if not file_states:
        return

    file_indices_by_path: Dict[str, List[int]] = {}
    for state in file_states:
        file_indices_by_path.setdefault(state.path, []).append(state.file_seq)

    # Footer fetch: parallel payload reads + safe parse on caller thread.
    footers: Dict[str, dict] = {}
    footer_fetch_ns: Dict[str, int] = {}
    footer_futures: Dict[Future, str] = {}

    read_pool: ThreadPoolExecutor
    local_read_pool = False
    required_workers = max(max_workers, global_ranges_cap)
    if max_workers == 16 and required_workers <= 32:
        read_pool = _RANGE_POOL
    else:
        read_pool = ThreadPoolExecutor(
            max_workers=required_workers, thread_name_prefix="parquet-io-v2-local"
        )
        local_read_pool = True

    decode_pool = ThreadPoolExecutor(
        max_workers=max(decode_workers, 1), thread_name_prefix="parquet-decode-v2-local"
    )

    read_futures: Dict[Future, Tuple[int, int]] = {}
    decode_futures: Dict[Future, Tuple[int, int]] = {}
    try:

        def _known_file_size(p: str) -> Optional[int]:
            if not file_sizes:
                return None
            size = file_sizes.get(p)
            return size if isinstance(size, int) and size > 0 else None

        def _mark_footer_ready(path: str, meta: dict) -> None:
            footers[path] = meta
            total = len(meta.get("row_groups", []))
            for file_seq in file_indices_by_path.get(path, []):
                state = file_states[file_seq]
                state.total_rowgroups = total
                state.footer_ready = True

        prefetched_footers = prefetched_footers or {}

        for p in unique_paths:
            prefetch_meta = prefetched_footers.get(p)
            if prefetch_meta is not None:
                footer_fetch_ns[p] = 0
                cache.set_footer(p, prefetch_meta)
                _mark_footer_ready(p, prefetch_meta)
                continue
            cached = cache.get_footer(p)
            if cached is not None:
                footer_fetch_ns[p] = 0
                _mark_footer_ready(p, cached)
                continue
            known_size = _known_file_size(p)
            if known_size is None:
                fut = read_pool.submit(_read_footer_payload, filesystem, p, connector=connector)
            else:
                fut = read_pool.submit(_read_footer_payload, filesystem, p, known_size, connector)
            footer_futures[fut] = p

        scan_start_ns = time.monotonic_ns()
        first_rowgroup_emit_ns: Optional[int] = None
        ranges_in_flight = 0
        ranges_in_flight_peak = 0
        active_files_peak = 0
        active_rowgroups_peak = 0
        rg_pruned_total: int = 0
        scheduler_empty_wait_ns_total = 0
        scheduler_empty_wait_events = 0
        scheduler_empty_wait_ns_emitted = 0
        scheduler_empty_wait_events_emitted = 0

        # Rolling admission queue of file indexes with remaining row groups.
        active_file_indices: Deque[int] = deque()
        next_file_idx = 0
        admission_seq = 0
        active_rowgroups: Dict[Tuple[int, int], _RowGroupState] = {}
        ready_to_emit: Deque[Tuple[int, int]] = deque()
        pending_dispatch: List[Tuple[int, Tuple[int, int]]] = []
        raw_ring: Deque[Tuple[Tuple[int, int], _ColumnWorkItem, bytes]] = deque()
        read_rowgroups_in_flight = 0
        effective_files_in_flight = min(
            len(file_states),
            max(files_in_flight, global_ranges_cap),
        )

        def _admit_files() -> None:
            nonlocal next_file_idx, active_files_peak
            while len(active_file_indices) < effective_files_in_flight and next_file_idx < len(
                file_states
            ):
                active_file_indices.append(next_file_idx)
                next_file_idx += 1
            active_files_peak = max(active_files_peak, len(active_file_indices))

        def _queue_ready(state: _RowGroupState, now_ns: int) -> None:
            if state.queued_for_emit:
                return
            state.completed_ns = now_ns
            state.queued_for_emit = True
            state.ready_queue_depth_at_ready = len(ready_to_emit)
            ready_to_emit.append(state.key)

        def _admit_rowgroups() -> None:
            nonlocal admission_seq, active_rowgroups_peak, rg_pruned_total, read_rowgroups_in_flight
            # Keep enough ready work queued so dispatch can refill continuously
            # as soon as reads complete, without waiting for large cohort refills.
            admission_target = min(
                rowgroups_in_flight_cap,
                max(global_ranges_cap, files_in_flight),
            )
            while (
                (len(pending_dispatch) + ranges_in_flight) < admission_target
                and len(active_rowgroups) < rowgroups_in_flight_cap
                and active_file_indices
            ):
                cycle_len = len(active_file_indices)
                admitted_this_cycle = 0

                # Round-robin: admit at most one row group per file per cycle.
                for _ in range(cycle_len):
                    if len(active_rowgroups) >= rowgroups_in_flight_cap:
                        break
                    file_idx = active_file_indices.popleft()
                    fstate = file_states[file_idx]
                    keep_active = True

                    if not fstate.footer_ready:
                        # Footer still loading; keep file in the queue.
                        keep_active = True
                    elif fstate.next_rg_idx >= fstate.total_rowgroups:
                        keep_active = False
                    elif fstate.active_rowgroups >= rowgroups_per_file:
                        # Per-file cap reached for now; revisit after completions.
                        keep_active = True
                    else:
                        rg_idx = fstate.next_rg_idx
                        fstate.next_rg_idx += 1

                        rg_meta = footers[fstate.path]["row_groups"][rg_idx]

                        # Phase 1 predicate pushdown: prune row groups using min/max stats.
                        if predicates and not row_group_may_satisfy(rg_meta, predicates):
                            rg_pruned_total += 1
                        else:
                            fstate.active_rowgroups += 1
                            read_rowgroups_in_flight += 1
                            name_to_stats: Dict[str, dict] = {
                                col["name"]: col for col in rg_meta["columns"]
                            }

                            admitted_ns = time.monotonic_ns()
                            rowgroup_state = _RowGroupState(
                                file_seq=file_idx,
                                path=fstate.path,
                                rg_idx=rg_idx,
                                admission_seq=admission_seq,
                                admitted_ns=admitted_ns,
                                column_order=list(column_names),
                            )
                            admission_seq += 1

                            for col_name in column_names:
                                if col_name not in name_to_stats:
                                    raise KeyError(
                                        f"Column '{col_name}' not found in row group {rg_idx}. "
                                        f"Available columns: {list(name_to_stats.keys())}"
                                    )
                                col_stats = name_to_stats[col_name]
                                offset, length = _column_chunk_range(col_stats)
                                rowgroup_state.column_work.append(
                                    _ColumnWorkItem(
                                        name=col_name, stats=col_stats, offset=offset, length=length
                                    )
                                )
                                rowgroup_state.cache_misses += 1

                            rowgroup_state.pending_cols = len(rowgroup_state.column_work)
                            active_rowgroups[rowgroup_state.key] = rowgroup_state
                            heapq.heappush(
                                pending_dispatch, (rowgroup_state.admission_seq, rowgroup_state.key)
                            )
                            admitted_this_cycle += 1

                        keep_active = fstate.next_rg_idx < fstate.total_rowgroups

                    if keep_active:
                        active_file_indices.append(file_idx)

                    if (len(pending_dispatch) + ranges_in_flight) >= admission_target:
                        break

                active_rowgroups_peak = max(active_rowgroups_peak, len(active_rowgroups))

                # Refill active file queue as slots open up from exhausted files.
                _admit_files()

        def _dispatch_reads() -> None:
            nonlocal ranges_in_flight, ranges_in_flight_peak
            while (
                ranges_in_flight < global_ranges_cap
                and pending_dispatch
                and len(raw_ring) < raw_ring_cap
            ):
                _, key = heapq.heappop(pending_dispatch)
                target = active_rowgroups.get(key)
                if target is None or target.dispatched:
                    continue

                now_ns = time.monotonic_ns()
                target.first_dispatch_ns = now_ns
                target.dispatched = True

                fut = read_pool.submit(
                    _read_rowgroup_task,
                    filesystem,
                    target.path,
                    target.rg_idx,
                    target.column_work,
                    now_ns,
                    connector,
                )
                read_futures[fut] = target.key
                ranges_in_flight += 1
                ranges_in_flight_peak = max(ranges_in_flight_peak, ranges_in_flight)

        def _dispatch_decodes() -> None:
            while raw_ring and len(decode_futures) < decode_workers:
                key, work, raw_bytes = raw_ring.popleft()
                state = active_rowgroups.get(key)
                if state is None:
                    continue
                if not state.decode_started:
                    state.decode_started = True
                    if _cfg.OPTERYX_TRACE:
                        from opteryx.tracing import record_event as _rec_ds

                        kwargs = {
                            "file_id": state.path,
                            "component": "rowgroup",
                            "rg_idx": state.rg_idx,
                        }
                        if connector:
                            kwargs["connector"] = connector
                        _rec_ds("decode_start", **kwargs)
                submit_ns = time.monotonic_ns()
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
                decode_futures[fut] = (key, work.name)

        def _drain_completions(block: bool) -> bool:
            nonlocal ranges_in_flight, read_rowgroups_in_flight

            waiting = set(read_futures) | set(decode_futures) | set(footer_futures)
            if not waiting:
                return False

            if block:
                done, _ = wait(waiting, return_when=FIRST_COMPLETED)
            else:
                done, _ = wait(waiting, timeout=0, return_when=FIRST_COMPLETED)
                if not done:
                    return False

            for fut in done:
                footer_path = footer_futures.pop(fut, None)
                if footer_path is not None:
                    envelope, footer_bytes, fetch_ns = fut.result()
                    parse_start_ns = time.monotonic_ns()
                    meta = _parse_footer_envelope(footer_path, envelope, footer_bytes)
                    parse_ns = time.monotonic_ns() - parse_start_ns
                    cache.set_footer(footer_path, meta)
                    footer_fetch_ns[footer_path] = fetch_ns + parse_ns
                    _mark_footer_ready(footer_path, meta)
                    _admit_rowgroups()
                    _dispatch_reads()
                    continue

                read_key = read_futures.pop(fut, None)
                if read_key is not None:
                    ranges_in_flight -= 1
                    state = active_rowgroups.get(read_key)
                    if state is None:
                        continue
                    result = fut.result()
                    state.bytes_fetched += result["bytes_fetched"]
                    state.range_request_count += result["range_request_count"]
                    state.range_bytes_requested += result["bytes_requested"]
                    state.time_read_ranges_ns += result["read_ns"]
                    state.task_queue_wait_ns += result["queue_wait_ns"]
                    state.task_total_ns += result["task_total_ns"]
                    fstate = file_states[state.file_seq]
                    fstate.active_rowgroups = max(0, fstate.active_rowgroups - 1)
                    read_rowgroups_in_flight = max(0, read_rowgroups_in_flight - 1)
                    for _work, _raw in zip(state.column_work, result["raw_buffers"]):
                        raw_ring.append((read_key, _work, _raw))
                    continue

                decode_info = decode_futures.pop(fut, None)
                if decode_info is None:
                    continue
                decode_key, col_name = decode_info
                state = active_rowgroups.get(decode_key)
                if state is None:
                    continue
                result = fut.result()
                state.columns[col_name] = result["decoded"]
                state.time_decode_columns_ns += result["decode_ns"]
                state.task_queue_wait_ns += result["queue_wait_ns"]
                state.task_total_ns += result["task_total_ns"]
                state.pending_cols -= 1
                if state.pending_cols == 0:
                    if _cfg.OPTERYX_TRACE:
                        from opteryx.tracing import record_event as _rec_dc

                        rows_decoded = sum(
                            getattr(v, "num_rows", 0) for v in state.columns.values()
                        )
                        kwargs = {
                            "file_id": state.path,
                            "component": "rowgroup",
                            "rg_idx": state.rg_idx,
                            "rows_decoded": rows_decoded,
                        }
                        if connector:
                            kwargs["connector"] = connector
                        _rec_dc("decode_complete", **kwargs)
                    _queue_ready(state, time.monotonic_ns())
            return True

        while True:
            # Drain completions first so freed slots can be reused before we emit.
            _drain_completions(block=False)
            _dispatch_decodes()
            _admit_files()
            _admit_rowgroups()
            _dispatch_reads()
            _dispatch_decodes()

            if ready_to_emit:
                key = ready_to_emit.popleft()
                state = active_rowgroups.get(key)
                if state is None:
                    continue

                completed_ns = state.completed_ns or time.monotonic_ns()
                if first_rowgroup_emit_ns is None:
                    first_rowgroup_emit_ns = completed_ns
                emit_ns = time.monotonic_ns()
                emit_wait_ns = max(0, emit_ns - completed_ns)
                scheduler_empty_wait_ns_delta = (
                    scheduler_empty_wait_ns_total - scheduler_empty_wait_ns_emitted
                )
                scheduler_empty_wait_events_delta = (
                    scheduler_empty_wait_events - scheduler_empty_wait_events_emitted
                )
                scheduler_empty_wait_ns_emitted = scheduler_empty_wait_ns_total
                scheduler_empty_wait_events_emitted = scheduler_empty_wait_events

                row_group = {name: state.columns[name] for name in state.column_order}
                footer_bytes = (
                    footers[state.path].get("__footer_bytes__", 0) if state.rg_idx == 0 else 0
                )
                footer_time_ns = footer_fetch_ns.get(state.path, 0) if state.rg_idx == 0 else 0
                scheduler_wait_ns = max(
                    0, (state.first_dispatch_ns or completed_ns) - state.admitted_ns
                )

                row_group["__path__"] = state.path
                row_group["__row_group__"] = state.rg_idx
                row_group["__bytes_fetched__"] = state.bytes_fetched + footer_bytes
                row_group["__footer_bytes__"] = footer_bytes
                row_group["__footer_fetch_ns__"] = footer_time_ns
                row_group["__range_request_count__"] = state.range_request_count
                row_group["__range_bytes_requested__"] = state.range_bytes_requested
                row_group["__time_read_ranges_ns__"] = state.time_read_ranges_ns
                row_group["__time_decode_columns_ns__"] = state.time_decode_columns_ns
                row_group["__cache_column_hits__"] = 0
                row_group["__cache_column_misses__"] = state.cache_misses
                row_group["__task_queue_wait_ns__"] = state.task_queue_wait_ns
                row_group["__task_total_ns__"] = state.task_total_ns
                row_group["__scheduler_wait_ns__"] = scheduler_wait_ns
                row_group["__rowgroup_completion_latency_ns__"] = completed_ns - state.admitted_ns
                row_group["__rowgroup_peak_in_flight__"] = 1
                row_group["__ranges_in_flight_peak__"] = ranges_in_flight_peak
                row_group["__active_files_peak__"] = active_files_peak
                row_group["__active_rowgroups_peak__"] = active_rowgroups_peak
                row_group["__rowgroups_in_flight_cap__"] = rowgroups_in_flight_cap
                row_group["__emit_wait_ns__"] = emit_wait_ns
                row_group["__emit_queue_depth_at_ready__"] = state.ready_queue_depth_at_ready
                row_group["__scheduler_empty_wait_ns__"] = scheduler_empty_wait_ns_delta
                row_group["__scheduler_empty_wait_events__"] = scheduler_empty_wait_events_delta
                row_group["__time_to_first_rowgroup_ns__"] = (
                    completed_ns - scan_start_ns if first_rowgroup_emit_ns == completed_ns else 0
                )
                row_group["__row_groups_pruned__"] = rg_pruned_total

                fstate = file_states[state.file_seq]
                del active_rowgroups[key]

                if (
                    fstate.next_rg_idx >= fstate.total_rowgroups
                    and state.file_seq in active_file_indices
                ):
                    active_file_indices.remove(state.file_seq)

                yield row_group
                continue

            if not read_futures and not decode_futures and not footer_futures:
                all_files_exhausted = all(
                    state.footer_ready
                    and state.next_rg_idx >= state.total_rowgroups
                    and state.active_rowgroups == 0
                    for state in file_states
                )
                if (
                    not active_rowgroups
                    and not pending_dispatch
                    and not raw_ring
                    and not ready_to_emit
                    and all_files_exhausted
                ):
                    break
                continue

            wait_start_ns = time.monotonic_ns()
            _drain_completions(block=True)
            waited_ns = time.monotonic_ns() - wait_start_ns
            if waited_ns > 0:
                scheduler_empty_wait_ns_total += waited_ns
                scheduler_empty_wait_events += 1

    finally:
        for fut in list(read_futures):
            fut.cancel()
        for fut in list(decode_futures):
            fut.cancel()
        for fut in list(footer_futures):
            fut.cancel()
        if local_read_pool:
            read_pool.shutdown(wait=False, cancel_futures=True)
        decode_pool.shutdown(wait=False, cancel_futures=True)


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

    if features.io_process_rowgroup_ring:
        from opteryx.parquet_io.io_process_ring import iter_row_groups_io_process_v2

        yield from iter_row_groups_io_process_v2(
            paths,
            column_names,
            max_workers=max_workers,
            predicates=predicates,
            file_sizes=file_sizes,
            connector=connector,
            query_id=query_id,
            prefetched_footers=prefetched_footers,
        )
        return

    if features.parquet_rowgroup_scheduler_v2:
        yield from _iter_row_groups_v2(
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
        )
        return

    yield from _iter_row_groups_v1(
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
    )
