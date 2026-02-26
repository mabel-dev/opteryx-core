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

import struct
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

from opteryx.parquet_io.cache import InMemoryParquetCache
from opteryx.parquet_io.cache import ParquetCache


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

# Module-level thread pool shared across all queries (I/O bound; GIL released
# on socket/disk reads so real parallelism is achieved).
_RANGE_POOL: ThreadPoolExecutor = ThreadPoolExecutor(
    max_workers=16,
    thread_name_prefix="parquet-io",
)


def fetch_footer(filesystem: Any, path: str, cache: Optional[ParquetCache] = None) -> dict:
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

    try:
        from opteryx.rugo import parquet as rugo_parquet
    except ImportError:
        raise RuntimeError(
            "rugo.parquet is required but not available. "
            "Ensure rugo is compiled and in the Python path."
        )

    # Step 1: resolve file size.
    file_info = filesystem.get_file_info(path)
    file_size: int = file_info.size
    if file_size < _PARQUET_FOOTER_SUFFIX:
        raise ValueError(f"File {path!r} is too small to be a valid Parquet file ({file_size} B)")

    # Step 2: read last 8 bytes -> (footer_length uint32 LE, magic "PAR1").
    suffix_offset = file_size - _PARQUET_FOOTER_SUFFIX
    (suffix_bytes,) = filesystem.read_ranges(path, [(suffix_offset, _PARQUET_FOOTER_SUFFIX)])

    magic = suffix_bytes[4:]
    if magic != _PARQUET_MAGIC:
        raise ValueError(
            f"File {path!r} does not end with Parquet magic bytes "
            f"(got {magic!r}, expected {_PARQUET_MAGIC!r})"
        )
    (footer_length,) = struct.unpack_from("<I", suffix_bytes, 0)

    if footer_length == 0 or footer_length > file_size - _PARQUET_FOOTER_SUFFIX:
        raise ValueError(
            f"Invalid footer length {footer_length} in {path!r} (file_size={file_size})"
        )

    # Step 3: read the exact footer bytes.
    footer_offset = file_size - _PARQUET_FOOTER_SUFFIX - footer_length
    (footer_bytes,) = filesystem.read_ranges(path, [(footer_offset, footer_length)])

    # Step 4: wrap footer bytes in a minimal Parquet envelope and parse with rugo.
    # read_metadata_from_bytes expects: PAR1 + thrift_footer + uint32_LE(len) + PAR1.
    # It does NOT accept raw thrift bytes — it validates the PAR1 magic at offset 0.
    envelope = _PARQUET_MAGIC + footer_bytes + struct.pack("<I", footer_length) + _PARQUET_MAGIC
    try:
        meta = rugo_parquet.read_metadata_from_bytes(envelope)
    except Exception as exc:
        raise RuntimeError(f"Failed to parse Parquet footer from {path!r}: {exc}") from exc

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
        else:
            misses.append(col_name)

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
            ranges.append((base_offset, compressed_size))

        # Batch read all missing column chunks
        raw_buffers = filesystem.read_ranges(path, ranges)

        # Decode each chunk and populate cache
        for col_name, raw_bytes in zip(misses, raw_buffers):
            col_stats = name_to_stats[col_name]

            try:
                decoded = decoder(raw_bytes, col_stats)
                if decoded is None:
                    raise RuntimeError(
                        f"Decoder returned None for column '{col_name}' "
                        f"(codec={col_stats.get('compression_codec')}, "
                        f"encodings={col_stats.get('encodings')})"
                    )
            except Exception as e:
                raise RuntimeError(f"Failed to decode column '{col_name}' from {path}: {e}") from e

            # Cache and add to results
            cache.set_column(path, rg_idx, col_name, decoded)
            results[col_name] = decoded

    return {col_name: results[col_name] for col_name in column_names}


def iter_row_groups(
    filesystem: Any,
    paths: List[str],
    column_names: List[str],
    cache: Optional[ParquetCache] = None,
    max_workers: int = 16,
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

        # ── Phase 1: fetch all footers synchronously in the calling thread ────
        # Footer reads are two small range requests + rugo C++ parse.  Running
        # rugo inside a ThreadPoolExecutor thread risks std::terminate() if the
        # C++ extension raises std::runtime_error before Cython's except+ can
        # convert it to a Python exception.  Footers are small and typically
        # cached after the first call, so sequential is fine here.
        footers: Dict[str, dict] = {}
        for p in unique_paths:
            footers[p] = fetch_footer(filesystem, p, cache)

        # Phase 2: fan out all (path, rg_idx) work units.
        rg_futures: Dict[Future, Tuple[str, int]] = {}
        for p in paths:
            for rg_idx in range(len(footers[p]["row_groups"])):
                fut = pool.submit(fetch_columns, filesystem, p, rg_idx, column_names, cache)
                rg_futures[fut] = (p, rg_idx)

        # Yield row groups as they complete — no head-of-line blocking.
        for fut in as_completed(rg_futures):
            p, rg_idx = rg_futures[fut]
            row_group = fut.result()  # propagates per-task exceptions
            row_group["__path__"] = p
            row_group["__row_group__"] = rg_idx
            yield row_group

    finally:
        if local_pool:
            pool.shutdown(wait=False)
