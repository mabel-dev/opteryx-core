"""
Parquet column-chunk reader: footer parsing and column fetch.

Public API:
- fetch_footer(...)
- fetch_columns(...)
- ListColumnError
"""

from __future__ import annotations

import struct
import time
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from opteryx.compiled.structures.footer_cache import ParquetFooterBytesCache
from opteryx.connectors.parquet_io.cache import InMemoryParquetCache
from opteryx.connectors.parquet_io.cache import ParquetCache
from opteryx.tracing.event_recorder import record_event as _record_event

from opteryx import config as _cfg

_PARQUET_MAGIC = b"PAR1"
_PARQUET_FOOTER_SUFFIX = 8
_FOOTER_PREFETCH = 65536


class ListColumnError(ValueError):
    """Raised when a column's decoded length doesn't match the row group row count."""


def _logical_timestamp_unit(logical_type: str) -> str:
    """
    Extract timestamp unit from footer logical type text.

    Examples:
      'timestamp[ms,UTC]' -> 'ms'
      'timestamp[us]'     -> 'us'
      'timestamp'         -> 'us' (safe default)
    """
    logical_lower = logical_type.lower()
    if not logical_lower.startswith("timestamp"):
        return "us"

    lb = logical_lower.find("[")
    rb = logical_lower.find("]", lb + 1) if lb >= 0 else -1
    if lb >= 0 and rb > lb + 1:
        token = logical_lower[lb + 1 : rb].split(",", 1)[0].strip()
        if token in ("s", "ms", "us", "ns"):
            return token
    return "us"


def _coerce_temporal_vector(decoded: Any, col_stats: dict) -> Any:
    """
    Ensure parquet logical temporal columns materialize as temporal vectors.

    The rugo decoder currently emits Int64Vector for date32/timestamp columns.
    This helper normalizes those to Date32Vector/TimestampVector at the parquet
    -> vector boundary.
    """
    logical_type = str(col_stats.get("logical_type", "") or "").lower()
    if not logical_type:
        return decoded

    cls_name = decoded.__class__.__name__
    if cls_name != "Int64Vector":
        return decoded

    if not (logical_type.startswith("date32") or logical_type.startswith("timestamp")):
        return decoded

    from opteryx.compiled.draken.vectors.date32_vector import from_int64_vector as int64_to_date32
    from opteryx.compiled.draken.vectors.timestamp_vector import (
        from_int64_vector as int64_to_timestamp,
    )

    if logical_type.startswith("date32"):
        return int64_to_date32(decoded)

    unit = _logical_timestamp_unit(logical_type)
    return int64_to_timestamp(decoded, unit)


def _trace_enabled() -> bool:
    return bool(_cfg.OPTERYX_TRACE)


def _trace(**kwargs) -> None:
    _record_event(kwargs.pop("event_type"), **kwargs)


def _trace_io_started(**kwargs) -> None:
    _trace(event_type="download_start", **kwargs)


def _trace_io_completed(**kwargs) -> None:
    _trace(event_type="download_complete", **kwargs)


def _trace_buffer_started(**kwargs) -> None:
    _trace(event_type="buffer_start", **kwargs)


def _trace_buffer_completed(**kwargs) -> None:
    _trace(event_type="buffer_complete", **kwargs)


def _trace_decode_started(**kwargs) -> None:
    _trace(event_type="decode_start", **kwargs)


def _trace_decode_completed(**kwargs) -> None:
    _trace(event_type="decode_complete", **kwargs)



def _resolve_decoder(decoder: Optional[Any]) -> Any:
    if decoder is not None:
        return decoder
    try:
        from opteryx.compiled.rugo.parquet import decode_column_from_chunk  # type: ignore[import]
    except ImportError:
        raise RuntimeError(
            "rugo.parquet is required but not available. "
            "Ensure rugo is compiled and in the Python path."
        )
    return decode_column_from_chunk
    return decode_column_from_chunk


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

        if gap >= 0 and gap <= 64 and next_span <= 32 * 1024 * 1024:
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
    expanded: List[memoryview] = [memoryview(b"")] * expected_parts
    for buffer, parts in zip(merged_buffers, merged_parts):
        mv = memoryview(buffer)
        for original_idx, rel_offset, length in parts:
            expanded[original_idx] = mv[rel_offset : rel_offset + length]
    return expanded


def _read_footer_payload(
    filesystem: Any,
    path: str,
    file_size: Optional[int] = None,
    connector: Optional[str] = None,
    footer_cache: Optional[ParquetFooterBytesCache] = None,
) -> Tuple[bytes, int, int]:
    if _trace_enabled():
        _trace_io_started(file_id=path, component="footer", connector=connector)

    start_ns = time.monotonic_ns()

    if file_size is None or file_size <= 0:
        file_info = filesystem.get_file_info(path)
        file_size = file_info.size
    if file_size is None or file_size < _PARQUET_FOOTER_SUFFIX:
        raise ValueError(f"File {path!r} is too small to be a valid Parquet file ({file_size} B)")

    # Check footer bytes cache before I/O
    if footer_cache is not None:
        cached = footer_cache.get(path)
        if cached is not None:
            return cached, 0, 0

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
        footer_start = len(tail_bytes) - total_footer_payload
        footer_bytes_data = tail_bytes[footer_start : footer_start + footer_length]
        bytes_fetched = prefetch_size
    else:
        footer_offset = file_size - _PARQUET_FOOTER_SUFFIX - footer_length
        (footer_bytes_data,) = filesystem.read_ranges(path, [(footer_offset, footer_length)])
        bytes_fetched = prefetch_size + footer_length

    if _trace_enabled():
        _trace_io_completed(
            file_id=path, component="footer", bytes_received=bytes_fetched, connector=connector
        )

    envelope = (
        _PARQUET_MAGIC + footer_bytes_data + struct.pack("<I", footer_length) + _PARQUET_MAGIC
    )

    # Store in footer bytes cache
    if footer_cache is not None:
        footer_cache.put(path, envelope)

    return envelope, bytes_fetched, (time.monotonic_ns() - start_ns)


def _parse_footer_envelope(path: str, envelope: bytes, footer_bytes: int) -> dict:
    try:
        from opteryx.compiled.rugo.parquet import read_metadata_from_bytes  # type: ignore[import]
    except ImportError:
        raise RuntimeError(
            "rugo.parquet is required but not available. "
            "Ensure rugo is compiled and in the Python path."
        )

    try:
        meta = read_metadata_from_bytes(envelope)
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
    footer_bytes_cache: Optional[ParquetFooterBytesCache] = None,
) -> dict:
    if cache is not None:
        cached = cache.get_footer(path)
        if cached is not None:
            return cached

    if file_size is None:
        envelope, footer_bytes, _ = _read_footer_payload(
            filesystem, path, connector=connector, footer_cache=footer_bytes_cache
        )
    else:
        envelope, footer_bytes, _ = _read_footer_payload(
            filesystem, path, file_size, connector, footer_cache=footer_bytes_cache
        )
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
    footer_bytes_cache: Optional[ParquetFooterBytesCache] = None,
) -> Dict[str, Any]:
    if cache is None:
        cache = InMemoryParquetCache()

    decoder = _resolve_decoder(decoder)
    meta = fetch_footer(filesystem, path, cache=cache, footer_bytes_cache=footer_bytes_cache)

    if rg_idx < 0 or rg_idx >= len(meta["row_groups"]):
        raise IndexError(f"Row group {rg_idx} out of range [0, {len(meta['row_groups'])})")

    rg_meta = meta["row_groups"][rg_idx]
    name_to_stats: Dict[str, dict] = {col["name"]: col for col in rg_meta["columns"]}

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

        cached = cache.get_column(path, rg_idx, col_name)
        if cached is not None:
            results[col_name] = cached
            cache_hits += 1
        else:
            misses.append(col_name)
            cache_misses += 1

    _pages_skipped_before: int = 0
    _pages_decoded_before: int = 0
    if row_mask is not None and misses:
        from opteryx.compiled.rugo.parquet import get_telemetry  # type: ignore[import]

        _tel_before = get_telemetry()
        _pages_skipped_before = _tel_before.get("parquet_pages_skipped", 0)
        _pages_decoded_before = _tel_before.get("parquet_pages_decoded", 0)

    if misses:
        ranges: List[Tuple[int, int]] = []
        for col_name in misses:
            col_stats = name_to_stats[col_name]
            dict_off = col_stats.get("dictionary_page_offset")
            data_off = col_stats["data_page_offset"]
            if dict_off is not None and dict_off >= 0 and dict_off < data_off:
                base_offset = dict_off
            else:
                base_offset = data_off

            compressed_size = col_stats["total_compressed_size"]
            range_bytes_requested += compressed_size
            ranges.append((base_offset, compressed_size))

        coalesced_ranges, coalesced_parts = _coalesce_ranges(ranges)
        range_request_count = len(coalesced_ranges)

        if _trace_enabled():
            _trace_io_started(
                file_id=path,
                component="columns",
                rg_idx=rg_idx,
                columns=misses,
                ranges=len(coalesced_ranges),
                connector=connector,
            )

        read_start_ns = time.monotonic_ns()
        merged_raw_buffers = filesystem.read_ranges(path, coalesced_ranges)
        time_read_ranges_ns += time.monotonic_ns() - read_start_ns
        bytes_fetched += sum(len(b) for b in merged_raw_buffers)
        raw_buffers = _split_coalesced_buffers(merged_raw_buffers, coalesced_parts, len(misses))

        if _trace_enabled():
            _trace_io_completed(
                file_id=path,
                component="columns",
                rg_idx=rg_idx,
                columns=misses,
                bytes_received=bytes_fetched,
                connector=connector,
            )
            _trace_buffer_started(
                file_id=path,
                component="columns",
                rg_idx=rg_idx,
                columns=misses,
                connector=connector,
            )

        def _decode_one(col_name: str, raw_bytes: Union[bytes, memoryview]) -> tuple:
            _col_stats = name_to_stats[col_name]
            if _trace_enabled():
                _trace_decode_started(
                    file_id=path,
                    component="column",
                    rg_idx=rg_idx,
                    column=col_name,
                    connector=connector,
                )

            # Convert memoryview to bytes if needed
            raw_bytes_arg = bytes(raw_bytes) if isinstance(raw_bytes, memoryview) else raw_bytes
            decoded = (
                decoder(raw_bytes_arg, _col_stats)  # type: ignore[misc]
                if row_mask is None
                else decoder(raw_bytes_arg, _col_stats, row_mask)  # type: ignore[misc]
            )
            if decoded is None:
                raise RuntimeError(
                    f"Decoder returned None for column '{col_name}' "
                    f"(codec={_col_stats.get('compression_codec')}, "
                    f"encodings={_col_stats.get('encodings')})"
                )
            decoded = _coerce_temporal_vector(decoded, _col_stats)

            if _trace_enabled():
                _trace_decode_completed(
                    file_id=path,
                    component="column",
                    rg_idx=rg_idx,
                    column=col_name,
                    rows_decoded=getattr(decoded, "num_rows", None) or 0,
                    connector=connector,
                )
            return col_name, decoded

        decode_start_ns = time.monotonic_ns()
        if len(misses) == 1:
            col_name = misses[0]
            if _trace_enabled():
                _trace_buffer_completed(
                    file_id=path,
                    component="column",
                    rg_idx=rg_idx,
                    column=col_name,
                    connector=connector,
                )
            try:
                col_name, decoded = _decode_one(col_name, raw_buffers[0])  # type: ignore[arg-type]
            except RuntimeError:
                raise
            except Exception as e:
                raise RuntimeError(
                    f"Failed to decode column '{path}:{rg_idx}:{col_name}': {e}"
                ) from e
            cache.set_column(path, rg_idx, col_name, decoded)
            results[col_name] = decoded
        else:
            # Inline sequential decode: fetch_columns is typically called from
            # iter_row_groups which already parallelises across row groups, so
            # outer-level parallelism covers all CPUs.  A shared decode pool
            # creates a global serialisation point that is strictly slower when
            # many row groups are in flight simultaneously.
            for col_name, raw_buffer in zip(misses, raw_buffers):
                if _trace_enabled():
                    _trace_buffer_completed(
                        file_id=path,
                        component="column",
                        rg_idx=rg_idx,
                        column=col_name,
                        connector=connector,
                    )
                try:
                    col_name, decoded = _decode_one(col_name, raw_buffer)  # type: ignore[arg-type]
                except RuntimeError:
                    raise
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to decode column '{path}:{rg_idx}:{col_name}': {e}"
                    ) from e
                cache.set_column(path, rg_idx, col_name, decoded)
                results[col_name] = decoded

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
        from opteryx.compiled.rugo.parquet import get_telemetry  # type: ignore[import]

        _tel_after = get_telemetry()
        result_dict["__pages_skipped__"] = (
            _tel_after.get("parquet_pages_skipped", 0) - _pages_skipped_before
        )
        result_dict["__pages_decoded__"] = (
            _tel_after.get("parquet_pages_decoded", 0) - _pages_decoded_before
        )
    return result_dict


@dataclass
class _ColumnWorkItem:
    name: str
    stats: dict
    offset: int
    length: int


