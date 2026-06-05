"""
Parquet column-chunk reader: column fetch.

Public API:
- fetch_columns(...)
- ListColumnError
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from opteryx.compiled.structures.footer_cache import ParquetFooterBytesCache
from opteryx.tracing.event_recorder import record_event as _record_event

from opteryx import config as _cfg


class ListColumnError(ValueError):
    """Raised when a column's decoded length doesn't match the row group row count."""


# NOTE (type-system migration, 2026-06-03): the former `_logical_timestamp_unit`,
# `_coerce_decimal_vector` and `_coerce_temporal_vector` helpers were removed here.
# They were dead no-ops from the pre-unified-vector era: each guarded on
# `decoded.__class__.__name__ == "Integer64Vector"`, but the unified rebuild deleted
# the per-type vector classes (the only class now is `Vector`), so that guard could
# never pass — and the bodies imported `draken.vectors.{decimal,date32,timestamp}_vector`
# modules that no longer exist (a latent ImportError had the guard ever passed). Rugo
# now emits correctly-typed unified vectors directly (verified: a Parquet DECIMAL column
# materialises as DRAKEN_DECIMAL with the right scale; a date column as DRAKEN_DATE32).
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
        from rugo.parquet_reader import decode_column_from_chunk  # type: ignore[import]
    except ImportError:
        raise RuntimeError(
            "rugo.parquet_reader is required but not available. "
            "Ensure rugo is compiled and in the Python path."
        )
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

        # Tuned thresholds for range coalescing:
        # - gap: increased from 64 to 128 bytes (typical disk I/O overhead is "free")
        # - span: increased from 32MB to 48MB (better for multi-file scans)
        if gap >= 0 and gap <= 128 and next_span <= 48 * 1024 * 1024:
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


def fetch_columns(
    filesystem: Any,
    path: str,
    rg_idx: int,
    column_names: List[str],
    decoder: Optional[Any] = None,
    connector: Optional[str] = None,
    row_mask=None,
    footer_bytes_cache: Optional[ParquetFooterBytesCache] = None,
) -> Dict[str, Any]:
    from opteryx.connectors.parquet_io.pool_reader import fetch_column_chunk_info
    decoder = _resolve_decoder(decoder)
    # IndexError raised by fetch_column_chunk_info if rg_idx out of range.
    name_to_stats: Dict[str, dict] = fetch_column_chunk_info(
        path, rg_idx, column_names, footer_bytes_cache=footer_bytes_cache
    )

    results: Dict[str, Any] = {}
    misses: List[str] = []
    bytes_fetched: int = 0
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

        misses.append(col_name)

    _pages_skipped_before: int = 0
    _pages_decoded_before: int = 0
    if row_mask is not None and misses:
        from rugo.parquet_reader import get_telemetry  # type: ignore[import]

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

            decoded = (
                decoder(raw_bytes, _col_stats)  # type: ignore[misc]
                if row_mask is None
                else decoder(raw_bytes, _col_stats, row_mask)  # type: ignore[misc]
            )
            if decoded is None:
                raise RuntimeError(
                    f"Decoder returned None for column '{col_name}' "
                    f"(codec={_col_stats.get('compression_codec')}, "
                    f"encodings={_col_stats.get('encodings')})"
                )
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
                results[col_name] = decoded

        time_decode_columns_ns = time.monotonic_ns() - decode_start_ns

    # Build typed metadata object; separate from column data dict.
    from rugo.parquet_reader import _make_scan_row_group, get_telemetry  # type: ignore[import]

    telemetry_dict = {
        '__bytes_fetched__': bytes_fetched,
        '__range_request_count__': range_request_count,
        '__range_bytes_requested__': range_bytes_requested,
        '__time_read_ranges_ns__': time_read_ranges_ns,
        '__time_decode_columns_ns__': time_decode_columns_ns,
    }

    if row_mask is not None:
        _tel_after = get_telemetry()
        telemetry_dict["__pages_skipped__"] = (
            _tel_after.get("parquet_pages_skipped", 0) - _pages_skipped_before
        )
        telemetry_dict["__pages_decoded__"] = (
            _tel_after.get("parquet_pages_decoded", 0) - _pages_decoded_before
        )

    scan_rg = _make_scan_row_group(path, rg_idx, 'range-read', telemetry_dict)
    # result_dict is now pure {col: Vector}; clean for the operator.
    result_dict = {col_name: results[col_name] for col_name in column_names}
    return (scan_rg, result_dict)


@dataclass
class _ColumnWorkItem:
    name: str
    stats: dict
    offset: int
    length: int


