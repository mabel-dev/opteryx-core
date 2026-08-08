# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
rugo.parquet — unified read/write facade for Parquet.

A thin, dependency-free wrapper over the native reader/writer extensions
(`rugo.parquet_reader`, `rugo.parquet_writer`). It gives reading and writing a
single, symmetric surface that accepts either a filename or an in-memory
buffer, supports streaming iteration over row-group Morsels, and applies
predicate pushdown at the row-group level followed by row-level filtering on
surviving morsels.

    from rugo import parquet

    # read (streaming, projected, with row-group pruning + row-level filtering)
    with parquet.read_parquet("planets.parquet",
                              columns=["id", "name"],
                              predicates=[("id", ">", 4)]) as reader:
        for morsel in reader:
            ...

    # write
    data = parquet.write_parquet(morsel)            # -> bytes (ZSTD)
    with open("out.parquet", "wb") as f:
        f.write(data)

`predicates` are applied in two stages:
  1. ROW-GROUP elimination via footer min/max statistics, and bloom filter
     probing for equality predicates on file-backed sources.
  2. ROW-LEVEL filtering on each surviving morsel — only rows that satisfy
     all predicates are included in the yielded Morsel.
"""

import struct
from typing import List, Optional, Sequence, Tuple, Union

import draken.draken_native as _draken_native
from rugo import rugo_native as _native

__all__ = [
    "read_parquet",
    "read_metadata",
    "read_metadata_from_memoryview",
    "write_parquet",
    "write_parquet_with_bounds",
    "decode_value",
    "_make_scan_row_group",
]

# Re-export internals used by opteryx's parquet connector
from rugo.rugo_native import read_metadata_from_memoryview
from rugo.rugo_native import decode_value
from rugo.rugo_native import _make_scan_row_group

Source = Union[str, bytes, bytearray, memoryview]
Predicate = Tuple[str, str, object]

# op -> predicate that returns True when a row group [mn, mx] CANNOT match.
_EXCLUDE = {
    "=":      lambda v, mn, mx: v < mn or v > mx,
    "==":     lambda v, mn, mx: v < mn or v > mx,
    "!=":     lambda v, mn, mx: mn == mx == v,
    ">":      lambda v, mn, mx: mx <= v,
    ">=":     lambda v, mn, mx: mx < v,
    "<":      lambda v, mn, mx: mn >= v,
    "<=":     lambda v, mn, mx: mn > v,
    "in":     lambda v, mn, mx: not any(mn <= x <= mx for x in v),
    "not in": lambda v, mn, mx: mn == mx and mn in v,
}

# Row-level comparison: returns a Python callable (value, row_value) -> bool
_ROW_OP_CODE = {
    "=":  0, "==": 0,
    "!=": 1,
    ">":  2,
    ">=": 3,
    "<":  4,
    "<=": 5,
}


def _to_bytes(source: Source) -> bytes:
    if isinstance(source, str):
        with open(source, "rb") as f:
            return f.read()
    if isinstance(source, (bytes, bytearray, memoryview)):
        return bytes(source)
    raise TypeError("source must be a filename (str) or bytes/bytearray/memoryview")


def _bloom_plain_encode(value) -> Optional[bytes]:
    """Encode a scalar value to its Parquet PLAIN bytes for bloom filter probing.
    Returns None if the type is not encodable (bloom probe is skipped)."""
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, bytes):
        return value
    if isinstance(value, int):
        try:
            return struct.pack("<q", value)   # int64 little-endian
        except struct.error:
            return None
    if isinstance(value, float):
        return struct.pack("<d", value)       # float64 little-endian
    return None


def _row_group_mask(data: bytes, path: Optional[str], predicates: Sequence[Predicate]) -> List[int]:
    """1 = keep, 0 = prune.

    Two pruning stages:
      - Min/max statistics (all operators).
      - Bloom filter probing for == and 'in' on file-backed sources.
    A row group is pruned when ANY predicate proves it cannot match.
    """
    row_groups = _native.read_rowgroup_stats(data)
    mask: List[int] = [1] * len(row_groups)
    for rg_idx, rg in enumerate(row_groups):
        if mask[rg_idx] == 0:
            continue
        by_name = {c["name"]: c for c in rg["columns"]}
        for col, op, value in predicates:
            excl = _EXCLUDE.get(op)
            if excl is None:
                raise ValueError(f"unsupported predicate operator: {op!r}")
            col_stats = by_name.get(col)
            if col_stats is None:
                continue
            # Min/max pruning
            if col_stats["min"] is not None and col_stats["max"] is not None:
                pt = col_stats["physical_type"].encode("utf-8")
                lt = col_stats["logical_type"].encode("utf-8")
                mn = _native.decode_value(pt, lt, col_stats["min"], True)
                mx = _native.decode_value(pt, lt, col_stats["max"], True)
                try:
                    if excl(value, mn, mx):
                        mask[rg_idx] = 0
                        break
                except TypeError:
                    pass  # type mismatch — don't prune
            # Bloom filter pruning (equality only, file-backed)
            if mask[rg_idx] and path is not None and op in ("=", "==", "in"):
                bloom_offset = col_stats.get("bloom_offset")
                bloom_length = col_stats.get("bloom_length")
                if bloom_offset is not None:
                    candidates = value if op == "in" else [value]
                    # Prune only if NONE of the candidates could be present
                    any_maybe = False
                    for candidate in candidates:
                        encoded = _bloom_plain_encode(candidate)
                        if encoded is None:
                            any_maybe = True  # can't encode → can't prune
                            break
                        try:
                            if _native.bloom_filter_maybe_contains(
                                path, bloom_offset, bloom_length, encoded
                            ):
                                any_maybe = True
                                break
                        except Exception:
                            any_maybe = True
                            break
                    if not any_maybe:
                        mask[rg_idx] = 0
    return mask


def _row_filter(morsel, predicates: Sequence[Predicate]):
    """Apply row-level predicates to a Morsel via native compare_scalar + bool_and.

    Each predicate produces a DRAKEN_BOOL vector via Vector._compare_scalar()
    (which calls into the C++ compare kernel — no Python loop over rows).
    Multi-predicate masks are reduced with bool_and(), also native.
    Columns absent from the morsel are skipped (fail-open).
    """
    col_names = list(morsel.column_names)
    mask = None

    for col, op, value in predicates:
        op_code = _ROW_OP_CODE.get(op)
        if op_code is None:
            raise ValueError(f"unsupported predicate operator: {op!r}")
        col_bytes = col.encode() if isinstance(col, str) else col
        if col_bytes not in col_names:
            continue

        # compare_scalar expects bytes for string columns
        scalar = value.encode() if isinstance(value, str) else value
        vec = morsel.column(col_bytes)
        pred_mask = vec._compare_scalar(scalar, op_code)

        if mask is None:
            mask = pred_mask
        else:
            mask = mask.and_vector(pred_mask)

    if mask is None:
        return morsel
    return morsel.filter_mask(mask)


def _parse_timestamp_unit(logical_type: str) -> Optional[str]:
    """Return the draken unit string ("ms"/"us"/"ns") if `logical_type` denotes a
    Parquet TIMESTAMP column, else None. Handles both the modern LogicalType
    annotation ("timestamp[ms]" / "timestamp[us,UTC]") and the legacy
    ConvertedType spelling ("TIMESTAMP_MILLIS" / "TIMESTAMP_MICROS")."""
    if logical_type.startswith("timestamp["):
        unit = logical_type[len("timestamp["):].split(",", 1)[0].rstrip("]")
        return unit if unit in ("s", "ms", "us", "ns") else None
    if logical_type == "TIMESTAMP_MILLIS":
        return "ms"
    if logical_type == "TIMESTAMP_MICROS":
        return "us"
    return None


def _is_date_logical_type(logical_type: str) -> bool:
    """True if `logical_type` denotes a Parquet DATE column. Handles both the
    modern LogicalType annotation ("date32[day]") and the legacy ConvertedType
    spelling ("DATE")."""
    return logical_type == "date32[day]" or logical_type == "DATE"


def _temporal_column_maps(source: Source):
    """({timestamp_col: unit_str}, {date_col}) — column names (bytes) keyed by
    their Parquet schema annotation, read once per _ParquetReader from the
    footer metadata."""
    meta = read_metadata(source)
    units = {}
    dates = set()
    for col in meta.schema_columns:
        name = col.name.encode("utf-8")
        unit = _parse_timestamp_unit(col.logical_type)
        if unit is not None:
            units[name] = unit
        elif _is_date_logical_type(col.logical_type):
            dates.add(name)
    return units, dates


def _coerce_temporal_columns(morsel, unit_map: dict, date_set: set):
    """Retag/reinterpret INT64 columns that carry a Parquet TIMESTAMP or DATE
    annotation to DRAKEN_TIMESTAMP64/DATE32, mirroring the schema-driven
    coercion the SQL engine's own scan applies
    (opteryx/operators/parquet_read/parquet_read.pyx). The IPC/direct decode
    paths serialise DATE/TIMESTAMP as their bare physical INT64 stream — the
    logical type never crosses the wire — so this reinterpret has to happen
    here, against the file's own schema, once per morsel."""
    if not unit_map and not date_set:
        return morsel
    from draken.morsels import Morsel

    names = list(morsel.column_names)
    vectors = []
    changed = False
    for name in names:
        v_nb = morsel.column(name)._nb
        # TIMESTAMP is physical int64; DATE is physical int32 and decodes at that
        # width, so the date branch must accept INT32 as well as INT64.
        if v_nb.type == _draken_native.DrakenType.INT64:
            unit = unit_map.get(name)
            if unit is not None:
                v_nb = _draken_native.vector_retag_int64_as_timestamp64(v_nb, unit)
                changed = True
            elif name in date_set:
                v_nb = _draken_native.vector_reinterpret_as_date32(v_nb)
                changed = True
        elif v_nb.type == _draken_native.DrakenType.INT32 and name in date_set:
            v_nb = _draken_native.vector_reinterpret_as_date32(v_nb)
            changed = True
        vectors.append(v_nb)
    if not changed:
        return morsel
    return Morsel.from_vectors(names, vectors)


class _ParquetReader:
    """Context-managed, streaming reader over row-group Morsels.

    Decode is performed lazily on iteration. For file sources, row groups are
    pruned by footer statistics and bloom filters before decoding. Each
    surviving morsel is then filtered at the row level.
    """

    def __init__(self, source: Source, columns, predicates):
        self._path = source if isinstance(source, str) else None
        self._data = None if self._path else _to_bytes(source)
        self._columns = list(columns) if columns is not None else None
        self._predicates = list(predicates) if predicates else None

    def __enter__(self) -> "_ParquetReader":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def __iter__(self):
        if self._path is not None:
            if self._predicates:
                data = _to_bytes(self._path)
                mask = _row_group_mask(data, self._path, self._predicates)
            else:
                mask = None
            morsels = _native.read_parquet_from_path(
                self._path, column_names=self._columns, row_group_mask=mask
            )
        else:
            mask = (_row_group_mask(self._data, None, self._predicates)
                    if self._predicates else None)
            morsels = _native.read_parquet(
                self._data, column_names=self._columns, row_group_mask=mask
            )

        unit_map, date_set = _temporal_column_maps(self._path if self._path is not None else self._data)

        for morsel in (morsels or []):
            morsel = _coerce_temporal_columns(morsel, unit_map, date_set)
            if self._predicates:
                morsel = _row_filter(morsel, self._predicates)
            if morsel is not None:
                yield morsel


def read_parquet(
    source: Source,
    columns: Optional[Sequence[str]] = None,
    predicates: Optional[Sequence[Predicate]] = None,
) -> _ParquetReader:
    """Open a Parquet file or buffer for streaming reads.

    Args:
        source: filename (str) OR bytes/bytearray/memoryview of the whole file.
        columns: column names to project, or None for all.
        predicates: list of (column, op, value) tuples.
            Stage 1 — ROW-GROUP pruning via footer min/max statistics and bloom
            filters (equality ops on file sources). Coarse: whole row groups.
            Stage 2 — ROW-LEVEL filtering on each surviving morsel. Exact.
            Ops: =, ==, !=, <, <=, >, >=, in, not in.

    Returns a context manager that yields one filtered Morsel per surviving
    row group (row groups that produce zero rows after filtering are skipped).
    """
    return _ParquetReader(source, columns, predicates)


def read_metadata(source: Source):
    """Return ParquetMetadata (num_rows, schema_columns) for a file or buffer."""
    if isinstance(source, str):
        return _native.read_metadata(source)
    return _native.read_metadata_from_bytes(_to_bytes(source))


def write_parquet(morsel, compression: str = "zstd", bloom_filters=True,
                  dictionary: bool = True,
                  max_rows_per_row_group: int = 262144,
                  max_page_bytes: int = 0,
                  sorted_by=None, sorted_descending: bool = False,
                  profile: str = "fast") -> bytes:
    """Serialize a Morsel to Parquet bytes.

    compression: "zstd" (default) or "none".
    profile: "fast" (default) or "storage" — how hard to compress. The zstd
        level is not a caller knob; it is chosen per column from the column's
        physical type, because only BYTE_ARRAY columns respond to it. "fast"
        suits CTAS and uploads; "storage" raises only the string level and is
        for the defragmenter. Requires compression="zstd".
    bloom_filters: True (all equality-friendly columns), False, or an iterable
        of column names. Split-block bloom filters; floats/bools are excluded.
    dictionary: True (default) dictionary-encodes eligible columns; False
        forces PLAIN everywhere.
    max_rows_per_row_group: maximum rows per row group (default 2^18 = 262144).
        Pass 0 to write a single row group regardless of size.
    max_page_bytes: split each column chunk into multiple data pages once its
        estimated size exceeds this many bytes (default 0 = single page per
        chunk). Independent per column. Dictionary-encoded chunks are
        unaffected.
    sorted_by: name of a column the CALLER asserts is already ordered within
        every row group of this morsel (e.g. a clustering key merged from
        pre-sorted runs). Written verbatim into each row group's parquet
        sorting_columns field — rugo does NOT verify it; an untrue hint is a
        correctness bug in the caller. A reader only trusts this claim back
        from a file whose created_by identifies rugo as the writer.
    sorted_descending: sort direction for sorted_by (default ascending).
        nulls_first is implied (True for ascending, False for descending).
    """
    return _native.write_parquet(morsel, compression=compression,
                                 bloom_filters=bloom_filters,
                                 dictionary=dictionary,
                                 max_rows_per_row_group=max_rows_per_row_group,
                                 max_page_bytes=max_page_bytes,
                                 sorted_by=sorted_by,
                                 sorted_descending=sorted_descending,
                                 profile=profile)


def write_parquet_with_bounds(morsel, compression: str = "zstd", bloom_filters=True,
                              dictionary: bool = True,
                              max_rows_per_row_group: int = 262144,
                              max_page_bytes: int = 0,
                              sorted_by=None, sorted_descending: bool = False,
                              profile: str = "fast"):
    """Like write_parquet but also returns {col_index: (min, max)} bounds.

    Note: bounds are only populated for single-row-group files.
    sorted_by / sorted_descending / profile: see write_parquet.
    """
    return _native.write_parquet_with_bounds(morsel, compression=compression,
                                             bloom_filters=bloom_filters,
                                             dictionary=dictionary,
                                             max_rows_per_row_group=max_rows_per_row_group,
                                             max_page_bytes=max_page_bytes,
                                             sorted_by=sorted_by,
                                             sorted_descending=sorted_descending,
                                             profile=profile)


def open_parquet_writer(sink, compression: str = "zstd", bloom_filters=True,
                        dictionary: bool = True, max_page_bytes: int = 0,
                        sorted_by=None, sorted_descending: bool = False,
                        profile: str = "fast"):
    """Open a streaming, constant-memory Parquet writer.

    Unlike write_parquet (whole morsel in, whole file out), this writes one row
    group per write_row_group(morsel) call, pushing each produced chunk of bytes
    to `sink` as it goes, so peak memory stays ~one row group regardless of the
    total file size. The footer/statistics are accumulated incrementally and
    emitted on close().

    Args:
        sink: a callable taking bytes. Called with each chunk of the file as row
            groups are written, and once more with the footer on close. A file
            object's .write bound method, or a GCS resumable-upload adapter, both
            satisfy this.
        compression: "zstd" (default) or "none".
        profile: "fast" (default) or "storage"; as write_parquet, applied to
            every row group.
        bloom_filters / dictionary / max_page_bytes: as write_parquet; applied to
            every row group.
        sorted_by / sorted_descending: as write_parquet; applied to every row
            group written by this writer.

    Returns a context manager:

        with open_parquet_writer(f.write) as w:
            for batch in batches:
                w.write_row_group(batch)   # one row group per call

    Every batch must share the same column schema (names/types).
    """
    return _native.open_parquet_writer(sink, compression=compression,
                                       bloom_filters=bloom_filters,
                                       dictionary=dictionary,
                                       max_page_bytes=max_page_bytes,
                                       sorted_by=sorted_by,
                                       sorted_descending=sorted_descending,
                                       profile=profile)


def write_parquet_stream(morsel_iter, sink, compression: str = "zstd",
                         bloom_filters=True, dictionary: bool = True,
                         max_page_bytes: int = 0,
                         sorted_by=None, sorted_descending: bool = False,
                         profile: str = "fast") -> int:
    """Stream an iterable of Morsels to a byte-chunk `sink` as one Parquet file.

    Thin wrapper over open_parquet_writer: one row group per yielded morsel,
    constant memory. Empty morsels (no rows) are skipped. Returns the number of
    row groups written. See open_parquet_writer for the `sink` contract and
    write_parquet for sorted_by / sorted_descending.
    """
    return _native.write_parquet_stream(morsel_iter, sink, compression=compression,
                                        bloom_filters=bloom_filters,
                                        dictionary=dictionary,
                                        max_page_bytes=max_page_bytes,
                                        sorted_by=sorted_by,
                                        sorted_descending=sorted_descending,
                                        profile=profile)
