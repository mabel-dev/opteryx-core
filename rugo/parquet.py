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
predicate pushdown at the row-group level.

    from rugo import parquet

    # read (streaming, projected, with row-group pruning)
    with parquet.read_parquet("planets.parquet",
                              columns=["id", "name"],
                              filters=[("id", ">", 4)]) as reader:
        for morsel in reader:
            ...

    # write
    data = parquet.write_parquet(morsel)            # -> bytes (ZSTD)
    with open("out.parquet", "wb") as f:
        f.write(data)

`filters` perform ROW-GROUP elimination only (via footer statistics): a row
group that cannot contain a matching row is never decoded. Rows within a
surviving row group are NOT filtered — apply row-level predicates downstream.
"""

from typing import Iterable, List, Optional, Sequence, Tuple, Union

from rugo import parquet_reader as _reader
from rugo import parquet_writer as _writer

__all__ = [
    "read_parquet",
    "read_metadata",
    "write_parquet",
    "write_parquet_with_bounds",
]

Source = Union[str, bytes, bytearray, memoryview]
Filter = Tuple[str, str, object]

# op -> predicate that returns True when a row group [mn, mx] CANNOT match.
_EXCLUDE = {
    "=": lambda v, mn, mx: v < mn or v > mx,
    "==": lambda v, mn, mx: v < mn or v > mx,
    "!=": lambda v, mn, mx: mn == mx == v,
    ">": lambda v, mn, mx: mx <= v,
    ">=": lambda v, mn, mx: mx < v,
    "<": lambda v, mn, mx: mn >= v,
    "<=": lambda v, mn, mx: mn > v,
    "in": lambda v, mn, mx: not any(mn <= x <= mx for x in v),
    "not in": lambda v, mn, mx: mn == mx and mn in v,
}


def _to_bytes(source: Source) -> bytes:
    """Read a filename to bytes, or pass an in-memory buffer through.
    Only used for the row-group stats / filter path — not the decode path."""
    if isinstance(source, str):
        with open(source, "rb") as f:
            return f.read()
    if isinstance(source, (bytes, bytearray, memoryview)):
        return bytes(source)
    raise TypeError("source must be a filename (str) or bytes/bytearray/memoryview")


def _row_group_mask(data: bytes, filters: Sequence[Filter]) -> List[int]:
    """1 = keep, 0 = prune. A row group is pruned when ANY filter proves it
    cannot contain a matching row (AND semantics across filters)."""
    row_groups = _reader.read_rowgroup_stats(data)
    mask: List[int] = [1] * len(row_groups)
    for rg_idx, rg in enumerate(row_groups):
        by_name = {c["name"]: c for c in rg["columns"]}
        for col, op, value in filters:
            excl = _EXCLUDE.get(op)
            if excl is None:
                raise ValueError(f"unsupported filter operator: {op!r}")
            col_stats = by_name.get(col)
            if col_stats is None or col_stats["min"] is None or col_stats["max"] is None:
                continue  # no stats -> cannot prune on this filter
            pt = col_stats["physical_type"].encode("utf-8")
            lt = col_stats["logical_type"].encode("utf-8")
            mn = _reader.decode_value(pt, lt, col_stats["min"], True)
            mx = _reader.decode_value(pt, lt, col_stats["max"], True)
            try:
                if excl(value, mn, mx):
                    mask[rg_idx] = 0
                    break
            except TypeError:
                continue  # type mismatch -> don't prune
    return mask


class _ParquetReader:
    """Context-managed, streaming reader over row-group Morsels.

    Decode is performed lazily when iteration begins (on __iter__/__enter__
    body entry), not at construction.  When source is a file path the decode
    uses mmap — the file is never materialised into a Python bytes object.
    """

    def __init__(self, source: Source, columns, filters):
        self._path = source if isinstance(source, str) else None
        self._data = None if self._path else _to_bytes(source)
        self._columns = list(columns) if columns is not None else None
        self._filters = list(filters) if filters else None

    def __enter__(self) -> "_ParquetReader":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def __iter__(self):
        if self._path is not None:
            # File path: row-group stats still need the footer bytes for filter
            # pruning; read_metadata already goes native end-to-end.
            if self._filters:
                data = _to_bytes(self._path)
                mask = _row_group_mask(data, self._filters)
            else:
                mask = None
            morsels = _reader.read_parquet_from_path(
                self._path, column_names=self._columns, row_group_mask=mask
            )
        else:
            mask = _row_group_mask(self._data, self._filters) if self._filters else None
            morsels = _reader.read_parquet(
                self._data, column_names=self._columns, row_group_mask=mask
            )
        return iter(morsels or [])


def read_parquet(
    source: Source,
    columns: Optional[Sequence[str]] = None,
    filters: Optional[Sequence[Filter]] = None,
) -> _ParquetReader:
    """Open a parquet file or buffer for streaming reads.

    Args:
        source: filename (str) OR bytes/bytearray/memoryview of the whole file.
        columns: column names to project, or None for all.
        filters: list of (column, op, value) for ROW-GROUP pruning via footer
            stats. Ops: =, ==, !=, <, <=, >, >=, in, not in. Pruning is coarse
            (whole row groups); rows in surviving groups are not filtered.

    Returns a context manager that yields one Morsel per (surviving) row group.
    """
    return _ParquetReader(source, columns, filters)


def read_metadata(source: Source):
    """Return ParquetMetadata (num_rows, schema_columns) for a file or buffer."""
    if isinstance(source, str):
        return _reader.read_metadata(source)
    return _reader.read_metadata_from_bytes(_to_bytes(source))


def write_parquet(morsel, compression: str = "zstd", bloom_filters=True) -> bytes:
    """Serialize a Morsel to parquet bytes.

    compression: "zstd" (default) or "none".
    bloom_filters: True (all equality-friendly columns), False, or an iterable
        of column names. Split-block bloom filters; floats/bools are excluded.
    """
    return _writer.write_parquet(morsel, compression=compression,
                                 bloom_filters=bloom_filters)


def write_parquet_with_bounds(morsel, compression: str = "zstd", bloom_filters=True):
    """Like write_parquet but also returns {col_index: (min, max)} bounds."""
    return _writer.write_parquet_with_bounds(morsel, compression=compression,
                                             bloom_filters=bloom_filters)
