# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parquet writer for opteryx-managed relations.

Uses the native rugo writer (rugo.parquet_writer) — NO pyarrow. Morsels are
serialized straight to well-formed, PyArrow-readable parquet bytes.
"""

import os
import struct
from typing import Dict, Optional, Tuple

from draken.morsels.morsel import Morsel
from opteryx.models.file_entry import FileEntry
from opteryx.utils import unique_id


class LocalDataFileWriter:
    """One parquet file in `relation_dir`, written a row group at a time.

    Streams through rugo's constant-memory writer into a `.tmp` file that is
    renamed into place on `close`, so a reader never sees a partial file.
    `abort` removes the temporary file; nothing is left behind.

    Per-column bounds are folded in per row group for INT64 and FLOAT64
    columns - the two the native min/max kernels answer for. The whole-morsel
    writer this replaced took bounds from the parquet statistics of single-
    row-group files, which also covered BOOL and UTF8; those two columns kinds
    carry no bounds on this connector now. This store has no catalog and no
    statistics pass, so bounds here are a pruning convenience, not a contract.
    """

    def __init__(self, relation_dir: str, sorted_by: Optional[str], sorted_descending: bool,
                 write_profile: str):
        from draken.draken_native import DrakenType
        from rugo.parquet import open_parquet_writer

        self._bounded_types = (DrakenType.INT64, DrakenType.FLOAT64)
        self.file_name = f"data-{unique_id()}.parquet"
        self._full_path = os.path.join(relation_dir, self.file_name)
        self._tmp_path = f"{self._full_path}.tmp"
        # Held open across row groups; closed by close() or abort(), not a `with`.
        self._fh = open(self._tmp_path, "wb")  # noqa: SIM115
        self._writer = open_parquet_writer(
            self._fh.write,
            compression="zstd",
            sorted_by=sorted_by,
            sorted_descending=sorted_descending,
            profile=write_profile,
        )
        self._rows = 0
        self._bytes = 0
        self._row_groups = 0
        self._bounds: Dict[int, Tuple[object, object]] = {}
        self._done = False

    @property
    def file_path(self) -> str:
        return self.file_name

    @property
    def record_count(self) -> int:
        return self._rows

    @property
    def uncompressed_size_in_bytes(self) -> int:
        return self._bytes

    def write_row_group(self, morsel: Morsel) -> None:
        if self._done:
            raise ValueError(f"write_row_group on a finished writer for '{self.file_name}'")
        if len(morsel) == 0:
            raise ValueError("cannot write an empty row group")
        self._writer.write_row_group(morsel)
        self._rows += len(morsel)
        self._bytes += morsel.nbytes
        self._row_groups += 1
        self._fold_bounds(morsel)

    def _fold_bounds(self, morsel: Morsel) -> None:
        rows = len(morsel)
        for index, name in enumerate(morsel.column_names):
            # `_cxx_column`: the engine's morsels are substrate-backed and
            # refuse PyObject column access; this reads either backing.
            vec = morsel._cxx_column(name)
            if vec.type not in self._bounded_types or vec.null_count() == rows:
                continue
            lo, hi = vec.min(), vec.max()
            held = self._bounds.get(index)
            if held is None:
                self._bounds[index] = (lo, hi)
            else:
                self._bounds[index] = (min(held[0], lo), max(held[1], hi))

    def close(self) -> FileEntry:
        if self._done:
            raise ValueError(f"close on a finished writer for '{self.file_name}'")
        if self._row_groups == 0:
            raise ValueError(f"close on '{self.file_name}' with no row groups written; abort it instead")
        self._done = True
        self._writer.close()
        self._fh.close()
        os.replace(self._tmp_path, self._full_path)
        lower_bounds, upper_bounds = _bounds_to_entry(self._bounds)
        return FileEntry(
            file_path=self.file_name,
            file_format="PARQUET",
            record_count=self._rows,
            file_size_in_bytes=os.path.getsize(self._full_path),
            uncompressed_size_in_bytes=self._bytes,
            row_group_count=self._row_groups,
            lower_bounds=lower_bounds,
            upper_bounds=upper_bounds,
            null_value_counts=None,
            min_values=None,
            max_values=None,
            column_uncompressed_sizes_in_bytes=None,
        )

    def abort(self) -> None:
        if self._done:
            return
        self._done = True
        self._writer = None
        self._fh.close()
        if os.path.exists(self._tmp_path):
            os.remove(self._tmp_path)


def open_data_file_writer(
    relation_dir: str,
    sorted_by: Optional[str] = None,
    sorted_descending: bool = False,
    write_profile: str = "fast",
) -> LocalDataFileWriter:
    """Open a streaming parquet file in `relation_dir` (must already exist).

    See Writable.open_data_file_writer for the handle's contract."""
    return LocalDataFileWriter(relation_dir, sorted_by, sorted_descending, write_profile)


def _bounds_to_entry(
    bounds: Dict[int, Tuple[object, object]],
) -> Tuple[Optional[Dict[int, bytes]], Optional[Dict[int, bytes]]]:
    """Serialize {col_index: (min, max)} typed values into the FileEntry bound
    byte format (keyed by column index). Returns (None, None) if empty."""
    if not bounds:
        return (None, None)
    lower: Dict[int, bytes] = {}
    upper: Dict[int, bytes] = {}
    for idx, (col_min, col_max) in bounds.items():
        lower[idx] = _serialize_bound(col_min)
        upper[idx] = _serialize_bound(col_max)
    return (lower or None, upper or None)


def _serialize_bound(value) -> bytes:
    """Serialize a min/max value to bytes for storage.

    Handles int, float, bool, str, bytes types.
    """
    if isinstance(value, bool):
        return b"\x01" if value else b"\x00"
    elif isinstance(value, int):
        return value.to_bytes(8, "big", signed=True)
    elif isinstance(value, float):
        return struct.pack(">d", value)
    elif isinstance(value, str):
        return value.encode("utf-8")
    elif isinstance(value, bytes):
        return value
    else:
        raise ValueError(f"Cannot serialize bound value of type {type(value)}: {value}")
