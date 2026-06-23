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
from rugo.parquet_writer import write_parquet_with_bounds

from opteryx.models.file_entry import FileEntry
from opteryx.utils import random_string


def write_morsel(morsel: Morsel, relation_dir: str) -> FileEntry:
    """Write a Morsel as a single parquet file in relation_dir.

    File name is data-{random_string}.parquet (relative path stored in FileEntry).

    Args:
        morsel: Draken Morsel containing the rows to write.
        relation_dir: Path to the relation directory. Must already exist.

    Returns:
        FileEntry with file_path (relative to relation_dir), file_format="PARQUET",
        record_count, file_size_in_bytes, and lower_bounds/upper_bounds populated
        from the writer's per-column min/max statistics where available.

    Raises:
        ValueError: If morsel is empty (zero rows).
        OSError: If write fails.
    """
    if len(morsel) == 0:
        raise ValueError("cannot write empty morsel")

    data, bounds = write_parquet_with_bounds(morsel, compression="zstd")

    file_name = f"data-{random_string(32)}.parquet"
    full_path = os.path.join(relation_dir, file_name)
    tmp_path = f"{full_path}.tmp"

    with open(tmp_path, "wb") as f:
        f.write(data)
    os.replace(tmp_path, full_path)

    file_size_in_bytes = os.path.getsize(full_path)
    lower_bounds, upper_bounds = _bounds_to_entry(bounds)

    return FileEntry(
        file_path=file_name,
        file_format="PARQUET",
        record_count=len(morsel),
        file_size_in_bytes=file_size_in_bytes,
        uncompressed_size_in_bytes=None,
        lower_bounds=lower_bounds,
        upper_bounds=upper_bounds,
        null_value_counts=None,
        min_k_hashes=None,
        histogram_counts=None,
        histogram_bins=None,
        min_values=None,
        max_values=None,
        column_uncompressed_sizes_in_bytes=None,
    )


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
