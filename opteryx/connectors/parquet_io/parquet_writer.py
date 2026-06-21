# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parquet writer for opteryx-managed relations.

This module is the SOLE permitted location for pyarrow usage in opteryx.
Do not import pyarrow elsewhere in production code.
"""

import os
import struct
from typing import Dict, Optional, Tuple

from draken.morsels.morsel import Morsel

from opteryx.models.file_entry import FileEntry
from opteryx.utils import random_string


def write_morsel(morsel: Morsel, relation_dir: str) -> FileEntry:
    """Write a Morsel as a single parquet file in relation_dir.

    File name is data-{random_string}.parquet (relative path stored in FileEntry).

    Args:
        morsel: Draken Morsel containing the rows to write.
        relation_dir: Absolute or relative path to the relation directory.
                      Must already exist.

    Returns:
        FileEntry with file_path (relative to relation_dir), file_format="PARQUET",
        record_count, file_size_in_bytes, and lower_bounds/upper_bounds populated
        from parquet footer metadata where available.

    Raises:
        ValueError: If morsel is empty (zero rows).
        OSError: If write fails.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    if len(morsel) == 0:
        raise ValueError("cannot write empty morsel")

    table = morsel.to_arrow()
    file_name = f"data-{random_string(32)}.parquet"
    full_path = os.path.join(relation_dir, file_name)
    tmp_path = f"{full_path}.tmp"

    pq.write_table(table, tmp_path, compression="snappy")
    os.replace(tmp_path, full_path)

    pf = pq.ParquetFile(full_path)

    record_count = pf.metadata.num_rows
    file_size_in_bytes = os.path.getsize(full_path)
    lower_bounds, upper_bounds = _extract_bounds(pf.metadata, table.schema)

    return FileEntry(
        file_path=file_name,
        file_format="PARQUET",
        record_count=record_count,
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


def _extract_bounds(
    metadata, schema
) -> Tuple[Optional[Dict[int, bytes]], Optional[Dict[int, bytes]]]:
    """Extract min/max bounds from parquet file metadata.

    Iterates row groups and columns, computing file-level min/max statistics.
    Skips columns with missing statistics or nested types.

    Args:
        metadata: pyarrow parquet metadata object
        schema: pyarrow table schema

    Returns:
        Tuple of (lower_bounds dict, upper_bounds dict) indexed by field_id (column index).
        Either or both may be None if no statistics are available.
    """
    lower_bounds = {}
    upper_bounds = {}

    for field_idx in range(len(schema)):
        schema_field = schema.field(field_idx)

        if _is_nested_type(schema_field.type):
            continue

        col_min = None
        col_max = None

        for row_group_idx in range(metadata.num_row_groups):
            rg = metadata.row_group(row_group_idx)
            col_meta = rg.column(field_idx)

            if not col_meta.is_stats_set:
                continue

            stats = col_meta.statistics
            if not stats or not stats.has_min_max:
                continue

            min_val = stats.min
            max_val = stats.max

            if col_min is None:
                col_min = min_val
                col_max = max_val
            else:
                col_min = min(col_min, min_val)
                col_max = max(col_max, max_val)

        if col_min is not None and col_max is not None:
            lower_bounds[field_idx] = _serialize_bound(col_min)
            upper_bounds[field_idx] = _serialize_bound(col_max)

    return (
        lower_bounds if lower_bounds else None,
        upper_bounds if upper_bounds else None,
    )


def _is_nested_type(arrow_type) -> bool:
    """Check if an Arrow type is nested (struct, list, map, fixed_size_list, etc)."""
    import pyarrow as pa

    return (
        pa.types.is_struct(arrow_type)
        or pa.types.is_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
        or pa.types.is_map(arrow_type)
    )


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
