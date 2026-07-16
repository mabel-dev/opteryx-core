# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Native (catalog-free) manifest Parquet reader/writer.

For connectors that manage their own file list (currently LocalStoreConnector)
without an external Iceberg-style catalog. Uses the SAME on-disk schema and
nested-array sketch encoding as the catalog's manifest Parquet
(opteryx_catalog.opteryx_catalog.OpteryxCatalog.write_parquet_manifest /
opteryx_catalog.catalog.manifest.read_manifest_columns) so there is exactly one
manifest format, whether the file list came from the external catalog or from
LocalStoreConnector — no divergence, no JSON-boxed duplicate.

This module depends only on draken + rugo (both already opteryx-core
dependencies) — never on opteryx_catalog. That is what makes it usable by
LocalStoreConnector, which is not catalog-backed.
"""

import struct
from typing import Dict, List, Tuple

from opteryx.models.file_entry import FileEntry

# Column order/dtypes mirror opteryx_catalog's write_parquet_manifest exactly —
# this is the single manifest format shared by both writers. Keep in sync if
# that schema ever changes.
_MANIFEST_COLUMNS = {
    "file_path": "VARCHAR",
    "file_format": "VARCHAR",
    "record_count": "INTEGER",
    "file_size_in_bytes": "INTEGER",
    "uncompressed_size_in_bytes": "INTEGER",
    "column_uncompressed_sizes_in_bytes": "ARRAY",
    "null_counts": "ARRAY",
    "min_k_hashes": "ARRAY",
    "histogram_counts": "ARRAY",
    "histogram_bins": "INTEGER",
    "min_values": "ARRAY",
    "max_values": "ARRAY",
    "min_values_display": "ARRAY",
    "max_values_display": "ARRAY",
    "min_lengths": "ARRAY",
    "max_lengths": "ARRAY",
}

# Columns whose whole-column native draken Vector the planner reduces with
# native kernels (KMV NDV, histogram fold) rather than the boxed per-file lists.
_SKETCH_VECTOR_COLUMNS = ("min_k_hashes", "histogram_counts")


def _decode_bound(raw: bytes, physical):
    """Reverse opteryx.connectors.parquet_io.parquet_writer._serialize_bound.

    `raw` is the exact byte encoding that writer produces (big-endian signed
    int64 or big-endian float64) — decode using the schema's physical type for
    this field, since the bytes carry no type tag of their own.

    Returns None for physical types the manifest's min_values/max_values
    columns don't carry real bounds for: DECIMAL (the writer never bounds it —
    see test_bounds_omit_logical_typed_columns) and VARCHAR/string family. The
    manifest schema is shared with the catalog's own manifest format, whose
    min_values/max_values are INT64-only (matching _COMPRESSIBLE_CATEGORIES in
    opteryx_catalog's stats computation — VARCHAR never gets real min/max there,
    only min_values_display/max_values_display). Giving LocalStore string bounds
    the catalog format can't carry would be a capability mismatch, not a fix —
    string bounds stay in FileEntry.lower_bounds/upper_bounds only (used
    directly by prune_files) and are not persisted into this shared column.
    """
    from draken.draken_native import DrakenType

    if physical in (
        DrakenType.INT8,
        DrakenType.INT16,
        DrakenType.INT32,
        DrakenType.INT64,
    ):
        return int.from_bytes(raw, "big", signed=True)
    if physical in (DrakenType.FLOAT32, DrakenType.FLOAT64):
        return struct.unpack(">d", raw)[0]
    return None


def _file_entry_bounds_as_values(file_entry: FileEntry, schema) -> Tuple[List, List]:
    """Return (min_values, max_values) as dense, decoded per-field-id lists.

    Prefers file_entry.min_values/max_values if already populated (e.g. a
    catalog-origin FileEntry passed through unchanged); otherwise decodes
    lower_bounds/upper_bounds — the serialized-bytes bound format produced by
    the local Parquet writer — using each field's schema physical type.
    """
    if file_entry.min_values is not None and file_entry.max_values is not None:
        return list(file_entry.min_values), list(file_entry.max_values)

    lower = file_entry.lower_bounds or {}
    upper = file_entry.upper_bounds or {}
    if not lower and not upper:
        return [], []

    max_field_id = max(list(lower.keys()) + list(upper.keys()))
    min_values = [None] * (max_field_id + 1)
    max_values = [None] * (max_field_id + 1)
    for field_id, raw in lower.items():
        if raw is None or field_id >= len(schema.columns):
            continue
        physical = schema.columns[field_id].column_type.physical if schema.columns[field_id].column_type else None
        min_values[field_id] = _decode_bound(raw, physical) if isinstance(raw, (bytes, bytearray)) else raw
    for field_id, raw in upper.items():
        if raw is None or field_id >= len(schema.columns):
            continue
        physical = schema.columns[field_id].column_type.physical if schema.columns[field_id].column_type else None
        max_values[field_id] = _decode_bound(raw, physical) if isinstance(raw, (bytes, bytearray)) else raw
    return min_values, max_values


def _file_entry_to_manifest_dict(file_entry: FileEntry, schema) -> dict:
    min_values, max_values = _file_entry_bounds_as_values(file_entry, schema)
    return {
        "file_path": file_entry.file_path,
        "file_format": file_entry.file_format,
        "record_count": file_entry.record_count,
        "file_size_in_bytes": file_entry.file_size_in_bytes,
        "uncompressed_size_in_bytes": file_entry.uncompressed_size_in_bytes,
        "column_uncompressed_sizes_in_bytes": file_entry.column_uncompressed_sizes_in_bytes or [],
        "null_counts": [],
        "min_k_hashes": file_entry.min_k_hashes or [],
        "histogram_counts": file_entry.histogram_counts or [],
        "histogram_bins": file_entry.histogram_bins or 0,
        "min_values": min_values,
        "max_values": max_values,
        "min_values_display": [],
        "max_values_display": [],
        "min_lengths": [],
        "max_lengths": [],
    }


def write_manifest_parquet(file_entries: List[FileEntry], schema) -> bytes:
    """Serialize a list of FileEntry into one manifest Parquet file (bytes).

    Same schema/nested-array encoding as the catalog's write_parquet_manifest —
    ported without any catalog import (draken + rugo only). `schema` supplies
    per-field physical types for decoding serialized-bytes bounds (see
    _decode_bound); pass the relation's RelationSchema.
    """
    from draken import draken_native as _dn
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    normalized = [_file_entry_to_manifest_dict(fe, schema) for fe in file_entries]

    morsel = Morsel()
    for name, dtype in _MANIFEST_COLUMNS.items():
        values = [e.get(name) for e in normalized]
        if name == "min_k_hashes":
            # UINT64 leaf: xxhash values span the full unsigned range; a signed
            # leaf would read back negative above INT64_MAX and corrupt min-k
            # ordering (see opteryx_catalog's own write_parquet_manifest).
            def _norm_col(col):
                if col is None:
                    return None
                return [None if h is None else (int(h) & 0xFFFFFFFFFFFFFFFF) for h in col]

            values = [
                None if entry is None else [_norm_col(col) for col in entry] for entry in values
            ]
            morsel.append_vector(
                name,
                _dn.vector_array_from_sequence(
                    values, element_type=_dn.DrakenType.UINT64.value, nesting_depth=2
                ),
            )
        else:
            morsel.append_vector(name, vector_from_sequence(values, dtype=dtype))

    return write_parquet(morsel, compression="zstd", bloom_filters=True)


def read_manifest_columns(data: bytes, keep_native: Tuple[str, ...] = ()) -> Tuple[dict, int, dict]:
    """Decode manifest parquet bytes into ({column: [values...]}, row_count, native).

    Catalog-free port of opteryx_catalog.catalog.manifest.read_manifest_columns
    — identical semantics (same nested-array decode, same keep_native contract)
    so the two writers/readers stay format-compatible. `native` retains the
    whole-column draken Vector for each name in `keep_native` (combined across
    row groups via Morsel.combine when the manifest spans more than one).
    """
    if not data:
        return {}, 0, {}

    from rugo import parquet as _rugo_parquet

    column_data: Dict[str, list] = {}
    row_count = 0
    kept_morsels: list = []
    with _rugo_parquet.read_parquet(bytes(data)) as reader:
        for morsel in reader:
            row_count += morsel.num_rows
            if keep_native:
                kept_morsels.append(morsel)
            for name_b in morsel.column_names:
                name = (
                    name_b.decode("utf-8") if isinstance(name_b, (bytes, bytearray)) else name_b
                )
                column_data.setdefault(name, []).extend(morsel.column(name_b).to_pylist())

    native: dict = {}
    if kept_morsels:
        combined = (
            kept_morsels[0] if len(kept_morsels) == 1 else kept_morsels[0].combine(kept_morsels)
        )
        for name in keep_native:
            name_b = name.encode("utf-8")
            if name_b in combined.column_names or name in combined.column_names:
                native[name] = combined.column(name_b)

    return column_data, row_count, native


def read_manifest_file_entries(data: bytes) -> Tuple[List[FileEntry], dict]:
    """Decode a manifest parquet into (FileEntry list, native sketch vectors).

    lower_bounds/upper_bounds are rebuilt as int-keyed dicts of the SAME decoded
    values as min_values/max_values (matching FileEntry.from_datafile's
    dict-path convention), so Manifest.prune_files compares like-for-like.
    """
    columns, row_count, native = read_manifest_columns(data, keep_native=_SKETCH_VECTOR_COLUMNS)
    if row_count == 0:
        return [], native

    entries = []
    for i in range(row_count):
        min_values = columns["min_values"][i] or []
        max_values = columns["max_values"][i] or []
        lower_bounds = {j: v for j, v in enumerate(min_values) if v is not None} or None
        upper_bounds = {j: v for j, v in enumerate(max_values) if v is not None} or None
        entries.append(
            FileEntry(
                file_path=columns["file_path"][i],
                file_format=columns["file_format"][i],
                record_count=columns["record_count"][i],
                file_size_in_bytes=columns["file_size_in_bytes"][i],
                uncompressed_size_in_bytes=columns["uncompressed_size_in_bytes"][i],
                lower_bounds=lower_bounds,
                upper_bounds=upper_bounds,
                min_k_hashes=columns["min_k_hashes"][i] or None,
                histogram_counts=columns["histogram_counts"][i] or None,
                histogram_bins=columns["histogram_bins"][i] or None,
                min_values=min_values or None,
                max_values=max_values or None,
                column_uncompressed_sizes_in_bytes=columns["column_uncompressed_sizes_in_bytes"][i]
                or None,
            )
        )
    return entries, native
