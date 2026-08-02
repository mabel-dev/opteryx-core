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
    "field_ids": "ARRAY",
    "min_lengths": "ARRAY",
    "max_lengths": "ARRAY",
    "char_class_counts": "ARRAY",
    "char_total_bytes": "ARRAY",
}

# Columns whose whole-column native draken Vector the planner reduces with
# native kernels (KMV NDV, histogram fold, char-class fold) rather than the
# boxed per-file lists.
_SKETCH_VECTOR_COLUMNS = ("min_k_hashes", "histogram_counts", "char_class_counts")

# SQL-visible type for every _MANIFEST_COLUMNS entry, used only for
# manifest_output_schema() below — _MANIFEST_COLUMNS' own dtype tags feed the
# writer's `vector_from_sequence(values, dtype=...)` calls and stay as-is.
# min_values/max_values are ARRAY(VARIANT): positionally-by-field-id bounds
# span whatever physical types the dataset's own columns have (int, float, ...)
# and are resolved dynamically at Vector construction (see
# file_entries_to_manifest_morsel) rather than declared statically — the same
# open element-type gap noted for ARRAY_AGG.
def _manifest_column_types():
    from opteryx.types import logical_type as _lt

    return {
        "file_path": _lt.VARCHAR,
        "file_format": _lt.VARCHAR,
        "record_count": _lt.INT64,
        "file_size_in_bytes": _lt.INT64,
        "uncompressed_size_in_bytes": _lt.INT64,
        "column_uncompressed_sizes_in_bytes": _lt.ARRAY(_lt.INT64),
        "null_counts": _lt.ARRAY(_lt.INT64),
        "min_k_hashes": _lt.ARRAY(_lt.ARRAY(_lt.UINT64)),
        "histogram_counts": _lt.ARRAY(_lt.ARRAY(_lt.INT64)),
        "histogram_bins": _lt.INT64,
        "min_values": _lt.ARRAY(_lt.VARIANT),
        "max_values": _lt.ARRAY(_lt.VARIANT),
        "field_ids": _lt.ARRAY(_lt.INT64),
        "min_lengths": _lt.ARRAY(_lt.INT64),
        "max_lengths": _lt.ARRAY(_lt.INT64),
        "char_class_counts": _lt.ARRAY(_lt.ARRAY(_lt.INT64)),
        "char_total_bytes": _lt.ARRAY(_lt.INT64),
    }


def manifest_output_schema(relation_name: str = "$manifest"):
    """The fixed RelationSchema `SHOW MANIFEST FOR <table>` always returns.

    One row per file, every _MANIFEST_COLUMNS column — never trimmed,
    filtered, or projected (SHOW MANIFEST FOR has no WHERE/column-list
    grammar to do so with). row_count_estimate is left unset: the caller
    (visit_show_manifest) knows the real file count from the bound Manifest
    and should set it there instead of this being guessed here.
    """
    from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity

    column_types = _manifest_column_types()
    return RelationSchema(
        name=relation_name,
        columns=[
            SchemaColumn(
                name=name,
                column_type=column_types[name],
                identity=mint_column_identity(relation_name, name),
            )
            for name in _MANIFEST_COLUMNS
        ],
    )

# Fixed equi-width histogram bucket count, shared by every producer (ANALYZE,
# the catalog writer) so `histogram_bins` means the same thing regardless of
# which one wrote a given manifest row.
HISTOGRAM_BINS = 32

# The per-dataset manifest ANALYZE writes for a plain filesystem dataset, stored
# alongside the data files it describes.
#
# It is itself a Parquet file living inside a directory whose data files are
# discovered by a RECURSIVE listing filtered on the `.parquet` suffix — so it
# would be read back as a data file unless explicitly excluded. `is_dataset_manifest`
# is that exclusion and MUST be applied by every parquet-discovery path (the
# scan's and ANALYZE's own — otherwise ANALYZE analyzes its own manifest).
# A reserved, opteryx-prefixed basename is matched exactly, so the guard can
# never subtract a real data file from a dataset.
DATASET_MANIFEST_NAME = "_opteryx_manifest.parquet"


def is_dataset_manifest(path: str) -> bool:
    """True when `path` is a dataset manifest, not a data file. See DATASET_MANIFEST_NAME."""
    return path.replace("\\", "/").rsplit("/", 1)[-1] == DATASET_MANIFEST_NAME


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


def _file_entry_to_manifest_dict(
    file_entry: FileEntry,
    schema,
    sketch: Optional[List],
    histogram: Optional[List],
    char_class: Optional[List],
) -> dict:
    min_values, max_values = _file_entry_bounds_as_values(file_entry, schema)
    return {
        "file_path": file_entry.file_path,
        "file_format": file_entry.file_format,
        "record_count": file_entry.record_count,
        "file_size_in_bytes": file_entry.file_size_in_bytes,
        "uncompressed_size_in_bytes": file_entry.uncompressed_size_in_bytes,
        "column_uncompressed_sizes_in_bytes": file_entry.column_uncompressed_sizes_in_bytes or [],
        "null_counts": file_entry.null_counts or [],
        "min_k_hashes": sketch or [],
        "histogram_counts": histogram or [],
        "histogram_bins": HISTOGRAM_BINS if histogram else 0,
        "min_values": min_values,
        "max_values": max_values,
        "field_ids": list(range(len(schema.columns))),
        "min_lengths": file_entry.min_lengths or [],
        "max_lengths": file_entry.max_lengths or [],
        "char_class_counts": char_class or [],
        "char_total_bytes": file_entry.char_total_bytes or [],
    }


def _nested_int64_array_column(dn, values: List, element_type: int):
    """A positional-per-field, fixed-width-per-field nested INT64 array column
    (histogram_counts' per-file [[bin_count,...] per field], char_class_counts'
    per-file [[class_count,...]*8 per field]) — same two-level nesting shape
    min_k_hashes uses, minus its xxhash-specific unsigned-wraparound masking
    (these are small non-negative counts, never near even int64's range, let
    alone needing an unsigned leaf)."""
    return dn.vector_array_from_sequence(values, element_type=element_type, nesting_depth=2)


def file_entries_to_manifest_morsel(
    file_entries: List[FileEntry],
    schema,
    sketches: Optional[Dict[str, List]] = None,
    histograms: Optional[Dict[str, List]] = None,
    char_classes: Optional[Dict[str, List]] = None,
):
    """Build one manifest Morsel (the `_MANIFEST_COLUMNS` shape) from FileEntry rows.

    Same schema/nested-array encoding as the catalog's write_parquet_manifest —
    ported without any catalog import (draken + rugo only). `schema` supplies
    per-field physical types for decoding serialized-bytes bounds (see
    _decode_bound); pass the relation's RelationSchema.

    `sketches`, `histograms`, and `char_classes` each carry one nested-array
    statistic as ``{file_path: positional-by-field-id list}``: KMV min-k
    hashes, per-column histogram bin counts, and per-column 8-class byte
    counts respectively. They are passed explicitly rather than read off
    FileEntry: on the read side these live only as native vectors (the
    planner's kernels consume those directly), so FileEntry does not carry
    boxed copies that could drift from the vector. Producers that compute
    them (ANALYZE) supply the relevant dict; everyone else omits it and the
    column is written empty.

    NOTE on min_values/max_values semantics: for FileEntry produced by
    ANALYZE's native per-file pass, these are `Vector.ordinalize()` ordinal
    keys (see draken/ops/ordinalize.h), not real decoded values — an int64
    ordinal key IS the real value only for plain signed-integer physical
    types (an identity widen); for float/uint64/string/interval it is a
    monotonic but lossy transform. A literal must be converted with
    `ColumnType.ordinalize(value)` (opteryx/types/logical_type.py) before
    comparing against these bounds. This differs from the pre-existing
    parquet-footer-stats path (`_decode_bound`/`_serialize_bound`), which
    stores real decoded values — the two representations are NOT
    interchangeable; do not compare one against the other. Consumer:
    filesystem_connector.py's `_read_dataset_manifest` reads these bounds into
    FileEntry.lower_bounds/upper_bounds and constructs its Manifest with
    `bounds_are_ordinal=True`, so `Manifest.prune_files` knows to ordinalize
    predicate literals before comparing (see Manifest.__init__'s
    `bounds_are_ordinal` docstring).
    """
    from draken import draken_native as _dn
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel

    sketches = sketches or {}
    histograms = histograms or {}
    char_classes = char_classes or {}
    normalized = [
        _file_entry_to_manifest_dict(
            fe,
            schema,
            sketches.get(fe.file_path),
            histograms.get(fe.file_path),
            char_classes.get(fe.file_path),
        )
        for fe in file_entries
    ]

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
        elif name == "histogram_counts":
            # INT64 leaf: matches the reader kernel's contract exactly
            # (opteryx/compiled/nanobind/vector_sketch_reduce.cpp's
            # histogram_field_slices throws TypeError on anything else).
            morsel.append_vector(
                name, _nested_int64_array_column(_dn, values, _dn.DrakenType.INT64.value)
            )
        elif name == "char_class_counts":
            # INT64 leaf for the same reason and for consistency with
            # histogram_counts — counts, never near uint64 wraparound.
            morsel.append_vector(
                name, _nested_int64_array_column(_dn, values, _dn.DrakenType.INT64.value)
            )
        else:
            morsel.append_vector(name, vector_from_sequence(values, dtype=dtype))

    return morsel


def write_manifest_parquet(
    file_entries: List[FileEntry],
    schema,
    sketches: Optional[Dict[str, List]] = None,
    histograms: Optional[Dict[str, List]] = None,
    char_classes: Optional[Dict[str, List]] = None,
) -> bytes:
    """Serialize a list of FileEntry into one manifest Parquet file (bytes).

    See `file_entries_to_manifest_morsel` for the column shape and the
    sketches/histograms/char_classes contract — this just writes that morsel
    to Parquet.
    """
    from rugo.parquet import write_parquet

    morsel = file_entries_to_manifest_morsel(
        file_entries, schema, sketches=sketches, histograms=histograms, char_classes=char_classes
    )
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

    Sketches are returned ONLY as the native vectors (second element) — the
    planner's kernels read those directly. They are deliberately not boxed onto
    FileEntry: one representation, no per-file Python copy to fall out of step
    with the vector. The vectors' rows are positional to this entry list, so
    callers must keep the two in the same order.
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
        min_lengths = columns["min_lengths"][i] or []
        max_lengths = columns["max_lengths"][i] or []
        # Field_id-correct dict form alongside the positional min_lengths/
        # max_lengths list below -- same reasoning as lower_bounds/upper_bounds
        # above. Local field_id == position (this manifest's own producer,
        # ANALYZE, writes it that way), so plain enumerate() is correct here.
        min_length_bounds = {j: v for j, v in enumerate(min_lengths) if v is not None} or None
        max_length_bounds = {j: v for j, v in enumerate(max_lengths) if v is not None} or None
        entries.append(
            FileEntry(
                file_path=columns["file_path"][i],
                file_format=columns["file_format"][i],
                record_count=columns["record_count"][i],
                file_size_in_bytes=columns["file_size_in_bytes"][i],
                uncompressed_size_in_bytes=columns["uncompressed_size_in_bytes"][i],
                lower_bounds=lower_bounds,
                upper_bounds=upper_bounds,
                min_values=min_values or None,
                max_values=max_values or None,
                column_uncompressed_sizes_in_bytes=columns["column_uncompressed_sizes_in_bytes"][i]
                or None,
                null_counts=columns["null_counts"][i] or None,
                min_lengths=min_lengths or None,
                max_lengths=max_lengths or None,
                min_length_bounds=min_length_bounds,
                max_length_bounds=max_length_bounds,
                char_total_bytes=columns["char_total_bytes"][i] or None,
            )
        )
    return entries, native


def _read_manifest_nested_column(data: bytes, column: str) -> Dict[str, List]:
    """Boxed per-file nested-array column as ``{file_path: positional list}``,
    for any of the three per-file nested statistics (min_k_hashes,
    histogram_counts, char_class_counts). Shared implementation for producers
    that must merge with what is already stored (ANALYZE preserving columns/
    files it isn't re-analyzing). Boxing is fine here: this is admin-path, not
    the planner's hot path — which reads the native vectors instead.
    """
    columns, row_count, _native = read_manifest_columns(data)
    if row_count == 0:
        return {}
    return {
        columns["file_path"][i]: [list(col or []) for col in (columns[column][i] or [])]
        for i in range(row_count)
    }


def read_manifest_sketches(data: bytes) -> Dict[str, List]:
    """Boxed per-file min-k sketches as ``{file_path: positional list}``. See
    _read_manifest_nested_column."""
    return _read_manifest_nested_column(data, "min_k_hashes")


def read_manifest_histograms(data: bytes) -> Dict[str, List]:
    """Boxed per-file histogram bin counts as ``{file_path: positional list}``.
    See _read_manifest_nested_column."""
    return _read_manifest_nested_column(data, "histogram_counts")


def read_manifest_char_classes(data: bytes) -> Dict[str, List]:
    """Boxed per-file char-class byte counts as ``{file_path: positional list}``.
    See _read_manifest_nested_column."""
    return _read_manifest_nested_column(data, "char_class_counts")
