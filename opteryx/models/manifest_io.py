# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The manifest Parquet format's Python-side constants: its column set (the SHOW
MANIFEST output shape), the shared histogram width, the per-dataset manifest's
reserved name, and SHOW MANIFEST's text rendering of a bound.

Reading and writing the format is native (NativeManifest: manifest_decode.hpp,
manifest_encode.hpp). The format is the SAME one opteryx_catalog writes, so there
is exactly one manifest format whichever producer wrote it.
"""

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
    # OPTIONAL, and ESTIMATE-ONLY. Positional per-field-id distinct-value
    # counts, for a producer whose source publishes NDV as a number rather than
    # as something mergeable (the control plane's PostgreSQL/CockroachDB stats
    # refresh: `SHOW STATISTICS`' distinct_count, `pg_stats`' n_distinct).
    #
    # Appended LAST on purpose. A manifest written before this column existed
    # simply does not carry it and must keep reading (manifest_decode.hpp treats
    # the column as optional), so no stored manifest needs rewriting.
    #
    # The exactness flag of a distinct count is deliberately NOT persisted:
    # everything read back out of this column is marked not exact. That is the safe direction - an exact count read back as
    # an estimate loses an optimisation (it can no longer be a BOUND for
    # _exact_cardinality_from_footers, so never prunes or answers a DISTINCT),
    # where an estimate read back as exact would lose rows. Exact NDV travels
    # by its own routes (parquet footer `column_stats`, skene's sketches), not
    # through here.
    "distinct_counts": "ARRAY",
}

# SQL-visible type for every _MANIFEST_COLUMNS entry, used only for
# manifest_output_schema() below.
# min_values/max_values are ARRAY(VARCHAR) HERE and nowhere else: this schema
# describes the SHOW MANIFEST output only, which renders bounds as text
# (NativeManifest.show_morsel). One positional list
# holds one bound per field id, so it spans whatever physical types the
# dataset's own columns have — int for one field, str for the next when the
# source is an external catalog carrying real decoded bounds. A draken ARRAY
# vector has ONE child type for the whole column, so that mixture has no typed
# representation; text is the only encoding that can carry all of them. The
# persisted manifest (NativeManifest.to_parquet) keeps the typed encoding.
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
        "min_values": _lt.ARRAY(_lt.VARCHAR),
        "max_values": _lt.ARRAY(_lt.VARCHAR),
        "field_ids": _lt.ARRAY(_lt.INT64),
        "min_lengths": _lt.ARRAY(_lt.INT64),
        "max_lengths": _lt.ARRAY(_lt.INT64),
        "char_class_counts": _lt.ARRAY(_lt.ARRAY(_lt.INT64)),
        "char_total_bytes": _lt.ARRAY(_lt.INT64),
        "distinct_counts": _lt.ARRAY(_lt.INT64),
    }


def manifest_output_schema(relation_name: str = "$manifest"):
    """The fixed RelationDescriptor `SHOW MANIFEST FOR <table>` always returns.

    One row per file, every _MANIFEST_COLUMNS column — never trimmed,
    filtered, or projected (SHOW MANIFEST FOR has no WHERE/column-list
    grammar to do so with). row_count_estimate is left unset: the caller
    (visit_show_manifest) knows the real file count from the bound Manifest
    and should set it there instead of this being guessed here.
    """
    from opteryx.types.schema import ColumnDescriptor, RelationDescriptor

    column_types = _manifest_column_types()
    return RelationDescriptor(
        name=relation_name,
        columns=[
            ColumnDescriptor(
                name=name,
                column_type=column_types[name],
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


def _bound_as_text(value):
    """One manifest bound rendered for SHOW MANIFEST, or None.

    A row's `min_values`/`max_values` is one list holding one bound per field
    id, so its elements are as heterogeneous as the dataset's columns are: an
    int64 ordinal from opteryx_catalog's own stats builder, a real `str` for a
    VARCHAR column and a real `float` for a DOUBLE from an external catalog
    that decodes its manifest bounds (opteryx-iceberg). A draken ARRAY vector
    carries ONE child type for the whole column, so that mixture cannot be
    expressed typed — the first non-null element fixes the leaf and the next
    element of another type is a hard error. Text is what all of them share.

    This is a DISPLAY encoding and is never persisted: the manifest writer
    (NativeManifest.to_parquet) keeps the typed encoding the shared manifest
    format (and file pruning) depends on.

    Bytes are decoded as UTF-8 when they are UTF-8 and rendered as hex when
    they are not, rather than as a Python `b'...'` repr, which would be this
    module's repr leaking into a result set.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            return bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            return bytes(value).hex()
    return str(value)
