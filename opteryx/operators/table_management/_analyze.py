# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
ANALYZE / DROP STATISTICS orchestration for filesystem datasets.

``ANALYZE TABLE t [FOR COLUMNS …]`` computes, per file and per named column (or
all columns): a KMV sketch, null count, min/max (as ``Vector.ordinalize()``
ordinal keys — ``draken/ops/ordinalize.h`` states what that means and does
not mean), a 32-bin equi-width
histogram, record count, uncompressed byte size (per column and per file, read
off the footer), and — for VARCHAR/NVARCHAR/VARBINARY columns — byte-class
counts, total byte count, and min/max string length. All of it is
written into the dataset's single manifest — the same Parquet manifest format
the catalog and LocalStore use (see ``opteryx.models.manifest_io``). One
manifest per dataset, one format everywhere. ``DROP STATISTICS ON t [FOR
COLUMNS …]`` removes those statistics.

Per-file orchestration (which files, concurrency, manifest read/write) is
plain Python — admin-path, not a hot path. Every PER-ROW reduction is native:
this engine runs at TB scale, where a Python-level ``min()``/``max()``/loop
over row data is not an admin-path nicety, it is a correctness-adjacent
performance bug (see the git history of this file). ``_sketch_one_file``'s
only Python-level work over already-native-reduced values is combining a
handful of per-morsel summaries (a handful of scalars, not rows) into one
per-file summary.

Scope: local filesystem datasets. Remote/object-store writes are a separate
increment; an unsupported backend fails loudly rather than silently no-op'ing.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem
from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models.manifest_io import DATASET_MANIFEST_NAME
from opteryx.models.manifest_io import HISTOGRAM_BINS
from opteryx.models.manifest_io import is_dataset_manifest
from opteryx.types.logical_type import LogicalCategory
from opteryx.utils.kmv import ColumnSketch

_PARQUET_SUFFIX = ".parquet"

# VARCHAR/NVARCHAR/VARBINARY only — the categories vector_char_class_stats
# accepts (see opteryx/compiled/nanobind/vector_char_class_stats.cpp).
_STRING_CATEGORIES = frozenset(
    {LogicalCategory.VARCHAR, LogicalCategory.NVARCHAR, LogicalCategory.VARBINARY}
)


def _is_catalog_backed(table_engine) -> bool:
    """True for a catalog-backed dataset, whose ANALYZE is delegated to the
    catalog itself (see _analyze_catalog.py) rather than computed here."""
    from opteryx.connectors.opteryx_connector import OpteryxTable

    return isinstance(table_engine, OpteryxTable)


def _require_local(table_engine) -> None:
    if not isinstance(getattr(table_engine, "filesystem", None), OpteryxLocalFileSystem):
        raise UnsupportedSyntaxError(
            "**ANALYZE** / DROP STATISTICS is not supported for this dataset's "
            "storage backend. Statistics can only be dropped for datasets held in the catalog."
        )


def _parquet_blobs(table_engine) -> List[str]:
    """The dataset's data files. Excludes the dataset manifest, which is itself a
    parquet file in the same tree — analyzing it would be nonsense."""
    blobs = table_engine.get_list_of_blob_names(table_engine.dataset)
    return [
        b
        for b in blobs
        if b.lower().endswith(_PARQUET_SUFFIX) and not is_dataset_manifest(b)
    ]


def _manifest_path(table_engine) -> str:
    return os.path.join(table_engine.dataset, DATASET_MANIFEST_NAME)


def _field_ids(table_engine) -> Dict[str, int]:
    schema = table_engine.get_dataset_schema()
    return {col.name: i for i, col in enumerate(schema.columns)}


def _resolve_targets(field_ids: Dict[str, int], columns: Optional[Sequence[str]]) -> List[str]:
    if not columns:
        return list(field_ids.keys())
    targets = []
    for name in columns:
        if name not in field_ids:
            raise ColumnNotFoundError(column=name)
        targets.append(name)
    return targets


def _schema_layout(schema) -> Tuple[tuple, tuple]:
    """The dataset's columns and their physical types, in load-time order - the
    positions every manifest list is keyed by."""
    return (
        tuple(col.name for col in schema.columns),
        tuple(col.column_type.physical for col in schema.columns),
    )


def _read_prior_manifest(manifest_path: str, schema):
    """The dataset manifest ANALYZE / DROP STATISTICS last wrote, decoded
    natively, or None when there is none. A column-subset run carries its
    untouched columns' statistics forward from it."""
    from opteryx.compiled.planner.native_manifest import decode_manifest_parquet

    if not os.path.exists(manifest_path):
        return None
    with open(manifest_path, "rb") as handle:
        data = handle.read()
    names, physical = _schema_layout(schema)
    return decode_manifest_parquet(data, names, physical, {}, True, True)


def _target_categories(schema, targets: List[str]) -> Dict[str, LogicalCategory]:
    by_name = {col.name: col for col in schema.columns}
    return {name: by_name[name].column_type.category for name in targets}


def _footer_size_stats(
    table_engine, blobs: List[str], schema
) -> Dict[str, Tuple[Optional[int], List[Optional[int]]]]:
    """Per-file ``(uncompressed_size_in_bytes, column_uncompressed_sizes)``,
    read off the parquet footers.

    Sizes are a property of the FILE, not of the columns ANALYZE was asked to
    analyze: a ``FOR COLUMNS`` subset still records every column's size, because
    a partially-filled size list read back positionally would attribute one
    column's bytes to another. They come from the footer rather than from
    `_analyze_one_file`'s decoded morsels for the same reason — the morsels only
    carry the target columns, and their in-memory size is not the file's
    on-disk uncompressed size anyway.

    ONE batched, GIL-released acquisition for the whole file set (the only
    sanctioned plan-time stats entry point — see fetch_column_stats_many, which
    forbids the per-file loop). A footer that cannot be read raises for the whole
    call, and ANALYZE lets it: the user asked for statistics over these files, so
    an unreadable file is a failure to report, not a column to quietly leave
    empty.

    Per column, `None` means the footer recorded no size for it — kept as None,
    never 0. One such column makes the FILE total None too: a partial sum
    understates the real size and is indistinguishable from a small file.
    """
    from opteryx.connectors.parquet_io.pool_reader import fetch_column_stats_many

    # field_id == position for a local dataset, which is the key bind_schema
    # assigns and the same key `_field_ids` hands the rest of this module.
    column_names = [col.name for col in schema.columns]
    file_sizes = {blob: os.path.getsize(blob) for blob in blobs}

    sizes: Dict[str, Tuple[Optional[int], List[Optional[int]]]] = {}
    # strict: the returned list is parallel to `blobs` by contract, and a silent
    # zip truncation would record one file's sizes against another's path.
    for blob, (_record_count, _row_groups, column_stats) in zip(
        blobs,
        fetch_column_stats_many(table_engine.filesystem, blobs, file_sizes),
        strict=True,
    ):
        column_stats.bind_schema(column_names)
        per_column = [
            column_stats.get_uncompressed_size(field_id)
            for field_id in range(len(column_names))
        ]
        total = None if any(size is None for size in per_column) else sum(per_column)
        sizes[blob] = (total, per_column)
    return sizes


def _analyze_one_file(blob: str, targets: List[str], categories: Dict[str, LogicalCategory]) -> dict:
    """Compute this file's full native statistics pass for each target column:
    KMV sketch, null count, min/max (ordinalize() ordinal keys — see
    draken/ops/ordinalize.h), a HISTOGRAM_BINS-wide
    equi-width histogram, record count, and — for string-family columns —
    byte-class counts, total byte count, and min/max string length.
    Self-contained (own reader, no shared state) so files analyze concurrently.

    min/max and the histogram need the FILE-WIDE ordinal range before any row
    can be bucketed, so each morsel's ordinalized column is buffered (a
    compact INT64 vector, not the raw column) rather than re-reading the file
    a second time: one pass over the on-disk data, min/max derived natively
    from the buffered vectors, then histogram bucketing natively against that
    range. Every per-row reduction (hash, null count, ordinalize, char-class
    counts, min/max, histogram) is a native kernel; the only Python-level work
    below is combining a handful of per-morsel summaries (scalars/short lists,
    not rows) into one per-file summary.
    """
    import rugo.parquet as rugo_parquet

    string_targets = {name for name in targets if categories[name] in _STRING_CATEGORIES}
    array_targets = {name for name in targets if categories[name] is LogicalCategory.ARRAY}
    # Integer columns get their EXACT sum (draken/ops/exact_sum.h) — the
    # statistic SUM/AVG are answered from. A morsel whose vector has no exact
    # sum makes the file's sum unknown (None), never a partial one.
    integer_targets = {name for name in targets if categories[name] is LogicalCategory.INTEGER}
    sums: Dict[str, Optional[int]] = {name: 0 for name in integer_targets}

    sketches = {name: ColumnSketch() for name in targets}
    null_counts = {name: 0 for name in targets}
    ordinal_vectors: Dict[str, list] = {name: [] for name in targets}
    char_counts = {name: [0] * 8 for name in string_targets}
    char_total_bytes = {name: 0 for name in string_targets}
    length_range: Dict[str, Optional[Tuple[int, int]]] = {name: None for name in string_targets}
    record_count = 0

    with rugo_parquet.read_parquet(blob, columns=targets) as reader:
        for morsel in reader:
            record_count += morsel.num_rows
            for name in targets:
                col = morsel.column(name)
                # ARRAY is the one category Vector.hash() declines -- no min-k
                # sketch for those, everything else works (mirrors the
                # catalog's own _compute_column_stats). Asked up front rather
                # than caught: the type is known before the call, and a throw
                # from hash() now means a MALFORMED column (an fp16 vector
                # missing its descriptor), which must fail ANALYZE rather than
                # silently cost it a sketch.
                if name not in array_targets:
                    sketches[name].update(col.hash())
                null_counts[name] += col.null_count()
                if name in integer_targets and sums[name] is not None:
                    summed = col.exact_sum()
                    sums[name] = None if summed is None else sums[name] + summed[0]
                # ordinalize() doesn't support ARRAY/VECTOR_FP16/DECIMAL128
                # (see draken/ops/ordinalize.h) -- no min/max/histogram for
                # those columns rather than crashing the whole ANALYZE.
                try:
                    ordinal_vectors[name].append(col.ordinalize())
                except ValueError:
                    pass
                if name in string_targets:
                    counts, total_bytes, lengths = col.char_class_stats()
                    for i in range(8):
                        char_counts[name][i] += counts[i]
                    char_total_bytes[name] += total_bytes
                    if lengths is not None:
                        lo, hi = lengths
                        cur = length_range[name]
                        length_range[name] = (
                            (lo, hi) if cur is None else (min(cur[0], lo), max(cur[1], hi))
                        )

    columns = {}
    for name in targets:
        vecs = ordinal_vectors[name]
        pairs = [p for p in (v.ordinal_min_max() for v in vecs) if p is not None]
        min_max = None
        histogram = None
        if pairs:
            vmin = min(p[0] for p in pairs)
            vmax = max(p[1] for p in pairs)
            min_max = (vmin, vmax)
            bins = [0] * HISTOGRAM_BINS
            for v in vecs:
                per = v.histogram_bucket(vmin, vmax, HISTOGRAM_BINS)
                for i in range(HISTOGRAM_BINS):
                    bins[i] += per[i]
            histogram = bins
        columns[name] = {
            "sketch": sketches[name].min_k(),
            "null_count": null_counts[name],
            "min_max": min_max,
            "histogram": histogram,
            "char_class_counts": char_counts.get(name),
            "char_total_bytes": char_total_bytes.get(name),
            "length_range": length_range.get(name),
            "sum": sums.get(name),
        }
    return {"record_count": record_count, "columns": columns}


def _worker_count(n_files: int) -> int:
    return max(1, min(n_files, (os.cpu_count() or 1)))


def _write_manifest_atomic(manifest_path: str, manifest) -> None:
    data = manifest.to_parquet()
    tmp = manifest_path + ".tmp"
    with open(tmp, "wb") as handle:
        handle.write(data)
    os.replace(tmp, manifest_path)


def analyze_table(
    table_engine, columns: Optional[Sequence[str]], author: Optional[str] = None
) -> int:
    """Compute native per-file statistics for ``columns`` (or all columns)
    over every parquet file of the dataset and write them into the dataset's
    single manifest — KMV sketch, null count, min/max, histogram, record
    count, uncompressed sizes, and (string columns) char-class counts / total
    bytes / min-max length. See _analyze_one_file for the per-file computation
    and _footer_size_stats for the sizes (whole schema, footer-read, not
    limited to `columns`).

    Files are analyzed concurrently — on the free-threaded build this is real
    parallelism across cores; each file is independent (own reader). The manifest
    is then written once, atomically.

    A column-subset ANALYZE merges: previously-analyzed columns of a file survive,
    and files not re-analyzed keep their existing statistics.

    Returns the number of files analyzed.

    A catalog-backed dataset is delegated to the catalog's own statistics
    refresh instead (see _analyze_catalog.analyze_table_catalog) — everything
    below this branch is the local-filesystem implementation. `author` is only
    meaningful on that catalog path (it records who committed the resulting
    snapshot); the local path has no snapshot chain and ignores it.
    """
    if _is_catalog_backed(table_engine):
        from opteryx.operators.table_management._analyze_catalog import analyze_table_catalog

        return analyze_table_catalog(table_engine, columns, author=author)

    _require_local(table_engine)
    schema = table_engine.get_dataset_schema()
    column_count = len(schema.columns)
    field_ids = _field_ids(table_engine)
    targets = _resolve_targets(field_ids, columns)
    categories = _target_categories(schema, targets)
    blobs = _parquet_blobs(table_engine)
    if not blobs:
        return 0

    manifest_path = _manifest_path(table_engine)
    prior = _read_prior_manifest(manifest_path, schema)
    file_sizes = _footer_size_stats(table_engine, blobs, schema)

    workers = _worker_count(len(blobs))
    if workers == 1:
        results = [_analyze_one_file(blob, targets, categories) for blob in blobs]
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # Surface any per-file exception by consuming the results.
            results = list(
                pool.map(lambda b: _analyze_one_file(b, targets, categories), blobs)
            )

    from opteryx.compiled.planner.native_manifest import NativeManifestBuilder

    names, physical = _schema_layout(schema)
    builder = NativeManifestBuilder(names, physical, True, True)
    for blob, result in zip(blobs, results):
        uncompressed_size, column_sizes = file_sizes[blob]
        row = builder.add_file(
            blob,
            "PARQUET",
            result["record_count"],
            os.path.getsize(blob),
            -1,
            -1 if uncompressed_size is None else uncompressed_size,
            HISTOGRAM_BINS,
        )
        prior_row = None if prior is None else prior.find_file(blob)
        if prior_row is not None:
            builder.carry_statistics(row, prior, prior_row)
        builder.start_sketch_rows(row)
        for position, size in enumerate(column_sizes or ()):
            if size is not None:
                builder.set_counts(row, position, uncompressed_size=size)

        for name in targets:
            fid = field_ids[name]
            col_stats = result["columns"][name]
            builder.clear_statistics(row, fid)

            builder.set_sketch(row, fid, "min_k", list(col_stats["sketch"]))
            builder.set_counts(row, fid, null_count=col_stats["null_count"])
            if col_stats["sum"] is not None:
                builder.set_sum(row, fid, col_stats["sum"])
            if col_stats["min_max"] is not None:
                low, high = col_stats["min_max"]
                builder.set_ordinal_bound(row, fid, True, low)
                builder.set_ordinal_bound(row, fid, False, high)
            if col_stats["histogram"] is not None:
                builder.set_sketch(row, fid, "histogram", list(col_stats["histogram"]))
            # A column that is not a string (or was re-typed since a prior
            # ANALYZE) keeps no char-class data under this position.
            if col_stats["char_class_counts"] is not None:
                builder.set_sketch(row, fid, "char_class", list(col_stats["char_class_counts"]))
                builder.set_counts(row, fid, char_total_bytes=col_stats["char_total_bytes"])
                if col_stats["length_range"] is not None:
                    low, high = col_stats["length_range"]
                    builder.set_counts(row, fid, min_length=low, max_length=high)

    _write_manifest_atomic(manifest_path, builder.build({}))
    return len(blobs)


def drop_statistics(table_engine, columns: Optional[Sequence[str]]) -> int:
    """Remove statistics from the dataset's manifest.

    No column list → delete the manifest entirely. With a column list → clear only
    those columns' statistics (sketch, null count, min/max, histogram, char-class,
    lengths), deleting the manifest when nothing remains. Idempotent: an absent
    manifest is not an error. Returns the number of files whose statistics were
    modified (or, for a whole-manifest delete, the file count it described).
    Never touches the parquet data files.

    Not supported for catalog-backed datasets: their manifest entries carry
    statistics from the moment each file is written, so there is no
    "statistics absent" state to drop back to — the manifest row itself would
    have to go, which would delete the dataset's record of the file.
    """
    if _is_catalog_backed(table_engine):
        raise UnsupportedSyntaxError("DROP STATISTICS is not supported for this dataset.")

    _require_local(table_engine)
    manifest_path = _manifest_path(table_engine)
    if not os.path.exists(manifest_path):
        return 0

    schema = table_engine.get_dataset_schema()
    prior = _read_prior_manifest(manifest_path, schema)

    if not columns:
        os.remove(manifest_path)
        return len(prior)

    field_ids = _field_ids(table_engine)
    drop_ids = sorted({field_ids[name] for name in _resolve_targets(field_ids, columns)})

    from opteryx.compiled.planner.native_manifest import NativeManifestBuilder

    names, physical = _schema_layout(schema)
    builder = NativeManifestBuilder(names, physical, True, True)
    touched = 0
    for prior_row in range(len(prior)):
        if prior.sketch_row_widths(prior_row) != (len(names),) * 3:
            # Statistics computed against a different column set - stale, so
            # not carried forward.
            touched += 1
            continue
        file = prior.file_row(prior_row)
        # Sizes and counts are facts about the FILE, not statistics about the
        # values in a column, so DROP STATISTICS leaves them alone.
        row = builder.add_file(
            file["file_path"],
            file["file_format"],
            -1 if file["record_count"] is None else file["record_count"],
            file["file_size_in_bytes"],
            -1,
            -1 if file["uncompressed_size_in_bytes"] is None else file["uncompressed_size_in_bytes"],
            -1 if file["histogram_bins"] is None else file["histogram_bins"],
        )
        builder.carry_statistics(row, prior, prior_row)
        for position in range(len(names)):
            size = prior.cell(prior_row, position)["uncompressed_size"]
            if size is not None:
                builder.set_counts(row, position, uncompressed_size=size)
        cleared = False
        for fid in drop_ids:
            cleared = builder.clear_statistics(row, fid) or cleared
        if cleared:
            touched += 1

    if builder.has_statistics():
        _write_manifest_atomic(manifest_path, builder.build({}))
    else:
        os.remove(manifest_path)

    return touched
