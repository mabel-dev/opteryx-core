# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
ANALYZE / DROP STATISTICS orchestration for filesystem datasets.

``ANALYZE TABLE t [FOR COLUMNS …]`` computes, per file and per named column (or
all columns): a KMV sketch, null count, min/max (as ``Vector.ordinalize()``
ordinal keys — see ``opteryx.models.manifest_io.write_manifest_parquet``'s
docstring for what that means and does not mean), a 32-bin equi-width
histogram, record count, and — for VARCHAR/NVARCHAR/VARBINARY columns —
byte-class counts, total byte count, and min/max string length. All of it is
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
from opteryx.models.file_entry import FileEntry
from opteryx.models.manifest_io import DATASET_MANIFEST_NAME
from opteryx.models.manifest_io import HISTOGRAM_BINS
from opteryx.models.manifest_io import is_dataset_manifest
from opteryx.models.manifest_io import read_manifest_char_classes
from opteryx.models.manifest_io import read_manifest_file_entries
from opteryx.models.manifest_io import read_manifest_histograms
from opteryx.models.manifest_io import read_manifest_sketches
from opteryx.models.manifest_io import write_manifest_parquet
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
            "ANALYZE / DROP STATISTICS is not supported for this dataset's "
            "storage backend."
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


def _empty_nested(column_count: int) -> List[list]:
    return [[] for _ in range(column_count)]


def _empty_scalar(column_count: int) -> List[Optional[int]]:
    return [None] * column_count


def _read_existing_stats(manifest_path: str, column_count: int) -> dict:
    """Existing per-file statistics from the dataset manifest — every nested
    stat (KMV sketch, histogram, char-class counts) plus the scalar-per-column
    stats already boxed on FileEntry (null_counts, min/max values, min/max
    lengths, char_total_bytes). A column-subset ANALYZE/DROP STATISTICS merges
    against this so a file's untouched columns survive.

    A file whose stored width no longer matches the current schema is dropped
    entirely: it was computed against a different column set, so its
    positional field_ids are meaningless now (the same staleness rule the
    previous sidecar format applied).
    """
    empty = {"sketch": {}, "histogram": {}, "char_class": {}, "entries": {}}
    if not os.path.exists(manifest_path):
        return empty
    with open(manifest_path, "rb") as handle:
        data = handle.read()

    def _filtered(d):
        return {path: v for path, v in d.items() if len(v) == column_count}

    entries, _native = read_manifest_file_entries(data)
    return {
        "sketch": _filtered(read_manifest_sketches(data)),
        "histogram": _filtered(read_manifest_histograms(data)),
        "char_class": _filtered(read_manifest_char_classes(data)),
        "entries": {e.file_path: e for e in entries},
    }


def _target_categories(schema, targets: List[str]) -> Dict[str, LogicalCategory]:
    by_name = {col.name: col for col in schema.columns}
    return {name: by_name[name].column_type.category for name in targets}


def _analyze_one_file(blob: str, targets: List[str], categories: Dict[str, LogicalCategory]) -> dict:
    """Compute this file's full native statistics pass for each target column:
    KMV sketch, null count, min/max (ordinalize() ordinal keys — see
    manifest_io.write_manifest_parquet's docstring), a HISTOGRAM_BINS-wide
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
                # ARRAY (and possibly other nested/complex types) don't
                # support native hashing -- no min-k sketch for those,
                # everything else works (mirrors the catalog's own
                # _compute_column_stats).
                try:
                    sketches[name].update(col.hash())
                except ValueError:
                    pass
                null_counts[name] += col.null_count()
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
        }
    return {"record_count": record_count, "columns": columns}


def _worker_count(n_files: int) -> int:
    return max(1, min(n_files, (os.cpu_count() or 1)))


def _write_manifest_atomic(
    manifest_path: str,
    entries: List[FileEntry],
    schema,
    sketches,
    histograms=None,
    char_classes=None,
) -> None:
    data = write_manifest_parquet(
        entries, schema, sketches=sketches, histograms=histograms, char_classes=char_classes
    )
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
    count, and (string columns) char-class counts / total bytes / min-max
    length. See _analyze_one_file for the per-file computation.

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
    existing = _read_existing_stats(manifest_path, column_count)

    workers = _worker_count(len(blobs))
    if workers == 1:
        results = [_analyze_one_file(blob, targets, categories) for blob in blobs]
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # Surface any per-file exception by consuming the results.
            results = list(
                pool.map(lambda b: _analyze_one_file(b, targets, categories), blobs)
            )

    entries: List[FileEntry] = []
    sketches: Dict[str, List[List[int]]] = {}
    histograms: Dict[str, List[List[int]]] = {}
    char_classes: Dict[str, List[List[int]]] = {}
    for blob, result in zip(blobs, results):
        prior_entry = existing["entries"].get(blob)

        sketch = list(existing["sketch"].get(blob) or _empty_nested(column_count))
        histogram = list(existing["histogram"].get(blob) or _empty_nested(column_count))
        char_class = list(existing["char_class"].get(blob) or _empty_nested(column_count))

        null_counts = list(prior_entry.null_counts) if prior_entry and prior_entry.null_counts else _empty_scalar(column_count)
        min_values = list(prior_entry.min_values) if prior_entry and prior_entry.min_values else _empty_scalar(column_count)
        max_values = list(prior_entry.max_values) if prior_entry and prior_entry.max_values else _empty_scalar(column_count)
        min_lengths = list(prior_entry.min_lengths) if prior_entry and prior_entry.min_lengths else _empty_scalar(column_count)
        max_lengths = list(prior_entry.max_lengths) if prior_entry and prior_entry.max_lengths else _empty_scalar(column_count)
        char_total_bytes = list(prior_entry.char_total_bytes) if prior_entry and prior_entry.char_total_bytes else _empty_scalar(column_count)

        for name in targets:
            fid = field_ids[name]
            col_stats = result["columns"][name]

            sketch[fid] = list(col_stats["sketch"])
            null_counts[fid] = col_stats["null_count"]

            if col_stats["min_max"] is not None:
                min_values[fid], max_values[fid] = col_stats["min_max"]
            else:
                min_values[fid] = None
                max_values[fid] = None

            histogram[fid] = list(col_stats["histogram"]) if col_stats["histogram"] is not None else []

            if col_stats["char_class_counts"] is not None:
                char_class[fid] = list(col_stats["char_class_counts"])
                char_total_bytes[fid] = col_stats["char_total_bytes"]
                if col_stats["length_range"] is not None:
                    min_lengths[fid], max_lengths[fid] = col_stats["length_range"]
                else:
                    min_lengths[fid] = None
                    max_lengths[fid] = None
            else:
                # Not a string column (or column re-typed since a prior
                # ANALYZE) — no stale char-class data survives under this id.
                char_class[fid] = []
                char_total_bytes[fid] = None
                min_lengths[fid] = None
                max_lengths[fid] = None

        sketches[blob] = sketch
        histograms[blob] = histogram
        char_classes[blob] = char_class
        entries.append(
            FileEntry(
                file_path=blob,
                file_format="PARQUET",
                record_count=result["record_count"],
                file_size_in_bytes=os.path.getsize(blob),
                null_counts=null_counts,
                min_values=min_values,
                max_values=max_values,
                min_lengths=min_lengths,
                max_lengths=max_lengths,
                char_total_bytes=char_total_bytes,
            )
        )

    _write_manifest_atomic(manifest_path, entries, schema, sketches, histograms, char_classes)
    return len(blobs)


def _clear_nested(col_list: List[list], drop_ids: set) -> List[list]:
    return [[] if idx in drop_ids else list(col) for idx, col in enumerate(col_list)]


def _clear_scalar(values: List, drop_ids: set) -> List:
    return [None if idx in drop_ids else v for idx, v in enumerate(values)]


def _entry_has_any_stats(entry: FileEntry) -> bool:
    for lst in (
        entry.null_counts,
        entry.min_values,
        entry.max_values,
        entry.min_lengths,
        entry.max_lengths,
        entry.char_total_bytes,
    ):
        if lst and any(v is not None for v in lst):
            return True
    return False


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
    column_count = len(schema.columns)

    if not columns:
        existing = _read_existing_stats(manifest_path, column_count)
        touched = len(existing["entries"])
        os.remove(manifest_path)
        return touched

    field_ids = _field_ids(table_engine)
    drop_ids = {field_ids[name] for name in _resolve_targets(field_ids, columns)}

    existing = _read_existing_stats(manifest_path, column_count)
    entries = list(existing["entries"].values())

    touched = 0
    sketches: Dict[str, List[List[int]]] = {}
    histograms: Dict[str, List[List[int]]] = {}
    char_classes: Dict[str, List[List[int]]] = {}
    kept_entries: List[FileEntry] = []
    for entry in entries:
        sketch = existing["sketch"].get(entry.file_path)
        histogram = existing["histogram"].get(entry.file_path)
        char_class = existing["char_class"].get(entry.file_path)
        if sketch is None or histogram is None or char_class is None:
            # Width mismatch against the current schema — _read_existing_stats
            # already filtered these out; stale, don't carry forward.
            touched += 1
            continue

        cleared_sketch = _clear_nested(sketch, drop_ids)
        cleared_histogram = _clear_nested(histogram, drop_ids)
        cleared_char_class = _clear_nested(char_class, drop_ids)
        cleared_entry = FileEntry(
            file_path=entry.file_path,
            file_format=entry.file_format,
            record_count=entry.record_count,
            file_size_in_bytes=entry.file_size_in_bytes,
            uncompressed_size_in_bytes=entry.uncompressed_size_in_bytes,
            null_counts=_clear_scalar(entry.null_counts or _empty_scalar(column_count), drop_ids),
            min_values=_clear_scalar(entry.min_values or _empty_scalar(column_count), drop_ids),
            max_values=_clear_scalar(entry.max_values or _empty_scalar(column_count), drop_ids),
            min_lengths=_clear_scalar(entry.min_lengths or _empty_scalar(column_count), drop_ids),
            max_lengths=_clear_scalar(entry.max_lengths or _empty_scalar(column_count), drop_ids),
            char_total_bytes=_clear_scalar(
                entry.char_total_bytes or _empty_scalar(column_count), drop_ids
            ),
        )
        if (
            cleared_sketch != sketch
            or cleared_histogram != histogram
            or cleared_char_class != char_class
        ):
            touched += 1

        sketches[entry.file_path] = cleared_sketch
        histograms[entry.file_path] = cleared_histogram
        char_classes[entry.file_path] = cleared_char_class
        kept_entries.append(cleared_entry)

    any_survives = any(
        any(col for col in sketches[e.file_path])
        or any(col for col in histograms[e.file_path])
        or any(col for col in char_classes[e.file_path])
        or _entry_has_any_stats(e)
        for e in kept_entries
    )
    if any_survives:
        _write_manifest_atomic(manifest_path, kept_entries, schema, sketches, histograms, char_classes)
    else:
        os.remove(manifest_path)

    return touched
