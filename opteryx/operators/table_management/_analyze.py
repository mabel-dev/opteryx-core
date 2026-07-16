# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
ANALYZE / DROP STATISTICS orchestration for filesystem datasets.

``ANALYZE TABLE t [FOR COLUMNS …]`` computes a per-file KMV sketch for the named
columns (or all columns) and writes them into the dataset's single manifest — the
same Parquet manifest format the catalog and LocalStore use (see
``opteryx.models.manifest_io``). One manifest per dataset, one format everywhere.
``DROP STATISTICS ON t [FOR COLUMNS …]`` removes those sketches.

This is admin orchestration, not a hot path — plain Python is appropriate. The
sketch contract lives in ``opteryx.utils.kmv``; reading is via the native rugo
reader (no pyarrow in the engine).

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

from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem
from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models.file_entry import FileEntry
from opteryx.models.manifest_io import DATASET_MANIFEST_NAME
from opteryx.models.manifest_io import is_dataset_manifest
from opteryx.models.manifest_io import read_manifest_file_entries
from opteryx.models.manifest_io import read_manifest_sketches
from opteryx.models.manifest_io import write_manifest_parquet
from opteryx.utils.kmv import ColumnSketch

_PARQUET_SUFFIX = ".parquet"


def _require_local(table_engine) -> None:
    if not isinstance(getattr(table_engine, "filesystem", None), OpteryxLocalFileSystem):
        raise UnsupportedSyntaxError(
            "ANALYZE / DROP STATISTICS is currently supported only for local "
            "filesystem datasets."
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


def _read_existing_sketches(manifest_path: str, column_count: int) -> Dict[str, List[List[int]]]:
    """Existing per-file sketches from the dataset manifest, as {file_path: positional list}.

    Sketches whose width no longer matches the schema are dropped: they were
    computed against a different column set, so their positional field_ids are
    meaningless now (the same staleness rule the previous sidecar format applied).
    """
    if not os.path.exists(manifest_path):
        return {}
    with open(manifest_path, "rb") as handle:
        data = handle.read()
    return {
        path: sketch
        for path, sketch in read_manifest_sketches(data).items()
        if len(sketch) == column_count
    }


def _sketch_one_file(blob: str, targets: List[str]) -> Dict[str, List[int]]:
    """Compute this file's KMV sketch for each target column. Self-contained (own
    reader, no shared state) so files sketch concurrently."""
    import rugo.parquet as rugo_parquet

    sketches = {name: ColumnSketch() for name in targets}
    with rugo_parquet.read_parquet(blob, columns=targets) as reader:
        for morsel in reader:
            for name in targets:
                # Native vector hash over the whole column — no per-value Python
                # hashing. Same hash space as the canonical catalog.
                sketches[name].update(morsel.column(name).hash())
    return {name: sketches[name].min_k() for name in targets}


def _worker_count(n_files: int) -> int:
    return max(1, min(n_files, (os.cpu_count() or 1)))


def _write_manifest_atomic(manifest_path: str, entries: List[FileEntry], schema, sketches) -> None:
    data = write_manifest_parquet(entries, schema, sketches=sketches)
    tmp = manifest_path + ".tmp"
    with open(tmp, "wb") as handle:
        handle.write(data)
    os.replace(tmp, manifest_path)


def analyze_table(table_engine, columns: Optional[Sequence[str]]) -> int:
    """Compute KMV sketches for ``columns`` (or all columns) over every parquet
    file of the dataset and write them into the dataset's single manifest.

    Files are sketched concurrently — on the free-threaded build this is real
    parallelism across cores; each file is independent (own reader). The manifest
    is then written once, atomically.

    A column-subset ANALYZE merges: previously-analyzed columns of a file survive,
    and files not re-analyzed keep their existing sketches.

    Returns the number of files analyzed.
    """
    _require_local(table_engine)
    schema = table_engine.get_dataset_schema()
    field_ids = _field_ids(table_engine)
    targets = _resolve_targets(field_ids, columns)
    blobs = _parquet_blobs(table_engine)
    if not blobs:
        return 0

    manifest_path = _manifest_path(table_engine)
    existing = _read_existing_sketches(manifest_path, len(schema.columns))

    workers = _worker_count(len(blobs))
    if workers == 1:
        results = [_sketch_one_file(blob, targets) for blob in blobs]
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # Surface any per-file exception by consuming the results.
            results = list(pool.map(lambda b: _sketch_one_file(b, targets), blobs))

    entries: List[FileEntry] = []
    sketches: Dict[str, List[List[int]]] = {}
    for blob, new_hashes in zip(blobs, results):
        # Start from this file's existing sketches so a column-subset ANALYZE
        # preserves columns analyzed earlier; overwrite only the targets.
        positional = existing.get(blob) or [[] for _ in schema.columns]
        for name in targets:
            positional[field_ids[name]] = list(new_hashes[name])
        sketches[blob] = positional
        entries.append(
            FileEntry(
                file_path=blob,
                file_format="PARQUET",
                record_count=0,
                file_size_in_bytes=os.path.getsize(blob),
            )
        )

    _write_manifest_atomic(manifest_path, entries, schema, sketches)
    return len(blobs)


def drop_statistics(table_engine, columns: Optional[Sequence[str]]) -> int:
    """Remove statistics from the dataset's manifest.

    No column list → delete the manifest entirely. With a column list → clear only
    those columns' sketches, deleting the manifest when nothing remains. Idempotent:
    an absent manifest is not an error. Returns the number of files whose sketches
    were modified (or, for a whole-manifest delete, the file count it described).
    Never touches the parquet data files.
    """
    _require_local(table_engine)
    manifest_path = _manifest_path(table_engine)
    if not os.path.exists(manifest_path):
        return 0

    schema = table_engine.get_dataset_schema()

    if not columns:
        touched = len(_read_existing_sketches(manifest_path, len(schema.columns)))
        os.remove(manifest_path)
        return touched

    field_ids = _field_ids(table_engine)
    drop_ids = {field_ids[name] for name in _resolve_targets(field_ids, columns)}

    with open(manifest_path, "rb") as handle:
        data = handle.read()
    entries, _native = read_manifest_file_entries(data)
    stored = read_manifest_sketches(data)

    touched = 0
    kept: Dict[str, List[List[int]]] = {}
    for entry in entries:
        sketch = stored.get(entry.file_path)
        if sketch is None or len(sketch) != len(schema.columns):
            # Width mismatch — stale against the current schema; the reader would
            # drop them anyway, so don't carry them forward.
            touched += 1
            continue
        cleared = [[] if idx in drop_ids else list(col) for idx, col in enumerate(sketch)]
        if cleared != sketch:
            touched += 1
        kept[entry.file_path] = cleared

    if any(any(col for col in sketch) for sketch in kept.values()):
        _write_manifest_atomic(manifest_path, entries, schema, kept)
    else:
        os.remove(manifest_path)

    return touched
