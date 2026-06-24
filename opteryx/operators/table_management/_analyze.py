# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
ANALYZE / DROP STATISTICS orchestration for filesystem datasets.

``ANALYZE TABLE t [FOR COLUMNS …]`` computes a per-file KMV sketch for the named
columns (or all columns) and writes the ``.stats.json`` sidecar the scan loads.
``DROP STATISTICS ON t [FOR COLUMNS …]`` removes those sketches.

This is admin orchestration, not a hot path — plain Python is appropriate. The
sketch contract and sidecar format live in ``opteryx.utils.kmv``; reading is via
the native rugo reader (no pyarrow in the engine).

Scope: local filesystem datasets. Remote/object-store writes are a separate
increment; an unsupported backend fails loudly rather than silently no-op'ing.
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence

from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem
from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.utils.kmv import STATS_SIDECAR_SUFFIX
from opteryx.utils.kmv import ColumnSketch
from opteryx.utils.kmv import merge_into_sidecar

_PARQUET_SUFFIX = ".parquet"


def _require_local(table_engine) -> None:
    if not isinstance(getattr(table_engine, "filesystem", None), OpteryxLocalFileSystem):
        raise UnsupportedSyntaxError(
            "ANALYZE / DROP STATISTICS is currently supported only for local "
            "filesystem datasets."
        )


def _parquet_blobs(table_engine) -> List[str]:
    blobs = table_engine.get_list_of_blob_names(table_engine.dataset)
    return [b for b in blobs if b.lower().endswith(_PARQUET_SUFFIX)]


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


def _read_sidecar(sidecar_path: str) -> Optional[dict]:
    if not os.path.exists(sidecar_path):
        return None
    try:
        with open(sidecar_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else None
    except (ValueError, OSError):
        return None


def _write_sidecar_atomic(sidecar_path: str, payload: dict) -> None:
    tmp = sidecar_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"))
    os.replace(tmp, sidecar_path)


def _analyze_one_file(blob: str, field_ids: Dict[str, int], targets: List[str]) -> None:
    """Compute and persist the sidecar for a single parquet file. Self-contained
    (own reader, own sidecar) so files analyze concurrently with no shared state."""
    import rugo.parquet as rugo_parquet

    sketches = {name: ColumnSketch() for name in targets}
    with rugo_parquet.read_parquet(blob, columns=targets) as reader:
        for morsel in reader:
            for name in targets:
                # Native vector hash over the whole column — no per-value Python
                # hashing. Same hash space as the canonical catalog.
                sketches[name].update(morsel.column(name).hash())

    new_hashes = {field_ids[name]: sketches[name].min_k() for name in targets}
    sidecar_path = blob + STATS_SIDECAR_SUFFIX
    payload = merge_into_sidecar(_read_sidecar(sidecar_path), field_ids, new_hashes)
    _write_sidecar_atomic(sidecar_path, payload)


def _worker_count(n_files: int) -> int:
    return max(1, min(n_files, (os.cpu_count() or 1)))


def analyze_table(table_engine, columns: Optional[Sequence[str]]) -> int:
    """Compute KMV sketches for ``columns`` (or all columns) over every parquet
    file of the dataset and write/merge the per-file ``.stats.json`` sidecar.

    Files are analyzed concurrently — on the free-threaded build this is real
    parallelism across cores; each file is independent (own reader, own sidecar).

    Returns the number of sidecar files written.
    """
    _require_local(table_engine)
    field_ids = _field_ids(table_engine)
    targets = _resolve_targets(field_ids, columns)
    blobs = _parquet_blobs(table_engine)
    if not blobs:
        return 0

    workers = _worker_count(len(blobs))
    if workers == 1:
        for blob in blobs:
            _analyze_one_file(blob, field_ids, targets)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # Surface any per-file exception by consuming the results.
            list(pool.map(lambda b: _analyze_one_file(b, field_ids, targets), blobs))

    return len(blobs)


def drop_statistics(table_engine, columns: Optional[Sequence[str]]) -> int:
    """Remove statistics sidecars for the dataset.

    No column list → delete the whole sidecar per file. With a column list →
    drop only those columns' sketches, deleting the file when nothing remains.
    Idempotent: absent sidecars are not an error. Returns the number of sidecar
    files modified or removed. Never touches the parquet data files.
    """
    _require_local(table_engine)
    blobs = _parquet_blobs(table_engine)
    drop_ids: Optional[set] = None
    if columns:
        field_ids = _field_ids(table_engine)
        drop_ids = {str(field_ids[name]) for name in _resolve_targets(field_ids, columns)}

    touched = 0
    for blob in blobs:
        sidecar_path = blob + STATS_SIDECAR_SUFFIX
        if not os.path.exists(sidecar_path):
            continue
        if drop_ids is None:
            os.remove(sidecar_path)
            touched += 1
            continue
        payload = _read_sidecar(sidecar_path)
        if payload is None or not isinstance(payload.get("min_k_hashes"), dict):
            # Malformed/unreadable — treat the whole sidecar as droppable.
            os.remove(sidecar_path)
            touched += 1
            continue
        remaining = {k: v for k, v in payload["min_k_hashes"].items() if k not in drop_ids}
        if remaining == payload["min_k_hashes"]:
            continue  # nothing dropped from this file
        if remaining:
            payload["min_k_hashes"] = remaining
            _write_sidecar_atomic(sidecar_path, payload)
        else:
            os.remove(sidecar_path)
        touched += 1

    return touched
