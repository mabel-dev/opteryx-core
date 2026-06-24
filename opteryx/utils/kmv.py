# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
K-Minimum-Values (KMV) column sketches and the ``.stats.json`` sidecar format.

This is the production home of the sketch contract consumed by
``opteryx.models.manifest.Manifest.estimate_cardinality`` and loaded by
``FileSystemTable._load_sidecar_min_k_hashes``. ``ANALYZE … FOR COLUMNS``
produces these sidecars; ``DROP STATISTICS`` removes them.

The hash is **draken's native vector hash** (``Vector.hash()``), the same hash
the canonical catalog stats engine uses, so sidecars produced here are
interchangeable with catalog-produced statistics. Hashing happens in C over the
whole column — never per-value in Python.

Sidecar format (schema_version 1):

    {
      "schema_version": 1,
      "field_ids": {"col_a": 0, "col_b": 1, ...},   # FULL schema, positional
      "min_k_hashes": {"0": [<sorted asc uint64>, ...], ...}  # analyzed cols only
    }

The loader requires ``field_ids`` to match the schema exactly (its staleness
check); ``min_k_hashes`` may be a subset, which is what makes ``FOR COLUMNS``
work — unanalyzed columns are simply absent.
"""

from __future__ import annotations

import heapq
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional

# Canonical format constants live with the loader; import to guarantee the
# producer and consumer never drift.
from opteryx.connectors.filesystem_connector import STATS_SCHEMA_VERSION
from opteryx.connectors.filesystem_connector import STATS_SIDECAR_SUFFIX

# Matches MIN_K_HASHES in the canonical catalog stats engine.
K = 32
# Soft cap on the working set before trimming back to the K smallest. Trimming
# to nsmallest(K) is exact for the result (the global K minima are always kept)
# and keeps memory flat regardless of column cardinality.
_TRIM_AT = 8 * K


class ColumnSketch:
    """Streaming KMV sketch fed native per-row hashes (``Vector.hash()``).

    ``update`` accepts a batch of hashes (one morsel's worth); ``min_k`` returns
    the sorted K smallest distinct hashes — the per-file sketch the sidecar
    stores and ``estimate_cardinality`` merges across files.
    """

    __slots__ = ("_seen",)

    def __init__(self) -> None:
        self._seen: set = set()

    def update(self, hashes: Iterable[int]) -> None:
        self._seen.update(hashes)
        if len(self._seen) > _TRIM_AT:
            self._seen = set(heapq.nsmallest(K, self._seen))

    def min_k(self) -> List[int]:
        return sorted(heapq.nsmallest(K, self._seen))


def empty_sidecar(field_ids: Dict[str, int]) -> dict:
    """A sidecar payload with the full schema field-id map and no sketches."""
    return {
        "schema_version": STATS_SCHEMA_VERSION,
        "field_ids": dict(field_ids),
        "min_k_hashes": {},
    }


def merge_into_sidecar(
    existing: Optional[dict],
    field_ids: Dict[str, int],
    new_hashes: Dict[int, List[int]],
) -> dict:
    """Produce a fresh sidecar payload: full ``field_ids`` plus the merged
    ``min_k_hashes``.

    ``new_hashes`` is {field_id: sorted hash list} for the columns just analyzed.
    Previously-analyzed columns survive *only* if the existing sidecar's
    ``field_ids`` still match the current schema — otherwise the old sketches are
    stale (the schema changed) and are dropped, exactly as the loader would.
    """
    payload = empty_sidecar(field_ids)
    if (
        isinstance(existing, dict)
        and existing.get("schema_version") == STATS_SCHEMA_VERSION
        and existing.get("field_ids") == field_ids
        and isinstance(existing.get("min_k_hashes"), dict)
    ):
        for fid_str, hashes in existing["min_k_hashes"].items():
            if isinstance(hashes, list):
                payload["min_k_hashes"][str(fid_str)] = [int(h) for h in hashes]
    for fid, hashes in new_hashes.items():
        payload["min_k_hashes"][str(fid)] = list(hashes)
    return payload


__all__ = [
    "K",
    "STATS_SCHEMA_VERSION",
    "STATS_SIDECAR_SUFFIX",
    "ColumnSketch",
    "empty_sidecar",
    "merge_into_sidecar",
]
