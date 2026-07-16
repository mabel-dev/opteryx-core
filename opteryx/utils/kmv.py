# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
K-Minimum-Values (KMV) column sketches.

This is the production home of the sketch contract consumed by
``opteryx.models.manifest.Manifest.estimate_cardinality``. ``ANALYZE … FOR
COLUMNS`` produces these sketches into the dataset's manifest (see
``opteryx.models.manifest_io``); ``DROP STATISTICS`` removes them.

The hash is **draken's native vector hash** (``Vector.hash()``), the same hash
the canonical catalog stats engine uses, so sketches produced here are
interchangeable with catalog-produced statistics. Hashing happens in C over the
whole column — never per-value in Python.
"""

from __future__ import annotations

import heapq
from typing import Iterable
from typing import List

# Matches MIN_K_HASHES in the canonical catalog stats engine.
K = 32
# Soft cap on the working set before trimming back to the K smallest. Trimming
# to nsmallest(K) is exact for the result (the global K minima are always kept)
# and keeps memory flat regardless of column cardinality.
_TRIM_AT = 8 * K


class ColumnSketch:
    """Streaming KMV sketch fed native per-row hashes (``Vector.hash()``).

    ``update`` accepts a batch of hashes (one morsel's worth); ``min_k`` returns
    the sorted K smallest distinct hashes — the per-file sketch the manifest
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


__all__ = [
    "K",
    "ColumnSketch",
]
