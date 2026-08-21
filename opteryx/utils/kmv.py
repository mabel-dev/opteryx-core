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


def merge_min_k(sketches: Iterable[Iterable[int]], k: int = K) -> List[int]:
    """Union of KMV sketches: the k smallest DISTINCT hashes across all of them.

    This is the whole reason a sketch is stored rather than a scalar. The union
    is EXACT: if a hash is among the k smallest of the combined set and it came
    from sketch A, it is necessarily among the k smallest of A, so no input can
    hide a hash the answer needs.

    ⛔ Every input must come from the SAME hash function. skene's stored sketches
    are XXH3 over value bytes; ANALYZE's are draken's ``Vector.hash()``. Merging
    across those two produces a number with no meaning — see skene format.h,
    ColumnSketchHeader.
    """
    seen: set = set()
    for sketch in sketches:
        seen.update(sketch)
    return sorted(seen)[:k]


def estimate_from_min_k(min_k: List[int], k: int = K) -> tuple:
    """``(distinct_count, is_exact)`` from a merged sketch.

    Fewer than k hashes means the sketch never filled, so it holds EVERY distinct
    value and its length is the exact answer — the regime that matters most,
    because it covers every low-cardinality column. At or above k it is the
    standard KMV estimator ``(k-1)/v`` with v the k-th smallest hash normalised
    into [0,1), relative standard error ~1/sqrt(k-2) (~18% at k=32).
    """
    if len(min_k) < k:
        return len(min_k), True
    v = min_k[k - 1] / 18446744073709551616.0  # 2^64
    if v <= 0.0:
        # Needs the k-th smallest hash to be 0 — report k rather than infinity,
        # matching skene's own estimator (value_order.cpp).
        return k, False
    return int((k - 1) / v + 0.5), False


__all__ = [
    "K",
    "ColumnSketch",
    "estimate_from_min_k",
    "merge_min_k",
]
