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

⛔ Every input to ``merge_min_k`` must come from the SAME hash family. A skene v3
file's sketches are family 2 — ``Vector.hash()``, a null row contributing
NULL_HASH once — so they ARE this family and union with these. A skene v2 file's
are family 1, XXH3 over value bytes, and disagree about nulls (never seen) and
about decimal identity (raw bits, where ``Vector.hash()`` collides an
int64-decimal with the DECIMAL128 of equal value), so merging a v2 sketch with
these produces a number with no meaning — see skene ``FORMAT.md`` §8 and
``SketchRecordHeader.hash_family``, architect rulings 2026-08-21 and 2026-09-24.

**This module holds no implementation.** The sketch is ``draken/core/kmv_sketch.h``
— one C++ class shared with skene's value-ordering decline and rugo's
dictionary-encoding decision — bound through
``opteryx.compiled.nanobind.vectors`` and re-exported here so the import path
every caller already uses keeps working. The hash family is part of the C++
TYPE, which is what makes a cross-family merge a compile error rather than a
comment someone has to have read.
"""

from __future__ import annotations

from opteryx.compiled.nanobind.vectors import ColumnSketch
from opteryx.compiled.nanobind.vectors import estimate_from_min_k
from opteryx.compiled.nanobind.vectors import merge_min_k

# Matches MIN_K_HASHES in the canonical catalog stats engine, skene's
# format.h::kSketchK, and ManifestSketch's width in vector_sketch_reduce.cpp.
# The native functions above are compiled at this width; it is no longer a
# per-call argument, because no caller ever passed a different one.
K = 32

__all__ = [
    "K",
    "ColumnSketch",
    "estimate_from_min_k",
    "merge_min_k",
]
