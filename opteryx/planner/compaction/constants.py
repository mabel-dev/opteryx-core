# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Compaction sizing constants.

Ported from opteryx-catalog's ``compaction.py``. The VALUES are deliberately
unchanged from what production has been running — this is a move, not a
retune — with one exception noted on ``PASS_BUDGET_RATIO``.

Every size here is in the manifest's ``uncompressed_size_in_bytes`` unit, which
is a producer-side estimate of in-memory footprint rather than bytes on disk.
The two diverge enormously: three ``opteryx.test.pypi`` files measure 11.97 GB
by this unit and 52 MB on disk. Anything comparing against on-disk bytes is
comparing against a different quantity.
"""

_MB = 1024 * 1024

# Ideal output size for a rewritten file.
TARGET_SIZE_MB = 4096  # 4.0 GB
TARGET_SIZE_BYTES = TARGET_SIZE_MB * _MB

# Lower bound of the acceptable band — a file under this is a merge candidate.
MIN_SIZE_MB = 3584  # 3.5 GB
MIN_SIZE_BYTES = MIN_SIZE_MB * _MB
SMALL_FILE_MB = MIN_SIZE_MB
SMALL_FILE_BYTES = SMALL_FILE_MB * _MB

# Hard cap on a single file.
MAX_FILE_SIZE_MB = 4198  # 4.1 GB
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * _MB

# Below this a file is "sub-floor": scattered tiny files, which hurt reads most
# and are cheapest to fix, so they are merged without sorting.
MIN_FILE_SIZE_MB = 512
MIN_FILE_SIZE_BYTES = MIN_FILE_SIZE_MB * _MB

# --- The per-pass budget ------------------------------------------------------
#
# How much a single pass may rewrite. Ratified 2026-09-04 as a MULTIPLE of the
# max file size rather than an absolute, so it rescales on its own if the target
# file size ever moves. 2.1x is two target-sized files plus headroom, which is
# exactly the motivating decluster case: two overlapping files combining and
# splitting back into two disjoint ones.
#
# The catalog carried this as a flat 8704 MB, which is 2.073x the same max file
# size — so this is the ratio already in use, expressed as one.
#
# ⛔ This bounds WORK PER PASS, not memory directly. But because the engine's
# SortSink has no spill (docs/SORT_SPILL_DESIGN.md is design-only), a pass must
# also FIT IN MEMORY, so the budget and the container size are coupled: at a
# 16 GiB container the sort's peak multiplier must come in at or under 1.62x for
# this budget to fit. See D-14 in the design doc. If it does not, this number is
# what gives — that is an architect decision, not a silent adjustment.
PASS_BUDGET_RATIO = 2.1
PASS_BUDGET_BYTES = int(MAX_FILE_SIZE_MB * PASS_BUDGET_RATIO) * _MB

# ⛔ ONE budget, where the catalog had two. It also carried
# ``MAX_SELECTED_BUDGET_BYTES``, derived from container RAM (~6.4 GiB) and
# applied to the brute rules, alongside the flat decluster cap applied to the
# sort-aware ones. D-5 and D-13 ratified a single byte budget, so the RAM-derived
# second gate is not reproduced here — it existed to bound the hold-everything
# executor's peak, and bounding the pass is what does that job now.
#
# This WIDENS the brute rules slightly, from ~6.4 GiB to 8.6 GiB per pass.

# Files above this floor are sort-aware merge candidates. Deliberately overlaps
# the sub-floor pool below it: a file can qualify for both rules, and the
# dispatch decides which applies.
SORT_AWARE_FLOOR_MB = 500
SORT_AWARE_FLOOR_BYTES = SORT_AWARE_FLOOR_MB * _MB

# Rule C: rewrite a file purely to shed merge-on-read delete debt once this
# fraction of its physical rows are deleted.
DELETE_DEBT_THRESHOLD = 0.10
