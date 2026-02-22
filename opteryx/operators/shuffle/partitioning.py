# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from __future__ import annotations

import math

_ALLOWED_BIN_COUNTS = (1, 2, 4, 8, 16)


def round_down_to_allowed_bins(value: int, allowed: tuple[int, ...] = _ALLOWED_BIN_COUNTS) -> int:
    if value <= 1:
        return 1
    for candidate in sorted(allowed, reverse=True):
        if candidate <= value:
            return candidate
    return min(allowed)


def normalize_num_bins(num_bins: int, allowed: tuple[int, ...] = _ALLOWED_BIN_COUNTS) -> int:
    if num_bins <= 0:
        raise ValueError("num_bins must be positive")
    if num_bins not in allowed:
        raise ValueError(f"num_bins must be one of {allowed}")
    return num_bins


def select_num_bins_from_rows(n_rows: int | None) -> int:
    if n_rows is None or n_rows <= 0:
        return 1
    raw_bins = min(max(math.ceil(math.log2(n_rows)) - 16, 1), 16)
    return round_down_to_allowed_bins(raw_bins)


def row_indexes_by_bin(hashes, num_bins: int, shift_bits: int = 0) -> list[list[int]]:
    normalize_num_bins(num_bins)
    if shift_bits < 0:
        raise ValueError("shift_bits must be zero or positive")

    mask = num_bins - 1
    bins: list[list[int]] = [[] for _ in range(num_bins)]
    for row_index, hash_value in enumerate(hashes):
        bin_id = (int(hash_value) >> shift_bits) & mask
        bins[bin_id].append(row_index)
    return bins
