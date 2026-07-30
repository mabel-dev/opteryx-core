"""
Correctness for the native ``Vector.ordinal_min_max()`` / ``Vector.histogram_bucket()``
kernels (draken/draken_native.cpp), which operate on ``Vector.ordinalize()``'s own
INT64 output. Added to back ANALYZE's per-file min/max/histogram statistics pass
(opteryx/operators/table_management/_analyze.py) with one native pipeline for
every column type.

Critical, non-obvious contract these two exist specifically to get right (see
draken_native.cpp's own comment on the binding): ordinalize()'s output vector
carries NO validity bitmap of its own — null input rows are encoded as the
ORDINAL_NULL (INT64_MIN) sentinel baked into the data. The generic
``Vector.min()``/``.max()`` trust the (absent) bitmap and would treat that
sentinel as real data — this was caught by testing during development (see the
plan history), not assumed correct. Every test below that includes nulls is a
guard against that specific regression re-appearing.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest

from draken.interop.vector_sequence import vector_from_sequence

_INT64_MIN = -(2**63)


def test_min_max_with_nulls_excludes_the_ordinal_null_sentinel():
    # Regression guard: calling Vector.min()/.max() directly on ordinalize()
    # output (instead of going through ordinal_min_max()) would see
    # INT64_MIN here instead of 0.
    vec = vector_from_sequence([5, None, 0, 999, None, 42], dtype="INT64")
    ordinal = vec.ordinalize()
    vmin, vmax = ordinal.ordinal_min_max()
    assert vmin == 0
    assert vmax == 999
    # The sentinel must never leak out as a "real" value.
    assert vmin != _INT64_MIN


def test_all_null_column_returns_none():
    vec = vector_from_sequence([None, None, None], dtype="INT64")
    ordinal = vec.ordinalize()
    assert ordinal.ordinal_min_max() is None


def test_empty_column_returns_none():
    vec = vector_from_sequence([], dtype="INT64")
    ordinal = vec.ordinalize()
    assert ordinal.ordinal_min_max() is None


def test_single_distinct_value_puts_everything_in_bin_zero():
    vec = vector_from_sequence([7, 7, 7, 7], dtype="INT64")
    ordinal = vec.ordinalize()
    vmin, vmax = ordinal.ordinal_min_max()
    assert vmin == vmax == 7
    bins = ordinal.histogram_bucket(vmin, vmax, 8)
    assert bins == [4, 0, 0, 0, 0, 0, 0, 0]


def test_uniform_distribution_bucket_counts_sum_to_non_null_count():
    values = list(range(100)) + [None] * 10
    vec = vector_from_sequence(values, dtype="INT64")
    ordinal = vec.ordinalize()
    vmin, vmax = ordinal.ordinal_min_max()
    assert (vmin, vmax) == (0, 99)
    bins = ordinal.histogram_bucket(vmin, vmax, 10)
    assert sum(bins) == 100  # nulls excluded, every non-null row counted once
    assert len(bins) == 10


def test_skewed_distribution_bucket_shape():
    # 90 rows clustered at the low end, 10 rows at the high end.
    values = [0] * 90 + [1000] * 10
    vec = vector_from_sequence(values, dtype="INT64")
    ordinal = vec.ordinalize()
    vmin, vmax = ordinal.ordinal_min_max()
    bins = ordinal.histogram_bucket(vmin, vmax, 4)
    assert sum(bins) == 100
    assert bins[0] == 90  # value 0 -> bin 0
    assert bins[-1] == 10  # value 1000 -> last bin (boundary value)


def test_boundary_value_lands_in_the_last_bin_not_one_past_it():
    # Floating-point rounding at the vmax boundary must clamp into range,
    # not overflow past the allocated bin count.
    vec = vector_from_sequence(list(range(1000)), dtype="INT64")
    ordinal = vec.ordinalize()
    vmin, vmax = ordinal.ordinal_min_max()
    bins = ordinal.histogram_bucket(vmin, vmax, 32)
    assert len(bins) == 32
    assert sum(bins) == 1000


def test_string_ordinalize_min_max_and_histogram():
    # ordinalize() supports strings (2026-07-30 rewrite) -- confirm the
    # min/max/histogram pipeline works uniformly on the string ordinal keys too.
    vec = vector_from_sequence(["apple", "banana", None, "cherry", "date"], dtype="VARCHAR")
    ordinal = vec.ordinalize()
    result = ordinal.ordinal_min_max()
    assert result is not None
    vmin, vmax = result
    bins = ordinal.histogram_bucket(vmin, vmax, 4)
    assert sum(bins) == 4  # 4 non-null rows


def test_wrong_leaf_type_raises():
    vec = vector_from_sequence([1.5, 2.5], dtype="FLOAT64")
    with pytest.raises(ValueError):
        vec.ordinal_min_max()  # not ordinalize()'d -- still FLOAT64, not INT64
    with pytest.raises(ValueError):
        vec.histogram_bucket(0, 1, 8)


def test_negative_n_bins_raises():
    vec = vector_from_sequence([1, 2, 3], dtype="INT64")
    ordinal = vec.ordinalize()
    vmin, vmax = ordinal.ordinal_min_max()
    with pytest.raises(ValueError):
        ordinal.histogram_bucket(vmin, vmax, 0)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
