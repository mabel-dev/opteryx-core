"""
Permutation-shape slice correctness (WP-04 regression).

A PERMUTATION vector has data_length == length but a non-identity selection
(e.g. the result of take-after-sort, or a dict whose unique-count equals the
row count). The slice kernels historically gated their physical-memcpy fast
path on `data_length == length`, which a permutation satisfies — so the memcpy
read rows in PHYSICAL order, silently reordering/dropping. The fix requires the
DRAKEN_SEL_IDENTITY flag for the memcpy path; permutations take the
selection-honouring gather.

vector_*_from_dict with `len(values) == len(codes)` and a non-identity `codes`
permutation builds exactly this shape (flags = 0, no IDENTITY bit).
"""

import datetime
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import draken.draken_native as dn

PERM = [3, 1, 4, 2, 5, 0, 6, 7]  # non-identity permutation of 0..7


def _expect(values, start, length):
    return [values[PERM[start + i]] for i in range(length)]


def test_int64_permutation_slice():
    v = dn.vector_from_dict(list(range(8)), PERM)
    assert v.to_pylist() == PERM  # logical order == permutation
    assert v.slice(2, 4).to_pylist() == _expect(list(range(8)), 2, 4)
    assert v.slice(0, 8).to_pylist() == PERM
    assert v.slice(5, 3).to_pylist() == _expect(list(range(8)), 5, 3)


def test_int32_permutation_slice():
    v = dn.vector_int32_from_dict(list(range(8)), PERM)
    assert v.slice(2, 4).to_pylist() == _expect(list(range(8)), 2, 4)


def test_int16_permutation_slice():
    v = dn.vector_int16_from_dict(list(range(8)), PERM)
    assert v.slice(1, 6).to_pylist() == _expect(list(range(8)), 1, 6)


def test_float64_permutation_slice():
    vals = [float(x) for x in range(8)]
    v = dn.vector_float64_from_dict(vals, PERM)
    assert v.slice(2, 4).to_pylist() == _expect(vals, 2, 4)


def test_date32_permutation_slice():
    dates = [datetime.date(2020, 1, 1 + x) for x in range(8)]
    v = dn.vector_date32_from_dict(dates, PERM)
    assert v.slice(2, 4).to_pylist() == _expect(dates, 2, 4)


def test_interval_permutation_slice():
    vals = [(0, x * 1000) for x in range(8)]  # (months, us)
    v = dn.vector_interval_from_dict(vals, PERM)
    assert v.slice(2, 4).to_pylist() == _expect(vals, 2, 4)


def test_permutation_slice_with_nulls():
    # nullable permutation: validity is per LOGICAL row; slicing must keep the
    # null mask aligned to the logical (selection) order, not physical.
    nullable = [True, False, True, True, False, True, True, True]  # by logical row
    v = dn.vector_from_dict(list(range(8)), PERM, nullable)
    full = v.to_pylist()
    expected_full = [PERM[i] if nullable[i] else None for i in range(8)]
    assert full == expected_full
    assert v.slice(2, 4).to_pylist() == expected_full[2:6]


def test_dense_slice_unaffected():
    # A genuinely dense-identity vector still slices via the fast path correctly.
    v = dn.vector_from_sequence([10, 20, 30, 40, 50])
    assert v.slice(1, 3).to_pylist() == [20, 30, 40]


if __name__ == "__main__":  # pragma: no cover
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✓ {name}")
    print("✅ okay")
