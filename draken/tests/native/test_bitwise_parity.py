"""
Functional parity tests for the 6 nanobind bitwise consumer functions.

These tests verify the C′ consumers against known inputs/outputs,
replacing the deleted vector_bitwise_*.pyx files (Milestone E.2, Part B).
The old pyx files are gone; inputs and expected outputs are derived from
the bitwise operation semantics directly.

Each test covers:
  - Normal values (no nulls)
  - NULL propagation
  - Edge values
  - Large vector (regression guard)
  - The exact function name exported by vector_bitwise NB_MODULE
"""

import glob
import importlib.util
import os

import draken.draken_native as dn


def _load_vector_bitwise():
    """Load vector_bitwise extension without triggering opteryx/__init__.py."""
    pattern = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "opteryx", "compiled", "nanobind", "vectors*.so"
    )
    matches = glob.glob(pattern)
    if not matches:
        raise RuntimeError("vector_bitwise extension not built — run make compile")
    spec = importlib.util.spec_from_file_location(
        "opteryx.compiled.nanobind.vectors", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


bw = _load_vector_bitwise()


def make(lst):
    return dn.vector_from_sequence(lst)


def pylist(v):
    return v.to_pylist()


# ---------------------------------------------------------------------------
# vector_bitwise_and
# ---------------------------------------------------------------------------

def test_and_normal():
    assert pylist(bw.vector_bitwise_and(make([12, 10, 255]), make([10, 12, 15]))) == [8, 8, 15]


def test_and_null_propagation():
    r = pylist(bw.vector_bitwise_and(make([None, 3, 5]), make([3, None, 5])))
    assert r == [None, None, 5]


def test_and_edge_values():
    INT64_MIN = -(2**63)
    INT64_MAX = 2**63 - 1
    r = pylist(bw.vector_bitwise_and(make([INT64_MIN, INT64_MAX, -1, 0]),
                                      make([INT64_MAX, INT64_MIN, -1, -1])))
    assert r == [0, 0, -1, 0]


def test_and_large():
    n = 10_000
    left  = make(list(range(n)))
    right = make([0xFF] * n)
    result = pylist(bw.vector_bitwise_and(left, right))
    expected = [i & 0xFF for i in range(n)]
    assert result == expected


# ---------------------------------------------------------------------------
# vector_bitwise_or
# ---------------------------------------------------------------------------

def test_or_normal():
    assert pylist(bw.vector_bitwise_or(make([1, 2, 4]), make([2, 4, 8]))) == [3, 6, 12]


def test_or_null_propagation():
    r = pylist(bw.vector_bitwise_or(make([None, 0, 5]), make([1, None, 5])))
    assert r == [None, None, 5]


def test_or_identity():
    assert pylist(bw.vector_bitwise_or(make([5, 0, -1]), make([0, 0, 0]))) == [5, 0, -1]


def test_or_large():
    n = 10_000
    left  = make([i & 0xFF for i in range(n)])
    right = make([(i >> 8) & 0xFF for i in range(n)])
    result = pylist(bw.vector_bitwise_or(left, right))
    expected = [(i & 0xFF) | ((i >> 8) & 0xFF) for i in range(n)]
    assert result == expected


# ---------------------------------------------------------------------------
# vector_bitwise_xor
# ---------------------------------------------------------------------------

def test_xor_normal():
    assert pylist(bw.vector_bitwise_xor(make([3, 5, 6]), make([5, 3, 6]))) == [6, 6, 0]


def test_xor_self_is_zero():
    vals = [42, -1, 0, 2**62]
    v = make(vals)
    assert pylist(bw.vector_bitwise_xor(v, v)) == [0, 0, 0, 0]


def test_xor_null_propagation():
    r = pylist(bw.vector_bitwise_xor(make([None, 7]), make([7, None])))
    assert r == [None, None]


def test_xor_large():
    n = 10_000
    vals = list(range(n))
    v = make(vals)
    # XOR with itself
    result = pylist(bw.vector_bitwise_xor(v, v))
    assert result == [0] * n


# ---------------------------------------------------------------------------
# vector_bitwise_not
# ---------------------------------------------------------------------------

def test_not_normal():
    assert pylist(bw.vector_bitwise_not(make([0, -1, 1, -2]))) == [-1, 0, -2, 1]


def test_not_involution():
    vals = [42, 0, -1, -(2**63), 2**63 - 1]
    v = make(vals)
    assert pylist(bw.vector_bitwise_not(bw.vector_bitwise_not(v))) == vals


def test_not_null_propagation():
    assert pylist(bw.vector_bitwise_not(make([None, 5, None]))) == [None, -6, None]


def test_not_large():
    n = 10_000
    vals = list(range(n))
    result = pylist(bw.vector_bitwise_not(make(vals)))
    expected = [~x for x in vals]
    assert result == expected


# ---------------------------------------------------------------------------
# vector_bitwise_shift_left
# ---------------------------------------------------------------------------

def test_shl_normal():
    assert pylist(bw.vector_bitwise_shift_left(make([1, 2, 3]), make([0, 1, 4]))) == [1, 4, 48]


def test_shl_null_propagation():
    r = pylist(bw.vector_bitwise_shift_left(make([None, 1, 2]), make([1, None, 2])))
    assert r == [None, None, 8]


def test_shl_valid_range_boundary():
    # shift by 62 = 2**62
    assert pylist(bw.vector_bitwise_shift_left(make([1]), make([62]))) == [2**62]


def test_shl_large():
    n = 1_000
    left  = make([1] * n)
    right = make([(i % 63) for i in range(n)])
    result = pylist(bw.vector_bitwise_shift_left(left, right))
    expected = [1 << (i % 63) for i in range(n)]
    # Convert to signed int64
    def to_i64(v): return v if v < 2**63 else v - 2**64
    expected = [to_i64(x) for x in expected]
    assert result == expected


# ---------------------------------------------------------------------------
# vector_bitwise_shift_right
# ---------------------------------------------------------------------------

def test_shr_normal():
    assert pylist(bw.vector_bitwise_shift_right(make([8, 16, 1]), make([2, 3, 0]))) == [2, 2, 1]


def test_shr_arithmetic_negative():
    # arithmetic shift: sign bit extends
    assert pylist(bw.vector_bitwise_shift_right(make([-8, -1]), make([2, 10]))) == [-2, -1]


def test_shr_null_propagation():
    r = pylist(bw.vector_bitwise_shift_right(make([None, 8]), make([1, None])))
    assert r == [None, None]


def test_shr_min_by_63():
    INT64_MIN = -(2**63)
    assert pylist(bw.vector_bitwise_shift_right(make([INT64_MIN]), make([63]))) == [-1]


def test_shr_large():
    n = 1_000
    left  = make([i * 64 for i in range(n)])
    right = make([3] * n)
    result = pylist(bw.vector_bitwise_shift_right(left, right))
    expected = [(i * 64) >> 3 for i in range(n)]
    assert result == expected
