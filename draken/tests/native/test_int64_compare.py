"""
Native correctness tests for Milestone C.3: int64 compare_scalar / compare_vector.

These tests assert the CORRECT answer.

Coverage matrix (per 04_testing.md §1 and C.3 acceptance criteria):
  ops:           all 6 (eq / ne / gt / ge / lt / le)
  nullability:   no-null / some-null / all-null input
  sizes:         0 / 1 / 2..7 (tail-only, never reaches whole-byte loop) /
                 8 (exact byte boundary) / 9 (one whole byte + one tail bit) /
                 large (100 000)
  edges:         INT64_MIN, INT64_MAX, 0, -1
  shapes:        dense (sequence) / constant / dict
  three-valued logic: NULL op x = NULL (validity bit 0, result bit 0)

Bit-boundary focus: sizes 1..9 are exercised explicitly because the partial-byte
tail is the classic bit-packing bug surface.
"""

import pytest
import draken.draken_native as dn

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

# Op codes
EQ, NE, GT, GE, LT, LE = 0, 1, 2, 3, 4, 5

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make(lst):
    return dn.vector_from_sequence(lst)

def make_const(value, length):
    return dn.vector_from_constant(value, length)

def make_dict(values, codes, nullable=None):
    return dn.vector_from_dict(values, codes, nullable)

def pylist(v):
    return v.to_pylist()

def cmp_s(v, scalar, op):
    return pylist(v.compare_scalar(scalar, op))

def cmp_v(a, b, op):
    return pylist(a.compare_vector(b, op))

def _py_cmp(op, a, b):
    """Reference: Python-level comparison, None propagating."""
    if a is None or b is None:
        return None
    return {EQ: a == b, NE: a != b, GT: a > b, GE: a >= b, LT: a < b, LE: a <= b}[op]


# ---------------------------------------------------------------------------
# compare_scalar — all six ops, non-null data
# ---------------------------------------------------------------------------

class TestCompareScalarNonnull:
    """All 6 ops against non-null dense sequences."""

    @pytest.mark.parametrize("op,expected", [
        (EQ, [False, True,  False]),
        (NE, [True,  False, True]),
        (GT, [False, False, True]),
        (GE, [False, True,  True]),
        (LT, [True,  False, False]),
        (LE, [True,  True,  False]),
    ])
    def test_all_ops_simple(self, op, expected):
        v = make([1, 2, 3])
        assert cmp_s(v, 2, op) == expected

    def test_result_type_is_bool(self):
        v = make([1, 2, 3])
        r = v.compare_scalar(2, EQ)
        assert r.type == dn.DrakenType.BOOL

    def test_result_length(self):
        v = make([10, 20, 30, 40, 50])
        r = v.compare_scalar(30, EQ)
        assert len(r) == 5

    def test_eq_all_true(self):
        v = make([7, 7, 7, 7])
        assert cmp_s(v, 7, EQ) == [True, True, True, True]

    def test_eq_all_false(self):
        v = make([1, 2, 3])
        assert cmp_s(v, 99, EQ) == [False, False, False]

    def test_ne_all_true(self):
        v = make([1, 2, 3])
        assert cmp_s(v, 99, NE) == [True, True, True]


# ---------------------------------------------------------------------------
# Bit-boundary: sizes 1..9 for compare_scalar
# ---------------------------------------------------------------------------

class TestCompareScalarBitBoundary:
    """Explicit coverage of sizes 1..9 — exercises both the tail-only path
    (n < 8) and the one-whole-byte-plus-tail path (n == 9)."""

    @pytest.mark.parametrize("n", range(1, 10))
    def test_eq_sizes_1_to_9(self, n):
        data = list(range(n))
        v = make(data)
        result = cmp_s(v, 0, EQ)
        expected = [x == 0 for x in data]
        assert result == expected, f"size={n}: {result} != {expected}"

    @pytest.mark.parametrize("n", range(1, 10))
    def test_gt_sizes_1_to_9(self, n):
        threshold = n // 2
        data = list(range(n))
        v = make(data)
        result = cmp_s(v, threshold, GT)
        expected = [x > threshold for x in data]
        assert result == expected, f"size={n}: {result} != {expected}"

    def test_exact_byte_boundary_8(self):
        data = [0, 1, 2, 3, 4, 5, 6, 7]
        v = make(data)
        assert cmp_s(v, 3, GE) == [x >= 3 for x in data]

    def test_one_past_byte_boundary_9(self):
        data = [0, 1, 2, 3, 4, 5, 6, 7, 8]
        v = make(data)
        assert cmp_s(v, 4, LT) == [x < 4 for x in data]


# ---------------------------------------------------------------------------
# Edge values
# ---------------------------------------------------------------------------

class TestCompareScalarEdges:
    """INT64_MIN, INT64_MAX, 0, -1 as data and as scalar."""

    def test_int64_max_eq(self):
        v = make([INT64_MAX, INT64_MAX - 1, 0])
        assert cmp_s(v, INT64_MAX, EQ) == [True, False, False]

    def test_int64_min_eq(self):
        v = make([INT64_MIN, INT64_MIN + 1, 0])
        assert cmp_s(v, INT64_MIN, EQ) == [True, False, False]

    def test_int64_max_gt(self):
        v = make([INT64_MAX, INT64_MAX - 1, 0])
        assert cmp_s(v, INT64_MAX - 1, GT) == [True, False, False]

    def test_int64_min_lt(self):
        v = make([INT64_MIN, INT64_MIN + 1, 0])
        assert cmp_s(v, INT64_MIN + 1, LT) == [True, False, False]

    def test_zero_ne(self):
        v = make([0, 1, -1])
        assert cmp_s(v, 0, NE) == [False, True, True]

    def test_negative_one_le(self):
        v = make([-1, 0, 1])
        assert cmp_s(v, -1, LE) == [True, False, False]

    def test_scalar_int64_min(self):
        v = make([INT64_MIN, 0, INT64_MAX])
        assert cmp_s(v, INT64_MIN, GE) == [True, True, True]

    def test_scalar_int64_max(self):
        v = make([INT64_MIN, 0, INT64_MAX])
        assert cmp_s(v, INT64_MAX, LE) == [True, True, True]


# ---------------------------------------------------------------------------
# Null semantics — three-valued logic
# ---------------------------------------------------------------------------

class TestCompareScalarNulls:
    """NULL op x = NULL (validity bit 0, result bit 0).

    Null input → null output (validity propagation). The result bit for null
    rows is 0 (false), validity bit is 0 (null). This is correct SQL 3VL.
    A null row is returned as None from to_pylist().
    """

    def test_all_null(self):
        v = make([None, None, None])
        for op in (EQ, NE, GT, GE, LT, LE):
            assert cmp_s(v, 0, op) == [None, None, None], f"op={op}"

    def test_mixed_null_and_valid(self):
        v = make([1, None, 3, None, 5])
        result = cmp_s(v, 3, EQ)
        assert result == [False, None, True, None, False]

    def test_null_at_byte_boundary(self):
        # Null at position 7 (last bit of first byte) and position 8 (first bit of second byte).
        data = [1, 2, 3, 4, 5, 6, 7, None, None, 10]
        v = make(data)
        result = cmp_s(v, 5, GE)
        expected = [_py_cmp(GE, x, 5) for x in data]
        assert result == expected

    def test_null_propagates_to_result_type(self):
        v = make([1, None, 3])
        r = v.compare_scalar(2, EQ)
        assert r.type == dn.DrakenType.BOOL
        assert pylist(r) == [False, None, False]

    def test_null_result_is_none_not_false(self):
        v = make([None])
        result = cmp_s(v, 0, EQ)
        assert result == [None]
        assert result[0] is None  # must be None, not False

    @pytest.mark.parametrize("n", range(1, 10))
    def test_all_null_sizes_1_to_9(self, n):
        v = make([None] * n)
        result = cmp_s(v, 42, EQ)
        assert result == [None] * n, f"size={n}"

    def test_last_null_size_7(self):
        data = [1, 2, 3, 4, 5, 6, None]
        v = make(data)
        result = cmp_s(v, 3, EQ)
        assert result == [False, False, True, False, False, False, None]

    def test_first_null_size_8(self):
        data = [None, 1, 2, 3, 4, 5, 6, 7]
        v = make(data)
        result = cmp_s(v, 1, EQ)
        assert result == [None, True, False, False, False, False, False, False]


# ---------------------------------------------------------------------------
# Empty vector
# ---------------------------------------------------------------------------

class TestCompareScalarEmpty:
    def test_empty_all_ops(self):
        v = make([])
        for op in (EQ, NE, GT, GE, LT, LE):
            assert cmp_s(v, 0, op) == []

    def test_empty_type(self):
        v = make([])
        r = v.compare_scalar(0, EQ)
        assert r.type == dn.DrakenType.BOOL
        assert len(r) == 0


# ---------------------------------------------------------------------------
# Constant shape (data_length == 1)
# ---------------------------------------------------------------------------

class TestCompareScalarConstant:
    def test_constant_all_true(self):
        v = make_const(10, 5)
        assert cmp_s(v, 10, EQ) == [True] * 5

    def test_constant_all_false(self):
        v = make_const(10, 5)
        assert cmp_s(v, 99, EQ) == [False] * 5

    def test_constant_null_all_ops(self):
        v = make_const(None, 4)
        for op in (EQ, NE, GT, GE, LT, LE):
            assert cmp_s(v, 0, op) == [None, None, None, None], f"op={op}"

    def test_constant_gt(self):
        v = make_const(5, 3)
        assert cmp_s(v, 3, GT) == [True, True, True]
        assert cmp_s(v, 5, GT) == [False, False, False]
        assert cmp_s(v, 7, GT) == [False, False, False]


# ---------------------------------------------------------------------------
# Dict shape
# ---------------------------------------------------------------------------

class TestCompareScalarDict:
    def test_dict_eq(self):
        # values: [10, 20, 30], codes: [0, 1, 2, 0, 1]
        # logical: [10, 20, 30, 10, 20]
        v = make_dict([10, 20, 30], [0, 1, 2, 0, 1])
        assert cmp_s(v, 10, EQ) == [True, False, False, True, False]

    def test_dict_with_nulls(self):
        v = make_dict([10, 20], [0, 1, 0, 1], [True, False, True, True])
        result = cmp_s(v, 10, EQ)
        assert result == [True, None, True, False]

    def test_dict_gt(self):
        v = make_dict([1, 5, 10], [0, 1, 2, 0, 2])
        assert cmp_s(v, 5, GT) == [False, False, True, False, True]


# ---------------------------------------------------------------------------
# compare_vector — all six ops, non-null
# ---------------------------------------------------------------------------

class TestCompareVectorNonnull:
    @pytest.mark.parametrize("op,expected", [
        (EQ, [False, True,  False]),
        (NE, [True,  False, True]),
        (GT, [True,  False, False]),
        (GE, [True,  True,  False]),
        (LT, [False, False, True]),
        (LE, [False, True,  True]),
    ])
    def test_all_ops_simple(self, op, expected):
        a = make([3, 2, 1])
        b = make([2, 2, 2])
        assert cmp_v(a, b, op) == expected

    def test_result_type_is_bool(self):
        a = make([1, 2])
        b = make([1, 3])
        r = a.compare_vector(b, EQ)
        assert r.type == dn.DrakenType.BOOL

    def test_self_eq(self):
        v = make([1, 2, 3, 4, 5])
        assert cmp_v(v, v, EQ) == [True] * 5

    def test_length_mismatch_raises(self):
        a = make([1, 2, 3])
        b = make([1, 2])
        with pytest.raises(Exception):
            a.compare_vector(b, EQ)


# ---------------------------------------------------------------------------
# Bit-boundary: sizes 1..9 for compare_vector
# ---------------------------------------------------------------------------

class TestCompareVectorBitBoundary:
    @pytest.mark.parametrize("n", range(1, 10))
    def test_eq_sizes_1_to_9(self, n):
        data_a = list(range(n))
        data_b = list(range(n - 1, -1, -1))
        a = make(data_a)
        b = make(data_b)
        result = cmp_v(a, b, EQ)
        expected = [x == y for x, y in zip(data_a, data_b)]
        assert result == expected, f"size={n}"

    def test_exact_byte_boundary_8(self):
        a = make([0, 1, 2, 3, 4, 5, 6, 7])
        b = make([7, 6, 5, 4, 3, 2, 1, 0])
        result = cmp_v(a, b, LT)
        expected = [x < y for x, y in zip([0,1,2,3,4,5,6,7], [7,6,5,4,3,2,1,0])]
        assert result == expected

    def test_one_past_byte_boundary_9(self):
        a = make([0, 1, 2, 3, 4, 5, 6, 7, 8])
        b = make([0, 0, 3, 3, 3, 5, 5, 7, 9])
        result = cmp_v(a, b, GT)
        expected = [x > y for x, y in zip(a.to_pylist(), b.to_pylist())]
        assert result == expected


# ---------------------------------------------------------------------------
# compare_vector null semantics
# ---------------------------------------------------------------------------

class TestCompareVectorNulls:
    """Row is null iff EITHER operand is null."""

    def test_null_in_a(self):
        a = make([None, 2, 3])
        b = make([1,    2, 3])
        result = cmp_v(a, b, EQ)
        assert result == [None, True, True]

    def test_null_in_b(self):
        a = make([1, 2, 3])
        b = make([1, None, 3])
        result = cmp_v(a, b, EQ)
        assert result == [True, None, True]

    def test_null_in_both(self):
        a = make([None, 2, None])
        b = make([None, 2, 3])
        result = cmp_v(a, b, EQ)
        assert result == [None, True, None]

    def test_null_in_both_same_position(self):
        a = make([None, 5])
        b = make([None, 5])
        result = cmp_v(a, b, EQ)
        # NULL == NULL is NULL in SQL 3VL (not True)
        assert result == [None, True]

    def test_mixed_nulls_at_bit_boundary(self):
        a_data = [1, 2, 3, 4, 5, 6, 7, None, None, 10]
        b_data = [1, 2, 3, 4, 5, 6, 7,    8, None, 10]
        a = make(a_data)
        b = make(b_data)
        result = cmp_v(a, b, EQ)
        expected = [_py_cmp(EQ, x, y) for x, y in zip(a_data, b_data)]
        assert result == expected

    def test_all_null_both(self):
        a = make([None, None])
        b = make([None, None])
        assert cmp_v(a, b, EQ) == [None, None]

    @pytest.mark.parametrize("n", range(1, 10))
    def test_all_null_both_sizes_1_to_9(self, n):
        a = make([None] * n)
        b = make([None] * n)
        assert cmp_v(a, b, EQ) == [None] * n


# ---------------------------------------------------------------------------
# compare_vector with constant and dict shapes
# ---------------------------------------------------------------------------

class TestCompareVectorShapes:
    def test_dense_vs_constant(self):
        a = make([1, 2, 3, 4, 5])
        b = make_const(3, 5)
        result = cmp_v(a, b, EQ)
        assert result == [False, False, True, False, False]

    def test_constant_vs_constant(self):
        a = make_const(7, 4)
        b = make_const(7, 4)
        assert cmp_v(a, b, EQ) == [True, True, True, True]

    def test_dict_vs_dense(self):
        a = make_dict([10, 20], [0, 1, 0, 1])  # [10, 20, 10, 20]
        b = make([10, 10, 20, 20])
        assert cmp_v(a, b, EQ) == [True, False, False, True]


# ---------------------------------------------------------------------------
# Large vector — smoke for correctness at scale
# ---------------------------------------------------------------------------

class TestCompareScalarLarge:
    N = 100_000

    def test_large_nonnull_eq(self):
        data = list(range(self.N))
        v = make(data)
        result = cmp_s(v, self.N // 2, EQ)
        assert result.count(True) == 1
        assert result[self.N // 2] is True

    def test_large_nonnull_ge(self):
        data = list(range(self.N))
        v = make(data)
        result = cmp_s(v, self.N // 2, GE)
        assert result.count(True) == self.N - (self.N // 2)

    def test_large_mixed_null(self):
        data = [i if i % 3 != 0 else None for i in range(self.N)]
        v = make(data)
        result = cmp_s(v, 50, LT)
        expected = [_py_cmp(LT, x, 50) for x in data]
        assert result == expected


class TestCompareVectorLarge:
    N = 100_000

    def test_large_self_eq(self):
        data = list(range(self.N))
        v = make(data)
        result = cmp_v(v, v, EQ)
        assert all(r is True for r in result)

    def test_large_mixed_nulls(self):
        a_data = [i if i % 5 != 0 else None for i in range(self.N)]
        b_data = [i if i % 7 != 0 else None for i in range(self.N)]
        a = make(a_data)
        b = make(b_data)
        result = cmp_v(a, b, EQ)
        expected = [_py_cmp(EQ, x, y) for x, y in zip(a_data, b_data)]
        assert result == expected
