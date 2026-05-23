"""
Native correctness tests for Milestone C.4: int64 between and in_list.

These tests assert the CORRECT answer.

Coverage matrix (per 04_testing.md §1 and C.4 acceptance criteria):

  between:
    inclusivity:  all 4 combos (lo_incl × hi_incl)
    nullability:  no-null / some-null / all-null
    sizes:        0 / 1 / 2..7 (tail-only) / 8 (byte boundary) / 9 (byte+1) / large
    edges:        INT64_MIN, INT64_MAX, lo==hi, lo>hi (empty range)
    shapes:       dense / constant / dict
    three-valued: null input → null output (validity=0, result bit=0)

  in_list:
    set sizes:    empty / 1 / large / with duplicates
    membership:   value present / absent / all present / none present
    nullability:  no-null / some-null / all-null
    sizes:        0 / 1 / 2..7 / 8 / 9 / large
    edges:        INT64_MIN, INT64_MAX in set or data
    shapes:       dense / constant / dict
    three-valued: null input → null output; null does NOT match any set member

Bit-boundary focus: sizes 1..9 exercised explicitly — partial-byte tail is the
classic bit-packing bug surface.

in_list is hash-only (§1 exception — CarcharSet stores hashes, not keys).
These tests confirm correctness at realistic sizes where collision probability
is negligible; they do not assert collision-freedom (untestable by design).
"""

import pytest
import draken.draken_native as dn

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


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

def between(v, lo, hi, lo_incl=True, hi_incl=True):
    return pylist(v.between(lo, hi, lo_incl, hi_incl))

def in_list(v, values):
    return pylist(v.in_list(values))

def _py_between(x, lo, hi, lo_incl, hi_incl):
    """Reference: Python between, None propagating."""
    if x is None:
        return None
    lo_ok = (lo <= x) if lo_incl else (lo < x)
    hi_ok = (x <= hi) if hi_incl else (x < hi)
    return lo_ok and hi_ok

def _py_in_list(x, s):
    """Reference: Python set membership, None propagating."""
    if x is None:
        return None
    return x in s


# ===========================================================================
# BETWEEN TESTS
# ===========================================================================

class TestBetweenResultType:
    def test_result_type_is_bool(self):
        v = make([1, 2, 3])
        r = v.between(1, 3)
        assert r.type == dn.DrakenType.BOOL

    def test_result_length_matches_input(self):
        v = make([10, 20, 30, 40, 50])
        r = v.between(15, 35)
        assert len(r) == 5


# ---------------------------------------------------------------------------
# All 4 inclusivity combos
# ---------------------------------------------------------------------------

class TestBetweenInclusivity:
    """Verify all 4 (lo_incl, hi_incl) combinations."""

    DATA = [1, 2, 3, 4, 5]
    LO, HI = 2, 4

    def test_closed_closed(self):
        v = make(self.DATA)
        assert between(v, self.LO, self.HI, True, True) == [False, True, True, True, False]

    def test_closed_open(self):
        v = make(self.DATA)
        assert between(v, self.LO, self.HI, True, False) == [False, True, True, False, False]

    def test_open_closed(self):
        v = make(self.DATA)
        assert between(v, self.LO, self.HI, False, True) == [False, False, True, True, False]

    def test_open_open(self):
        v = make(self.DATA)
        assert between(v, self.LO, self.HI, False, False) == [False, False, True, False, False]

    def test_default_is_closed_closed(self):
        v = make(self.DATA)
        # between() default: lo_inclusive=True, hi_inclusive=True
        assert between(v, self.LO, self.HI) == [False, True, True, True, False]


# ---------------------------------------------------------------------------
# Bit-boundary: sizes 1..9
# ---------------------------------------------------------------------------

class TestBetweenBitBoundary:
    @pytest.mark.parametrize("n", range(1, 10))
    def test_closed_sizes_1_to_9(self, n):
        data = list(range(n))
        v = make(data)
        lo, hi = n // 4, n * 3 // 4
        result = between(v, lo, hi)
        expected = [_py_between(x, lo, hi, True, True) for x in data]
        assert result == expected, f"size={n}: {result} != {expected}"

    def test_exact_byte_boundary_8(self):
        data = [0, 1, 2, 3, 4, 5, 6, 7]
        v = make(data)
        assert between(v, 2, 5) == [x >= 2 and x <= 5 for x in data]

    def test_one_past_byte_boundary_9(self):
        data = [0, 1, 2, 3, 4, 5, 6, 7, 8]
        v = make(data)
        result = between(v, 3, 6)
        expected = [_py_between(x, 3, 6, True, True) for x in data]
        assert result == expected

    @pytest.mark.parametrize("n", range(1, 10))
    def test_open_open_sizes_1_to_9(self, n):
        data = list(range(n))
        v = make(data)
        lo, hi = 0, n - 1
        result = between(v, lo, hi, False, False)
        expected = [_py_between(x, lo, hi, False, False) for x in data]
        assert result == expected, f"size={n}: {result} != {expected}"


# ---------------------------------------------------------------------------
# Edge values
# ---------------------------------------------------------------------------

class TestBetweenEdges:
    def test_int64_min_as_lo(self):
        v = make([INT64_MIN, INT64_MIN + 1, 0])
        assert between(v, INT64_MIN, 0) == [True, True, True]

    def test_int64_max_as_hi(self):
        v = make([0, INT64_MAX - 1, INT64_MAX])
        assert between(v, 0, INT64_MAX) == [True, True, True]

    def test_int64_min_max_bounds(self):
        v = make([INT64_MIN, 0, INT64_MAX])
        assert between(v, INT64_MIN, INT64_MAX) == [True, True, True]

    def test_lo_equals_hi_closed(self):
        # Only the exact value matches.
        v = make([1, 2, 3, 4, 5])
        assert between(v, 3, 3, True, True) == [False, False, True, False, False]

    def test_lo_equals_hi_open_open(self):
        # Open-open with lo==hi: empty range → all False.
        v = make([1, 2, 3, 4, 5])
        assert between(v, 3, 3, False, False) == [False, False, False, False, False]

    def test_lo_greater_than_hi(self):
        # Inverted range: nothing can satisfy lo < v < hi when lo > hi.
        v = make([1, 2, 3])
        assert between(v, 5, 1) == [False, False, False]

    def test_negative_range(self):
        v = make([-10, -5, 0, 5, 10])
        assert between(v, -7, 3) == [False, True, True, False, False]


# ---------------------------------------------------------------------------
# Null semantics (TVL)
# ---------------------------------------------------------------------------

class TestBetweenNulls:
    """Null input → null output (validity=0, result bit=0)."""

    def test_all_null(self):
        v = make([None, None, None])
        assert between(v, 1, 5) == [None, None, None]

    def test_mixed_nulls(self):
        data = [1, None, 3, None, 5]
        v = make(data)
        result = between(v, 2, 4)
        expected = [_py_between(x, 2, 4, True, True) for x in data]
        assert result == expected

    def test_null_not_false(self):
        v = make([None])
        result = between(v, 0, 10)
        assert result == [None]
        assert result[0] is None

    def test_null_at_byte_boundary(self):
        data = [1, 2, 3, 4, 5, 6, 7, None, None, 10]
        v = make(data)
        result = between(v, 3, 8)
        expected = [_py_between(x, 3, 8, True, True) for x in data]
        assert result == expected

    @pytest.mark.parametrize("n", range(1, 10))
    def test_all_null_sizes_1_to_9(self, n):
        v = make([None] * n)
        assert between(v, 0, 100) == [None] * n, f"size={n}"

    def test_null_between_boundary_all_combos(self):
        data = [None, 5, None]
        v = make(data)
        for lo_i in (True, False):
            for hi_i in (True, False):
                result = between(v, 1, 9, lo_i, hi_i)
                assert result[0] is None
                assert result[1] is True
                assert result[2] is None


# ---------------------------------------------------------------------------
# Empty vector
# ---------------------------------------------------------------------------

class TestBetweenEmpty:
    def test_empty_all_combos(self):
        v = make([])
        for lo_i in (True, False):
            for hi_i in (True, False):
                r = between(v, 1, 5, lo_i, hi_i)
                assert r == [], f"lo_incl={lo_i} hi_incl={hi_i}"

    def test_empty_result_type(self):
        v = make([])
        r = v.between(0, 10)
        assert r.type == dn.DrakenType.BOOL
        assert len(r) == 0


# ---------------------------------------------------------------------------
# Constant and dict shapes
# ---------------------------------------------------------------------------

class TestBetweenShapes:
    def test_constant_all_in_range(self):
        v = make_const(5, 4)
        assert between(v, 1, 10) == [True, True, True, True]

    def test_constant_out_of_range(self):
        v = make_const(5, 3)
        assert between(v, 10, 20) == [False, False, False]

    def test_constant_null(self):
        v = make_const(None, 3)
        assert between(v, 0, 100) == [None, None, None]

    def test_dict_between(self):
        # values: [1, 5, 10], codes: [0, 1, 2, 0, 2] → [1, 5, 10, 1, 10]
        v = make_dict([1, 5, 10], [0, 1, 2, 0, 2])
        assert between(v, 3, 8) == [False, True, False, False, False]

    def test_dict_with_nulls(self):
        v = make_dict([2, 8], [0, 1, 0, 1], [True, False, True, True])
        result = between(v, 1, 5)
        expected = [True, None, True, False]
        assert result == expected


# ---------------------------------------------------------------------------
# Large vector — correctness at scale
# ---------------------------------------------------------------------------

class TestBetweenLarge:
    N = 100_000

    def test_large_nonnull(self):
        data = list(range(self.N))
        v = make(data)
        lo, hi = self.N // 4, self.N * 3 // 4
        result = between(v, lo, hi)
        expected = [_py_between(x, lo, hi, True, True) for x in data]
        assert result == expected

    def test_large_mixed_null(self):
        data = [i if i % 7 != 0 else None for i in range(self.N)]
        v = make(data)
        lo, hi = 10_000, 80_000
        result = between(v, lo, hi)
        expected = [_py_between(x, lo, hi, True, True) for x in data]
        assert result == expected


# ===========================================================================
# IN_LIST TESTS
# ===========================================================================

class TestInListResultType:
    def test_result_type_is_bool(self):
        v = make([1, 2, 3])
        r = v.in_list([1, 3])
        assert r.type == dn.DrakenType.BOOL

    def test_result_length_matches_input(self):
        v = make([10, 20, 30, 40, 50])
        r = v.in_list([20, 40])
        assert len(r) == 5


# ---------------------------------------------------------------------------
# Basic membership
# ---------------------------------------------------------------------------

class TestInListMembership:
    def test_all_present(self):
        v = make([1, 2, 3])
        assert in_list(v, [1, 2, 3]) == [True, True, True]

    def test_none_present(self):
        v = make([1, 2, 3])
        assert in_list(v, [10, 20, 30]) == [False, False, False]

    def test_some_present(self):
        v = make([1, 2, 3, 4, 5])
        assert in_list(v, [2, 4]) == [False, True, False, True, False]

    def test_empty_set_all_false(self):
        v = make([1, 2, 3])
        assert in_list(v, []) == [False, False, False]

    def test_single_value_in_set(self):
        v = make([1, 2, 3])
        assert in_list(v, [2]) == [False, True, False]

    def test_duplicate_values_in_set(self):
        # Duplicates in the set should not affect correctness.
        v = make([1, 2, 3, 4])
        assert in_list(v, [2, 2, 2, 3, 3]) == [False, True, True, False]


# ---------------------------------------------------------------------------
# Bit-boundary: sizes 1..9
# ---------------------------------------------------------------------------

class TestInListBitBoundary:
    @pytest.mark.parametrize("n", range(1, 10))
    def test_sizes_1_to_9(self, n):
        data = list(range(n))
        v = make(data)
        s = {x for x in data if x % 2 == 0}
        result = in_list(v, list(s))
        expected = [_py_in_list(x, s) for x in data]
        assert result == expected, f"size={n}: {result} != {expected}"

    def test_exact_byte_boundary_8(self):
        data = [0, 1, 2, 3, 4, 5, 6, 7]
        s = {0, 3, 6}
        v = make(data)
        assert in_list(v, list(s)) == [_py_in_list(x, s) for x in data]

    def test_one_past_byte_boundary_9(self):
        data = [0, 1, 2, 3, 4, 5, 6, 7, 8]
        s = {1, 4, 7, 8}
        v = make(data)
        assert in_list(v, list(s)) == [_py_in_list(x, s) for x in data]


# ---------------------------------------------------------------------------
# Edge values
# ---------------------------------------------------------------------------

class TestInListEdges:
    def test_int64_min_in_set(self):
        v = make([INT64_MIN, 0, INT64_MAX])
        assert in_list(v, [INT64_MIN]) == [True, False, False]

    def test_int64_max_in_set(self):
        v = make([INT64_MIN, 0, INT64_MAX])
        assert in_list(v, [INT64_MAX]) == [False, False, True]

    def test_both_extremes_in_set(self):
        v = make([INT64_MIN, 0, INT64_MAX])
        assert in_list(v, [INT64_MIN, INT64_MAX]) == [True, False, True]

    def test_zero_in_set(self):
        v = make([-1, 0, 1])
        assert in_list(v, [0]) == [False, True, False]

    def test_negative_values(self):
        v = make([-5, -3, -1, 0, 1, 3, 5])
        s = {-3, 0, 3}
        assert in_list(v, list(s)) == [_py_in_list(x, s) for x in v.to_pylist()]


# ---------------------------------------------------------------------------
# Null semantics (TVL)
# ---------------------------------------------------------------------------

class TestInListNulls:
    """Null input → null output. Null does NOT match any set member."""

    def test_all_null(self):
        v = make([None, None, None])
        assert in_list(v, [1, 2, 3]) == [None, None, None]

    def test_mixed_nulls(self):
        data = [1, None, 3, None, 5]
        v = make(data)
        result = in_list(v, [1, 3])
        expected = [_py_in_list(x, {1, 3}) for x in data]
        assert result == expected

    def test_null_not_false(self):
        v = make([None])
        result = in_list(v, [0])
        assert result == [None]
        assert result[0] is None

    def test_null_with_empty_set(self):
        v = make([None, 1])
        result = in_list(v, [])
        assert result == [None, False]

    def test_null_at_byte_boundary(self):
        data = [1, 2, 3, 4, 5, 6, 7, None, None, 10]
        v = make(data)
        s = {1, 5, 10}
        result = in_list(v, list(s))
        expected = [_py_in_list(x, s) for x in data]
        assert result == expected

    @pytest.mark.parametrize("n", range(1, 10))
    def test_all_null_sizes_1_to_9(self, n):
        v = make([None] * n)
        result = in_list(v, [42])
        assert result == [None] * n, f"size={n}"

    def test_null_does_not_match_any_element(self):
        # Even if 0 (the null placeholder) is in the set, a null row stays null.
        data = [None, 0, 1]
        v = make(data)
        result = in_list(v, [0])
        assert result == [None, True, False]


# ---------------------------------------------------------------------------
# Empty vector
# ---------------------------------------------------------------------------

class TestInListEmpty:
    def test_empty_vector_empty_set(self):
        v = make([])
        assert in_list(v, []) == []

    def test_empty_vector_nonempty_set(self):
        v = make([])
        assert in_list(v, [1, 2, 3]) == []

    def test_empty_result_type(self):
        v = make([])
        r = v.in_list([1])
        assert r.type == dn.DrakenType.BOOL
        assert len(r) == 0


# ---------------------------------------------------------------------------
# Constant and dict shapes
# ---------------------------------------------------------------------------

class TestInListShapes:
    def test_constant_value_present(self):
        v = make_const(7, 4)
        assert in_list(v, [5, 7, 9]) == [True, True, True, True]

    def test_constant_value_absent(self):
        v = make_const(7, 3)
        assert in_list(v, [1, 2, 3]) == [False, False, False]

    def test_constant_null(self):
        v = make_const(None, 3)
        assert in_list(v, [0, 1, 2]) == [None, None, None]

    def test_dict_in_list(self):
        # values: [10, 20, 30], codes: [0, 1, 2, 0, 1] → [10, 20, 30, 10, 20]
        v = make_dict([10, 20, 30], [0, 1, 2, 0, 1])
        assert in_list(v, [10, 30]) == [True, False, True, True, False]

    def test_dict_with_nulls(self):
        v = make_dict([5, 15], [0, 1, 0, 1], [True, False, True, True])
        result = in_list(v, [5])
        expected = [True, None, True, False]
        assert result == expected


# ---------------------------------------------------------------------------
# Large vector — correctness at scale
# ---------------------------------------------------------------------------

class TestInListLarge:
    N = 100_000

    def test_large_nonnull(self):
        data = list(range(self.N))
        s = set(range(0, self.N, 100))  # every 100th value
        v = make(data)
        result = in_list(v, list(s))
        expected = [x in s for x in data]
        assert result == expected

    def test_large_mixed_null(self):
        data = [i if i % 13 != 0 else None for i in range(self.N)]
        s = set(range(0, self.N, 7))
        v = make(data)
        result = in_list(v, list(s))
        expected = [_py_in_list(x, s) for x in data]
        assert result == expected

    def test_large_empty_set(self):
        data = list(range(self.N))
        v = make(data)
        result = in_list(v, [])
        assert all(r is False for r in result)
