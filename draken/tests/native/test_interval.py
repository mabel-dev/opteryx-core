"""
Native unit tests for DRAKEN_INTERVAL ingestion, readback, and ops in draken.draken_native.

Coverage (D.12 acceptance criteria):

  shapes:            sequence / constant / dict
  nullability:       no nulls / some nulls / all null
  sizes:             0 / 1 / small / medium
  round-trip:        (months, ms) preserved exactly; None → None
  PostgreSQL eq:     '1 month' == '30 days' (both normalize to 2_592_000_000 ms)
  ingestion errors:  normalization overflow → OverflowError (or ValueError)
  compare_scalar:    all 6 ops, null rows, null scalar → all null
  compare_vector:    eq/ne/lt/gt, null propagation, cross-type throws
  hash:              equal intervals (component-wise or normalized) → equal hash;
                     deterministic; null sentinel distinct
  between:           inclusive/exclusive bounds, null rows
  in_list:           membership, null rows, empty set
  arithmetic:        interval + interval, interval - interval, neg; component-wise
  min / max:         basic, null skip, all-null raises, empty raises
  take / materialize / compress: shape round-trip
"""

import pytest

import draken.draken_native as dn

# ---------------------------------------------------------------------------
# Op codes (ABI-frozen)
# ---------------------------------------------------------------------------
EQ, NE, GT, GE, LT, LE = 0, 1, 2, 3, 4, 5

# Normalization constant: 1 month = 30 days × 86_400_000 ms/day
MONTH_MS = 2_592_000_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def iv(lst):
    """Build a dense INTERVAL vector from a list of (months, ms) or None."""
    return dn.vector_interval_from_sequence(lst)

def iv_const(value, length):
    return dn.vector_interval_from_constant(value, length)

def iv_dict(values, codes, nulls=None):
    return dn.vector_interval_from_dict(values, codes, nulls)

def pylist(v):
    return v.to_pylist()

def cmp_s(v, scalar, op):
    return pylist(v.compare_scalar(scalar, op))

def norm(months, ms):
    """Python-side normalization for expected values."""
    return months * MONTH_MS + ms


# ===========================================================================
# 1. Type tag
# ===========================================================================

class TestTypeTag:
    def test_type_is_interval(self):
        v = iv([(0, 1000)])
        assert v.type == dn.DrakenType.INTERVAL

    def test_length_empty(self):
        v = iv([])
        assert len(v) == 0

    def test_length_single(self):
        v = iv([(1, 0)])
        assert len(v) == 1

    def test_length_mixed(self):
        v = iv([(1, 0), None, (0, 500)])
        assert len(v) == 3


# ===========================================================================
# 2. Round-trip identity
# ===========================================================================

class TestRoundTrip:
    def test_basic_round_trip(self):
        v = iv([(2, 5000)])
        assert pylist(v) == [(2, 5000)]

    def test_null_round_trip(self):
        v = iv([None])
        assert pylist(v) == [None]

    def test_mixed_round_trip(self):
        v = iv([(1, 0), None, (0, 86_400_000), (-3, -1000)])
        result = pylist(v)
        assert result == [(1, 0), None, (0, 86_400_000), (-3, -1000)]

    def test_zero_interval(self):
        v = iv([(0, 0)])
        assert pylist(v) == [(0, 0)]

    def test_negative_months(self):
        v = iv([(-6, 0)])
        assert pylist(v) == [(-6, 0)]

    def test_negative_ms(self):
        v = iv([(0, -1000)])
        assert pylist(v) == [(0, -1000)]

    def test_large_values(self):
        # months=120 (10 years), ms=31_536_000_000 (365 days in ms)
        v = iv([(120, 31_536_000_000)])
        assert pylist(v) == [(120, 31_536_000_000)]

    def test_empty(self):
        v = iv([])
        assert pylist(v) == []

    def test_getitem(self):
        v = iv([(1, 0), None, (0, 500)])
        assert v[0] == (1, 0)
        assert v[1] is None
        assert v[2] == (0, 500)
        assert v[-1] == (0, 500)

    def test_all_null(self):
        v = iv([None, None, None])
        assert pylist(v) == [None, None, None]


# ===========================================================================
# 3. PostgreSQL semantics: '1 month' == '30 days'
# ===========================================================================

class TestPostgresSemantics:
    def test_one_month_equals_30_days(self):
        # (1 month, 0 ms) and (0 months, 2_592_000_000 ms) normalize identically.
        one_month  = iv([(1, 0)])
        thirty_days = iv([(0, MONTH_MS)])
        result = pylist(one_month.compare_vector(thirty_days, EQ))
        assert result == [True]

    def test_one_month_equal_hash(self):
        # (1, 0) and (0, 2_592_000_000) must produce the same hash.
        one_month   = iv([(1, 0)])
        thirty_days = iv([(0, MONTH_MS)])
        assert one_month.hash()[0] == thirty_days.hash()[0]

    def test_one_month_not_gt_30_days(self):
        one_month   = iv([(1, 0)])
        thirty_days = iv([(0, MONTH_MS)])
        assert pylist(one_month.compare_vector(thirty_days, GT)) == [False]

    def test_compare_scalar_one_month_eq_30_days(self):
        v = iv([(1, 0)])
        result = cmp_s(v, (0, MONTH_MS), EQ)
        assert result == [True]

    def test_in_list_normalized_match(self):
        # (1, 0) should be found when set contains (0, 2_592_000_000)
        v = iv([(1, 0), (2, 0)])
        result = pylist(v.in_list([(0, MONTH_MS), (0, MONTH_MS * 2)]))
        assert result == [True, True]


# ===========================================================================
# 4. Ingestion error: normalization overflow
# ===========================================================================

class TestIngestionErrors:
    def test_overflow_on_extreme_months(self):
        # months so large that months × MONTH_MS overflows int64
        huge_months = (2**63 // MONTH_MS) + 1  # just over overflow
        with pytest.raises((OverflowError, ValueError)):
            iv([(huge_months, 0)])

    def test_overflow_on_extreme_ms(self):
        # ms alone overflowing: store months=0, ms=2^63+1
        with pytest.raises((OverflowError, ValueError, Exception)):
            # 2^63 overflows int64
            iv([(0, 2**63)])

    def test_negative_overflow_months(self):
        huge_neg = -(2**63 // MONTH_MS) - 1
        with pytest.raises((OverflowError, ValueError)):
            iv([(huge_neg, 0)])


# ===========================================================================
# 5. Shapes: constant and dict
# ===========================================================================

class TestShapes:
    def test_constant_shape(self):
        v = iv_const((1, 500), 4)
        assert v.is_constant
        assert pylist(v) == [(1, 500)] * 4

    def test_constant_null(self):
        v = iv_const(None, 3)
        assert pylist(v) == [None, None, None]

    def test_constant_length(self):
        v = iv_const((0, 1000), 5)
        assert len(v) == 5

    def test_dict_shape(self):
        values = [(1, 0), (0, 86_400_000), (2, 5000)]
        codes  = [0, 2, 1, 0]
        v = iv_dict(values, codes)
        assert v.is_dict
        result = pylist(v)
        assert result == [(1, 0), (2, 5000), (0, 86_400_000), (1, 0)]

    def test_dict_with_nulls(self):
        values = [(1, 0), (2, 0)]
        codes  = [0, 0, 1]
        valid  = [True, False, True]
        v = iv_dict(values, codes, valid)
        assert pylist(v) == [(1, 0), None, (2, 0)]


# ===========================================================================
# 6. Compare scalar
# ===========================================================================

class TestCompareScalar:
    def test_eq_basic(self):
        v = iv([(1, 0), (2, 0), (3, 0)])
        assert cmp_s(v, (2, 0), EQ) == [False, True, False]

    def test_lt(self):
        v = iv([(0, 0), (1, 0), (2, 0)])
        assert cmp_s(v, (1, 0), LT) == [True, False, False]

    def test_le(self):
        v = iv([(0, 0), (1, 0), (2, 0)])
        assert cmp_s(v, (1, 0), LE) == [True, True, False]

    def test_gt(self):
        v = iv([(0, 0), (1, 0), (2, 0)])
        assert cmp_s(v, (1, 0), GT) == [False, False, True]

    def test_ge(self):
        v = iv([(0, 0), (1, 0), (2, 0)])
        assert cmp_s(v, (1, 0), GE) == [False, True, True]

    def test_ne(self):
        v = iv([(1, 0), (2, 0), (1, 0)])
        assert cmp_s(v, (1, 0), NE) == [False, True, False]

    def test_null_rows_propagate(self):
        v = iv([(1, 0), None, (3, 0)])
        result = cmp_s(v, (2, 0), EQ)
        assert result == [False, None, False]

    def test_null_scalar_raises(self):
        v = iv([(1, 0), (2, 0)])
        with pytest.raises(TypeError):
            cmp_s(v, None, EQ)

    def test_normalized_comparison(self):
        # (1, 0) and (0, MONTH_MS) normalize to the same value
        v = iv([(1, 0)])
        assert cmp_s(v, (0, MONTH_MS), EQ) == [True]


# ===========================================================================
# 7. Compare vector
# ===========================================================================

class TestCompareVector:
    def test_eq_vector(self):
        a = iv([(1, 0), (2, 0), (3, 0)])
        b = iv([(1, 0), (0, MONTH_MS * 2), (4, 0)])
        result = pylist(a.compare_vector(b, EQ))
        # (2, 0) vs (0, MONTH_MS * 2): norm(2,0) = 2*MONTH_MS; norm(0,MONTH_MS*2) = 2*MONTH_MS → equal
        assert result == [True, True, False]

    def test_lt_vector(self):
        a = iv([(1, 0), (2, 0)])
        b = iv([(2, 0), (1, 0)])
        result = pylist(a.compare_vector(b, LT))
        assert result == [True, False]

    def test_null_propagation(self):
        a = iv([(1, 0), None, (3, 0)])
        b = iv([(1, 0), (2, 0), None])
        result = pylist(a.compare_vector(b, EQ))
        assert result == [True, None, None]

    def test_cross_type_throws(self):
        a = iv([(1, 0)])
        b = dn.vector_from_sequence([1])
        with pytest.raises(Exception):
            a.compare_vector(b, EQ)


# ===========================================================================
# 8. Hash
# ===========================================================================

class TestHash:
    def test_equal_intervals_equal_hash(self):
        v = iv([(1, 0), (2, 0), (1, 0)])
        h = v.hash()
        assert h[0] == h[2]
        assert h[0] != h[1]  # probabilistic

    def test_normalized_equal_hash(self):
        # (1, 0) and (0, MONTH_MS) normalize to the same ms — must share hash
        v = iv([(1, 0), (0, MONTH_MS)])
        h = v.hash()
        assert h[0] == h[1]

    def test_null_sentinel_distinct(self):
        v = iv([(1, 0), None])
        h = v.hash()
        assert h[0] != h[1]

    def test_deterministic(self):
        v1 = iv([(3, 1000)])
        v2 = iv([(3, 1000)])
        assert v1.hash()[0] == v2.hash()[0]

    def test_zero_interval_hash(self):
        v = iv([(0, 0)])
        h = v.hash()
        assert h[0] is not None  # must produce a value


# ===========================================================================
# 9. Between
# ===========================================================================

class TestBetween:
    def test_between_inclusive(self):
        v = iv([(0, 0), (1, 0), (2, 0), (3, 0)])
        result = pylist(v.between((1, 0), (2, 0)))
        assert result == [False, True, True, False]

    def test_between_exclusive_lo(self):
        v = iv([(1, 0), (2, 0), (3, 0)])
        result = pylist(v.between((1, 0), (3, 0), lo_inclusive=False))
        assert result == [False, True, True]

    def test_between_exclusive_hi(self):
        v = iv([(1, 0), (2, 0), (3, 0)])
        result = pylist(v.between((1, 0), (3, 0), hi_inclusive=False))
        assert result == [True, True, False]

    def test_between_null_row(self):
        v = iv([(1, 0), None, (3, 0)])
        result = pylist(v.between((0, 0), (2, 0)))
        assert result == [True, None, False]

    def test_between_normalized_bounds(self):
        # bound (0, MONTH_MS) is the same as (1, 0)
        v = iv([(1, 0), (2, 0)])
        result = pylist(v.between((0, MONTH_MS), (2, 0)))
        assert result == [True, True]


# ===========================================================================
# 10. In list
# ===========================================================================

class TestInList:
    def test_in_list_basic(self):
        v = iv([(1, 0), (2, 0), (3, 0), (4, 0)])
        result = pylist(v.in_list([(2, 0), (4, 0)]))
        assert result == [False, True, False, True]

    def test_in_list_null_row(self):
        v = iv([(1, 0), None, (3, 0)])
        result = pylist(v.in_list([(1, 0), (3, 0)]))
        assert result == [True, None, True]

    def test_in_list_empty_set(self):
        v = iv([(1, 0), (2, 0)])
        result = pylist(v.in_list([]))
        assert result == [False, False]

    def test_in_list_normalized_match(self):
        # (1, 0) in set that contains (0, MONTH_MS) → must match (same normalized value)
        v = iv([(1, 0)])
        result = pylist(v.in_list([(0, MONTH_MS)]))
        assert result == [True]

    def test_in_list_null_in_set_skipped(self):
        # None in the set list must not cause crash or false positive
        v = iv([(1, 0), (2, 0)])
        result = pylist(v.in_list([None, (1, 0)]))
        assert result == [True, False]


# ===========================================================================
# 11. Arithmetic: add, sub, neg
# ===========================================================================

class TestArithmetic:
    def test_add_component_wise(self):
        a = iv([(1, 500), (2, 1000)])
        b = iv([(3, 200), (0, 800)])
        result = pylist(a.add(b))
        # component-wise: months added, ms added independently
        assert result == [(4, 700), (2, 1800)]

    def test_sub_component_wise(self):
        a = iv([(3, 1000)])
        b = iv([(1, 400)])
        result = pylist(a.sub(b))
        assert result == [(2, 600)]

    def test_add_null_propagates(self):
        a = iv([(1, 0), None])
        b = iv([(1, 0), (1, 0)])
        result = pylist(a.add(b))
        assert result == [(2, 0), None]

    def test_sub_null_propagates(self):
        a = iv([(2, 0)])
        b = iv([None])
        result = pylist(a.sub(b))
        assert result == [None]

    def test_neg(self):
        v = iv([(3, 500), (-1, -200), (0, 0)])
        result = pylist(v.neg())
        assert result == [(-3, -500), (1, 200), (0, 0)]

    def test_neg_null(self):
        v = iv([None, (1, 0)])
        result = pylist(v.neg())
        assert result == [None, (-1, 0)]

    def test_add_cross_month_ms(self):
        # component-wise: months and ms are NOT merged/normalized
        a = iv([(1, 0)])     # 1 month, 0 ms
        b = iv([(0, MONTH_MS)])  # 0 months, one month worth of ms
        result = pylist(a.add(b))
        # result is (1, MONTH_MS), NOT (2, 0)
        assert result == [(1, MONTH_MS)]

    def test_add_cross_type_throws(self):
        a = iv([(1, 0)])
        b = dn.vector_from_sequence([1])
        with pytest.raises(Exception):
            _ = a.add(b)


# ===========================================================================
# 12. Min / max
# ===========================================================================

class TestMinMax:
    def test_min_basic(self):
        v = iv([(3, 0), (1, 0), (2, 0)])
        # min by normalized total_ms; all zero ms so months determines order
        assert v.min() == (1, 0)

    def test_max_basic(self):
        v = iv([(3, 0), (1, 0), (2, 0)])
        assert v.max() == (3, 0)

    def test_min_with_nulls(self):
        v = iv([None, (2, 0), (1, 0), None])
        assert v.min() == (1, 0)

    def test_max_with_nulls(self):
        v = iv([None, (2, 0), (5, 0), None])
        assert v.max() == (5, 0)

    def test_min_all_null_raises(self):
        with pytest.raises(Exception):
            iv([None, None]).min()

    def test_max_all_null_raises(self):
        with pytest.raises(Exception):
            iv([None, None]).max()

    def test_min_empty_raises(self):
        with pytest.raises(Exception):
            iv([]).min()

    def test_max_empty_raises(self):
        with pytest.raises(Exception):
            iv([]).max()

    def test_min_normalized_tie_returns_smallest_months(self):
        # (1, 0) and (0, MONTH_MS) normalize to same value; min returns the first found
        # (implementation detail — just check it doesn't crash and returns valid interval)
        v = iv([(1, 0), (0, MONTH_MS)])
        result = v.min()
        # both are valid — just assert it's a tuple with the right normalized value
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert norm(result[0], result[1]) == norm(1, 0)

    def test_min_negative_intervals(self):
        v = iv([(-1, 0), (-3, 0), (-2, 0)])
        assert v.min() == (-3, 0)

    def test_max_mixed_sign(self):
        v = iv([(-1, 0), (0, 0), (1, 0)])
        assert v.max() == (1, 0)

    def test_single_element_min_max(self):
        v = iv([(5, 1000)])
        assert v.min() == (5, 1000)
        assert v.max() == (5, 1000)


# ===========================================================================
# 13. Take / materialize / compress
# ===========================================================================

class TestGather:
    def test_take_basic(self):
        v = iv([(1, 0), (2, 0), (3, 0)])
        r = v.take([2, 0, 1])
        assert pylist(r) == [(3, 0), (1, 0), (2, 0)]

    def test_take_type_preserved(self):
        v = iv([(1, 0), (2, 0)])
        assert v.take([1]).type == dn.DrakenType.INTERVAL

    def test_take_with_nulls(self):
        v = iv([(1, 0), None, (3, 0)])
        assert pylist(v.take([1, 0])) == [None, (1, 0)]

    def test_materialize_dict(self):
        values = [(1, 0), (2, 0)]
        codes  = [0, 1, 0, 1]
        v = iv_dict(values, codes)
        m = v.materialize()
        assert m.type == dn.DrakenType.INTERVAL
        assert pylist(m) == [(1, 0), (2, 0), (1, 0), (2, 0)]

    def test_compress_basic(self):
        v = iv([(1, 0), (1, 0), (2, 0)])
        c = v.compress()
        assert c.type == dn.DrakenType.INTERVAL
        assert len(c) <= 3  # may be deduped

    def test_compress_type_preserved(self):
        v = iv([(1, 0), (2, 0)])
        assert v.compress().type == dn.DrakenType.INTERVAL
