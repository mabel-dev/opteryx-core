"""
D.7 acceptance tests: float32 / float64 vectors.

Covers:
  - Round-trip ingestion: finite, inf, -inf, NaN, -0.0, None.
  - -0.0 == 0.0: compare-equal AND hash-equal (same GROUP BY group).
  - NaN == NaN: total-order equality, NaN > all finite/inf.
  - min/max semantics: null-skipping; NaN participates as highest.
  - IEEE arithmetic: 1.0/0.0 → +inf; -1.0/0.0 → -inf; 0.0/0.0 → NaN.
  - compare_scalar / compare_vector (6 ops) with total-order semantics.
  - between / in_list with canonical NaN and -0.0.
  - take / materialize / compress (layout ops).
  - Constant and dict shapes.
  - Cross-type int64 × float64 throws (no silent lossy promotion).
"""

import math
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

import draken.draken_native as dn

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def f64(vals):
    return dn.vector_float64_from_sequence(vals)

def f32(vals):
    return dn.vector_float32_from_sequence(vals)

def sv(vals):
    return dn.vector_from_string_sequence(vals)

NAN = float("nan")
INF = float("inf")


# ---------------------------------------------------------------------------
# Round-trip ingestion
# ---------------------------------------------------------------------------

class TestRoundTrip:
    def test_f64_finite(self):
        v = f64([1.5, -2.0, 0.0, 1e300])
        assert v.to_pylist() == [1.5, -2.0, 0.0, 1e300]

    def test_f64_none(self):
        v = f64([1.0, None, 3.0])
        lst = v.to_pylist()
        assert lst[0] == 1.0
        assert lst[1] is None
        assert lst[2] == 3.0

    def test_f64_inf(self):
        v = f64([INF, -INF])
        lst = v.to_pylist()
        assert math.isinf(lst[0]) and lst[0] > 0
        assert math.isinf(lst[1]) and lst[1] < 0

    def test_f64_nan_canonical(self):
        v = f64([NAN])
        lst = v.to_pylist()
        assert math.isnan(lst[0])

    def test_f64_neg_zero_canonicalized(self):
        # -0.0 is canonicalized to +0.0 at ingestion.
        v = f64([-0.0])
        assert v.to_pylist()[0] == 0.0
        # Verify: bit pattern must be +0.0 (== 0.0, not -0.0).
        import struct
        bits = struct.pack("d", v.to_pylist()[0])
        assert bits == struct.pack("d", 0.0)

    def test_f32_finite(self):
        v = f32([1.5, -2.0, 0.0])
        lst = v.to_pylist()
        # float32 precision — compare at f32 tolerance
        assert abs(lst[0] - 1.5) < 1e-6
        assert abs(lst[1] - (-2.0)) < 1e-6
        assert lst[2] == 0.0

    def test_f32_none(self):
        v = f32([None, 1.0])
        lst = v.to_pylist()
        assert lst[0] is None
        assert abs(lst[1] - 1.0) < 1e-6

    def test_f64_empty(self):
        v = f64([])
        assert v.to_pylist() == []
        assert len(v) == 0

    def test_f64_all_null(self):
        v = f64([None, None, None])
        assert v.to_pylist() == [None, None, None]


class TestStringToFloat64:
    def test_parse_string_vector(self):
        v = dn.vector_cast_string_to_float64(sv(["1.5", "-2", "1e3"]))
        assert v.to_pylist() == [1.5, -2.0, 1000.0]

    def test_parse_trims_ascii_space_and_plus(self):
        v = dn.vector_cast_string_to_float64(sv(["  123.45  ", "+Infinity"]))
        out = v.to_pylist()
        assert out[0] == pytest.approx(123.45)
        assert math.isinf(out[1]) and out[1] > 0

    def test_invalid_and_null_become_null(self):
        v = dn.vector_cast_string_to_float64(sv(["not a double", "", None, "4.25"]))
        assert v.to_pylist() == [None, None, None, 4.25]

    def test_rejects_non_string_vector(self):
        with pytest.raises(Exception):
            dn.vector_cast_string_to_float64(f64([1.0]))


class TestFloat64ToString:
    def test_formats_float64_with_ryu_fixed_default_precision(self):
        v = dn.vector_cast_float64_to_string(f64([1.5, -2.0, 1.2345678]))
        assert v.to_pylist() == ["1.5", "-2.0", "1.234568"]

    def test_null_and_special_values(self):
        v = dn.vector_cast_float64_to_string(f64([None, float("nan"), INF, -INF]))
        assert v.to_pylist() == [None, "NaN", "Infinity", "-Infinity"]

    def test_precision_argument(self):
        v = dn.vector_cast_float64_to_string(f64([1.2345]), precision=2)
        assert v.to_pylist() == ["1.23"]

    def test_rejects_non_float_vector(self):
        with pytest.raises(Exception):
            dn.vector_cast_float64_to_string(sv(["1.0"]))


# ---------------------------------------------------------------------------
# Hashing: -0.0 and 0.0 must hash identically; NaN always has the same hash.
# ---------------------------------------------------------------------------

class TestHashing:
    def test_neg_zero_hash_equals_pos_zero(self):
        h_neg = f64([-0.0]).hash()[0]
        h_pos = f64([0.0]).hash()[0]
        assert h_neg == h_pos, "-0.0 and 0.0 must hash identically"

    def test_nan_hash_is_deterministic(self):
        # All NaN values (any bit-pattern) canonicalize to the same quiet NaN.
        h1 = f64([NAN]).hash()[0]
        h2 = f64([NAN]).hash()[0]
        assert h1 == h2

    def test_distinct_values_different_hashes(self):
        h1 = f64([1.0]).hash()[0]
        h2 = f64([2.0]).hash()[0]
        assert h1 != h2

    def test_null_hash_differs_from_nan(self):
        h_null = f64([None]).hash()[0]
        h_nan  = f64([NAN]).hash()[0]
        # Null uses NULL_HASH sentinel; NaN uses canonical bits — they must differ.
        assert h_null != h_nan

    def test_f32_neg_zero_hash(self):
        h_neg = f32([-0.0]).hash()[0]
        h_pos = f32([0.0]).hash()[0]
        assert h_neg == h_pos


# ---------------------------------------------------------------------------
# Reductions: sum / min / max
# ---------------------------------------------------------------------------

class TestReductions:
    def test_sum_finite(self):
        v = f64([1.0, 2.0, 3.0])
        assert v.sum() == pytest.approx(6.0)

    def test_sum_skips_null(self):
        v = f64([1.0, None, 3.0])
        assert v.sum() == pytest.approx(4.0)

    def test_sum_empty(self):
        v = f64([])
        assert v.sum() == 0.0

    def test_sum_all_null(self):
        v = f64([None, None])
        assert v.sum() == 0.0

    def test_min_finite(self):
        v = f64([3.0, 1.0, 2.0])
        assert v.min() == 1.0

    def test_min_skips_null(self):
        v = f64([None, 5.0, 3.0])
        assert v.min() == 3.0

    def test_min_nan_participates(self):
        # NaN is highest; finite wins min when NaN is present.
        v = f64([1.0, NAN])
        assert v.min() == pytest.approx(1.0)

    def test_min_all_nan(self):
        # All-NaN: min returns NaN (it's the only value).
        v = f64([NAN, NAN])
        assert math.isnan(v.min())

    def test_max_finite(self):
        v = f64([1.0, 3.0, 2.0])
        assert v.max() == 3.0

    def test_max_skips_null(self):
        v = f64([None, 5.0, 3.0])
        assert v.max() == 5.0

    def test_max_nan_wins(self):
        # NaN is highest in total order; max([1.0, NaN]) → NaN.
        v = f64([1.0, NAN])
        assert math.isnan(v.max())

    def test_max_all_nan(self):
        v = f64([NAN, NAN])
        assert math.isnan(v.max())

    def test_min_empty_raises(self):
        with pytest.raises(Exception):
            f64([]).min()

    def test_min_all_null_raises(self):
        with pytest.raises(Exception):
            f64([None, None]).min()

    def test_max_empty_raises(self):
        with pytest.raises(Exception):
            f64([]).max()

    def test_max_all_null_raises(self):
        with pytest.raises(Exception):
            f64([None, None]).max()

    def test_f32_sum(self):
        v = f32([1.0, 2.0, 3.0])
        assert v.sum() == pytest.approx(6.0, abs=1e-5)


# ---------------------------------------------------------------------------
# IEEE arithmetic
# ---------------------------------------------------------------------------

class TestArithmetic:
    def test_div_by_zero_positive(self):
        # float: 1.0/0.0 → +inf (IEEE, NOT int's div0→0 rule)
        v = f64([1.0])
        result = v.div(f64([0.0])).to_pylist()
        assert math.isinf(result[0]) and result[0] > 0

    def test_div_by_zero_negative(self):
        v = f64([-1.0])
        result = v.div(f64([0.0])).to_pylist()
        assert math.isinf(result[0]) and result[0] < 0

    def test_zero_div_zero_nan(self):
        v = f64([0.0])
        result = v.div(f64([0.0])).to_pylist()
        assert math.isnan(result[0])

    def test_add_scalar(self):
        v = f64([1.0, 2.0, 3.0])
        result = v.add(1.0).to_pylist()
        assert result == pytest.approx([2.0, 3.0, 4.0])

    def test_sub_scalar(self):
        v = f64([5.0, 3.0])
        result = v.sub(2.0).to_pylist()
        assert result == pytest.approx([3.0, 1.0])

    def test_mul_scalar(self):
        v = f64([2.0, 3.0])
        result = v.mul(4.0).to_pylist()
        assert result == pytest.approx([8.0, 12.0])

    def test_div_scalar(self):
        v = f64([10.0, 20.0])
        result = v.div(4.0).to_pylist()
        assert result == pytest.approx([2.5, 5.0])

    def test_mod_scalar(self):
        v = f64([10.0, 7.5])
        result = v.mod(3.0).to_pylist()
        assert result == pytest.approx([1.0, 1.5])

    def test_add_vector(self):
        a = f64([1.0, 2.0])
        b = f64([3.0, 4.0])
        result = a.add(b).to_pylist()
        assert result == pytest.approx([4.0, 6.0])

    def test_neg(self):
        v = f64([1.0, -2.0, 0.0])
        result = v.neg().to_pylist()
        assert result == pytest.approx([-1.0, 2.0, 0.0])

    def test_neg_nan_stays_nan(self):
        v = f64([NAN])
        result = v.neg().to_pylist()
        assert math.isnan(result[0])

    def test_null_propagation_add(self):
        a = f64([1.0, None])
        b = f64([2.0, 3.0])
        result = a.add(b).to_pylist()
        assert result[0] == pytest.approx(3.0)
        assert result[1] is None

    def test_arithmetic_result_canonical(self):
        # Arithmetic on a vector that produces -0.0 should canonicalize it.
        v = f64([-0.0])
        result = v.add(0.0).to_pylist()
        # Result should be 0.0 (canonical); hash must match +0.0.
        h_result = v.add(f64([0.0])).hash()[0]
        h_pos = f64([0.0]).hash()[0]
        assert h_result == h_pos

    def test_f32_div_by_zero(self):
        v = f32([1.0])
        result = v.div(f32([0.0])).to_pylist()
        assert math.isinf(result[0])


# ---------------------------------------------------------------------------
# compare_scalar (6 ops)
# ---------------------------------------------------------------------------

class TestCompareScalar:
    # Op codes: 0=eq 1=ne 2=gt 3=ge 4=lt 5=le
    def test_eq(self):
        v = f64([1.0, 2.0, 3.0])
        mask = v.compare_scalar(2.0, 0)
        vals = [mask[i] for i in range(3)]
        assert vals == [False, True, False]

    def test_ne(self):
        v = f64([1.0, 2.0])
        mask = v.compare_scalar(2.0, 1)
        assert [mask[i] for i in range(2)] == [True, False]

    def test_lt(self):
        v = f64([1.0, 2.0, 3.0])
        mask = v.compare_scalar(2.0, 4)
        assert [mask[i] for i in range(3)] == [True, False, False]

    def test_nan_eq_nan(self):
        # Total-order: NaN == NaN is True.
        v = f64([NAN])
        mask = v.compare_scalar(NAN, 0)
        assert mask[0] is True

    def test_nan_gt_inf(self):
        # NaN is highest in total order.
        v = f64([NAN])
        mask = v.compare_scalar(INF, 2)
        assert mask[0] is True

    def test_nan_gt_finite(self):
        v = f64([NAN])
        mask = v.compare_scalar(1e308, 2)
        assert mask[0] is True

    def test_neg_zero_eq_pos_zero(self):
        # -0.0 == 0.0 after canonicalization.
        v = f64([-0.0])
        mask = v.compare_scalar(0.0, 0)
        assert mask[0] is True

    def test_null_propagates(self):
        v = f64([None, 1.0])
        mask = v.compare_scalar(1.0, 0)
        assert mask[0] is None
        assert mask[1] is True

    def test_none_scalar_raises(self):
        v = f64([1.0, 2.0])
        with pytest.raises(TypeError):
            v.compare_scalar(None, 0)


# ---------------------------------------------------------------------------
# compare_vector
# ---------------------------------------------------------------------------

class TestCompareVector:
    def test_eq_vector(self):
        a = f64([1.0, 2.0, 3.0])
        b = f64([1.0, 5.0, 3.0])
        mask = a.compare_vector(b, 0)
        assert [mask[i] for i in range(3)] == [True, False, True]

    def test_nan_eq_nan_vector(self):
        a = f64([NAN])
        b = f64([NAN])
        mask = a.compare_vector(b, 0)
        assert mask[0] is True

    def test_null_propagates_vector(self):
        a = f64([None, 1.0])
        b = f64([1.0, 1.0])
        mask = a.compare_vector(b, 0)
        assert mask[0] is None
        assert mask[1] is True


# ---------------------------------------------------------------------------
# between
# ---------------------------------------------------------------------------

class TestBetween:
    def test_closed_range(self):
        v = f64([1.0, 2.0, 3.0, 4.0])
        mask = v.between(2.0, 3.0)
        assert [mask[i] for i in range(4)] == [False, True, True, False]

    def test_open_range(self):
        v = f64([1.0, 2.0, 3.0, 4.0])
        mask = v.between(2.0, 3.0, lo_inclusive=False, hi_inclusive=False)
        assert [mask[i] for i in range(4)] == [False, False, False, False]

    def test_null_propagates_between(self):
        v = f64([None, 2.0])
        mask = v.between(1.0, 3.0)
        assert mask[0] is None
        assert mask[1] is True

    def test_nan_outside_range(self):
        # NaN is highest — NaN > 3.0, so NaN > hi in closed range.
        v = f64([NAN])
        mask = v.between(0.0, 3.0)
        assert mask[0] is False  # NaN > hi

    def test_nan_at_boundary(self):
        # NaN is highest — NaN >= NaN is true.
        v = f64([NAN])
        mask = v.between(NAN, NAN)
        assert mask[0] is True


# ---------------------------------------------------------------------------
# in_list
# ---------------------------------------------------------------------------

class TestInList:
    def test_basic(self):
        v = f64([1.0, 2.0, 3.0, 4.0])
        mask = v.in_list([1.0, 3.0])
        assert [mask[i] for i in range(4)] == [True, False, True, False]

    def test_neg_zero_matches_pos_zero(self):
        # -0.0 and 0.0 must hash identically → in_list([0.0]) matches -0.0.
        v = f64([-0.0])
        mask = v.in_list([0.0])
        assert mask[0] is True

    def test_nan_in_list(self):
        # NaN has a canonical hash; in_list([NaN]) should match NaN row.
        v = f64([NAN, 1.0])
        mask = v.in_list([NAN])
        assert mask[0] is True
        assert mask[1] is False

    def test_null_propagates(self):
        v = f64([None, 1.0])
        mask = v.in_list([1.0])
        assert mask[0] is None
        assert mask[1] is True

    def test_empty_list(self):
        v = f64([1.0, 2.0])
        mask = v.in_list([])
        assert [mask[i] for i in range(2)] == [False, False]


# ---------------------------------------------------------------------------
# Layout ops: take / materialize / compress
# ---------------------------------------------------------------------------

class TestLayoutOps:
    def test_take(self):
        v = f64([10.0, 20.0, 30.0])
        t = v.take([2, 0])
        assert t.to_pylist() == pytest.approx([30.0, 10.0])

    def test_materialize(self):
        v = f64([1.0, 2.0, 3.0])
        m = v.materialize()
        assert m.to_pylist() == pytest.approx([1.0, 2.0, 3.0])

    def test_compress_deduplicates(self):
        v = f64([1.0, 2.0, 1.0, 3.0])
        c = v.compress()
        assert c.is_dict
        assert c.data_length < 4
        vals = sorted(x for x in c.to_pylist() if x is not None)
        assert vals == pytest.approx([1.0, 1.0, 2.0, 3.0])

    def test_compress_nan_dedup(self):
        # Multiple NaN rows compress to a single distinct value — that is the
        # constant shape (data_length == 1), not dict (which is 1 < dl < length).
        v = f64([NAN, NAN, NAN])
        c = v.compress()
        assert c.is_constant
        assert c.is_compressed
        assert not c.is_dict
        assert c.data_length == 1

    def test_take_preserves_null(self):
        v = f64([None, 2.0])
        t = v.take([0, 1])
        lst = t.to_pylist()
        assert lst[0] is None
        assert lst[1] == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# Constant shape
# ---------------------------------------------------------------------------

class TestConstantShape:
    def test_constant_value(self):
        v = dn.vector_float64_from_constant(3.14, 5)
        assert v.is_constant
        assert len(v) == 5
        assert all(x == pytest.approx(3.14) for x in v.to_pylist())

    def test_constant_null(self):
        v = dn.vector_float64_from_constant(None, 3)
        assert v.is_constant
        assert v.to_pylist() == [None, None, None]

    def test_constant_nan(self):
        v = dn.vector_float64_from_constant(NAN, 4)
        assert all(math.isnan(x) for x in v.to_pylist())


# ---------------------------------------------------------------------------
# Dict shape
# ---------------------------------------------------------------------------

class TestDictShape:
    def test_dict_roundtrip(self):
        # dict: values=[1.0, 2.0, 3.0]; codes=[0,1,2,0,1]
        v = dn.vector_float64_from_dict(
            [1.0, 2.0, 3.0], [0, 1, 2, 0, 1])
        assert v.is_dict
        assert v.to_pylist() == pytest.approx([1.0, 2.0, 3.0, 1.0, 2.0])

    def test_dict_with_nulls(self):
        v = dn.vector_float64_from_dict(
            [1.0, 2.0], [0, 1, 0],
            nullable=[True, False, True])
        lst = v.to_pylist()
        assert lst[0] == pytest.approx(1.0)
        assert lst[1] is None
        assert lst[2] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Cross-type: int64 × float64 must throw, no silent promotion.
# ---------------------------------------------------------------------------

class TestCrossType:
    def test_int64_float64_add_throws(self):
        iv = dn.vector_from_sequence([1, 2, 3])
        fv = f64([1.0, 2.0, 3.0])
        with pytest.raises(Exception):
            iv.add(fv)

    def test_float64_int64_add_throws(self):
        iv = dn.vector_from_sequence([1, 2, 3])
        fv = f64([1.0, 2.0, 3.0])
        with pytest.raises(Exception):
            fv.add(iv)


# ---------------------------------------------------------------------------
# Size tails: ensure correct results at n < 8 (tail byte handling)
# ---------------------------------------------------------------------------

class TestSmallSizes:
    @pytest.mark.parametrize("n", [1, 2, 3, 4, 5, 6, 7])
    def test_compare_scalar_small(self, n):
        data = list(range(1, n + 1))
        v = f64([float(x) for x in data])
        mask = v.compare_scalar(float(n // 2 + 1), 4)  # < (n//2+1)
        expected = [float(x) < float(n // 2 + 1) for x in data]
        assert [mask[i] for i in range(n)] == expected

    @pytest.mark.parametrize("n", [1, 2, 3, 5, 7])
    def test_hash_small(self, n):
        v = f64([float(i) for i in range(n)])
        h = v.hash()
        assert len(h) == n


# ---------------------------------------------------------------------------
# float32 specific
# ---------------------------------------------------------------------------

class TestFloat32:
    def test_f32_nan_semantics(self):
        v = f32([NAN, 1.0])
        assert math.isnan(v.max())
        assert v.min() == pytest.approx(1.0, abs=1e-5)

    def test_f32_neg_zero_canonical(self):
        v = f32([-0.0])
        h = v.hash()[0]
        h2 = f32([0.0]).hash()[0]
        assert h == h2

    def test_f32_compare_scalar_nan(self):
        v = f32([NAN])
        mask = v.compare_scalar(NAN, 0)  # eq
        assert mask[0] is True

    def test_f32_in_list(self):
        v = f32([1.0, 2.0, 3.0])
        mask = v.in_list([1.0, 3.0])
        assert [mask[i] for i in range(3)] == [True, False, True]
