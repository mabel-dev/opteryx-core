"""
Native unit tests for int8/int16/int32 ops (Milestone D.6).

Coverage per type:
  ingestion:    dense / constant / dict shapes; None→null; range check
  readback:     __getitem__ / to_pylist; sign extension to int64 for Python
  hash:         parity with int64 for values representable in all widths
  compare:      compare_scalar (eq/ne/gt/ge/lt/le) with null rows
  between:      closed / half-open; null rows
  in_list:      hash-only probe; null rows
  reductions:   sum / min / max; empty→0/raises; all-null
  arithmetic:   add/sub/mul/div/mod homogeneous; div-by-zero→0; null propagation
                result type is NextWider<T>
  neg:          -INT8_MIN=128→INT16; -INT16_MIN=32768→INT32; -INT32_MIN→INT64
  cross-width:  INT8+INT16→INT32; INT8+INT32→INT64; INT16+INT32→INT64
                compare_vector cross-width: all 6 ops, both directions, discriminating values
  gather:       take / materialize / compress
"""

import pytest
import draken.draken_native as dn


INT8_MIN   = -128
INT8_MAX   = 127
INT16_MIN  = -32768
INT16_MAX  = 32767
INT32_MIN  = -2147483648
INT32_MAX  = 2147483647


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def i8(lst):     return dn.vector_int8_from_sequence(lst)
def i16(lst):    return dn.vector_int16_from_sequence(lst)
def i32(lst):    return dn.vector_int32_from_sequence(lst)
def i64(lst):    return dn.vector_from_sequence(lst)
def py(v):       return v.to_pylist()


# ---------------------------------------------------------------------------
# INGESTION — range checks, nulls, readback
# ---------------------------------------------------------------------------

class TestIngestionInt8:
    def test_basic_roundtrip(self):
        assert py(i8([1, -1, 127, -128])) == [1, -1, 127, -128]

    def test_null_roundtrip(self):
        assert py(i8([None, 5, None])) == [None, 5, None]

    def test_type_tag(self):
        assert i8([1]).type == dn.DrakenType.INT8

    def test_empty(self):
        assert py(i8([])) == []

    def test_range_overflow_high(self):
        with pytest.raises(OverflowError):
            i8([128])

    def test_range_overflow_low(self):
        with pytest.raises(OverflowError):
            i8([-129])

    def test_boundary_values(self):
        v = i8([INT8_MIN, INT8_MAX, 0])
        assert py(v) == [INT8_MIN, INT8_MAX, 0]


class TestIngestionInt16:
    def test_basic_roundtrip(self):
        assert py(i16([1000, -1000, 32767, -32768])) == [1000, -1000, 32767, -32768]

    def test_null_roundtrip(self):
        assert py(i16([None, 100, None])) == [None, 100, None]

    def test_type_tag(self):
        assert i16([1]).type == dn.DrakenType.INT16

    def test_range_overflow_high(self):
        with pytest.raises(OverflowError):
            i16([32768])

    def test_range_overflow_low(self):
        with pytest.raises(OverflowError):
            i16([-32769])

    def test_boundary_values(self):
        v = i16([INT16_MIN, INT16_MAX, 0])
        assert py(v) == [INT16_MIN, INT16_MAX, 0]


class TestIngestionInt32:
    def test_basic_roundtrip(self):
        assert py(i32([100000, -100000, 2147483647, -2147483648])) == [
            100000, -100000, 2147483647, -2147483648]

    def test_null_roundtrip(self):
        assert py(i32([None, 1, None])) == [None, 1, None]

    def test_type_tag(self):
        assert i32([1]).type == dn.DrakenType.INT32

    def test_range_overflow_high(self):
        with pytest.raises(OverflowError):
            i32([2147483648])

    def test_range_overflow_low(self):
        with pytest.raises(OverflowError):
            i32([-2147483649])

    def test_boundary_values(self):
        v = i32([INT32_MIN, INT32_MAX, 0])
        assert py(v) == [INT32_MIN, INT32_MAX, 0]


class TestFactoryShapes:
    def test_int8_constant(self):
        v = dn.vector_int8_from_constant(42, 4)
        assert py(v) == [42, 42, 42, 42]

    def test_int8_constant_null(self):
        v = dn.vector_int8_from_constant(None, 3)
        assert py(v) == [None, None, None]

    def test_int16_constant(self):
        v = dn.vector_int16_from_constant(1000, 3)
        assert py(v) == [1000, 1000, 1000]

    def test_int32_constant(self):
        v = dn.vector_int32_from_constant(100000, 2)
        assert py(v) == [100000, 100000]

    def test_int8_dict(self):
        v = dn.vector_int8_from_dict([10, 20, 30], [0, 1, 2, 0])
        assert py(v) == [10, 20, 30, 10]

    def test_int16_dict(self):
        v = dn.vector_int16_from_dict([100, 200], [1, 0, 1])
        assert py(v) == [200, 100, 200]

    def test_int32_dict_nullable(self):
        v = dn.vector_int32_from_dict([10000], [0, 0, 0],
                                       [True, False, True])
        assert py(v) == [10000, None, 10000]

    def test_int8_constant_range_overflow(self):
        with pytest.raises(OverflowError):
            dn.vector_int8_from_constant(200, 1)


# ---------------------------------------------------------------------------
# HASH — parity with int64 for same values
# ---------------------------------------------------------------------------

class TestHash:
    def test_i8_hash_matches_i64(self):
        vals = [0, 1, -1, 5, 127, -128]
        h8  = i8(vals).hash()
        h64 = i64(vals).hash()
        assert h8 == h64, "int8 hash must match int64 for same values"

    def test_i16_hash_matches_i64(self):
        vals = [0, 100, -100, 32767, -32768]
        h16  = i16(vals).hash()
        h64  = i64(vals).hash()
        assert h16 == h64, "int16 hash must match int64 for same values"

    def test_i32_hash_matches_i64(self):
        vals = [0, 100000, -100000, 2147483647, -2147483648]
        h32  = i32(vals).hash()
        h64  = i64(vals).hash()
        assert h32 == h64, "int32 hash must match int64 for same values"

    def test_null_hash_distinct_from_zero(self):
        h_null = i8([None]).hash()
        h_zero = i8([0]).hash()
        assert h_null[0] != h_zero[0]


# ---------------------------------------------------------------------------
# COMPARE SCALAR
# ---------------------------------------------------------------------------

class TestCompareScalar:
    def test_i8_eq(self):
        v = i8([1, 2, 3, 2])
        assert py(v.compare_scalar(2, 0)) == [False, True, False, True]

    def test_i16_gt(self):
        v = i16([10, 20, 30])
        assert py(v.compare_scalar(15, 2)) == [False, True, True]

    def test_i32_lt(self):
        v = i32([100, 200, 300])
        assert py(v.compare_scalar(200, 4)) == [True, False, False]

    def test_null_row_yields_null(self):
        v = i8([None, 5])
        r = v.compare_scalar(5, 0)
        assert py(r) == [None, True]

    def test_i8_ne(self):
        v = i8([1, 2, 3])
        assert py(v.compare_scalar(2, 1)) == [True, False, True]

    def test_i8_le(self):
        v = i8([1, 5, 10])
        assert py(v.compare_scalar(5, 5)) == [True, True, False]


# ---------------------------------------------------------------------------
# BETWEEN
# ---------------------------------------------------------------------------

class TestBetween:
    def test_i8_closed(self):
        v = i8([1, 5, 10, 15, 20])
        assert py(v.between(5, 15)) == [False, True, True, True, False]

    def test_i16_half_open_low(self):
        v = i16([100, 200, 300])
        r = v.between(100, 300, lo_inclusive=False, hi_inclusive=True)
        assert py(r) == [False, True, True]

    def test_i32_null_rows(self):
        v = i32([None, 10, 20])
        assert py(v.between(5, 15)) == [None, True, False]

    def test_i8_boundary_values(self):
        v = i8([INT8_MIN, 0, INT8_MAX])
        assert py(v.between(INT8_MIN, INT8_MAX)) == [True, True, True]


# ---------------------------------------------------------------------------
# IN_LIST
# ---------------------------------------------------------------------------

class TestInList:
    def test_i8_basic(self):
        v = i8([1, 5, 10, 15])
        assert py(v.in_list([5, 15])) == [False, True, False, True]

    def test_i16_basic(self):
        v = i16([100, 200, 300])
        assert py(v.in_list([200])) == [False, True, False]

    def test_i32_basic(self):
        v = i32([10000, 20000, 30000])
        assert py(v.in_list([10000, 30000])) == [True, False, True]

    def test_null_rows_yield_null(self):
        v = i8([None, 5, 10])
        r = v.in_list([5])
        assert py(r) == [None, True, False]

    def test_i8_parity_with_i64_in_list(self):
        vals = [1, 5, 10]
        r8  = i8(vals).in_list([5])
        r64 = i64(vals).in_list([5])
        assert py(r8) == py(r64), "int8 in_list must match int64 for same values"

    def test_empty_set(self):
        v = i8([1, 2, 3])
        assert py(v.in_list([])) == [False, False, False]


# ---------------------------------------------------------------------------
# REDUCTIONS: sum / min / max
# ---------------------------------------------------------------------------

class TestReductions:
    def test_i8_sum(self):
        assert i8([10, 20, 30]).sum() == 60

    def test_i16_sum_all_null(self):
        assert i16([None, None]).sum() == 0

    def test_i32_sum_empty(self):
        assert i32([]).sum() == 0

    def test_i8_min(self):
        assert i8([-5, 10, 3]).min() == -5

    def test_i16_max(self):
        assert i16([100, 200, 50]).max() == 200

    def test_i32_min_raises_empty(self):
        with pytest.raises(Exception):
            i32([]).min()

    def test_i8_min_raises_all_null(self):
        with pytest.raises(Exception):
            i8([None, None]).min()

    def test_i8_sum_with_nulls(self):
        assert i8([None, 10, 20]).sum() == 30

    def test_i8_min_skips_nulls(self):
        assert i8([None, 5, 1]).min() == 1


# ---------------------------------------------------------------------------
# ARITHMETIC — homogeneous (result is NextWider<T>)
# ---------------------------------------------------------------------------

class TestArithmeticHomogeneous:
    def test_i8_add_result_type_is_i16(self):
        r = i8([1, 2]).add(i8([3, 4]))
        assert r.type == dn.DrakenType.INT16

    def test_i16_add_result_type_is_i32(self):
        r = i16([1, 2]).add(i16([3, 4]))
        assert r.type == dn.DrakenType.INT32

    def test_i32_add_result_type_is_i64(self):
        r = i32([1, 2]).add(i32([3, 4]))
        assert r.type == dn.DrakenType.INT64

    def test_i8_add_values(self):
        assert py(i8([10, 20]).add(i8([5, 5]))) == [15, 25]

    def test_i8_sub(self):
        assert py(i8([20, 10]).sub(i8([5, 3]))) == [15, 7]

    def test_i8_mul(self):
        assert py(i8([3, 4]).mul(i8([2, 5]))) == [6, 20]

    def test_i8_div(self):
        assert py(i8([10, 7]).div(i8([3, 2]))) == [3, 3]

    def test_i8_div_by_zero(self):
        assert py(i8([10]).div(i8([0]))) == [0]

    def test_i8_mod(self):
        assert py(i8([10, 7]).mod(i8([3, 2]))) == [1, 1]

    def test_i8_mod_by_zero(self):
        assert py(i8([10]).mod(i8([0]))) == [0]

    def test_null_propagation_binary(self):
        r = i8([None, 5]).add(i8([3, 5]))
        assert py(r) == [None, 10]

    def test_i8_add_scalar(self):
        r = i8([10, 20]).add(5)
        assert r.type == dn.DrakenType.INT16
        assert py(r) == [15, 25]

    def test_i16_sub_scalar(self):
        assert py(i16([100, 200]).sub(50)) == [50, 150]

    def test_i32_mul_scalar(self):
        assert py(i32([10, 20]).mul(3)) == [30, 60]


# ---------------------------------------------------------------------------
# NEG — result is NextWider<T>; INT_MIN negates without wrap
# ---------------------------------------------------------------------------

class TestNeg:
    def test_i8_neg_normal(self):
        r = i8([5, -5, 0]).neg()
        assert r.type == dn.DrakenType.INT16
        assert py(r) == [-5, 5, 0]

    def test_i8_neg_min_no_overflow(self):
        r = i8([INT8_MIN]).neg()
        assert r.type == dn.DrakenType.INT16
        assert py(r) == [128]  # would wrap if stayed in int8

    def test_i16_neg_min_no_overflow(self):
        r = i16([INT16_MIN]).neg()
        assert r.type == dn.DrakenType.INT32
        assert py(r) == [32768]

    def test_i32_neg_min_no_overflow(self):
        r = i32([INT32_MIN]).neg()
        assert r.type == dn.DrakenType.INT64
        assert py(r) == [2147483648]

    def test_neg_null_propagation(self):
        r = i8([None, 5]).neg()
        assert py(r) == [None, -5]


# ---------------------------------------------------------------------------
# CROSS-WIDTH ARITHMETIC
# ---------------------------------------------------------------------------

class TestCrossWidth:
    def test_i8_plus_i16_type(self):
        r = i8([1]).add(i16([2]))
        assert r.type == dn.DrakenType.INT32

    def test_i8_plus_i32_type(self):
        r = i8([1]).add(i32([2]))
        assert r.type == dn.DrakenType.INT64

    def test_i16_plus_i32_type(self):
        r = i16([1]).add(i32([2]))
        assert r.type == dn.DrakenType.INT64

    def test_i8_plus_i64_type(self):
        r = i8([1]).add(i64([2]))
        assert r.type == dn.DrakenType.INT64

    def test_cross_values_correct(self):
        a = i8([100, 120])
        c = i16([1000, 2000])
        r = a.add(c)
        assert py(r) == [1100, 2120]

    def test_cross_sub(self):
        r = i16([200]).sub(i8([50]))
        assert py(r) == [150]

    def test_cross_mul(self):
        r = i8([5]).mul(i32([10000]))
        assert py(r) == [50000]


# ---------------------------------------------------------------------------
# CROSS-WIDTH COMPARE — compare_vector between different integer widths
#
# Discriminating values are required: 300 exceeds int8 range (fits int16+int64);
# INT16_MAX=32767 fits int16 and int64 but not int8; mixed signs.
# Every test would fail against the buggy (no-promotion) implementation.
# ---------------------------------------------------------------------------

EQ, NE, GT, GE, LT, LE = 0, 1, 2, 3, 4, 5

def _py_cmp_list(op, xs, ys):
    """Reference: element-wise Python comparison, None-propagating."""
    results = []
    for a, b in zip(xs, ys):
        if a is None or b is None:
            results.append(None)
        elif op == EQ: results.append(a == b)
        elif op == NE: results.append(a != b)
        elif op == GT: results.append(a > b)
        elif op == GE: results.append(a >= b)
        elif op == LT: results.append(a < b)
        else:          results.append(a <= b)
    return results


class TestCrossWidthCompare:
    """compare_vector with mixed integer widths — all 6 ops, both directions."""

    # Discriminating data: 300 fits int16/int32/int64 but NOT int8.
    # Mismatched widths without promotion truncate/misread 300 → 44 or garbage.
    DATA_A16 = [5, 300, 5]     # int16
    DATA_B64 = [5, 300, 9]     # int64 — row1: 300==300 must be True

    def _check(self, a_vec, b_vec, xs, ys):
        for op in (EQ, NE, GT, GE, LT, LE):
            got = py(a_vec.compare_vector(b_vec, op))
            expected = _py_cmp_list(op, xs, ys)
            assert got == expected, f"op={op} got={got} expected={expected}"

    def test_i16_vs_i64_all_ops(self):
        self._check(i16(self.DATA_A16), i64(self.DATA_B64),
                    self.DATA_A16, self.DATA_B64)

    def test_i64_vs_i16_all_ops(self):
        self._check(i64(self.DATA_B64), i16(self.DATA_A16),
                    self.DATA_B64, self.DATA_A16)

    def test_i8_vs_i64_all_ops(self):
        xs = [5, 100, -1]
        ys = [5, 100, 0]
        self._check(i8(xs), i64(ys), xs, ys)

    def test_i64_vs_i8_all_ops(self):
        xs = [5, 100, -1]
        ys = [5, 100, 0]
        self._check(i64(xs), i8(ys), xs, ys)

    def test_i8_vs_i16_all_ops(self):
        # 300 not in int8 range; use int8-range values with sign variety
        xs = [-128, 0, 127]
        ys = [-128, 1, 127]
        self._check(i8(xs), i16(ys), xs, ys)

    def test_i16_vs_i8_all_ops(self):
        xs = [-128, 0, 127]
        ys = [-128, 1, 127]
        self._check(i16(xs), i8(ys), xs, ys)

    def test_i8_vs_i32_all_ops(self):
        xs = [5, 100, -1]
        ys = [5, 100, 0]
        self._check(i8(xs), i32(ys), xs, ys)

    def test_i32_vs_i8_all_ops(self):
        xs = [5, 100, -1]
        ys = [5, 100, 0]
        self._check(i32(xs), i8(ys), xs, ys)

    def test_i16_vs_i32_all_ops(self):
        xs = [5, 300, INT16_MAX]
        ys = [5, 300, INT16_MAX - 1]
        self._check(i16(xs), i32(ys), xs, ys)

    def test_i32_vs_i16_all_ops(self):
        xs = [5, 300, INT16_MAX]
        ys = [5, 300, INT16_MAX - 1]
        self._check(i32(xs), i16(ys), xs, ys)

    def test_i16_vs_i64_discriminating_eq(self):
        # Specifically the reported bug: 300==300 must be True.
        a = i16([5, 300, 5])
        b = i64([5, 300, 9])
        assert py(a.compare_vector(b, EQ)) == [True, True, False]

    def test_i64_vs_i16_not_all_false(self):
        # Specifically the reported bug: int64×int16 returned all-False.
        a = i64([5, 300, 5])
        b = i16([5, 300, 9])
        assert py(a.compare_vector(b, EQ)) == [True, True, False]

    def test_i16_vs_i64_gt_discriminating(self):
        a = i16([5, 300, 5])
        b = i64([5, 300, 9])
        assert py(a.compare_vector(b, GT)) == [False, False, False]

    def test_cross_width_null_propagation(self):
        a = i16([None, 300, 5])
        b = i64([5,    300, None])
        got = py(a.compare_vector(b, EQ))
        assert got == [None, True, None]


# ---------------------------------------------------------------------------
# GATHER — take / materialize / compress
# ---------------------------------------------------------------------------

class TestGather:
    def test_i8_take(self):
        v = i8([10, 20, 30, 40])
        assert py(v.take([3, 0, 2])) == [40, 10, 30]

    def test_i16_take_null_source(self):
        v = i16([None, 100, 200])
        r = v.take([0, 1])
        assert py(r) == [None, 100]

    def test_i32_materialize(self):
        v = dn.vector_int32_from_constant(99, 4)
        m = v.materialize()
        assert py(m) == [99, 99, 99, 99]

    def test_i8_compress_and_materialize(self):
        v = i8([1, 2, 1, 3, 2])
        c = v.compress()
        m = c.materialize()
        assert py(m) == [1, 2, 1, 3, 2]

    def test_i8_take_type_preserved(self):
        v = i8([1, 2, 3])
        assert v.take([0]).type == dn.DrakenType.INT8

    def test_i16_compress_type_preserved(self):
        v = i16([10, 20, 10])
        assert v.compress().type == dn.DrakenType.INT16

    def test_i32_materialize_type_preserved(self):
        v = i32([100])
        assert v.materialize().type == dn.DrakenType.INT32

    def test_i8_take_empty_indices(self):
        v = i8([1, 2, 3])
        assert py(v.take([])) == []

    def test_null_propagates_through_take(self):
        v = i8([None, 5, None])
        assert py(v.take([0, 1, 2, 1])) == [None, 5, None, 5]
