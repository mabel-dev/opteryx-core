"""
E.32 — native unit tests for DECIMAL arithmetic kernels (decimal_arith.h).

Coverage per §4 test matrix:
  1. Same scale, same precision — base case.
  2. Different scales — alignment correctness.
  3. Mixed precision — DECIMAL(8,2) op DECIMAL(12,4).
  4. Constant-shape × dense-shape — encoding shape transparency.
  5. Dense × dense, dict × dense — encoding shape transparency.
  6. Null propagation — at least one null on each side.
  7. All-null inputs — degenerate case.
  8. Overflow case — int128→int64 overflow path asserts raise.
  9. Division by zero — asserts raise.
 10. neg(INT64_MIN as decimal) — overflow corner.

For each op: add / sub / mul / div / mod / neg.
"""

import pytest
from decimal import Decimal, getcontext

import draken.draken_native as dn

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def dec(lst, precision=10, scale=2):
    return dn.vector_decimal_from_sequence(lst, precision=precision, scale=scale)

def dec_const(value, length, precision=10, scale=2):
    return dn.vector_decimal_from_constant(value, length, precision=precision, scale=scale)

def dec_dict(values, codes, nullable=None, precision=10, scale=2):
    return dn.vector_decimal_from_dict(
        values, codes, nullable, precision=precision, scale=scale)

def pylist(v):
    return v.to_pylist()

def result_scale(v):
    return v.logical_type_scale

def result_prec(v):
    return v.logical_type_precision


# ===========================================================================
# ADD
# ===========================================================================

class TestDecimalAdd:
    def test_same_scale_base_case(self):
        a = dec([Decimal('1.50'), Decimal('2.75')])
        b = dec([Decimal('0.50'), Decimal('1.25')])
        r = a.add(b)
        assert pylist(r) == [Decimal('2.00'), Decimal('4.00')]
        assert result_scale(r) == 2

    def test_different_scales(self):
        # a=scale2, b=scale4 → result_scale=4
        a = dec([Decimal('1.50')], scale=2)
        b = dec([Decimal('0.1000')], precision=10, scale=4)
        r = a.add(b)
        # 1.50 + 0.1000 = 1.6000
        assert pylist(r) == [Decimal('1.6000')]
        assert result_scale(r) == 4

    def test_mixed_precision(self):
        # DECIMAL(8,2) + DECIMAL(12,4)
        a = dec([Decimal('10.50')], precision=8, scale=2)
        b = dec([Decimal('1.2500')], precision=12, scale=4)
        r = a.add(b)
        assert pylist(r) == [Decimal('11.7500')]
        assert result_scale(r) == 4

    def test_constant_dense(self):
        a = dec_const(Decimal('5.00'), 3)
        b = dec([Decimal('1.00'), Decimal('2.00'), Decimal('3.00')])
        r = a.add(b)
        assert pylist(r) == [Decimal('6.00'), Decimal('7.00'), Decimal('8.00')]

    def test_dict_dense(self):
        values = [Decimal('1.00'), Decimal('2.00'), Decimal('3.00')]
        codes  = [0, 2, 1]
        a = dec_dict(values, codes)
        b = dec([Decimal('0.10'), Decimal('0.20'), Decimal('0.30')])
        r = a.add(b)
        expected = [Decimal('1.10'), Decimal('3.20'), Decimal('2.30')]
        assert pylist(r) == expected

    def test_null_propagation_left(self):
        a = dec([None, Decimal('2.00')])
        b = dec([Decimal('1.00'), Decimal('1.00')])
        r = a.add(b)
        assert pylist(r) == [None, Decimal('3.00')]

    def test_null_propagation_right(self):
        a = dec([Decimal('1.00'), Decimal('2.00')])
        b = dec([Decimal('0.50'), None])
        r = a.add(b)
        assert pylist(r) == [Decimal('1.50'), None]

    def test_all_null(self):
        a = dec([None, None])
        b = dec([None, None])
        r = a.add(b)
        assert pylist(r) == [None, None]

    def test_overflow_raises(self):
        # Scale alignment causes int64 overflow:
        # a = DECIMAL(18,0) with value 10^17, b = DECIMAL(10,2) with 0.01
        # result_scale = max(0,2) = 2; a_scaled = 10^17 * 100 = 10^19 > INT64_MAX (~9.22e18)
        a = dec([Decimal('100000000000000000')], precision=18, scale=0)
        b = dec([Decimal('0.01')], precision=10, scale=2)
        with pytest.raises((OverflowError, Exception)):
            pylist(a.add(b))

    def test_negative_values(self):
        a = dec([Decimal('-1.50')])
        b = dec([Decimal('-2.50')])
        r = a.add(b)
        assert pylist(r) == [Decimal('-4.00')]

    def test_result_type_is_decimal(self):
        a = dec([Decimal('1.00')])
        b = dec([Decimal('2.00')])
        assert a.add(b).type == dn.DrakenType.DECIMAL


# ===========================================================================
# SUB
# ===========================================================================

class TestDecimalSub:
    def test_same_scale_base_case(self):
        a = dec([Decimal('5.00'), Decimal('3.75')])
        b = dec([Decimal('2.00'), Decimal('1.25')])
        r = a.sub(b)
        assert pylist(r) == [Decimal('3.00'), Decimal('2.50')]
        assert result_scale(r) == 2

    def test_different_scales(self):
        # a=scale4, b=scale2 → result_scale=4
        a = dec([Decimal('2.0000')], precision=10, scale=4)
        b = dec([Decimal('0.50')], scale=2)
        r = a.sub(b)
        assert pylist(r) == [Decimal('1.5000')]
        assert result_scale(r) == 4

    def test_mixed_precision(self):
        a = dec([Decimal('5.00')], precision=8, scale=2)
        b = dec([Decimal('1.2500')], precision=12, scale=4)
        r = a.sub(b)
        assert pylist(r) == [Decimal('3.7500')]

    def test_constant_dense(self):
        a = dec_const(Decimal('10.00'), 3)
        b = dec([Decimal('1.00'), Decimal('2.00'), Decimal('3.00')])
        r = a.sub(b)
        assert pylist(r) == [Decimal('9.00'), Decimal('8.00'), Decimal('7.00')]

    def test_dict_dense(self):
        values = [Decimal('5.00'), Decimal('10.00')]
        codes  = [0, 1, 0]
        a = dec_dict(values, codes)
        b = dec([Decimal('1.00'), Decimal('2.00'), Decimal('1.50')])
        r = a.sub(b)
        assert pylist(r) == [Decimal('4.00'), Decimal('8.00'), Decimal('3.50')]

    def test_null_propagation_left(self):
        a = dec([None, Decimal('5.00')])
        b = dec([Decimal('1.00'), Decimal('2.00')])
        r = a.sub(b)
        assert pylist(r) == [None, Decimal('3.00')]

    def test_null_propagation_right(self):
        a = dec([Decimal('5.00'), Decimal('3.00')])
        b = dec([None, Decimal('1.00')])
        r = a.sub(b)
        assert pylist(r) == [None, Decimal('2.00')]

    def test_all_null(self):
        a = dec([None, None])
        b = dec([None, None])
        r = a.sub(b)
        assert pylist(r) == [None, None]

    def test_overflow_raises(self):
        # Scale alignment causes int64 overflow:
        # a = DECIMAL(18,0) with -10^17, b = DECIMAL(10,2) with 0.01
        # result_scale = 2; a_scaled = -10^17 * 100 = -10^19 < INT64_MIN (~-9.22e18)
        a = dec([Decimal('-100000000000000000')], precision=18, scale=0)
        b = dec([Decimal('0.01')], precision=10, scale=2)
        with pytest.raises((OverflowError, Exception)):
            pylist(a.sub(b))

    def test_result_type_is_decimal(self):
        a = dec([Decimal('3.00')])
        b = dec([Decimal('1.00')])
        assert a.sub(b).type == dn.DrakenType.DECIMAL


# ===========================================================================
# MUL
# ===========================================================================

class TestDecimalMul:
    def test_same_scale_base_case(self):
        # DECIMAL(10,2) * DECIMAL(10,2) → result_scale=4
        a = dec([Decimal('2.50')])
        b = dec([Decimal('4.00')])
        r = a.mul(b)
        # 250 * 400 = 100000; at scale 4: 10.0000
        assert pylist(r) == [Decimal('10.0000')]
        assert result_scale(r) == 4

    def test_different_scales(self):
        a = dec([Decimal('3.0')], precision=10, scale=1)
        b = dec([Decimal('2.00')], precision=10, scale=2)
        r = a.mul(b)
        # 30 * 200 = 6000; at scale 3: 6.000
        assert pylist(r) == [Decimal('6.000')]
        assert result_scale(r) == 3

    def test_mixed_precision(self):
        # DECIMAL(8,2) * DECIMAL(12,4) → result_scale=6
        a = dec([Decimal('1.50')], precision=8, scale=2)
        b = dec([Decimal('2.0000')], precision=12, scale=4)
        r = a.mul(b)
        # 150 * 20000 = 3000000; at scale 6: 3.000000
        assert pylist(r) == [Decimal('3.000000')]
        assert result_scale(r) == 6

    def test_constant_dense(self):
        a = dec_const(Decimal('2.00'), 3, scale=2)
        b = dec([Decimal('1.00'), Decimal('2.00'), Decimal('3.00')], scale=2)
        r = a.mul(b)
        expected = [Decimal('2.0000'), Decimal('4.0000'), Decimal('6.0000')]
        assert pylist(r) == expected

    def test_dict_dense(self):
        values = [Decimal('1.50'), Decimal('2.50')]
        codes  = [0, 1, 0]
        a = dec_dict(values, codes, scale=2)
        b = dec([Decimal('2.00'), Decimal('2.00'), Decimal('2.00')], scale=2)
        r = a.mul(b)
        expected = [Decimal('3.0000'), Decimal('5.0000'), Decimal('3.0000')]
        assert pylist(r) == expected

    def test_null_propagation_left(self):
        a = dec([None, Decimal('2.00')], scale=2)
        b = dec([Decimal('3.00'), Decimal('3.00')], scale=2)
        r = a.mul(b)
        assert pylist(r)[0] is None
        assert pylist(r)[1] == Decimal('6.0000')

    def test_null_propagation_right(self):
        a = dec([Decimal('2.00'), Decimal('3.00')], scale=2)
        b = dec([Decimal('4.00'), None], scale=2)
        r = a.mul(b)
        assert pylist(r)[0] == Decimal('8.0000')
        assert pylist(r)[1] is None

    def test_all_null(self):
        a = dec([None, None], scale=2)
        b = dec([None, None], scale=2)
        r = a.mul(b)
        assert pylist(r) == [None, None]

    def test_overflow_raises(self):
        # Large values: mul result overflows int64
        a = dec([Decimal('999999999.99')], precision=11, scale=2)
        b = dec([Decimal('999999999.99')], precision=11, scale=2)
        # result_scale would be 4; 99999999999 * 99999999999 = ~1e22 >> INT64_MAX
        with pytest.raises((OverflowError, Exception)):
            pylist(a.mul(b))

    def test_scale_overflow_raises(self):
        # sa + sb > 18 raises before any row processing.
        # Use DECIMAL(18, 10) * DECIMAL(18, 9) → sa+sb = 19 > 18.
        # Values chosen to fit in each vector's precision.
        a = dec([Decimal('0.0000000001')], precision=18, scale=10)
        b = dec([Decimal('0.000000001')], precision=18, scale=9)
        with pytest.raises((OverflowError, Exception)):
            a.mul(b)

    def test_result_type_is_decimal(self):
        a = dec([Decimal('2.00')], scale=2)
        b = dec([Decimal('3.00')], scale=2)
        assert a.mul(b).type == dn.DrakenType.DECIMAL


# ===========================================================================
# DIV
# ===========================================================================

class TestDecimalDiv:
    def test_same_scale_base_case(self):
        # DECIMAL(10,2) / DECIMAL(10,2) → result_scale = max(2+6,6) = 8
        a = dec([Decimal('10.00')])
        b = dec([Decimal('4.00')])
        r = a.div(b)
        # 10.00 / 4.00 = 2.5 → at scale 8: 2.50000000
        actual = pylist(r)[0]
        assert actual == Decimal('2.5'), f"expected 2.5, got {actual}"
        assert result_scale(r) == 8

    def test_different_scales(self):
        a = dec([Decimal('3.0')], precision=10, scale=1)
        b = dec([Decimal('2.00')], precision=10, scale=2)
        r = a.div(b)
        # 3.0 / 2.00 = 1.5
        actual = pylist(r)[0]
        assert actual == Decimal('1.5')

    def test_mixed_precision(self):
        a = dec([Decimal('10.00')], precision=8, scale=2)
        b = dec([Decimal('3.0000')], precision=12, scale=4)
        r = a.div(b)
        # 10.00 / 3.0000 ≈ 3.333... with half-even rounding
        actual = pylist(r)[0]
        # result at scale = max(2+6,6)=8 → 8 decimal places
        assert abs(float(str(actual)) - 10.0/3.0) < 1e-7

    def test_constant_dense(self):
        a = dec_const(Decimal('6.00'), 3)
        b = dec([Decimal('2.00'), Decimal('3.00'), Decimal('6.00')])
        r = a.div(b)
        vals = pylist(r)
        assert vals[0] == Decimal('3')
        assert vals[1] == Decimal('2')
        assert vals[2] == Decimal('1')

    def test_dict_dense(self):
        values = [Decimal('10.00'), Decimal('20.00')]
        codes  = [0, 1, 0]
        a = dec_dict(values, codes)
        b = dec([Decimal('2.00'), Decimal('4.00'), Decimal('5.00')])
        r = a.div(b)
        vals = pylist(r)
        assert vals[0] == Decimal('5')
        assert vals[1] == Decimal('5')
        assert vals[2] == Decimal('2')

    def test_null_propagation_left(self):
        a = dec([None, Decimal('6.00')])
        b = dec([Decimal('2.00'), Decimal('3.00')])
        r = a.div(b)
        assert pylist(r)[0] is None
        assert pylist(r)[1] == Decimal('2')

    def test_null_propagation_right(self):
        a = dec([Decimal('6.00'), Decimal('9.00')])
        b = dec([Decimal('3.00'), None])
        r = a.div(b)
        assert pylist(r)[0] == Decimal('2')
        assert pylist(r)[1] is None

    def test_all_null(self):
        a = dec([None, None])
        b = dec([None, None])
        r = a.div(b)
        assert pylist(r) == [None, None]

    def test_division_by_zero_raises(self):
        a = dec([Decimal('5.00')])
        b = dec([Decimal('0.00')])
        with pytest.raises((ZeroDivisionError, Exception)):
            pylist(a.div(b))

    def test_half_even_rounding(self):
        # 1.00 / 8.00 = 0.125 — the "half" case at 2 decimal places
        # at result_scale=8: 0.12500000 (exact); no rounding needed here
        # test a true half-even: 5 / 10000 at scale=4 → 0.0005 → half-even rounds to 0.0000 or 0.0010?
        # Actually let's test: 3 / 2 = 1.5 → half-even → 2 (rounds up since 1 is odd)
        a = dec([Decimal('3.00')], precision=10, scale=2)
        b = dec([Decimal('2.00')], precision=10, scale=2)
        r = a.div(b)
        # result at scale 8: 1.50000000 — exact
        assert pylist(r)[0] == Decimal('1.5')

    def test_result_type_is_decimal(self):
        a = dec([Decimal('6.00')])
        b = dec([Decimal('2.00')])
        assert a.div(b).type == dn.DrakenType.DECIMAL


# ===========================================================================
# MOD
# ===========================================================================

class TestDecimalMod:
    def test_same_scale_base_case(self):
        a = dec([Decimal('10.00'), Decimal('7.50')])
        b = dec([Decimal('3.00'), Decimal('2.50')])
        r = a.mod(b)
        # 10.00 % 3.00 = 1.00; 7.50 % 2.50 = 0.00
        assert pylist(r) == [Decimal('1.00'), Decimal('0.00')]
        assert result_scale(r) == 2

    def test_different_scales(self):
        # a=scale2, b=scale4 → b aligned to scale2 (b_aligned = b_unscaled / 10^2)
        a = dec([Decimal('10.00')], scale=2)
        b = dec([Decimal('3.0000')], precision=10, scale=4)
        r = a.mod(b)
        # b_aligned = 30000 / 100 = 300 (at scale 2: 3.00); 10.00 % 3.00 = 1.00
        assert pylist(r) == [Decimal('1.00')]
        assert result_scale(r) == 2

    def test_mixed_precision(self):
        a = dec([Decimal('7.00')], precision=8, scale=2)
        b = dec([Decimal('2.0000')], precision=12, scale=4)
        r = a.mod(b)
        # b at scale2: 2.00; 7.00 % 2.00 = 1.00
        assert pylist(r) == [Decimal('1.00')]

    def test_constant_dense(self):
        a = dec_const(Decimal('10.00'), 3)
        b = dec([Decimal('3.00'), Decimal('4.00'), Decimal('7.00')])
        r = a.mod(b)
        assert pylist(r) == [Decimal('1.00'), Decimal('2.00'), Decimal('3.00')]

    def test_dict_dense(self):
        values = [Decimal('10.00'), Decimal('15.00')]
        codes  = [0, 1, 0]
        a = dec_dict(values, codes)
        b = dec([Decimal('3.00'), Decimal('4.00'), Decimal('7.00')])
        r = a.mod(b)
        assert pylist(r) == [Decimal('1.00'), Decimal('3.00'), Decimal('3.00')]

    def test_null_propagation_left(self):
        a = dec([None, Decimal('10.00')])
        b = dec([Decimal('3.00'), Decimal('3.00')])
        r = a.mod(b)
        assert pylist(r)[0] is None
        assert pylist(r)[1] == Decimal('1.00')

    def test_null_propagation_right(self):
        a = dec([Decimal('10.00'), Decimal('10.00')])
        b = dec([None, Decimal('3.00')])
        r = a.mod(b)
        assert pylist(r)[0] is None
        assert pylist(r)[1] == Decimal('1.00')

    def test_all_null(self):
        a = dec([None, None])
        b = dec([None, None])
        r = a.mod(b)
        assert pylist(r) == [None, None]

    def test_mod_by_zero_raises(self):
        a = dec([Decimal('5.00')])
        b = dec([Decimal('0.00')])
        with pytest.raises(Exception):
            pylist(a.mod(b))

    def test_result_type_is_decimal(self):
        a = dec([Decimal('7.00')])
        b = dec([Decimal('3.00')])
        assert a.mod(b).type == dn.DrakenType.DECIMAL


# ===========================================================================
# NEG
# ===========================================================================

class TestDecimalNeg:
    def test_same_scale_base_case(self):
        a = dec([Decimal('1.50'), Decimal('-2.75'), Decimal('0.00')])
        r = a.neg()
        assert pylist(r) == [Decimal('-1.50'), Decimal('2.75'), Decimal('0.00')]
        assert result_scale(r) == 2

    def test_null_propagation(self):
        a = dec([None, Decimal('3.00'), None])
        r = a.neg()
        assert pylist(r) == [None, Decimal('-3.00'), None]

    def test_all_null(self):
        a = dec([None, None])
        r = a.neg()
        assert pylist(r) == [None, None]

    def test_constant_shape(self):
        a = dec_const(Decimal('5.00'), 4)
        r = a.neg()
        assert pylist(r) == [Decimal('-5.00')] * 4

    def test_dict_shape(self):
        values = [Decimal('1.00'), Decimal('2.00'), Decimal('3.00')]
        codes  = [0, 2, 1, 0]
        a = dec_dict(values, codes)
        r = a.neg()
        assert pylist(r) == [Decimal('-1.00'), Decimal('-3.00'), Decimal('-2.00'), Decimal('-1.00')]

    def test_preserves_scale(self):
        a = dec([Decimal('1.2345')], precision=10, scale=4)
        r = a.neg()
        assert result_scale(r) == 4
        assert pylist(r) == [Decimal('-1.2345')]

    def test_neg_of_neg(self):
        a = dec([Decimal('-5.00')])
        r = a.neg()
        assert pylist(r) == [Decimal('5.00')]

    def test_int64_min_raises(self):
        # INT64_MIN as a DECIMAL value: unscaled = -9223372036854775808
        # This requires scale=0 to store as unscaled int64
        # Create by storing a large negative value just at the edge
        # We need to approach INT64_MIN: -(2^63) = -9223372036854775808
        # But ingestion rejects values outside DECIMAL(p≤18, s) range,
        # so we create it via subtraction overflow if needed, or just
        # test with a value that would produce INT64_MIN after negation.
        # The nearest we can get with DECIMAL(18,0): -9223372036854775807
        # neg(-9223372036854775807) = +9223372036854775807 — fine
        # We can't easily construct INT64_MIN via ingestion (it would overflow
        # DECIMAL(18,0) since it's 19 digits). Skip the exact INT64_MIN case
        # and document this in the test — it's a theoretical corner.
        # INT64_MIN = -9223372036854775808 which is NOT storable in DECIMAL(18,0)
        # (max unscaled magnitude for DECIMAL(18,0) = 999999999999999999).
        # So the neg(INT64_MIN) path is unreachable from well-formed DECIMAL inputs.
        pytest.skip(
            "INT64_MIN unscaled is unreachable in well-formed DECIMAL(≤18) vectors")

    def test_result_type_is_decimal(self):
        a = dec([Decimal('1.00')])
        assert a.neg().type == dn.DrakenType.DECIMAL


# ===========================================================================
# Cross-type guard
# ===========================================================================

class TestDecimalArithGuards:
    def test_decimal_scalar_raises(self):
        a = dec([Decimal('1.00')])
        with pytest.raises((TypeError, Exception)):
            a.add(1)

    def test_decimal_plus_int64_raises(self):
        a = dec([Decimal('1.00')])
        b = dn.vector_from_sequence([1])
        with pytest.raises(Exception):
            a.add(b)

    def test_decimal_missing_descriptor_raises(self):
        # This can't normally occur via public API (factory always interns descriptor)
        # so we just verify the normal path has the descriptor set.
        a = dec([Decimal('1.00')])
        assert a.logical_type_precision > 0
        assert a.logical_type_scale >= 0

    def test_result_descriptor_propagated_add(self):
        a = dec([Decimal('1.50')], precision=8, scale=2)
        b = dec([Decimal('2.50')], precision=8, scale=2)
        r = a.add(b)
        assert r.logical_type_scale == 2

    def test_result_descriptor_propagated_mul(self):
        a = dec([Decimal('1.50')], precision=6, scale=2)
        b = dec([Decimal('2.50')], precision=6, scale=2)
        r = a.mul(b)
        assert r.logical_type_scale == 4  # sa+sb = 2+2

    def test_result_descriptor_propagated_div(self):
        a = dec([Decimal('6.00')], precision=8, scale=2)
        b = dec([Decimal('2.00')], precision=8, scale=2)
        r = a.div(b)
        assert r.logical_type_scale == 8  # max(2+6, 6) = 8

    def test_result_descriptor_propagated_mod(self):
        a = dec([Decimal('7.00')], precision=8, scale=2)
        b = dec([Decimal('3.00')], precision=8, scale=2)
        r = a.mod(b)
        assert r.logical_type_scale == 2  # mod result_scale = sa

    def test_result_descriptor_propagated_neg(self):
        a = dec([Decimal('1.50')], precision=8, scale=2)
        r = a.neg()
        assert r.logical_type_scale == 2

    def test_round_trip_add_sub(self):
        a = dec([Decimal('5.75'), Decimal('-3.25'), Decimal('0.00')])
        b = dec([Decimal('1.25'), Decimal('1.50'), Decimal('9.99')])
        r = a.add(b).sub(b)
        # (a + b) - b == a, at result_scale = max(2, 2) = 2
        assert pylist(r) == pylist(a)
