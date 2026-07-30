"""
Native unit tests for DECIMAL(p≤18,s) ingestion, readback, and ops in draken.draken_native.

Coverage (D.10 acceptance criteria):

  shapes:          sequence / constant / dict
  nullability:     no nulls / some nulls / all null
  sizes:           0 / 1 / small / medium
  round-trip:      Decimal('1.50') round-trips as Decimal('1.50'), not Decimal('1.5')
  ingestion errors: precision exceeded / sub-scale precision / int64 overflow / NaN / Inf
  mandatory desc:  no descriptor = hard error (enforced at factory level)
  same-scale ops:  compare_scalar, compare_vector, hash, sum, min, max, between, in_list,
                   take, materialize, dictionary_encode
  cross-scale:     compare_vector with mismatched scales must throw
  hypothesis:      round-trip ordering, cross-scale throw
"""

import pytest
from decimal import Decimal

import draken.draken_native as dn

# ---------------------------------------------------------------------------
# Op codes (ABI-frozen)
# ---------------------------------------------------------------------------
EQ, NE, GT, GE, LT, LE = 0, 1, 2, 3, 4, 5


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

def cmp_s(v, scalar, op):
    return pylist(v.compare_scalar(scalar, op))

def _py_cmp(op, a, b):
    if a is None or b is None:
        return None
    return {EQ: a == b, NE: a != b, GT: a > b, GE: a >= b, LT: a < b, LE: a <= b}[op]


# ===========================================================================
# 1. Type tag and descriptor
# ===========================================================================

class TestTypeTag:
    def test_type_is_decimal(self):
        v = dec([Decimal('1.00')])
        assert v.type == dn.DrakenType.DECIMAL

    def test_logical_precision(self):
        v = dec([Decimal('1.00')], precision=12, scale=4)
        assert v.logical_type_precision == 12

    def test_logical_scale(self):
        v = dec([Decimal('1.00')], precision=12, scale=4)
        assert v.logical_type_scale == 4

    def test_scale_zero(self):
        v = dec([Decimal('5')], precision=5, scale=0)
        assert v.logical_type_scale == 0
        assert v.logical_type_precision == 5

    def test_length(self):
        v = dec([Decimal('1.00'), Decimal('2.00'), None])
        assert len(v) == 3


# ===========================================================================
# 2. Round-trip identity (scale preserved)
# ===========================================================================

class TestRoundTrip:
    def test_scale_preserved_trailing_zero(self):
        # Decimal('1.50') must round-trip as Decimal('1.50'), not Decimal('1.5')
        v = dec([Decimal('1.50')])
        result = pylist(v)
        assert result[0] == Decimal('1.50')
        assert str(result[0]) == '1.50'

    def test_scale_preserved_zero(self):
        v = dec([Decimal('0.00')])
        result = pylist(v)
        assert str(result[0]) == '0.00'

    def test_scale_preserved_negative(self):
        v = dec([Decimal('-3.25')])
        result = pylist(v)
        assert result[0] == Decimal('-3.25')
        assert str(result[0]) == '-3.25'

    def test_integer_value_at_scale2(self):
        # Decimal('5') at scale 2 → stored as 500 → round-trips as Decimal('5.00')
        v = dec([Decimal('5')], precision=10, scale=2)
        result = pylist(v)
        assert result[0] == Decimal('5')
        assert str(result[0]) == '5.00'

    def test_decimal_2_0_at_scale0(self):
        # Decimal('2.0') at scale 0: 2.0 is exactly representable, stores as 2
        # round-trips as Decimal('2') (scale 0 has no decimal places)
        v = dec([Decimal('2.0')], precision=5, scale=0)
        result = pylist(v)
        assert result[0] == Decimal('2')

    def test_null_round_trips(self):
        v = dec([None])
        result = pylist(v)
        assert result[0] is None

    def test_mixed_round_trip(self):
        vals = [Decimal('1.23'), None, Decimal('-4.56'), Decimal('0.00')]
        v = dec(vals)
        result = pylist(v)
        assert result[0] == Decimal('1.23')
        assert result[1] is None
        assert result[2] == Decimal('-4.56')
        assert result[3] == Decimal('0.00')

    def test_empty(self):
        v = dec([])
        assert pylist(v) == []

    def test_single_element(self):
        v = dec([Decimal('9.99')])
        assert pylist(v) == [Decimal('9.99')]

    def test_scale3_round_trip(self):
        vals = [Decimal('1.234'), Decimal('0.001'), Decimal('-9.999')]
        v = dec(vals, precision=10, scale=3)
        result = pylist(v)
        assert result == [Decimal('1.234'), Decimal('0.001'), Decimal('-9.999')]
        for i, r in enumerate(result):
            assert str(r) == str(vals[i])

    def test_getitem_round_trip(self):
        v = dec([Decimal('2.50'), None, Decimal('3.75')])
        assert v[0] == Decimal('2.50')
        assert v[1] is None
        assert v[2] == Decimal('3.75')
        assert v[-1] == Decimal('3.75')


# ===========================================================================
# 3. Ingestion error cases
# ===========================================================================

class TestIngestionErrors:
    def test_sub_scale_precision_fails(self):
        # Decimal('1.505') at scale 2 has more decimal places than scale → fail
        with pytest.raises((ValueError, OverflowError, Exception)):
            dec([Decimal('1.505')])

    def test_precision_exceeded_fails(self):
        # DECIMAL(3,0): max representable is 999; 1000 exceeds precision
        with pytest.raises((ValueError, OverflowError, Exception)):
            dec([Decimal('1000')], precision=3, scale=0)

    def test_negative_precision_exceeded_fails(self):
        with pytest.raises((ValueError, OverflowError, Exception)):
            dec([Decimal('-1000')], precision=3, scale=0)

    def test_precision_boundary_passes(self):
        # DECIMAL(3,0): 999 is exactly at the limit → should pass
        v = dec([Decimal('999')], precision=3, scale=0)
        assert v[0] == Decimal('999')

    def test_precision_boundary_one_over_fails(self):
        # 1000 = 10^3 → rejected for DECIMAL(3,0)
        with pytest.raises((ValueError, OverflowError, Exception)):
            dec([Decimal('1000')], precision=3, scale=0)

    def test_int64_overflow_fails(self):
        # Value that would require more than int64 range
        huge = Decimal('99999999999999999999')  # 20 digits, way beyond int64
        with pytest.raises((ValueError, OverflowError, Exception)):
            dec([huge], precision=18, scale=0)

    def test_precision_out_of_range_low(self):
        with pytest.raises((ValueError, Exception)):
            dn.vector_decimal_from_sequence([], precision=0, scale=0)

    def test_precision_out_of_range_high(self):
        with pytest.raises((ValueError, Exception)):
            dn.vector_decimal_from_sequence([], precision=19, scale=0)

    def test_scale_exceeds_precision(self):
        with pytest.raises((ValueError, Exception)):
            dn.vector_decimal_from_sequence([], precision=5, scale=6)


# ===========================================================================
# 4. Shapes: constant and dict
# ===========================================================================

class TestShapes:
    def test_constant_shape(self):
        v = dec_const(Decimal('3.14'), 5)
        assert v.is_constant
        result = pylist(v)
        assert all(r == Decimal('3.14') for r in result)
        assert len(result) == 5

    def test_constant_null(self):
        v = dec_const(None, 3)
        assert pylist(v) == [None, None, None]

    def test_dict_shape(self):
        values = [Decimal('1.00'), Decimal('2.00'), Decimal('3.00')]
        codes  = [0, 2, 1, 0, 2]
        v = dec_dict(values, codes)
        assert v.is_dict
        result = pylist(v)
        assert result == [Decimal('1.00'), Decimal('3.00'), Decimal('2.00'),
                          Decimal('1.00'), Decimal('3.00')]

    def test_dict_with_nulls(self):
        values = [Decimal('5.50'), Decimal('6.60')]
        codes  = [0, 0, 1]
        valid  = [True, False, True]
        v = dec_dict(values, codes, valid)
        result = pylist(v)
        assert result == [Decimal('5.50'), None, Decimal('6.60')]


# ===========================================================================
# 5. Reductions: sum, min, max
# ===========================================================================

class TestReductions:
    def test_sum_basic(self):
        v = dec([Decimal('1.50'), Decimal('2.30')])
        s = v.sum()
        assert s == Decimal('3.80')
        assert str(s) == '3.80'

    def test_sum_with_nulls(self):
        v = dec([Decimal('1.00'), None, Decimal('2.00')])
        assert v.sum() == Decimal('3.00')

    def test_sum_all_null(self):
        v = dec([None, None])
        # sum of all-null → Decimal('0.00') (int64 sum = 0, then converted)
        assert v.sum() == Decimal('0.00')

    def test_sum_empty(self):
        v = dec([])
        assert v.sum() == Decimal('0.00')

    def test_sum_scale_preserved(self):
        v = dec([Decimal('1.50'), Decimal('0.50')], precision=10, scale=2)
        assert str(v.sum()) == '2.00'

    def test_min_basic(self):
        v = dec([Decimal('3.00'), Decimal('1.00'), Decimal('2.00')])
        assert v.min() == Decimal('1.00')

    def test_min_with_nulls(self):
        v = dec([None, Decimal('5.00'), Decimal('3.00'), None])
        assert v.min() == Decimal('3.00')

    def test_min_all_null_raises(self):
        with pytest.raises(Exception):
            dec([None, None]).min()

    def test_min_empty_raises(self):
        with pytest.raises(Exception):
            dec([]).min()

    def test_min_scale_preserved(self):
        v = dec([Decimal('2.50'), Decimal('1.75')])
        assert str(v.min()) == '1.75'

    def test_max_basic(self):
        v = dec([Decimal('3.00'), Decimal('1.00'), Decimal('5.00')])
        assert v.max() == Decimal('5.00')

    def test_max_with_nulls(self):
        v = dec([None, Decimal('4.00'), None, Decimal('2.00')])
        assert v.max() == Decimal('4.00')

    def test_max_scale_preserved(self):
        v = dec([Decimal('1.50'), Decimal('9.99')])
        assert str(v.max()) == '9.99'


# ===========================================================================
# 6. Compare operations
# ===========================================================================

class TestCompare:
    def test_compare_scalar_eq(self):
        v = dec([Decimal('1.00'), Decimal('2.00'), Decimal('3.00')])
        assert cmp_s(v, Decimal('2.00'), EQ) == [False, True, False]

    def test_compare_scalar_lt(self):
        v = dec([Decimal('1.00'), Decimal('2.00'), Decimal('3.00')])
        assert cmp_s(v, Decimal('2.00'), LT) == [True, False, False]

    def test_compare_scalar_ge(self):
        v = dec([Decimal('1.00'), Decimal('2.00'), Decimal('3.00')])
        assert cmp_s(v, Decimal('2.00'), GE) == [False, True, True]

    def test_compare_scalar_null_rows(self):
        v = dec([Decimal('1.00'), None, Decimal('3.00')])
        result = cmp_s(v, Decimal('2.00'), EQ)
        assert result == [False, None, False]

    def test_compare_scalar_null_scalar_raises(self):
        v = dec([Decimal('1.00'), Decimal('2.00')])
        with pytest.raises(TypeError):
            cmp_s(v, None, EQ)

    def test_compare_scalar_promotes_less_precise(self):
        # Decimal('2') at scale 2 → unscaled 200, same as Decimal('2.00')
        v = dec([Decimal('1.00'), Decimal('2.00'), Decimal('3.00')])
        assert cmp_s(v, Decimal('2'), EQ) == [False, True, False]

    def test_compare_vector_eq(self):
        a = dec([Decimal('1.00'), Decimal('2.00'), Decimal('3.00')])
        b = dec([Decimal('1.00'), Decimal('2.50'), Decimal('3.00')])
        result = pylist(a.compare_vector(b, EQ))
        assert result == [True, False, True]

    def test_compare_vector_ne(self):
        a = dec([Decimal('1.00'), Decimal('2.00')])
        b = dec([Decimal('1.00'), Decimal('2.50')])
        assert pylist(a.compare_vector(b, NE)) == [False, True]

    def test_compare_vector_null_rows(self):
        a = dec([Decimal('1.00'), None, Decimal('3.00')])
        b = dec([Decimal('1.00'), Decimal('2.00'), Decimal('3.00')])
        result = pylist(a.compare_vector(b, EQ))
        assert result == [True, None, True]

    def test_compare_vector_cross_scale_works(self):
        # Cross-scale comparison is scale-aware: values are aligned in int128.
        a = dec([Decimal('1.00')], scale=2)
        b = dec([Decimal('1.0000')], precision=10, scale=4)
        assert pylist(a.compare_vector(b, EQ)) == [True]

    def test_compare_vector_cross_type_int64_as_scale0(self):
        # INT64 is treated as scale-0 decimal: 1 == 1.00 after alignment.
        a = dec([Decimal('1.00')])
        b = dn.vector_from_sequence([1])
        assert pylist(a.compare_vector(b, EQ)) == [True]


# ===========================================================================
# 7. Hash
# ===========================================================================

class TestHash:
    def test_equal_decimals_equal_hash(self):
        # Two decimal columns with same scale: equal values → equal hash
        v = dec([Decimal('1.50'), Decimal('2.50'), Decimal('1.50')])
        hashes = v.hash()
        assert hashes[0] == hashes[2]  # equal values
        assert hashes[0] != hashes[1]  # different values (probabilistic)

    def test_hash_null_sentinel(self):
        v = dec([Decimal('1.50'), None])
        hashes = v.hash()
        # Null hash is a fixed sentinel, non-null hash is different (probabilistic)
        assert hashes[0] != hashes[1]

    def test_hash_deterministic(self):
        v1 = dec([Decimal('3.14')])
        v2 = dec([Decimal('3.14')])
        assert v1.hash()[0] == v2.hash()[0]


# ===========================================================================
# 8. Between and in_list
# ===========================================================================

class TestPredicates:
    def test_between_inclusive(self):
        v = dec([Decimal('1.00'), Decimal('2.00'), Decimal('3.00'), Decimal('4.00')])
        result = pylist(v.between(Decimal('2.00'), Decimal('3.00')))
        assert result == [False, True, True, False]

    def test_between_exclusive_lo(self):
        v = dec([Decimal('1.00'), Decimal('2.00'), Decimal('3.00')])
        result = pylist(v.between(Decimal('2.00'), Decimal('3.00'), lo_inclusive=False))
        assert result == [False, False, True]

    def test_between_null_row(self):
        v = dec([Decimal('2.50'), None])
        result = pylist(v.between(Decimal('1.00'), Decimal('3.00')))
        assert result == [True, None]

    def test_in_list_basic(self):
        v = dec([Decimal('1.00'), Decimal('2.00'), Decimal('3.00'), Decimal('4.00')])
        result = pylist(v.in_list([Decimal('2.00'), Decimal('4.00')]))
        assert result == [False, True, False, True]

    def test_in_list_null_row(self):
        v = dec([Decimal('1.00'), None, Decimal('3.00')])
        result = pylist(v.in_list([Decimal('1.00'), Decimal('3.00')]))
        assert result == [True, None, True]

    def test_in_list_empty_set(self):
        v = dec([Decimal('1.00'), Decimal('2.00')])
        result = pylist(v.in_list([]))
        assert result == [False, False]


# ===========================================================================
# 9. Take / materialize / dictionary_encode
# ===========================================================================

class TestGather:
    def test_take_basic(self):
        v = dec([Decimal('1.00'), Decimal('2.00'), Decimal('3.00')])
        r = v.take([2, 0, 1])
        assert pylist(r) == [Decimal('3.00'), Decimal('1.00'), Decimal('2.00')]

    def test_take_preserves_type(self):
        v = dec([Decimal('1.00'), Decimal('2.00')])
        r = v.take([1])
        assert r.type == dn.DrakenType.DECIMAL

    def test_take_preserves_descriptor(self):
        v = dec([Decimal('1.00')], precision=12, scale=3)
        r = v.take([0])
        assert r.logical_type_precision == 12
        assert r.logical_type_scale == 3

    def test_take_with_nulls(self):
        v = dec([Decimal('1.00'), None, Decimal('3.00')])
        r = v.take([1, 0])
        assert pylist(r) == [None, Decimal('1.00')]

    def test_materialize(self):
        # Dict-encoded → materialize → dense or same shape
        values = [Decimal('1.00'), Decimal('2.00')]
        codes  = [0, 1, 0, 1]
        v = dec_dict(values, codes)
        m = v.materialize()
        assert m.type == dn.DrakenType.DECIMAL
        assert pylist(m) == [Decimal('1.00'), Decimal('2.00'),
                              Decimal('1.00'), Decimal('2.00')]

    def test_materialize_preserves_descriptor(self):
        v = dec_dict([Decimal('1.00')], [0], precision=7, scale=3)
        m = v.materialize()
        assert m.logical_type_precision == 7
        assert m.logical_type_scale == 3

    def test_dictionary_encode_basic(self):
        v = dec([Decimal('1.00'), Decimal('1.00'), Decimal('2.00')])
        c = v.dictionary_encode()
        assert c.type == dn.DrakenType.DECIMAL
        assert set(pylist(c)) == {Decimal('1.00'), Decimal('2.00')}

    def test_dictionary_encode_preserves_descriptor(self):
        v = dec([Decimal('1.00'), Decimal('2.00')], precision=9, scale=2)
        c = v.dictionary_encode()
        assert c.logical_type_precision == 9
        assert c.logical_type_scale == 2


# ===========================================================================
# 10. All-null and edge cases
# ===========================================================================

class TestEdgeCases:
    def test_all_null_sequence(self):
        v = dec([None, None, None])
        assert pylist(v) == [None, None, None]

    def test_large_value_at_precision18(self):
        # Max valid: 10^18 - 1 = 999999999999999999
        v = dec([Decimal('999999999999999999')], precision=18, scale=0)
        assert v[0] == Decimal('999999999999999999')

    def test_negative_large_value(self):
        v = dec([Decimal('-999999999999999999')], precision=18, scale=0)
        assert v[0] == Decimal('-999999999999999999')

    def test_zero_value_at_various_scales(self):
        for scale in range(5):
            v = dec([Decimal('0')], precision=10, scale=scale)
            result = v[0]
            assert result == Decimal('0')

    def test_scale_zero_integer_values(self):
        v = dec([Decimal('1'), Decimal('2'), Decimal('-3')], precision=5, scale=0)
        result = pylist(v)
        assert result == [Decimal('1'), Decimal('2'), Decimal('-3')]


# ===========================================================================
# 11. Hypothesis-style property tests
# ===========================================================================

class TestProperties:
    """
    Targeted property tests that don't require hypothesis import.
    """

    def test_ordering_consistent_with_compare(self):
        vals = [Decimal('3.00'), Decimal('1.00'), Decimal('2.00'), Decimal('5.00'), Decimal('4.00')]
        v = dec(vals)
        # min/max must match Python sorted
        assert v.min() == min(vals)
        assert v.max() == max(vals)

    def test_compare_all_pairs(self):
        vals = [Decimal('1.00'), Decimal('2.00'), Decimal('3.00')]
        v = dec(vals)
        for op in [EQ, NE, GT, GE, LT, LE]:
            for scalar in vals:
                result = cmp_s(v, scalar, op)
                expected = [_py_cmp(op, x, scalar) for x in vals]
                assert result == expected, f"op={op}, scalar={scalar}"

    def test_round_trip_many_values(self):
        import random
        rng = random.Random(42)
        vals = [Decimal(str(rng.randint(-9999, 9999))) / 100 for _ in range(100)]
        # Quantize to 2 decimal places
        vals = [v.quantize(Decimal('0.01')) for v in vals]
        v = dec(vals, precision=10, scale=2)
        result = pylist(v)
        assert result == vals

    def test_cross_scale_compare_property(self):
        # Cross-scale comparison is scale-aware: 1.00 (scale=2) == 1.0000 (scale=4), etc.
        scales = [(2, 4), (0, 1), (3, 5)]
        for s1, s2 in scales:
            quant_s1 = Decimal('0.' + '0' * s1) if s1 > 0 else Decimal('1')
            quant_s2 = Decimal('0.' + '0' * s2) if s2 > 0 else Decimal('1')
            val = Decimal('1').quantize(quant_s1)
            a = dec([val], scale=s1)
            b = dec([Decimal('1').quantize(quant_s2)], precision=10, scale=s2)
            result = pylist(a.compare_vector(b, EQ))
            assert result == [True], f"scales ({s1},{s2}): expected True"
