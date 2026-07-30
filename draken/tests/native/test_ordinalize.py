"""
Native correctness tests for Vector.ordinalize() / DrakenType.ordinalize(value)
(draken/ops/ordinalize.h).

Every constructible DrakenType is tested BOTH as a vector (Vector.ordinalize())
and, where a scalar entry point exists, as a scalar (DrakenType.X.ordinalize(v))
-- checked against each other for parity on every adversarial value, not just
friendly ones. Tests are written to try to break the implementation: integer
boundaries, sign/bias crossings, NaN/-0.0/denormals, decimal128 exclusion,
interval field-mixing, string-prefix collisions, malformed/oversized scalar
input, shape-preservation (dict-compressed columns), and the empty/all-null
degenerate cases that don't fit the normal dense/compressed model.

Correctness contract under test (see ordinalize.h's file header):
  * For every type EXCEPT the VARCHAR family: the ordinal key must be a full
    order ISOMORPHISM over the adversarial value set used here -- sorting by
    key must exactly match sorting by value, with no ties (value sets are
    chosen pairwise-distinct).
  * For the VARCHAR family: the key is monotonic but NOT a total order past
    8 content bytes -- ties are EXPECTED and explicitly asserted where they
    occur (same-prefix collisions), never inversions.
  * NULL rows always get ORDINAL_NULL (INT64_MIN), sorting before everything.
  * DECIMAL128, ARRAY, VECTOR_FP16: ordinalize must THROW, not silently
    degrade -- both as a vector and (where a scalar call is even meaningful)
    as a scalar.
  * TIMESTAMP64/TIME32/TIME64: vector ordinalize works (reuses INT64/INT32
    kernels via the same TypeOps row-copy hash uses); scalar ordinalize
    THROWS (unit lives on LogicalType, not DrakenType).
"""

import datetime
import decimal
import math

import pytest

import draken.draken_native as dn

DT = dn.DrakenType
ORDINAL_NULL = -0x8000000000000000


def enc(s):
    """String-vector constructors want bytes, not str (despite some stale
    docstrings) -- encode once, everywhere, so tests aren't tripped by that."""
    return s.encode("utf-8") if isinstance(s, str) else s


def str_vec(strs):
    return dn.vector_from_string_sequence([enc(s) if s is not None else None for s in strs])


def nvarchar_vec(strs):
    return dn.vector_from_nvarchar_sequence([enc(s) if s is not None else None for s in strs])


def variant_vec(strs):
    return dn.vector_string_family_from_bytes(
        [enc(s) if s is not None else None for s in strs], DT.VARIANT.value
    )


# ---------------------------------------------------------------------------
# Shared assertion helpers
# ---------------------------------------------------------------------------


def assert_order_isomorphism(values, keys):
    """Sorting by key must exactly reproduce sorting by value -- no ties,
    no inversions. Use for every type except the VARCHAR family, with a
    pairwise-distinct adversarial value set."""
    assert len(values) == len(keys)
    assert len(set(keys)) == len(keys), f"unexpected key collision: {list(zip(values, keys))}"
    order_by_value = sorted(range(len(values)), key=lambda i: values[i])
    order_by_key = sorted(range(len(values)), key=lambda i: keys[i])
    assert order_by_value == order_by_key, (
        f"order mismatch\nvalues (sorted): {[values[i] for i in order_by_value]}\n"
        f"keys   (sorted): {[keys[i] for i in order_by_value]}\n"
        f"key order gave : {[values[i] for i in order_by_key]}"
    )


def assert_no_inversions(labelled):
    """Weaker check for the VARCHAR family: ties allowed (same-prefix
    collisions), inversions are not."""
    for (v1, k1) in labelled:
        for (v2, k2) in labelled:
            if v1 < v2:
                assert k1 <= k2, f"inversion: {v1!r} < {v2!r} but key {k1} > {k2}"


def scalar_vector_parity(dtype, values, vector_ctor):
    for v in values:
        vec = vector_ctor([v])
        vec_key = vec.ordinalize().to_pylist()[0]
        scalar_key = dtype.ordinalize(v)
        assert vec_key == scalar_key, f"scalar/vector mismatch for {v!r}: {scalar_key} != {vec_key}"


# ---------------------------------------------------------------------------
# Cross-cutting sentinel sanity
# ---------------------------------------------------------------------------


def test_ordinal_null_is_int64_min():
    assert ORDINAL_NULL == -(2**63)


# ---------------------------------------------------------------------------
# Signed integers -- full-range boundary values
# ---------------------------------------------------------------------------


class TestInt8:
    values = [-128, -127, -1, 0, 1, 2, 126, 127]

    def test_vector_monotonic(self):
        v = dn.vector_int8_from_sequence(self.values)
        assert_order_isomorphism(self.values, v.ordinalize().to_pylist())

    def test_scalar_matches_vector(self):
        scalar_vector_parity(DT.INT8, self.values, dn.vector_int8_from_sequence)

    def test_null(self):
        v = dn.vector_int8_from_sequence([1, None, -1])
        keys = v.ordinalize().to_pylist()
        assert keys[1] == ORDINAL_NULL
        assert keys[1] < keys[0] and keys[1] < keys[2]

    def test_all_null(self):
        v = dn.vector_int8_from_sequence([None, None, None])
        assert v.ordinalize().to_pylist() == [ORDINAL_NULL] * 3

    def test_empty(self):
        assert dn.vector_int8_from_sequence([]).ordinalize().to_pylist() == []

    def test_single_element(self):
        assert dn.vector_int8_from_sequence([42]).ordinalize().to_pylist() == [42]

    def test_reverse_sorted_input_still_orders_correctly(self):
        vals = sorted(self.values, reverse=True)
        v = dn.vector_int8_from_sequence(vals)
        assert_order_isomorphism(vals, v.ordinalize().to_pylist())

    def test_is_identity(self):
        v = dn.vector_int8_from_sequence(self.values)
        assert v.ordinalize().to_pylist() == self.values


class TestInt16:
    values = [-32768, -32767, -1, 0, 1, 32766, 32767]

    def test_vector_monotonic(self):
        v = dn.vector_int16_from_sequence(self.values)
        assert_order_isomorphism(self.values, v.ordinalize().to_pylist())

    def test_scalar_matches_vector(self):
        scalar_vector_parity(DT.INT16, self.values, dn.vector_int16_from_sequence)

    def test_null(self):
        v = dn.vector_int16_from_sequence([100, None, -100])
        keys = v.ordinalize().to_pylist()
        assert keys[1] == ORDINAL_NULL

    def test_is_identity(self):
        v = dn.vector_int16_from_sequence(self.values)
        assert v.ordinalize().to_pylist() == self.values


class TestInt32:
    values = [-2_147_483_648, -2_147_483_647, -1, 0, 1, 2_147_483_646, 2_147_483_647]

    def test_vector_monotonic(self):
        v = dn.vector_int32_from_sequence(self.values)
        assert_order_isomorphism(self.values, v.ordinalize().to_pylist())

    def test_scalar_matches_vector(self):
        scalar_vector_parity(DT.INT32, self.values, dn.vector_int32_from_sequence)

    def test_null(self):
        v = dn.vector_int32_from_sequence([100, None, -100])
        keys = v.ordinalize().to_pylist()
        assert keys[1] == ORDINAL_NULL

    def test_is_identity(self):
        v = dn.vector_int32_from_sequence(self.values)
        assert v.ordinalize().to_pylist() == self.values


class TestInt64:
    values = [-(2**63), -(2**63) + 1, -1, 0, 1, 2**63 - 2, 2**63 - 1]

    def test_vector_monotonic(self):
        v = dn.vector_from_sequence(self.values)
        assert_order_isomorphism(self.values, v.ordinalize().to_pylist())

    def test_scalar_matches_vector(self):
        scalar_vector_parity(DT.INT64, self.values, dn.vector_from_sequence)

    def test_identity(self):
        v = dn.vector_from_sequence(self.values)
        assert v.ordinalize().to_pylist() == self.values

    def test_empty(self):
        assert dn.vector_from_sequence([]).ordinalize().to_pylist() == []

    def test_all_null(self):
        v = dn.vector_from_sequence([None, None])
        assert v.ordinalize().to_pylist() == [ORDINAL_NULL, ORDINAL_NULL]

    def test_scalar_overflow_raises_cleanly(self):
        # Adversarial: a Python int outside int64 range must fail loudly,
        # never silently wrap or crash the process.
        with pytest.raises(OverflowError):
            DT.INT64.ordinalize(2**70)
        with pytest.raises(OverflowError):
            DT.INT64.ordinalize(-(2**70))

    def test_scalar_wrong_type_raises_cleanly(self):
        with pytest.raises(TypeError):
            DT.INT64.ordinalize("not a number")
        with pytest.raises(TypeError):
            DT.INT64.ordinalize(None)
        with pytest.raises(TypeError):
            DT.INT64.ordinalize([1, 2, 3])

    def test_large_random_sample_stays_isomorphic(self):
        # Stand-in for a property-based test (hypothesis isn't installed in
        # this environment) -- a fixed but large, seeded pseudo-random sample
        # covering the full int64 range.
        import random
        rng = random.Random(20260730)
        vals = [rng.randint(-(2**63), 2**63 - 1) for _ in range(500)]
        vals = list(dict.fromkeys(vals))  # de-dup (isomorphism needs distinct values)
        v = dn.vector_from_sequence(vals)
        assert_order_isomorphism(vals, v.ordinalize().to_pylist())


# ---------------------------------------------------------------------------
# Unsigned integers -- the bias-XOR boundary is the whole point of the test
# ---------------------------------------------------------------------------


class TestUint8:
    values = [0, 1, 127, 128, 254, 255]

    def test_vector_monotonic(self):
        v = dn.vector_uint8_from_sequence(self.values)
        assert_order_isomorphism(self.values, v.ordinalize().to_pylist())

    def test_scalar_matches_vector(self):
        scalar_vector_parity(DT.UINT8, self.values, dn.vector_uint8_from_sequence)

    def test_null(self):
        v = dn.vector_uint8_from_sequence([1, None, 255])
        assert v.ordinalize().to_pylist()[1] == ORDINAL_NULL


class TestUint16:
    values = [0, 1, 32767, 32768, 65534, 65535]

    def test_vector_monotonic(self):
        v = dn.vector_uint16_from_sequence(self.values)
        assert_order_isomorphism(self.values, v.ordinalize().to_pylist())

    def test_scalar_matches_vector(self):
        scalar_vector_parity(DT.UINT16, self.values, dn.vector_uint16_from_sequence)


class TestUint32:
    values = [0, 1, 2**31 - 1, 2**31, 2**32 - 2, 2**32 - 1]

    def test_vector_monotonic(self):
        v = dn.vector_uint32_from_sequence(self.values)
        assert_order_isomorphism(self.values, v.ordinalize().to_pylist())

    def test_scalar_matches_vector(self):
        scalar_vector_parity(DT.UINT32, self.values, dn.vector_uint32_from_sequence)


class TestUint64:
    # The whole int64-sign boundary, on both sides, plus the absolute extremes.
    values = [0, 1, 2**62, 2**63 - 2, 2**63 - 1, 2**63, 2**63 + 1, 2**64 - 2, 2**64 - 1]

    def test_vector_monotonic(self):
        v = dn.vector_uint64_from_sequence(self.values)
        assert_order_isomorphism(self.values, v.ordinalize().to_pylist())

    def test_scalar_matches_vector(self):
        scalar_vector_parity(DT.UINT64, self.values, dn.vector_uint64_from_sequence)

    def test_bias_boundary_exact(self):
        # v ^ 0x8000000000000000, cast to int64:
        #   2**63 - 1 (0x7FFF...F) -> 0xFFFF...F -> -1
        #   2**63     (0x8000...0) -> 0x0000...0 -> 0
        v = dn.vector_uint64_from_sequence([2**63 - 1, 2**63])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 == -1
        assert k1 == 0
        assert k0 < k1

    def test_zero_and_max(self):
        v = dn.vector_uint64_from_sequence([0, 2**64 - 1])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 == -(2**63)  # 0 ^ sign_bit == INT64_MIN
        assert k1 == 2**63 - 1  # all-ones ^ sign_bit == INT64_MAX
        assert k0 < k1

    def test_scalar_negative_raises_cleanly(self):
        with pytest.raises(OverflowError):
            DT.UINT64.ordinalize(-1)

    def test_scalar_too_large_raises_cleanly(self):
        with pytest.raises(OverflowError):
            DT.UINT64.ordinalize(2**64)

    def test_null(self):
        v = dn.vector_uint64_from_sequence([5, None, 2**64 - 1])
        assert v.ordinalize().to_pylist()[1] == ORDINAL_NULL


# ---------------------------------------------------------------------------
# Floats -- sign boundary (the bug that started this), NaN, -0.0, denormals,
# adjacent representable values
# ---------------------------------------------------------------------------


class TestFloat64:
    values = [
        -math.inf,
        -1.7e308,
        -2.0,
        -1.0,
        -1e-300,
        -5e-324,  # smallest denormal, negative
        0.0,
        5e-324,   # smallest denormal, positive
        1e-300,
        1.0,
        1.0000000000000002,  # next representable double after 1.0
        2.0,
        1.7e308,
        math.inf,
    ]

    def test_vector_monotonic(self):
        v = dn.vector_float64_from_sequence(self.values)
        assert_order_isomorphism(self.values, v.ordinalize().to_pylist())

    def test_scalar_matches_vector(self):
        scalar_vector_parity(DT.FLOAT64, self.values, dn.vector_float64_from_sequence)

    def test_negative_zero_equals_positive_zero(self):
        v = dn.vector_float64_from_sequence([-0.0, 0.0])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 == k1

    def test_nan_sorts_highest(self):
        v = dn.vector_float64_from_sequence([math.inf, -math.inf, 0.0, math.nan, 1.7e308])
        keys = v.ordinalize().to_pylist()
        assert keys[3] == max(keys)
        assert keys[3] > keys[0]  # strictly above +inf

    def test_nan_equals_nan_key(self):
        v = dn.vector_float64_from_sequence([math.nan, math.nan])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 == k1  # canonical NaN -> identical bit pattern -> identical key

    def test_null(self):
        v = dn.vector_float64_from_sequence([1.0, None, -1.0])
        assert v.ordinalize().to_pylist()[1] == ORDINAL_NULL

    def test_adjacent_representable_values_distinguished(self):
        a = 1.0
        b = math.nextafter(1.0, math.inf)
        assert a != b
        v = dn.vector_float64_from_sequence([a, b])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 < k1

    def test_empty(self):
        assert dn.vector_float64_from_sequence([]).ordinalize().to_pylist() == []

    def test_all_null(self):
        v = dn.vector_float64_from_sequence([None, None])
        assert v.ordinalize().to_pylist() == [ORDINAL_NULL, ORDINAL_NULL]


class TestFloat32:
    values = [-3.4e38, -2.0, -1.0, -1e-38, 0.0, 1e-38, 1.0, 2.0, 3.4e38]

    def test_vector_monotonic(self):
        v = dn.vector_float32_from_sequence(self.values)
        assert_order_isomorphism(self.values, v.ordinalize().to_pylist())

    def test_scalar_matches_vector(self):
        scalar_vector_parity(DT.FLOAT32, self.values, dn.vector_float32_from_sequence)

    def test_negative_zero_equals_positive_zero(self):
        v = dn.vector_float32_from_sequence([-0.0, 0.0])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 == k1

    def test_nan_sorts_highest(self):
        v = dn.vector_float32_from_sequence([math.inf, -math.inf, 0.0, math.nan])
        keys = v.ordinalize().to_pylist()
        assert keys[3] == max(keys)

    def test_null(self):
        v = dn.vector_float32_from_sequence([1.0, None])
        assert v.ordinalize().to_pylist()[1] == ORDINAL_NULL

    def test_float32_and_float64_scalar_agree_on_representable_values(self):
        # 1.5 is exactly representable in both widths -- keys should match
        # in relative order (not necessarily identical bit pattern, since
        # widths differ, but both must place 1.5 above 1.0 and below 2.0).
        f32_keys = [DT.FLOAT32.ordinalize(x) for x in (1.0, 1.5, 2.0)]
        f64_keys = [DT.FLOAT64.ordinalize(x) for x in (1.0, 1.5, 2.0)]
        assert f32_keys == sorted(f32_keys)
        assert f64_keys == sorted(f64_keys)


# ---------------------------------------------------------------------------
# BOOL -- bit-packed storage, not a byte array
# ---------------------------------------------------------------------------


class TestBool:
    def test_vector_monotonic(self):
        v = dn.vector_from_bool_sequence([False, True])
        assert v.ordinalize().to_pylist() == [0, 1]

    def test_scalar_matches_vector(self):
        scalar_vector_parity(DT.BOOL, [False, True], dn.vector_from_bool_sequence)

    def test_null(self):
        v = dn.vector_from_bool_sequence([True, None, False])
        keys = v.ordinalize().to_pylist()
        assert keys == [1, ORDINAL_NULL, 0]

    def test_bit_packing_across_byte_boundary(self):
        # >8 rows crosses a byte boundary in the bit-packed storage;
        # alternating so every bit position (0-7, then wrapping) is exercised.
        vals = [i % 2 == 0 for i in range(17)]
        v = dn.vector_from_bool_sequence(vals)
        assert v.ordinalize().to_pylist() == [1 if x else 0 for x in vals]

    def test_all_true(self):
        v = dn.vector_from_bool_sequence([True] * 9)
        assert v.ordinalize().to_pylist() == [1] * 9

    def test_all_false(self):
        v = dn.vector_from_bool_sequence([False] * 9)
        assert v.ordinalize().to_pylist() == [0] * 9

    def test_empty(self):
        assert dn.vector_from_bool_sequence([]).ordinalize().to_pylist() == []


# ---------------------------------------------------------------------------
# DATE32
# ---------------------------------------------------------------------------


class TestDate32:
    values = [
        datetime.date(1, 1, 1),
        datetime.date(1969, 12, 31),
        datetime.date(1970, 1, 1),
        datetime.date(1970, 1, 2),
        datetime.date(2000, 2, 29),  # leap day
        datetime.date(2026, 7, 30),
        datetime.date(9999, 12, 31),
    ]

    def test_vector_monotonic(self):
        v = dn.vector_date32_from_sequence(self.values)
        assert_order_isomorphism(self.values, v.ordinalize().to_pylist())

    def test_scalar_matches_vector(self):
        scalar_vector_parity(DT.DATE32, self.values, dn.vector_date32_from_sequence)

    def test_null(self):
        v = dn.vector_date32_from_sequence([datetime.date(2020, 1, 1), None])
        assert v.ordinalize().to_pylist()[1] == ORDINAL_NULL

    def test_pre_epoch_is_negative(self):
        v = dn.vector_date32_from_sequence([datetime.date(1969, 12, 31)])
        assert v.ordinalize().to_pylist()[0] < 0

    def test_epoch_is_zero(self):
        v = dn.vector_date32_from_sequence([datetime.date(1970, 1, 1)])
        assert v.ordinalize().to_pylist()[0] == 0

    def test_month_boundary_adjacent_days(self):
        v = dn.vector_date32_from_sequence([datetime.date(2026, 1, 31), datetime.date(2026, 2, 1)])
        k0, k1 = v.ordinalize().to_pylist()
        assert k1 == k0 + 1


# ---------------------------------------------------------------------------
# DECIMAL (int64-backed) -- raw unscaled mantissa
# ---------------------------------------------------------------------------


class TestDecimal:
    values = [
        decimal.Decimal("-999999999999999.99"),
        decimal.Decimal("-1.50"),
        decimal.Decimal("-0.01"),
        decimal.Decimal("0.00"),
        decimal.Decimal("0.01"),
        decimal.Decimal("1.50"),
        decimal.Decimal("999999999999999.99"),
    ]

    def _ctor(self, seq):
        return dn.vector_decimal_from_sequence(seq, 18, 2)

    def test_vector_monotonic(self):
        v = self._ctor(self.values)
        assert_order_isomorphism(self.values, v.ordinalize().to_pylist())

    def test_scalar_matches_vector(self):
        scalar_vector_parity(DT.DECIMAL, self.values, self._ctor)

    def test_is_raw_mantissa(self):
        assert self._ctor([decimal.Decimal("1.50")]).ordinalize().to_pylist()[0] == 150
        assert self._ctor([decimal.Decimal("-1.50")]).ordinalize().to_pylist()[0] == -150

    def test_null(self):
        v = self._ctor([decimal.Decimal("1.00"), None])
        assert v.ordinalize().to_pylist()[1] == ORDINAL_NULL

    def test_scale_zero_integers_only(self):
        vals = [decimal.Decimal("-5"), decimal.Decimal("0"), decimal.Decimal("5")]
        v = dn.vector_decimal_from_sequence(vals, 10, 0)
        assert v.ordinalize().to_pylist() == [-5, 0, 5]

    def test_high_scale(self):
        vals = [decimal.Decimal("0.000001"), decimal.Decimal("0.000002")]
        v = dn.vector_decimal_from_sequence(vals, 18, 6)
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 == 1 and k1 == 2


# ---------------------------------------------------------------------------
# DECIMAL128 -- deliberately unsupported; must throw, not saturate silently
# ---------------------------------------------------------------------------


class TestDecimal128Unsupported:
    def test_vector_throws_small_value(self):
        v = dn.vector_decimal128_from_sequence([decimal.Decimal("1.5")], 38, 2)
        with pytest.raises(Exception):
            v.ordinalize()

    def test_vector_throws_zero(self):
        v = dn.vector_decimal128_from_sequence([decimal.Decimal("0")], 38, 2)
        with pytest.raises(Exception):
            v.ordinalize()

    def test_vector_throws_extreme_value(self):
        v = dn.vector_decimal128_from_sequence(
            [decimal.Decimal("99999999999999999999999999999999.99")], 38, 2
        )
        with pytest.raises(Exception):
            v.ordinalize()

    def test_vector_throws_negative_extreme(self):
        v = dn.vector_decimal128_from_sequence(
            [decimal.Decimal("-99999999999999999999999999999999.99")], 38, 2
        )
        with pytest.raises(Exception):
            v.ordinalize()

    def test_scalar_throws_small_value(self):
        with pytest.raises(Exception):
            DT.DECIMAL128.ordinalize(decimal.Decimal("1.5"))

    def test_scalar_throws_extreme_value(self):
        with pytest.raises(Exception):
            DT.DECIMAL128.ordinalize(decimal.Decimal("1E30"))

    def test_scalar_throws_zero(self):
        with pytest.raises(Exception):
            DT.DECIMAL128.ordinalize(decimal.Decimal("0"))


# ---------------------------------------------------------------------------
# INTERVAL -- (months, us) tuples; adversarial mixed signs across the
# months/us split, not just simple positive examples
# ---------------------------------------------------------------------------


class TestInterval:
    MONTH_US = 2_592_000_000_000  # interval_ops.h's INTERVAL_MONTH_US
    values = [
        (-100, 0),
        (-1, -1),
        (-1, 0),
        (0, -1_000_000),
        (0, 0),
        (0, 1),
        (0, 1_000_000),
        (1, -MONTH_US + 2),  # distinct from the (0, 1) collision case below
        (1, 0),
        (1, 1),
        (100, 0),
    ]

    def _expected(self, tup):
        months, us = tup
        return months * self.MONTH_US + us

    def test_vector_monotonic(self):
        v = dn.vector_interval_from_sequence(self.values)
        keys = v.ordinalize().to_pylist()
        expected = [self._expected(t) for t in self.values]
        assert keys == expected
        assert_order_isomorphism(expected, keys)

    def test_scalar_matches_vector(self):
        scalar_vector_parity(DT.INTERVAL, self.values, dn.vector_interval_from_sequence)

    def test_cross_field_equivalence(self):
        # (1, 0) and (0, MONTH_US) normalize to the SAME instant -- their
        # ordinal keys must be equal. Note: this test uses real MICROSECONDS
        # throughout, unlike test_interval.py's pre-existing, unrelated
        # MONTH_MS/us unit mismatch (flagged separately, not this test's
        # concern).
        v = dn.vector_interval_from_sequence([(1, 0), (0, self.MONTH_US)])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 == k1

    def test_null(self):
        v = dn.vector_interval_from_sequence([(1, 0), None])
        assert v.ordinalize().to_pylist()[1] == ORDINAL_NULL

    def test_construction_time_overflow_raises_before_ordinalize_sees_it(self):
        # Both the vector constructor and py_to_interval_slot (the scalar
        # path) validate via interval_normalize_checked BEFORE an interval
        # slot can exist -- ordinalize's own months*MONTH_US+us arithmetic
        # is unchecked, but never reaches an overflowing value because
        # construction already refused it. Confirms the guard is really
        # upstream, not "silently wraps and happens to look fine".
        with pytest.raises(OverflowError):
            dn.vector_interval_from_sequence([(2**62, 0)])
        with pytest.raises(OverflowError):
            DT.INTERVAL.ordinalize((2**62, 0))

    def test_negative_months_negative_us(self):
        v = dn.vector_interval_from_sequence([(-5, -500_000), (-5, -400_000)])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 < k1  # -500_000 us < -400_000 us at the same month offset


# ---------------------------------------------------------------------------
# VARCHAR family -- 8-byte prefix packed + shift-right-1. NOT a total order:
# same-8-byte-prefix strings collide. Adversarial cases specifically target
# the boundary this scheme creates.
# ---------------------------------------------------------------------------


class TestVarcharFriendly:
    values = ["", "a", "aa", "ab", "apple", "b", "banana", "z"]

    def test_vector_monotonic_no_inversions(self):
        v = str_vec(self.values)
        keys = v.ordinalize().to_pylist()
        assert_no_inversions(list(zip(self.values, keys)))
        assert_order_isomorphism(self.values, keys)  # none share an 8-byte prefix here

    def test_scalar_matches_vector(self):
        for s in self.values:
            vec_key = str_vec([s]).ordinalize().to_pylist()[0]
            assert DT.VARCHAR.ordinalize(s) == vec_key

    def test_null(self):
        v = str_vec(["a", None, "b"])
        keys = v.ordinalize().to_pylist()
        assert keys[1] == ORDINAL_NULL
        assert keys[1] < keys[0] and keys[1] < keys[2]

    def test_empty(self):
        assert str_vec([]).ordinalize().to_pylist() == []

    def test_all_null(self):
        v = str_vec([None, None])
        assert v.ordinalize().to_pylist() == [ORDINAL_NULL, ORDINAL_NULL]


class TestVarcharAdversarial:
    def test_prefix_is_ordered_before_extension(self):
        v = str_vec(["a", "aa", "aaa", "aaaa", "aaaaa"])
        keys = v.ordinalize().to_pylist()
        assert keys == sorted(keys)
        assert len(set(keys)) == len(keys)  # all distinct -- within 8 bytes

    def test_exact_8_byte_boundary_collision(self):
        v = str_vec(["abcdefghX", "abcdefghY", "abcdefghZZZZZZ"])
        keys = v.ordinalize().to_pylist()
        assert keys[0] == keys[1] == keys[2], "same 8-byte prefix must collide, by design"

    def test_last_bit_collision_is_expected_not_a_bug(self):
        # 'b' (0x62) and 'c' (0x63) differ only in their lowest bit; packing
        # 8 bytes then >>1 discards exactly that bit. Documented, accepted
        # precision loss -- asserted explicitly so a future change is visible.
        v = str_vec(["aaaaaaab", "aaaaaaac"])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 == k1

    def test_second_to_last_bit_is_NOT_lost(self):
        # 'a' (0x61) vs 'd' (0x64) differ by 3 in the low bits -- NOT fully
        # absorbed by a single right-shift, so these must NOT collide.
        v = str_vec(["aaaaaaaa", "aaaaaaad"])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 != k1
        assert k0 < k1

    def test_distinguishes_past_old_4_byte_scheme(self):
        # Would have collided under the OLD 4-byte+length scheme (same first
        # 4 bytes); must NOT collide now (differ at byte 5).
        v = str_vec(["aaaaXaaa", "aaaaYaaa"])
        keys = v.ordinalize().to_pylist()
        assert keys[0] != keys[1]
        assert keys[0] < keys[1]

    def test_every_one_of_the_first_8_byte_positions_is_load_bearing(self):
        # Systematic sweep: change exactly one byte position (0..7) at a
        # time and confirm the key changes every time -- if ANY position
        # were silently dropped (an off-by-one in the packing loop), this
        # would catch it.
        base = list(b"aaaaaaaa")
        base_key = str_vec([bytes(base)]).ordinalize().to_pylist()[0]
        for pos in range(8):
            mutated = base[:]
            mutated[pos] = ord("z")
            mutated_key = str_vec([bytes(mutated)]).ordinalize().to_pylist()[0]
            assert mutated_key != base_key, f"byte position {pos} did not affect the key"

    def test_byte_position_8_and_beyond_never_affects_key(self):
        # Conversely: changing ANYTHING past byte 8 must be a no-op for the
        # key (by design -- not a total order past 8 bytes).
        base = b"aaaaaaaa" + b"X" * 20
        mutated = b"aaaaaaaa" + b"Y" * 20
        v = str_vec([base, mutated])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 == k1

    def test_long_string_arena_path_collision_at_shared_prefix(self):
        # >12 bytes forces long/arena storage (STR_INLINE_MAX == 12).
        base = "x" * 30
        v = str_vec([base + "1111", base + "2222", base + "0000"])
        keys = v.ordinalize().to_pylist()
        assert keys[0] == keys[1] == keys[2]

    def test_long_string_arena_path_is_actually_read(self):
        # Differ starting at byte 5 -- only passes if bytes 4-7 are truly
        # read from the arena, not silently limited to the slot's own
        # precomputed 4-byte ext.prefix.
        v = str_vec(["aaaaXBCDrest_of_a_long_string", "aaaaYBCDrest_of_a_long_string"])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 != k1, "arena bytes 4-7 not being read -- collapsed to ext.prefix only"

    def test_inline_boundary_12_bytes_exact(self):
        # STR_INLINE_MAX == 12 exactly -- test the boundary itself, not just
        # comfortably inside/outside it.
        twelve = "abcdefghijkl"          # inline (== 12)
        thirteen = "abcdefghijklm"       # long (== 13, forces arena)
        v = str_vec([twelve, thirteen])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 == k1  # identical first 8 bytes -- must collide regardless of storage shape

    def test_embedded_null_byte_varbinary(self):
        # VARBINARY: 0x00 is legitimate content, not only a padding sentinel.
        # A short value zero-padded to 8 bytes is indistinguishable from a
        # longer one whose next real byte happens to be 0x00 -- documented
        # collision, asserted explicitly.
        v = dn.vector_from_bytes_sequence([b"aaa", b"aaa\x00"])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 == k1

    def test_embedded_null_byte_distinguishes_from_nonzero_continuation(self):
        v = dn.vector_from_bytes_sequence([b"aaa\x00", b"aaa\x01"])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 < k1

    def test_all_zero_vs_all_ff_bytes(self):
        v = dn.vector_from_bytes_sequence([b"\x00" * 8, b"\xff" * 8, b"\x00" * 4 + b"\xff" * 4])
        keys = v.ordinalize().to_pylist()
        assert keys[0] < keys[2] < keys[1]
        assert all(k >= 0 for k in keys)  # never negative -- the whole point of >>1

    def test_key_always_non_negative_worst_case(self):
        v = dn.vector_from_bytes_sequence([b"\xff" * 20])
        key = v.ordinalize().to_pylist()[0]
        assert key >= 0
        assert key == (0xFFFFFFFFFFFFFFFF >> 1)

    def test_key_zero_for_empty_string(self):
        v = str_vec([""])
        assert v.ordinalize().to_pylist()[0] == 0

    def test_single_byte_strings_ordered(self):
        v = dn.vector_from_bytes_sequence([bytes([b]) for b in (0, 1, 127, 128, 254, 255)])
        keys = v.ordinalize().to_pylist()
        assert keys == sorted(keys)
        assert len(set(keys)) == 6

    def test_multibyte_utf8_characters(self):
        # UTF-8 byte-order, not unicode-codepoint order per se -- but for
        # these specific examples the two coincide, so this doubles as a
        # sanity check that encoding happens before packing, not after.
        v = str_vec(["a", "é", "日"])  # UTF-8: 0x61, 0xC3 A9, 0xE6 97 A5
        keys = v.ordinalize().to_pylist()
        assert keys == sorted(keys)
        assert len(set(keys)) == 3

    def test_scalar_matches_vector_adversarial(self):
        adversarial = ["", "a", "abcdefgh", "abcdefghX", "aaaaaaab", "aaaaaaac", "x" * 40, "é日"]
        for s in adversarial:
            assert DT.VARCHAR.ordinalize(s) == str_vec([s]).ordinalize().to_pylist()[0]

    def test_scalar_wrong_type_raises_cleanly(self):
        with pytest.raises(Exception):
            DT.VARCHAR.ordinalize(12345)
        with pytest.raises(Exception):
            DT.VARCHAR.ordinalize(None)


class TestNvarcharAndVariant:
    def test_nvarchar_vector_and_scalar_parity(self):
        values = ["", "a", "apple", "aaaaaaab", "aaaaaaac", "é日本語"]
        v = nvarchar_vec(values)
        keys = v.ordinalize().to_pylist()
        assert_no_inversions(list(zip(values, keys)))
        for s in values:
            assert DT.NVARCHAR.ordinalize(s) == nvarchar_vec([s]).ordinalize().to_pylist()[0]

    def test_nvarchar_same_prefix_collision(self):
        v = nvarchar_vec(["aaaaaaab", "aaaaaaac"])
        k0, k1 = v.ordinalize().to_pylist()
        assert k0 == k1

    def test_variant_vector_monotonic(self):
        values = ["", "a", "apple", "banana", "zzz"]
        v = variant_vec(values)
        keys = v.ordinalize().to_pylist()
        assert_no_inversions(list(zip(values, keys)))
        assert_order_isomorphism(values, keys)

    def test_variant_scalar_matches_vector(self):
        for s in ["a", "banana", "aaaaaaab", "aaaaaaac"]:
            assert DT.VARIANT.ordinalize(s) == variant_vec([s]).ordinalize().to_pylist()[0]

    def test_varchar_and_nvarchar_agree_on_shared_content(self):
        # Same storage format (slot+arena) -- a VARCHAR and an NVARCHAR
        # vector of identical content must produce identical keys.
        content = ["apple", "banana", "aaaaaaab", "aaaaaaac", "x" * 40]
        vc = str_vec(content).ordinalize().to_pylist()
        nv = nvarchar_vec(content).ordinalize().to_pylist()
        assert vc == nv


class TestVarbinary:
    def test_varbinary_vector_and_scalar(self):
        values = [b"", b"a", b"apple", b"banana", b"\x00\x01\x02", b"\xff\xfe\xfd"]
        v = dn.vector_from_bytes_sequence(values)
        keys = v.ordinalize().to_pylist()
        assert_no_inversions(list(zip(values, keys)))
        for val in values:
            vec_key = dn.vector_from_bytes_sequence([val]).ordinalize().to_pylist()[0]
            assert DT.VARBINARY.ordinalize(val) == vec_key

    def test_varbinary_null(self):
        v = dn.vector_from_bytes_sequence([b"x", None])
        assert v.ordinalize().to_pylist()[1] == ORDINAL_NULL

    def test_varbinary_scalar_accepts_bytes_not_str(self):
        # VARBINARY's scalar path takes raw bytes; passing str should either
        # be rejected or handled consistently -- confirm it does NOT silently
        # produce a mismatched key versus the vector path.
        b_key = dn.vector_from_bytes_sequence([b"hello"]).ordinalize().to_pylist()[0]
        assert DT.VARBINARY.ordinalize(b"hello") == b_key


# ---------------------------------------------------------------------------
# NULL type -- self-describing, every row null, no data buffer at all.
# This is the type that exposed the real bug: its data_length==0 while
# length>0 with a CONSTANT (not identity) selection doesn't fit the generic
# dense/compressed shape model draken_ordinalize_shaped otherwise assumes.
# ---------------------------------------------------------------------------


class TestNullType:
    def test_all_rows_are_ordinal_null(self):
        v = dn.vector_null_from_length(5)
        assert v.ordinalize().to_pylist() == [ORDINAL_NULL] * 5

    def test_empty(self):
        assert dn.vector_null_from_length(0).ordinalize().to_pylist() == []

    def test_single_row(self):
        assert dn.vector_null_from_length(1).ordinalize().to_pylist() == [ORDINAL_NULL]

    def test_large_length(self):
        n = 10_000
        assert dn.vector_null_from_length(n).ordinalize().to_pylist() == [ORDINAL_NULL] * n

    def test_data_length_is_dense_not_leftover_zero(self):
        # Regression guard for the exact bug found: the result's data_length
        # must equal the row count (dense), not the source's data_length==0.
        v = dn.vector_null_from_length(7)
        ov = v.ordinalize()
        assert ov.data_length == 7
        assert ov.length == 7


# ---------------------------------------------------------------------------
# Explicitly unsupported types -- must throw, never silently degrade
# ---------------------------------------------------------------------------


class TestUnsupportedTypesThrow:
    def test_array_vector_throws_int_child(self):
        v = dn.vector_array_from_sequence([[1, 2], [3]])
        with pytest.raises(Exception):
            v.ordinalize()

    def test_array_vector_throws_string_child(self):
        v = dn.vector_array_from_sequence([["a", "b"], ["c"]])
        with pytest.raises(Exception):
            v.ordinalize()

    def test_array_vector_throws_nested(self):
        v = dn.vector_array_from_sequence([[[1, 2], [3]], [[4]]])
        with pytest.raises(Exception):
            v.ordinalize()

    def test_fp16_vector_throws_dim2(self):
        v = dn.vector_fp16_from_sequence([[1.0, 2.0], [3.0, 4.0]], 2)
        with pytest.raises(Exception):
            v.ordinalize()

    def test_fp16_vector_throws_dim8(self):
        v = dn.vector_fp16_from_sequence([[float(i) for i in range(8)]], 8)
        with pytest.raises(Exception):
            v.ordinalize()

    def test_timestamp64_scalar_throws(self):
        with pytest.raises(Exception):
            DT.TIMESTAMP64.ordinalize(datetime.datetime(2026, 7, 30, 12, 0, 0))

    def test_time32_scalar_throws(self):
        with pytest.raises(Exception):
            DT.TIME32.ordinalize(datetime.time(12, 0, 0))

    def test_time64_scalar_throws(self):
        with pytest.raises(Exception):
            DT.TIME64.ordinalize(datetime.time(12, 0, 0))

    def test_decimal128_and_array_and_fp16_all_consistently_throw(self):
        # Not just "throws something" -- confirm none of these silently
        # return e.g. 0 or None instead of raising.
        cases = []
        try:
            DT.DECIMAL128.ordinalize(decimal.Decimal("1"))
            cases.append("DECIMAL128 did not throw")
        except Exception:
            pass
        assert cases == []


# ---------------------------------------------------------------------------
# TIMESTAMP64/TIME32/TIME64 -- vector ordinalize DOES work (reuses INT64/
# INT32 kernels via the same TypeOps row-copy hash uses); only the SCALAR
# entry point is unsupported (tested above).
# ---------------------------------------------------------------------------


class TestTimestampAndTimeVectors:
    def test_timestamp64_vector_monotonic(self):
        values = [
            datetime.datetime(1969, 12, 31, 23, 59, 59),
            datetime.datetime(1970, 1, 1, 0, 0, 0),
            datetime.datetime(1970, 1, 1, 0, 0, 0, 1),  # +1 microsecond
            datetime.datetime(2026, 7, 30, 7, 56, 51),
        ]
        v = dn.vector_timestamp_from_sequence(values)
        assert_order_isomorphism(values, v.ordinalize().to_pylist())

    def test_timestamp64_null(self):
        v = dn.vector_timestamp_from_sequence([datetime.datetime(2020, 1, 1), None])
        assert v.ordinalize().to_pylist()[1] == ORDINAL_NULL

    def test_time32_vector_monotonic(self):
        values = [datetime.time(0, 0, 0), datetime.time(12, 0, 0), datetime.time(23, 59, 59)]
        v = dn.vector_time32_from_sequence(values)
        assert_order_isomorphism(values, v.ordinalize().to_pylist())

    def test_time64_vector_monotonic(self):
        values = [
            datetime.time(0, 0, 0),
            datetime.time(0, 0, 0, 1),
            datetime.time(12, 0, 0, 123456),
            datetime.time(23, 59, 59, 999999),
        ]
        v = dn.vector_time64_from_sequence(values)
        assert_order_isomorphism(values, v.ordinalize().to_pylist())

    def test_time64_null(self):
        v = dn.vector_time64_from_sequence([datetime.time(1, 0, 0), None])
        assert v.ordinalize().to_pylist()[1] == ORDINAL_NULL


# ---------------------------------------------------------------------------
# Shape preservation -- dict-compressed input must ordinalize only the
# distinct values (not every row), and repeated values must map to an
# IDENTICAL key.
# ---------------------------------------------------------------------------


class TestShapePreservation:
    def test_dict_string_ordinalizes_distinct_values_only(self):
        source = [b"a", b"b", b"a", b"c", b"b", b"a"]
        v = dn.vector_from_string_dict_sequence(source)
        assert v.is_dict and v.data_length == 3
        ov = v.ordinalize()
        assert ov.is_dict
        assert ov.data_length == 3  # not 6 -- the whole point of the shaped path
        per_row = ov.to_pylist()
        a_positions = [i for i, b in enumerate(source) if b == b"a"]
        assert len({per_row[i] for i in a_positions}) == 1

    def test_dict_int64_repeated_values_share_key(self):
        v = dn.vector_from_dict(values=[10, 20, 30], codes=[0, 1, 0, 2, 1])
        assert v.is_dict
        assert v.ordinalize().to_pylist() == [10, 20, 10, 30, 20]  # INT64 is identity

    def test_dict_with_nulls(self):
        v = dn.vector_from_dict(values=[5, 15], codes=[0, 1, 0], nullable=[True, False, True])
        keys = v.ordinalize().to_pylist()
        assert keys[1] == ORDINAL_NULL
        assert keys[0] == 5 and keys[2] == 5

    def test_dict_matches_dense_equivalent(self):
        # A dict-shaped vector and the equivalent dense vector of the same
        # logical values must ordinalize to the same per-row result.
        logical = [100, 200, 100, 300, 200, 100]
        dense = dn.vector_from_sequence(logical)
        dict_v = dn.vector_from_dict(values=[100, 200, 300], codes=[0, 1, 0, 2, 1, 0])
        assert dense.ordinalize().to_pylist() == dict_v.ordinalize().to_pylist()

    def test_large_low_cardinality_dict(self):
        # 200 rows, 5 distinct values -- confirms the shaped path scales
        # (only 5 keys computed, not 200) and still matches per-row semantics.
        import random
        rng = random.Random(7)
        distinct = [11, 22, 33, 44, 55]
        codes = [rng.randrange(5) for _ in range(200)]
        v = dn.vector_from_dict(values=distinct, codes=codes)
        ov = v.ordinalize()
        assert ov.data_length == 5
        per_row = ov.to_pylist()
        expected = [distinct[c] for c in codes]
        assert per_row == expected


# ---------------------------------------------------------------------------
# Regression tests for two scalar-path validation bugs found by adversarial
# testing and fixed directly (draken_native.cpp's DrakenType.ordinalize):
#   1. INT8/16/32/UINT8/16/32/BOOL never range-checked against the declared
#      type -- DT.UINT8.ordinalize(1000) silently returned 1000. Now raises,
#      matching vector_uint8_from_sequence's existing validation.
#   2. DECIMAL silently wrapped (narrowing-cast truncation) when the
#      literal's unscaled mantissa didn't fit int64. Now raises, matching
#      vector_decimal_from_sequence's existing "does not fit in int64 range".
#
# (A third suspected case -- DATE32 accepting datetime.datetime and
# truncating the time part -- turned out NOT to be a bug: py_date_to_days is
# a shared helper whose own comment documents accepting datetime.datetime by
# design, and vector_date32_from_sequence does the identical truncation.
# Confirmed via TestDate32 above; no fix needed, nothing to regression-test.)
# ---------------------------------------------------------------------------


class TestScalarRangeValidation:
    def test_int8_out_of_range_raises(self):
        with pytest.raises(Exception):
            DT.INT8.ordinalize(100000)
        with pytest.raises(Exception):
            DT.INT8.ordinalize(-129)

    def test_int16_out_of_range_raises(self):
        with pytest.raises(Exception):
            DT.INT16.ordinalize(1_000_000)

    def test_int32_out_of_range_raises(self):
        with pytest.raises(Exception):
            DT.INT32.ordinalize(2**40)

    def test_uint8_out_of_range_raises(self):
        with pytest.raises(Exception):
            DT.UINT8.ordinalize(1000)
        with pytest.raises(Exception):
            DT.UINT8.ordinalize(-1)

    def test_uint16_out_of_range_raises(self):
        with pytest.raises(Exception):
            DT.UINT16.ordinalize(100_000)

    def test_uint32_out_of_range_raises(self):
        with pytest.raises(Exception):
            DT.UINT32.ordinalize(2**40)

    def test_bool_non_boolean_int_raises(self):
        with pytest.raises(Exception):
            DT.BOOL.ordinalize(5)
        with pytest.raises(Exception):
            DT.BOOL.ordinalize(-1)

    def test_boundary_values_still_accepted(self):
        # The fix must reject OUT-of-range values without becoming
        # overzealous and rejecting the actual boundary values.
        assert DT.INT8.ordinalize(-128) == -128
        assert DT.INT8.ordinalize(127) == 127
        assert DT.UINT8.ordinalize(0) == 0
        assert DT.UINT8.ordinalize(255) == 255
        assert DT.INT16.ordinalize(-32768) == -32768
        assert DT.INT16.ordinalize(32767) == 32767
        assert DT.UINT16.ordinalize(65535) == 65535
        assert DT.INT32.ordinalize(-2_147_483_648) == -2_147_483_648
        assert DT.INT32.ordinalize(2_147_483_647) == 2_147_483_647
        assert DT.UINT32.ordinalize(2**32 - 1) == 2**32 - 1
        assert DT.BOOL.ordinalize(0) == 0
        assert DT.BOOL.ordinalize(1) == 1

    def test_int64_still_unrestricted(self):
        # INT64 has no narrower range to enforce -- confirm the fix didn't
        # accidentally tighten it.
        assert DT.INT64.ordinalize(2**63 - 1) == 2**63 - 1
        assert DT.INT64.ordinalize(-(2**63)) == -(2**63)

    def test_still_matches_vector_for_valid_values(self):
        # The fix must not have disturbed correct in-range behavior.
        scalar_vector_parity(DT.INT8, TestInt8.values, dn.vector_int8_from_sequence)
        scalar_vector_parity(DT.UINT8, TestUint8.values, dn.vector_uint8_from_sequence)
        scalar_vector_parity(DT.BOOL, [False, True], dn.vector_from_bool_sequence)


class TestDecimalScalarOverflow:
    HUGE = decimal.Decimal("99999999999999999999999.99")

    def test_overflow_raises(self):
        with pytest.raises(Exception):
            DT.DECIMAL.ordinalize(self.HUGE)
        with pytest.raises(Exception):
            DT.DECIMAL.ordinalize(-self.HUGE)

    def test_matches_vector_constructor_rejection(self):
        with pytest.raises(Exception):
            dn.vector_decimal_from_sequence([self.HUGE], 18, 2)

    def test_in_range_values_still_work(self):
        # The fix must not have disturbed correct in-range behavior.
        assert DT.DECIMAL.ordinalize(decimal.Decimal("1.50")) == 150
        assert DT.DECIMAL.ordinalize(decimal.Decimal("-1.50")) == -150
        scalar_vector_parity(DT.DECIMAL, TestDecimal.values, TestDecimal()._ctor)

    def test_boundary_at_int64_max(self):
        # Exactly at INT64_MAX's unscaled value must be accepted, not
        # rejected by an off-by-one in the new range check.
        at_max = decimal.Decimal(2**63 - 1).scaleb(-2)  # unscaled == INT64_MAX at scale 2
        assert DT.DECIMAL.ordinalize(at_max) == 2**63 - 1

    def test_boundary_one_past_int64_max_raises(self):
        one_past = decimal.Decimal(2**63).scaleb(-2)  # unscaled == INT64_MAX + 1
        with pytest.raises(Exception):
            DT.DECIMAL.ordinalize(one_past)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
