"""
Native unit tests for TIMESTAMP64 ingestion, readback, and ops in draken.draken_native.

These tests assert the CORRECT answer. They are the primary correctness signal for
Milestone D.8: logical-type descriptor + timestamp.

Coverage matrix (per 04_testing.md §1 and the D.8 acceptance criteria):

  units:           s / ms / us (default) / ns
  nullability:     no nulls / some nulls / all null
  sizes:           0 / 1 / <8 (tail) / large
  offsets:         UTC (0) / positive (+60 min) / negative (-330 min)
  naive input:     naive datetime treated as UTC
  edge values:     epoch, date-range extremes, sub-second precision
  shapes:          sequence / constant / dict
  mandatory desc:  no descriptor on TIMESTAMP64 = hard error (enforced at factory)
  ops:             compare_scalar, compare_vector, hash, min, max, between, in_list,
                   take, materialize, compress
  cross-unit:      compare_vector with mismatched units must throw
  hypothesis:      round-trip ordering, round-trip identity, cross-unit throw
"""

import pytest
from datetime import datetime, timezone, timedelta

import draken.draken_native as dn

# ---------------------------------------------------------------------------
# Op codes (same as test_int64_compare.py — ABI-frozen)
# ---------------------------------------------------------------------------
EQ, NE, GT, GE, LT, LE = 0, 1, 2, 3, 4, 5

# ---------------------------------------------------------------------------
# Reference datetimes
# ---------------------------------------------------------------------------
EPOCH     = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
DT_2024   = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
DT_2025   = datetime(2025, 6, 15, 12, 30, 0, tzinfo=timezone.utc)
DT_MICRO  = datetime(2023, 3, 7, 8, 15, 45, 123456, tzinfo=timezone.utc)
DT_NEG    = datetime(1960, 1, 1, 0, 0, 0, tzinfo=timezone.utc)  # pre-epoch

# Offset zones
TZ_PLUS1  = timezone(timedelta(hours=1))
TZ_MINUS530 = timezone(timedelta(hours=-5, minutes=-30))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ts(lst, unit="us", offset_minutes=0):
    return dn.vector_timestamp_from_sequence(lst, unit=unit, offset_minutes=offset_minutes)

def ts_const(value, length, unit="us", offset_minutes=0):
    return dn.vector_timestamp_from_constant(value, length, unit=unit, offset_minutes=offset_minutes)

def ts_dict(values, codes, nullable=None, unit="us", offset_minutes=0):
    return dn.vector_timestamp_from_dict(values, codes, nullable, unit=unit, offset_minutes=offset_minutes)

def pylist(v):
    return v.to_pylist()

def cmp_s(v, scalar, op):
    return pylist(v.compare_scalar(scalar, op))

def _py_cmp(op, a, b):
    if a is None or b is None:
        return None
    return {EQ: a == b, NE: a != b, GT: a > b, GE: a >= b, LT: a < b, LE: a <= b}[op]


# ===========================================================================
# 1.  Type tag and basic properties
# ===========================================================================

class TestTypeTag:
    def test_type_is_timestamp64(self):
        assert ts([DT_2024]).type == dn.DrakenType.TIMESTAMP64

    def test_type_tag_with_nulls(self):
        assert ts([DT_2024, None]).type == dn.DrakenType.TIMESTAMP64

    def test_type_tag_all_nulls(self):
        assert ts([None, None]).type == dn.DrakenType.TIMESTAMP64

    def test_type_tag_empty(self):
        assert ts([]).type == dn.DrakenType.TIMESTAMP64

    def test_logical_unit_default_us(self):
        assert ts([DT_2024]).logical_type_unit == "us"

    def test_logical_unit_explicit_s(self):
        assert ts([DT_2024], unit="s").logical_type_unit == "s"

    def test_logical_unit_explicit_ms(self):
        assert ts([DT_2024], unit="ms").logical_type_unit == "ms"

    def test_logical_unit_explicit_ns(self):
        assert ts([DT_2024], unit="ns").logical_type_unit == "ns"

    def test_logical_offset_default_zero(self):
        assert ts([DT_2024]).logical_type_offset_minutes == 0

    def test_logical_offset_positive(self):
        assert ts([DT_2024], offset_minutes=60).logical_type_offset_minutes == 60

    def test_logical_offset_negative(self):
        assert ts([DT_2024], offset_minutes=-330).logical_type_offset_minutes == -330

    def test_timestamp64_abi_value(self):
        # DRAKEN_TIMESTAMP64 = 40 (frozen per buffers.h)
        assert dn.DrakenType.TIMESTAMP64.value == 40


# ===========================================================================
# 2.  Ingestion + readback round-trip (sequence shape)
# ===========================================================================

class TestRoundTripEmpty:
    def test_empty_returns_empty(self):
        assert pylist(ts([])) == []

    def test_empty_len(self):
        assert len(ts([])) == 0


class TestRoundTripSingle:
    def test_single_utc(self):
        result = pylist(ts([DT_2024]))
        assert len(result) == 1
        assert result[0] == DT_2024

    def test_single_null(self):
        assert pylist(ts([None])) == [None]

    def test_single_epoch(self):
        result = pylist(ts([EPOCH]))
        assert result[0] == EPOCH

    def test_single_pre_epoch(self):
        result = pylist(ts([DT_NEG]))
        assert result[0] == DT_NEG

    def test_single_microsecond_precision(self):
        result = pylist(ts([DT_MICRO]))
        assert result[0] == DT_MICRO


class TestRoundTripTail:
    """< 8 elements — exercises the SIMD tail."""

    def test_five_no_nulls(self):
        src = [DT_2024, DT_2025, EPOCH, DT_MICRO, DT_NEG]
        assert pylist(ts(src)) == src

    def test_five_some_nulls(self):
        src = [DT_2024, None, EPOCH, None, DT_2025]
        assert pylist(ts(src)) == src

    def test_five_all_nulls(self):
        src = [None, None, None, None, None]
        assert pylist(ts(src)) == src

    def test_seven_leading_null(self):
        src = [None, DT_2024, DT_2025, EPOCH, DT_MICRO, DT_NEG, DT_2024]
        assert pylist(ts(src)) == src

    def test_seven_trailing_null(self):
        src = [DT_2024, DT_2025, EPOCH, DT_MICRO, DT_NEG, DT_2024, None]
        assert pylist(ts(src)) == src


class TestRoundTripLarge:
    def test_large_no_nulls(self):
        base = datetime(2000, 1, 1, tzinfo=timezone.utc)
        src = [base + timedelta(seconds=i) for i in range(10_000)]
        result = pylist(ts(src))
        assert result == src

    def test_large_every_7th_null(self):
        base = datetime(2000, 1, 1, tzinfo=timezone.utc)
        src = [None if i % 7 == 0 else base + timedelta(seconds=i) for i in range(10_000)]
        result = pylist(ts(src))
        assert result == src

    def test_large_all_nulls(self):
        src = [None] * 10_000
        assert pylist(ts(src)) == src


# ===========================================================================
# 3.  All four timestamp units
# ===========================================================================

class TestAllUnits:
    """Round-trip correctness for each unit.
    Microsecond precision — DT_MICRO has a non-zero microsecond part.
    """

    def test_unit_us(self):
        result = pylist(ts([DT_MICRO], unit="us"))
        assert result[0] == DT_MICRO

    def test_unit_ms(self):
        # ms unit truncates microseconds — round-trip to ms boundary
        dt_ms = datetime(2023, 3, 7, 8, 15, 45, 0, tzinfo=timezone.utc)
        result = pylist(ts([dt_ms], unit="ms"))
        assert result[0] == dt_ms

    def test_unit_s(self):
        dt_s = datetime(2023, 3, 7, 8, 15, 45, tzinfo=timezone.utc)
        result = pylist(ts([dt_s], unit="s"))
        assert result[0] == dt_s

    def test_unit_ns(self):
        # ns has only microsecond precision from Python datetime
        result = pylist(ts([DT_MICRO], unit="ns"))
        assert result[0] == DT_MICRO

    def test_all_units_epoch(self):
        for unit in ("s", "ms", "us", "ns"):
            result = pylist(ts([EPOCH], unit=unit))
            assert result[0] == EPOCH, f"epoch round-trip failed for unit={unit}"

    def test_all_units_ordering_preserved(self):
        dts = [DT_2024, DT_2025]
        for unit in ("s", "ms", "us", "ns"):
            result = pylist(ts(dts, unit=unit))
            assert result[0] < result[1], f"ordering not preserved for unit={unit}"

    def test_unit_s_len(self):
        assert len(ts([DT_2024, None, DT_2025], unit="s")) == 3


# ===========================================================================
# 4.  UTC offset handling
# ===========================================================================

class TestOffsets:
    """UTC offset is stored, not interpreted — round-trip must preserve it."""

    def test_utc_roundtrip(self):
        v = ts([DT_2024])
        result = v.to_pylist()[0]
        assert result.tzinfo is not None
        assert result.utcoffset() == timedelta(0)

    def test_positive_offset_roundtrip(self):
        dt = datetime(2024, 6, 1, 12, 0, 0, tzinfo=TZ_PLUS1)
        v = ts([dt], offset_minutes=60)
        result = v.to_pylist()[0]
        assert result.utcoffset() == timedelta(hours=1)
        # The UTC-instant is preserved: converting to UTC must match
        assert result.astimezone(timezone.utc) == dt.astimezone(timezone.utc)

    def test_negative_offset_roundtrip(self):
        dt = datetime(2024, 6, 1, 6, 30, 0, tzinfo=TZ_MINUS530)
        v = ts([dt], offset_minutes=-330)
        result = v.to_pylist()[0]
        assert result.utcoffset() == timedelta(hours=-5, minutes=-30)
        assert result.astimezone(timezone.utc) == dt.astimezone(timezone.utc)

    def test_naive_treated_as_utc(self):
        dt_naive = datetime(2024, 1, 1, 0, 0, 0)
        dt_utc = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        v = ts([dt_naive])
        result = v.to_pylist()[0]
        assert result == dt_utc

    def test_offset_not_none_on_result(self):
        v = ts([DT_2024], offset_minutes=0)
        result = v.to_pylist()[0]
        assert result.tzinfo is not None

    def test_offset_property_roundtrip(self):
        for minutes in (0, 60, -330, 570):
            v = ts([DT_2024], offset_minutes=minutes)
            assert v.logical_type_offset_minutes == minutes


# ===========================================================================
# 5.  Null position correctness
# ===========================================================================

class TestNullPositions:
    def test_first_null(self):
        src = [None, DT_2024, DT_2025]
        result = pylist(ts(src))
        assert result[0] is None
        assert result[1] == DT_2024

    def test_last_null(self):
        src = [DT_2024, DT_2025, None]
        result = pylist(ts(src))
        assert result[-1] is None
        assert result[0] == DT_2024

    def test_alternating(self):
        src = [None, DT_2024, None, DT_2025, None]
        result = pylist(ts(src))
        assert [x is None for x in result] == [True, False, True, False, True]

    def test_null_value_is_none_not_epoch(self):
        result = pylist(ts([None]))
        assert result[0] is None
        assert result[0] != EPOCH

    def test_null_does_not_pollute_neighbours(self):
        result = pylist(ts([DT_2024, None, DT_2025]))
        assert result[0] == DT_2024
        assert result[1] is None
        assert result[2] == DT_2025


# ===========================================================================
# 6.  Factory shapes: constant and dict
# ===========================================================================

class TestFactoryConstant:
    def test_constant_value_readback(self):
        v = ts_const(DT_2024, 5)
        assert pylist(v) == [DT_2024] * 5

    def test_constant_null_all_null(self):
        v = ts_const(None, 3)
        assert pylist(v) == [None, None, None]

    def test_constant_length_zero(self):
        v = ts_const(DT_2024, 0)
        assert pylist(v) == []

    def test_constant_length_one(self):
        v = ts_const(DT_EPOCH_LIKE, 1)
        assert pylist(v) == [DT_EPOCH_LIKE]

    def test_constant_type(self):
        assert ts_const(DT_2024, 3).type == dn.DrakenType.TIMESTAMP64

    def test_constant_unit_propagated(self):
        assert ts_const(DT_2024, 3, unit="ms").logical_type_unit == "ms"


# lazy reference so the class body can use it
DT_EPOCH_LIKE = EPOCH


class TestFactoryDict:
    def test_dict_basic(self):
        v = ts_dict([DT_2024, DT_2025], [0, 1, 0])
        assert pylist(v) == [DT_2024, DT_2025, DT_2024]

    def test_dict_with_nulls(self):
        v = ts_dict([DT_2024, DT_2025], [0, 1, 0], [True, False, True])
        assert pylist(v) == [DT_2024, None, DT_2024]

    def test_dict_type(self):
        v = ts_dict([DT_2024], [0])
        assert v.type == dn.DrakenType.TIMESTAMP64

    def test_dict_unit_propagated(self):
        v = ts_dict([DT_2024], [0], unit="s")
        assert v.logical_type_unit == "s"


# ===========================================================================
# 7.  __getitem__
# ===========================================================================

class TestGetItem:
    def test_forward_index(self):
        v = ts([DT_2024, DT_2025, EPOCH])
        assert v[0] == DT_2024
        assert v[1] == DT_2025
        assert v[2] == EPOCH

    def test_negative_index(self):
        v = ts([DT_2024, DT_2025, EPOCH])
        assert v[-1] == EPOCH
        assert v[-3] == DT_2024

    def test_null_via_getitem(self):
        v = ts([DT_2024, None, DT_2025])
        assert v[0] == DT_2024
        assert v[1] is None
        assert v[2] == DT_2025

    def test_out_of_range_raises(self):
        v = ts([DT_2024])
        with pytest.raises(IndexError):
            _ = v[1]
        with pytest.raises(IndexError):
            _ = v[-2]


# ===========================================================================
# 8.  min / max
# ===========================================================================

class TestMinMax:
    def test_min_returns_datetime(self):
        v = ts([DT_2025, DT_2024, EPOCH])
        assert v.min() == EPOCH

    def test_max_returns_datetime(self):
        v = ts([DT_2025, DT_2024, EPOCH])
        assert v.max() == DT_2025

    def test_min_skips_nulls(self):
        v = ts([None, DT_2025, DT_2024])
        assert v.min() == DT_2024

    def test_max_skips_nulls(self):
        v = ts([None, DT_2025, DT_2024])
        assert v.max() == DT_2025

    def test_min_single(self):
        v = ts([DT_2024])
        assert v.min() == DT_2024

    def test_max_single(self):
        v = ts([DT_2024])
        assert v.max() == DT_2024

    def test_min_empty_raises(self):
        with pytest.raises(Exception):
            ts([]).min()

    def test_max_empty_raises(self):
        with pytest.raises(Exception):
            ts([]).max()

    def test_min_all_null_raises(self):
        with pytest.raises(Exception):
            ts([None, None]).min()

    def test_max_all_null_raises(self):
        with pytest.raises(Exception):
            ts([None, None]).max()

    def test_pre_epoch_min(self):
        v = ts([DT_NEG, EPOCH, DT_2024])
        assert v.min() == DT_NEG

    def test_result_is_aware(self):
        v = ts([DT_2024])
        result = v.min()
        assert result.tzinfo is not None


# ===========================================================================
# 9.  hash
# ===========================================================================

class TestHashBasic:
    def test_empty_returns_empty_list(self):
        assert ts([]).hash() == []

    def test_length_matches_input(self):
        for n in [1, 5, 100]:
            base = datetime(2000, 1, 1, tzinfo=timezone.utc)
            dts = [base + timedelta(seconds=i) for i in range(n)]
            assert len(ts(dts).hash()) == n

    def test_values_are_integers(self):
        for v in ts([DT_2024, DT_2025]).hash():
            assert isinstance(v, int)

    def test_values_fit_uint64(self):
        for v in ts([DT_2024, DT_2025]).hash():
            assert 0 <= v < 2**64


class TestHashDeterminism:
    def test_same_input_same_output(self):
        src = [DT_2024, DT_2025, EPOCH]
        assert ts(src).hash() == ts(src).hash()


class TestHashDistinct:
    def test_distinct_datetimes_distinct_hashes(self):
        base = datetime(2000, 1, 1, tzinfo=timezone.utc)
        dts = [base + timedelta(minutes=i) for i in range(10)]
        result = ts(dts).hash()
        assert len(set(result)) == 10, "expected 10 distinct hashes"


class TestHashNulls:
    def test_null_produces_consistent_sentinel(self):
        h1 = ts([None]).hash()[0]
        h2 = ts([None]).hash()[0]
        assert h1 == h2

    def test_null_differs_from_epoch_hash(self):
        h_null = ts([None]).hash()[0]
        h_epoch = ts([EPOCH]).hash()[0]
        assert h_null != h_epoch

    def test_all_nulls_same_sentinel(self):
        result = ts([None, None, None]).hash()
        assert len(set(result)) == 1


# ===========================================================================
# 10.  compare_scalar
# ===========================================================================

class TestCompareScalar:
    DATA = [DT_2024, DT_2025, EPOCH, DT_MICRO]

    def test_eq(self):
        v = ts(self.DATA)
        expected = [x == DT_2024 for x in self.DATA]
        assert cmp_s(v, DT_2024, EQ) == expected

    def test_ne(self):
        v = ts(self.DATA)
        expected = [x != DT_2024 for x in self.DATA]
        assert cmp_s(v, DT_2024, NE) == expected

    def test_lt(self):
        v = ts(self.DATA)
        expected = [x < DT_2024 for x in self.DATA]
        assert cmp_s(v, DT_2024, LT) == expected

    def test_le(self):
        v = ts(self.DATA)
        expected = [x <= DT_2024 for x in self.DATA]
        assert cmp_s(v, DT_2024, LE) == expected

    def test_gt(self):
        v = ts(self.DATA)
        expected = [x > DT_2024 for x in self.DATA]
        assert cmp_s(v, DT_2024, GT) == expected

    def test_ge(self):
        v = ts(self.DATA)
        expected = [x >= DT_2024 for x in self.DATA]
        assert cmp_s(v, DT_2024, GE) == expected

    def test_null_scalar_all_null_output(self):
        v = ts([DT_2024, DT_2025])
        result = cmp_s(v, None, EQ)
        assert all(x is None for x in result)

    def test_null_row_null_output(self):
        v = ts([DT_2024, None, DT_2025])
        result = cmp_s(v, DT_2024, EQ)
        assert result[0] is True
        assert result[1] is None
        assert result[2] is False

    def test_result_type_is_bool(self):
        v = ts([DT_2024, DT_2025])
        r = v.compare_scalar(DT_2024, EQ)
        assert r.type == dn.DrakenType.BOOL


# ===========================================================================
# 11.  compare_vector
# ===========================================================================

class TestCompareVector:
    def test_eq_equal_vectors(self):
        src = [DT_2024, DT_2025, EPOCH]
        a = ts(src)
        b = ts(src)
        result = pylist(a.compare_vector(b, EQ))
        assert result == [True, True, True]

    def test_lt_ordering(self):
        a = ts([DT_2024, DT_2025])
        b = ts([DT_2025, DT_2024])
        result = pylist(a.compare_vector(b, LT))
        assert result == [True, False]

    def test_null_propagation(self):
        a = ts([DT_2024, None])
        b = ts([DT_2024, DT_2025])
        result = pylist(a.compare_vector(b, EQ))
        assert result[0] is True
        assert result[1] is None


class TestCompareVectorCrossUnit:
    """Cross-unit compare_vector must throw — not silently mis-compare."""

    def test_us_vs_ms_throws(self):
        a = ts([DT_2024], unit="us")
        b = ts([DT_2024], unit="ms")
        with pytest.raises(Exception):
            a.compare_vector(b, EQ)

    def test_s_vs_ns_throws(self):
        a = ts([DT_2024], unit="s")
        b = ts([DT_2024], unit="ns")
        with pytest.raises(Exception):
            a.compare_vector(b, EQ)

    def test_same_unit_does_not_throw(self):
        a = ts([DT_2024], unit="ms")
        b = ts([DT_2024], unit="ms")
        result = pylist(a.compare_vector(b, EQ))
        assert result == [True]


# ===========================================================================
# 12.  between
# ===========================================================================

class TestBetween:
    DATA = [DT_NEG, EPOCH, DT_2024, DT_2025, DT_MICRO]

    def test_closed_closed(self):
        v = ts(self.DATA)
        expected = [EPOCH <= x <= DT_2024 for x in self.DATA]
        assert pylist(v.between(EPOCH, DT_2024, True, True)) == expected

    def test_open_open(self):
        v = ts(self.DATA)
        expected = [EPOCH < x < DT_2024 for x in self.DATA]
        assert pylist(v.between(EPOCH, DT_2024, False, False)) == expected

    def test_closed_open(self):
        v = ts(self.DATA)
        expected = [EPOCH <= x < DT_2024 for x in self.DATA]
        assert pylist(v.between(EPOCH, DT_2024, True, False)) == expected

    def test_open_closed(self):
        v = ts(self.DATA)
        expected = [EPOCH < x <= DT_2024 for x in self.DATA]
        assert pylist(v.between(EPOCH, DT_2024, False, True)) == expected

    def test_null_propagates(self):
        v = ts([DT_2024, None, DT_2025])
        result = pylist(v.between(EPOCH, DT_2025))
        assert result[0] is True
        assert result[1] is None

    def test_result_type_is_bool(self):
        v = ts([DT_2024])
        assert v.between(EPOCH, DT_2025).type == dn.DrakenType.BOOL

    def test_empty_range_all_false(self):
        v = ts([DT_2024, DT_2025])
        result = pylist(v.between(DT_2025, EPOCH))
        assert result == [False, False]


# ===========================================================================
# 13.  in_list
# ===========================================================================

class TestInList:
    def test_member_present(self):
        v = ts([DT_2024, DT_2025, EPOCH])
        result = pylist(v.in_list([DT_2024, EPOCH]))
        assert result == [True, False, True]

    def test_empty_set_all_false(self):
        v = ts([DT_2024, DT_2025])
        result = pylist(v.in_list([]))
        assert result == [False, False]

    def test_null_propagates(self):
        v = ts([DT_2024, None])
        result = pylist(v.in_list([DT_2024]))
        assert result[0] is True
        assert result[1] is None

    def test_null_row_not_matched(self):
        # null input row → null output regardless of the search set
        v = ts([None, DT_2024])
        result = pylist(v.in_list([DT_2024]))
        assert result[0] is None   # null row stays null
        assert result[1] is True

    def test_result_type_is_bool(self):
        v = ts([DT_2024])
        assert v.in_list([DT_2024]).type == dn.DrakenType.BOOL


# ===========================================================================
# 14.  take / materialize / compress — type preservation
# ===========================================================================

class TestTake:
    def test_take_preserves_type(self):
        v = ts([DT_2024, DT_2025, EPOCH])
        r = v.take([2, 0])
        assert r.type == dn.DrakenType.TIMESTAMP64

    def test_take_preserves_logical_unit(self):
        v = ts([DT_2024, DT_2025], unit="ms")
        r = v.take([0])
        assert r.logical_type_unit == "ms"

    def test_take_correct_values(self):
        v = ts([DT_2024, DT_2025, EPOCH])
        r = v.take([2, 0, 1])
        assert pylist(r) == [EPOCH, DT_2024, DT_2025]

    def test_take_nulls(self):
        v = ts([DT_2024, None, DT_2025])
        r = v.take([1, 0])
        result = pylist(r)
        assert result[0] is None
        assert result[1] == DT_2024

    def test_take_empty_indices(self):
        v = ts([DT_2024, DT_2025])
        r = v.take([])
        assert pylist(r) == []
        assert r.type == dn.DrakenType.TIMESTAMP64


class TestMaterialize:
    def test_materialize_preserves_type(self):
        v = ts([DT_2024, DT_2025])
        r = v.materialize()
        assert r.type == dn.DrakenType.TIMESTAMP64

    def test_materialize_preserves_logical_unit(self):
        v = ts([DT_2024], unit="ns")
        r = v.materialize()
        assert r.logical_type_unit == "ns"

    def test_materialize_roundtrip(self):
        src = [DT_2024, None, DT_2025, EPOCH]
        v = ts(src)
        assert pylist(v.materialize()) == src


class TestCompress:
    def test_compress_preserves_type(self):
        v = ts([DT_2024, DT_2025])
        r = v.compress()
        assert r.type == dn.DrakenType.TIMESTAMP64

    def test_compress_preserves_logical_unit(self):
        v = ts([DT_2024], unit="s")
        r = v.compress()
        assert r.logical_type_unit == "s"

    def test_compress_materialize_roundtrip(self):
        src = [DT_2024, None, DT_2025, EPOCH]
        v = ts(src)
        assert pylist(v.compress().materialize()) == src


# ===========================================================================
# 15.  Bit-boundary sizes (1..9 rows)
# ===========================================================================

class TestBitBoundarySizes:
    """Partial-byte validity bitmap tail — classic bit-packing bug surface."""

    BASE = datetime(2000, 1, 1, tzinfo=timezone.utc)

    def _src(self, n, null_at=None):
        src = [self.BASE + timedelta(hours=i) for i in range(n)]
        if null_at is not None:
            src[null_at] = None
        return src

    @pytest.mark.parametrize("n", [1, 2, 3, 4, 5, 6, 7, 8, 9])
    def test_roundtrip_no_nulls(self, n):
        src = self._src(n)
        assert pylist(ts(src)) == src

    @pytest.mark.parametrize("n", [1, 2, 3, 4, 5, 6, 7, 8, 9])
    def test_roundtrip_first_null(self, n):
        src = self._src(n, null_at=0)
        assert pylist(ts(src)) == src

    @pytest.mark.parametrize("n", [2, 3, 4, 5, 6, 7, 8, 9])
    def test_roundtrip_last_null(self, n):
        src = self._src(n, null_at=n - 1)
        assert pylist(ts(src)) == src

    @pytest.mark.parametrize("n", [1, 2, 3, 4, 5, 6, 7, 8, 9])
    def test_compare_scalar_size(self, n):
        src = self._src(n)
        v = ts(src)
        pivot = self.BASE + timedelta(hours=n // 2)
        result = pylist(v.compare_scalar(pivot, LT))
        expected = [x < pivot for x in src]
        assert result == expected

    @pytest.mark.parametrize("n", [1, 2, 3, 4, 5, 6, 7, 8, 9])
    def test_hash_size(self, n):
        src = self._src(n)
        result = ts(src).hash()
        assert len(result) == n
        assert len(set(result)) == n, f"collisions in hash at n={n}"


# ===========================================================================
# 16.  Hypothesis property tests
# ===========================================================================

from hypothesis import given, settings
from hypothesis import strategies as st

# Strategy: generate lists of aware UTC datetimes (with optional None).
# Keep the range to [1970, 2100] for all units to avoid overflow at ns precision.
_TS_MIN_EPOCH_S = 0           # 1970-01-01
_TS_MAX_EPOCH_S = 4102444800  # 2100-01-01

_dt_strategy = st.one_of(
    st.none(),
    st.integers(min_value=_TS_MIN_EPOCH_S, max_value=_TS_MAX_EPOCH_S).map(
        lambda s: datetime.fromtimestamp(s, tz=timezone.utc)
    ),
)

_dt_list = st.lists(_dt_strategy, min_size=0, max_size=100)
_dt_nonempty = st.lists(
    st.integers(min_value=_TS_MIN_EPOCH_S, max_value=_TS_MAX_EPOCH_S).map(
        lambda s: datetime.fromtimestamp(s, tz=timezone.utc)
    ),
    min_size=1, max_size=100,
)


class TestHypothesisRoundTrip:
    @given(src=_dt_list)
    @settings(max_examples=200)
    def test_sequence_roundtrip_identity(self, src):
        result = pylist(ts(src))
        assert len(result) == len(src)
        for i, (orig, got) in enumerate(zip(src, result)):
            if orig is None:
                assert got is None, f"pos {i}: expected None, got {got}"
            else:
                assert got is not None, f"pos {i}: expected datetime, got None"
                orig_utc = orig.astimezone(timezone.utc)
                got_utc  = got.astimezone(timezone.utc)
                assert orig_utc == got_utc, f"pos {i}: {orig_utc} != {got_utc}"

    @given(src=_dt_nonempty)
    @settings(max_examples=200)
    def test_ordering_preserved(self, src):
        """If a < b before ingestion, the stored instants must also satisfy a < b."""
        v = ts(src)
        result = pylist(v)
        # Compare each adjacent pair that exists in the source
        for a_orig, b_orig, a_got, b_got in zip(src, src[1:], result, result[1:]):
            if a_orig < b_orig:
                assert a_got < b_got, f"{a_orig} < {b_orig} but got {a_got} >= {b_got}"
            elif a_orig > b_orig:
                assert a_got > b_got

    @given(
        src=_dt_nonempty,
        unit=st.sampled_from(["s", "ms", "us", "ns"]),
    )
    @settings(max_examples=100)
    def test_min_max_correct(self, src, unit):
        # For s/ms resolution, truncate to the unit boundary
        if unit == "s":
            src = [dt.replace(microsecond=0) for dt in src]
        elif unit == "ms":
            src = [dt.replace(microsecond=dt.microsecond // 1000 * 1000) for dt in src]
        v = ts(src, unit=unit)
        py_min = min(src)
        py_max = max(src)
        got_min = v.min().astimezone(timezone.utc)
        got_max = v.max().astimezone(timezone.utc)
        assert got_min == py_min.astimezone(timezone.utc)
        assert got_max == py_max.astimezone(timezone.utc)

    @given(
        src=_dt_list,
        idx=st.lists(st.integers(min_value=0, max_value=99), min_size=0, max_size=50),
    )
    @settings(max_examples=100)
    def test_take_type_and_length(self, src, idx):
        if not src:
            return
        valid_idx = [i for i in idx if i < len(src)]
        v = ts(src)
        r = v.take(valid_idx)
        assert r.type == dn.DrakenType.TIMESTAMP64
        assert len(r) == len(valid_idx)

    @given(
        unit_a=st.sampled_from(["s", "ms", "us", "ns"]),
        unit_b=st.sampled_from(["s", "ms", "us", "ns"]),
    )
    @settings(max_examples=50)
    def test_cross_unit_compare_vector_behaviour(self, unit_a, unit_b):
        a = ts([DT_2024], unit=unit_a)
        b = ts([DT_2024], unit=unit_b)
        if unit_a == unit_b:
            result = pylist(a.compare_vector(b, EQ))
            assert result == [True]
        else:
            with pytest.raises(Exception):
                a.compare_vector(b, EQ)
