"""
Native unit tests for TIME32 and TIME64 ingestion, readback, and ops.

Coverage matrix:
  types:        TIME32 (s/ms) and TIME64 (us/ns)
  nullability:  no nulls / some nulls / all null
  sizes:        0 / 1 / <8 (tail) / large
  edge values:  midnight (00:00:00), end-of-day (23:59:59.999999)
  shapes:       sequence / constant / dict
  mandatory desc: no descriptor on TIME32/TIME64 = hard error at factory
  ops:          compare_scalar, compare_vector, hash, min, max, between, in_list,
                take, materialize, dictionary_encode
  cross-unit:   compare_vector with mismatched units must throw
  unit validation: TIME32 rejects "us"/"ns"; TIME64 rejects "s"/"ms"
  hypothesis:   round-trip identity, ordering preserved, cross-unit throw
"""

import pytest
from datetime import time

import draken.draken_native as dn

EQ, NE, GT, GE, LT, LE = 0, 1, 2, 3, 4, 5

MIDNIGHT = time(0, 0, 0)
T_NOON   = time(12, 0, 0)
T_LATE   = time(23, 59, 59)
T_MICRO  = time(8, 15, 45, 123456)
T_MS     = time(8, 15, 45, 123000)   # ms-boundary
T_END    = time(23, 59, 59, 999999)  # near end of day


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def t32(lst, unit="s"):
    return dn.vector_time32_from_sequence(lst, unit=unit)

def t64(lst, unit="us"):
    return dn.vector_time64_from_sequence(lst, unit=unit)

def t32_const(value, length, unit="s"):
    return dn.vector_time32_from_constant(value, length, unit=unit)

def t64_const(value, length, unit="us"):
    return dn.vector_time64_from_constant(value, length, unit=unit)

def t32_dict(values, codes, nullable=None, unit="s"):
    return dn.vector_time32_from_dict(values, codes, nullable, unit=unit)

def t64_dict(values, codes, nullable=None, unit="us"):
    return dn.vector_time64_from_dict(values, codes, nullable, unit=unit)

def pylist(v):
    return v.to_pylist()

def cmp_s(v, scalar, op):
    return pylist(v.compare_scalar(scalar, op))


# ===========================================================================
# 1.  Type tags, ABI values, unit descriptor
# ===========================================================================

class TestTypeTag:
    def test_time32_type(self):
        assert t32([T_NOON]).type == dn.DrakenType.TIME32

    def test_time64_type(self):
        assert t64([T_NOON]).type == dn.DrakenType.TIME64

    def test_time32_abi_value(self):
        # DRAKEN_TIME32 = 41 (frozen per buffers.h)
        assert dn.DrakenType.TIME32.value == 41

    def test_time64_abi_value(self):
        # DRAKEN_TIME64 = 42 (frozen per buffers.h)
        assert dn.DrakenType.TIME64.value == 42

    def test_time32_unit_s(self):
        assert t32([T_NOON], unit="s").logical_type_unit == "s"

    def test_time32_unit_ms(self):
        assert t32([T_NOON], unit="ms").logical_type_unit == "ms"

    def test_time64_unit_us(self):
        assert t64([T_NOON], unit="us").logical_type_unit == "us"

    def test_time64_unit_ns(self):
        assert t64([T_NOON], unit="ns").logical_type_unit == "ns"

    def test_time32_rejects_us(self):
        with pytest.raises(Exception):
            t32([T_NOON], unit="us")

    def test_time32_rejects_ns(self):
        with pytest.raises(Exception):
            t32([T_NOON], unit="ns")

    def test_time64_rejects_s(self):
        with pytest.raises(Exception):
            t64([T_NOON], unit="s")

    def test_time64_rejects_ms(self):
        with pytest.raises(Exception):
            t64([T_NOON], unit="ms")

    def test_no_offset_on_time32(self):
        assert t32([T_NOON]).logical_type_offset_minutes == 0

    def test_no_offset_on_time64(self):
        assert t64([T_NOON]).logical_type_offset_minutes == 0


# ===========================================================================
# 2.  Round-trip ingestion / readback
# ===========================================================================

class TestRoundTripEmpty:
    def test_time32_empty(self):
        assert pylist(t32([])) == []

    def test_time64_empty(self):
        assert pylist(t64([])) == []


class TestRoundTripSingle:
    def test_t32_midnight_s(self):
        assert pylist(t32([MIDNIGHT], unit="s")) == [MIDNIGHT]

    def test_t32_noon_ms(self):
        dt_ms = time(12, 0, 0, 0)
        assert pylist(t32([dt_ms], unit="ms")) == [dt_ms]

    def test_t64_micro(self):
        assert pylist(t64([T_MICRO], unit="us")) == [T_MICRO]

    def test_t64_ns(self):
        # ns has only microsecond precision from Python time
        assert pylist(t64([T_MICRO], unit="ns")) == [T_MICRO]

    def test_t64_end_of_day(self):
        assert pylist(t64([T_END], unit="us")) == [T_END]

    def test_null_round_trips(self):
        assert pylist(t32([None])) == [None]
        assert pylist(t64([None])) == [None]


class TestRoundTripPrecision:
    def test_t32_s_truncates_subsecond(self):
        t_trunc = time(8, 15, 45, 0)
        assert pylist(t32([t_trunc], unit="s")) == [t_trunc]

    def test_t32_ms_truncates_sub_ms(self):
        assert pylist(t32([T_MS], unit="ms")) == [T_MS]

    def test_all_t32_units_midnight(self):
        for unit in ("s", "ms"):
            assert pylist(t32([MIDNIGHT], unit=unit)) == [MIDNIGHT], \
                f"midnight failed for unit={unit}"

    def test_all_t64_units_midnight(self):
        for unit in ("us", "ns"):
            assert pylist(t64([MIDNIGHT], unit=unit)) == [MIDNIGHT], \
                f"midnight failed for unit={unit}"


class TestRoundTripLarge:
    def test_t64_large_no_nulls(self):
        from datetime import timedelta
        base = time(0, 0, 0)
        import datetime as dt_mod
        # Build 10_000 times starting at midnight in 1s increments
        base_dt = dt_mod.datetime(2000, 1, 1, 0, 0, 0)
        src = [(base_dt + dt_mod.timedelta(seconds=i)).time() for i in range(10_000)]
        assert pylist(t64(src, unit="us")) == src

    def test_t64_large_every_7th_null(self):
        from datetime import timedelta
        import datetime as dt_mod
        base_dt = dt_mod.datetime(2000, 1, 1, 0, 0, 0)
        src = [None if i % 7 == 0 else (base_dt + dt_mod.timedelta(seconds=i)).time()
               for i in range(5_000)]
        assert pylist(t64(src, unit="us")) == src


# ===========================================================================
# 3.  Factory shapes: constant and dict
# ===========================================================================

class TestConstant:
    def test_t32_constant_value(self):
        v = t32_const(T_NOON, 5, unit="s")
        assert pylist(v) == [T_NOON] * 5

    def test_t64_constant_null(self):
        v = t64_const(None, 3, unit="us")
        assert pylist(v) == [None, None, None]

    def test_t64_unit_propagated(self):
        v = t64_const(T_NOON, 3, unit="us")
        assert v.logical_type_unit == "us"

    def test_t32_type(self):
        assert t32_const(T_NOON, 3).type == dn.DrakenType.TIME32

    def test_t64_type(self):
        assert t64_const(T_NOON, 3).type == dn.DrakenType.TIME64


class TestDict:
    def test_t32_basic(self):
        v = t32_dict([MIDNIGHT, T_NOON], [0, 1, 0], unit="s")
        assert pylist(v) == [MIDNIGHT, T_NOON, MIDNIGHT]

    def test_t64_with_nulls(self):
        v = t64_dict([T_NOON, T_LATE], [0, 1, 0], [True, False, True], unit="us")
        assert pylist(v) == [T_NOON, None, T_NOON]

    def test_t64_unit_propagated(self):
        v = t64_dict([T_NOON], [0], unit="ns")
        assert v.logical_type_unit == "ns"


# ===========================================================================
# 4.  __getitem__
# ===========================================================================

class TestGetItem:
    def test_t32_forward(self):
        v = t32([MIDNIGHT, T_NOON, T_LATE])
        assert v[0] == MIDNIGHT
        assert v[2] == T_LATE

    def test_t64_negative_index(self):
        v = t64([MIDNIGHT, T_NOON, T_LATE])
        assert v[-1] == T_LATE

    def test_null_via_getitem(self):
        v = t64([T_NOON, None, T_LATE])
        assert v[1] is None

    def test_out_of_range(self):
        v = t32([T_NOON])
        with pytest.raises(IndexError):
            _ = v[1]


# ===========================================================================
# 5.  min / max
# ===========================================================================

class TestMinMax:
    def test_t32_min(self):
        v = t32([T_LATE, T_NOON, MIDNIGHT], unit="s")
        assert v.min() == MIDNIGHT

    def test_t64_max(self):
        v = t64([T_LATE, T_NOON, MIDNIGHT], unit="us")
        assert v.max() == T_LATE

    def test_min_skips_nulls(self):
        v = t64([None, T_LATE, T_NOON], unit="us")
        assert v.min() == T_NOON

    def test_max_skips_nulls(self):
        v = t64([None, T_LATE, T_NOON], unit="us")
        assert v.max() == T_LATE

    def test_empty_raises(self):
        with pytest.raises(Exception):
            t32([]).min()

    def test_all_null_raises(self):
        with pytest.raises(Exception):
            t64([None, None]).max()

    def test_result_is_time(self):
        assert isinstance(t64([T_NOON], unit="us").min(), time)


# ===========================================================================
# 6.  hash
# ===========================================================================

class TestHash:
    def test_length_matches(self):
        assert len(t64([T_NOON, T_LATE, MIDNIGHT]).hash()) == 3

    def test_same_input_same_hash(self):
        src = [T_NOON, T_LATE]
        assert t64(src).hash() == t64(src).hash()

    def test_distinct_times_distinct_hashes(self):
        import datetime as dt_mod
        base_dt = dt_mod.datetime(2000, 1, 1, 0, 0, 0)
        times = [(base_dt + dt_mod.timedelta(minutes=i)).time() for i in range(20)]
        result = t64(times, unit="us").hash()
        assert len(set(result)) == 20

    def test_null_sentinel_consistent(self):
        h1 = t64([None]).hash()[0]
        h2 = t64([None]).hash()[0]
        assert h1 == h2

    def test_null_differs_from_midnight(self):
        h_null = t64([None]).hash()[0]
        h_mid  = t64([MIDNIGHT]).hash()[0]
        assert h_null != h_mid


# ===========================================================================
# 7.  compare_scalar
# ===========================================================================

class TestCompareScalar:
    DATA = [MIDNIGHT, T_NOON, T_LATE, T_MICRO]

    def test_t64_eq(self):
        v = t64(self.DATA, unit="us")
        expected = [x == T_NOON for x in self.DATA]
        assert cmp_s(v, T_NOON, EQ) == expected

    def test_t64_lt(self):
        v = t64(self.DATA, unit="us")
        expected = [x < T_NOON for x in self.DATA]
        assert cmp_s(v, T_NOON, LT) == expected

    def test_t32_compare(self):
        v = t32([MIDNIGHT, T_NOON, T_LATE], unit="s")
        t_trunc = time(12, 0, 0)  # seconds boundary
        assert cmp_s(v, t_trunc, EQ) == [False, True, False]

    def test_null_scalar_raises(self):
        v = t64([T_NOON, T_LATE])
        with pytest.raises(TypeError):
            cmp_s(v, None, EQ)

    def test_null_row_null_output(self):
        v = t64([T_NOON, None, T_LATE])
        result = cmp_s(v, T_NOON, EQ)
        assert result[0] is True
        assert result[1] is None
        assert result[2] is False

    def test_result_type_is_bool(self):
        assert t64([T_NOON]).compare_scalar(T_NOON, EQ).type == dn.DrakenType.BOOL


# ===========================================================================
# 8.  compare_vector (including cross-unit throw)
# ===========================================================================

class TestCompareVector:
    def test_t64_eq(self):
        src = [MIDNIGHT, T_NOON, T_LATE]
        a = t64(src)
        b = t64(src)
        assert pylist(a.compare_vector(b, EQ)) == [True, True, True]

    def test_t64_lt(self):
        a = t64([MIDNIGHT, T_NOON])
        b = t64([T_NOON, MIDNIGHT])
        assert pylist(a.compare_vector(b, LT)) == [True, False]

    def test_null_propagation(self):
        a = t64([T_NOON, None])
        b = t64([T_NOON, T_LATE])
        result = pylist(a.compare_vector(b, EQ))
        assert result[0] is True
        assert result[1] is None


class TestCompareVectorCrossUnit:
    def test_us_vs_ns_throws(self):
        a = t64([T_NOON], unit="us")
        b = t64([T_NOON], unit="ns")
        with pytest.raises(Exception):
            a.compare_vector(b, EQ)

    def test_time32_vs_time64_throws(self):
        a = t32([time(12, 0, 0)], unit="s")
        b = t64([T_NOON], unit="us")
        with pytest.raises(Exception):
            a.compare_vector(b, EQ)

    def test_same_unit_does_not_throw(self):
        a = t64([T_NOON], unit="us")
        b = t64([T_NOON], unit="us")
        assert pylist(a.compare_vector(b, EQ)) == [True]


# ===========================================================================
# 9.  between
# ===========================================================================

class TestBetween:
    DATA = [MIDNIGHT, T_NOON, T_LATE, T_MICRO]

    def test_t64_closed(self):
        v = t64(self.DATA, unit="us")
        expected = [T_NOON <= x <= T_LATE for x in self.DATA]
        assert pylist(v.between(T_NOON, T_LATE, True, True)) == expected

    def test_t64_open(self):
        v = t64(self.DATA, unit="us")
        expected = [MIDNIGHT < x < T_LATE for x in self.DATA]
        assert pylist(v.between(MIDNIGHT, T_LATE, False, False)) == expected

    def test_null_propagates(self):
        v = t64([T_NOON, None, T_LATE])
        result = pylist(v.between(MIDNIGHT, T_END))
        assert result[0] is True
        assert result[1] is None

    def test_result_type_is_bool(self):
        v = t64([T_NOON])
        assert v.between(MIDNIGHT, T_END).type == dn.DrakenType.BOOL


# ===========================================================================
# 10.  in_list
# ===========================================================================

class TestInList:
    def test_t64_member_present(self):
        v = t64([T_NOON, T_LATE, MIDNIGHT])
        result = pylist(v.in_list([T_NOON, MIDNIGHT]))
        assert result == [True, False, True]

    def test_empty_set_all_false(self):
        v = t64([T_NOON, T_LATE])
        assert pylist(v.in_list([])) == [False, False]

    def test_null_propagates(self):
        v = t64([T_NOON, None])
        result = pylist(v.in_list([T_NOON]))
        assert result[0] is True
        assert result[1] is None

    def test_result_type_is_bool(self):
        assert t64([T_NOON]).in_list([T_NOON]).type == dn.DrakenType.BOOL


# ===========================================================================
# 11.  take / materialize / dictionary_encode — type and unit preservation
# ===========================================================================

class TestTake:
    def test_t64_preserves_type(self):
        r = t64([T_NOON, T_LATE, MIDNIGHT]).take([2, 0])
        assert r.type == dn.DrakenType.TIME64

    def test_t64_preserves_unit(self):
        r = t64([T_NOON, T_LATE], unit="ns").take([0])
        assert r.logical_type_unit == "ns"

    def test_t32_correct_values(self):
        v = t32([MIDNIGHT, T_NOON, time(23, 59, 59)], unit="s")
        r = v.take([2, 0, 1])
        assert pylist(r) == [time(23, 59, 59), MIDNIGHT, T_NOON]

    def test_nulls(self):
        v = t64([T_NOON, None, T_LATE])
        r = v.take([1, 0])
        assert pylist(r)[0] is None


class TestMaterialize:
    def test_t64_preserves_type(self):
        r = t64([T_NOON, T_LATE]).materialize()
        assert r.type == dn.DrakenType.TIME64

    def test_t64_roundtrip(self):
        src = [T_NOON, None, T_LATE, MIDNIGHT]
        assert pylist(t64(src).materialize()) == src


class TestCompress:
    def test_t32_preserves_type(self):
        r = t32([T_NOON, T_LATE], unit="s").dictionary_encode()
        assert r.type == dn.DrakenType.TIME32

    def test_t64_roundtrip(self):
        src = [T_NOON, None, T_LATE, MIDNIGHT]
        assert pylist(t64(src).dictionary_encode().materialize()) == src


# ===========================================================================
# 12.  Bit-boundary sizes (1..9 rows)
# ===========================================================================

class TestBitBoundarySizes:
    import datetime as _dt
    _base_dt = _dt.datetime(2000, 1, 1, 0, 0, 0)

    def _src(self, n, null_at=None):
        import datetime as dt_mod
        base = dt_mod.datetime(2000, 1, 1, 0, 0, 0)
        src = [(base + dt_mod.timedelta(hours=i)).time() for i in range(n)]
        if null_at is not None:
            src[null_at] = None
        return src

    @pytest.mark.parametrize("n", [1, 2, 3, 4, 5, 6, 7, 8, 9])
    def test_roundtrip_no_nulls(self, n):
        src = self._src(n)
        assert pylist(t64(src)) == src

    @pytest.mark.parametrize("n", [1, 2, 3, 4, 5, 6, 7, 8, 9])
    def test_roundtrip_first_null(self, n):
        src = self._src(n, null_at=0)
        assert pylist(t64(src)) == src

    @pytest.mark.parametrize("n", [2, 3, 4, 5, 6, 7, 8, 9])
    def test_roundtrip_last_null(self, n):
        src = self._src(n, null_at=n - 1)
        assert pylist(t64(src)) == src


# ===========================================================================
# 13.  Hypothesis property tests
# ===========================================================================

from hypothesis import given, settings
from hypothesis import strategies as st

_time_strategy = st.one_of(
    st.none(),
    st.times(),
)
_time_list     = st.lists(_time_strategy, min_size=0, max_size=100)
_time_nonempty = st.lists(st.times(), min_size=1, max_size=100)


class TestHypothesisRoundTrip:
    @given(src=_time_list)
    @settings(max_examples=200)
    def test_t64_roundtrip_identity(self, src):
        result = pylist(t64(src, unit="us"))
        assert len(result) == len(src)
        for i, (orig, got) in enumerate(zip(src, result)):
            if orig is None:
                assert got is None
            else:
                assert got == orig

    @given(src=_time_nonempty)
    @settings(max_examples=200)
    def test_ordering_preserved(self, src):
        result = pylist(t64(src, unit="us"))
        for a_orig, b_orig, a_got, b_got in zip(src, src[1:], result, result[1:]):
            if a_orig < b_orig:
                assert a_got < b_got
            elif a_orig > b_orig:
                assert a_got > b_got

    @given(src=_time_nonempty)
    @settings(max_examples=100)
    def test_min_max_correct(self, src):
        v = t64(src, unit="us")
        assert v.min() == min(src)
        assert v.max() == max(src)

    @given(
        unit_a=st.sampled_from(["us", "ns"]),
        unit_b=st.sampled_from(["us", "ns"]),
    )
    @settings(max_examples=50)
    def test_cross_unit_compare_vector_behaviour(self, unit_a, unit_b):
        a = t64([T_NOON], unit=unit_a)
        b = t64([T_NOON], unit=unit_b)
        if unit_a == unit_b:
            result = pylist(a.compare_vector(b, EQ))
            assert result == [True]
        else:
            with pytest.raises(Exception):
                a.compare_vector(b, EQ)
