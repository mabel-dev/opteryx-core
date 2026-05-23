"""
Native + parity tests for E.12: temporal conversion cluster via vector_temporal_convert consumer.

Loads the nanobind extension without triggering opteryx/__init__.py,
following the spec_from_file_location pattern established in E.9/E.10/E.11.

Coverage:
  vector_date32_to_timestamp:
    epoch → 0 in all units (s/ms/us/ns)
    known day → known timestamp value
    leap-year boundary round-trip: date32 → timestamp → date32 (identity)
    null TVL: null input row → null output row
    empty vector
    TypeError on non-DATE32 input
    default unit is "us"

  vector_timestamp_to_date32:
    epoch → 0
    one day past epoch (86400_000_000 us) → 1
    floor to day boundary (12:34:56 → same day)
    pre-epoch (negative timestamps) floor correctly
    null TVL
    all units (s/ms/us/ns)
    TypeError on non-TIMESTAMP64 input

  vector_unixtime:
    TIMESTAMP64 known values in each unit → correct unix seconds
    DATE32 known values → correct unix seconds
    pre-epoch timestamps → negative unix seconds
    null TVL (both types)
    TypeError on wrong type

  vector_floor_temporal:
    floor to second / minute / hour / day at known boundaries
    magnitude > 1 (e.g. 5-minute buckets)
    pre-epoch timestamps floor correctly
    null TVL
    case-insensitive unit names ("Hour" vs "hour")
    plural unit names ("minutes" vs "minute")
    ValueError on unsupported unit (e.g. "month")
    ValueError on non-positive magnitude
    TypeError on non-TIMESTAMP64 input

  round-trip:
    date32 → timestamp → date32 is identity for leap and non-leap years
    unixtime(date32(days)) = days × 86400

  output type tags:
    date32_to_timestamp → DRAKEN_TIMESTAMP64
    timestamp_to_date32 → DRAKEN_DATE32
    unixtime → DRAKEN_INT64
    floor_temporal → DRAKEN_TIMESTAMP64 (same unit as input)
"""

import datetime
import glob
import importlib.util
import os

import draken.draken_native as dn
import pytest


# ---------------------------------------------------------------------------
# Load vector_temporal_convert extension
# ---------------------------------------------------------------------------

def _load_vector_temporal_convert():
    pattern = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "opteryx", "compiled", "nanobind", "vector_temporal_convert*.so"
    )
    matches = glob.glob(pattern)
    if not matches:
        pytest.skip(
            "vector_temporal_convert extension not built — run make compile first",
            allow_module_level=True,
        )
    spec = importlib.util.spec_from_file_location(
        "opteryx.compiled.nanobind.vector_temporal_convert", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


vtc = _load_vector_temporal_convert()


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------

EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
D_EPOCH = datetime.date(1970, 1, 1)


def make_ts(datetimes, unit="us"):
    return dn.vector_timestamp_from_sequence(datetimes, unit=unit)


def make_date32(dates):
    return dn.vector_date32_from_sequence(dates)


def pylist(v):
    return v.to_pylist()


def raw_int64(v):
    """Extract list of raw int64 values (bypassing logical-type interpretation)."""
    return [int(v[i]) for i in range(len(v))]


# ---------------------------------------------------------------------------
# 1. vector_date32_to_timestamp
# ---------------------------------------------------------------------------

class TestDate32ToTimestamp:

    def test_epoch_us(self):
        v = make_date32([D_EPOCH])
        r = vtc.vector_date32_to_timestamp(v)
        assert r.type == dn.DrakenType.TIMESTAMP64
        assert r.logical_type_unit == "us"
        assert pylist(r) == [EPOCH]

    def test_epoch_ms(self):
        v = make_date32([D_EPOCH])
        r = vtc.vector_date32_to_timestamp(v, unit="ms")
        assert r.logical_type_unit == "ms"
        assert pylist(r) == [EPOCH]

    def test_epoch_s(self):
        v = make_date32([D_EPOCH])
        r = vtc.vector_date32_to_timestamp(v, unit="s")
        assert r.logical_type_unit == "s"
        assert pylist(r) == [EPOCH]

    def test_epoch_ns(self):
        v = make_date32([D_EPOCH])
        r = vtc.vector_date32_to_timestamp(v, unit="ns")
        assert r.logical_type_unit == "ns"
        assert pylist(r) == [EPOCH]

    def test_one_day_us(self):
        d = datetime.date(1970, 1, 2)
        v = make_date32([d])
        r = vtc.vector_date32_to_timestamp(v)
        # 1 day = 86_400_000_000 microseconds
        assert pylist(r) == [datetime.datetime(1970, 1, 2, tzinfo=datetime.timezone.utc)]

    def test_known_date_2024(self):
        d = datetime.date(2024, 1, 1)
        v = make_date32([d])
        r = vtc.vector_date32_to_timestamp(v)
        expected = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        assert pylist(r) == [expected]

    def test_pre_epoch_date(self):
        d = datetime.date(1969, 12, 31)  # -1 day
        v = make_date32([d])
        r = vtc.vector_date32_to_timestamp(v)
        expected = datetime.datetime(1969, 12, 31, tzinfo=datetime.timezone.utc)
        assert pylist(r) == [expected]

    def test_null_tvl(self):
        v = make_date32([D_EPOCH, None, datetime.date(2024, 6, 1)])
        r = vtc.vector_date32_to_timestamp(v)
        result = pylist(r)
        assert result[0] == EPOCH
        assert result[1] is None
        assert result[2] == datetime.datetime(2024, 6, 1, tzinfo=datetime.timezone.utc)

    def test_all_null(self):
        v = make_date32([None, None, None])
        r = vtc.vector_date32_to_timestamp(v)
        assert pylist(r) == [None, None, None]

    def test_empty_vector(self):
        v = make_date32([])
        r = vtc.vector_date32_to_timestamp(v)
        assert len(r) == 0
        assert r.type == dn.DrakenType.TIMESTAMP64

    def test_default_unit_is_us(self):
        v = make_date32([D_EPOCH])
        r = vtc.vector_date32_to_timestamp(v)
        assert r.logical_type_unit == "us"

    def test_type_error_on_non_date32(self):
        v = make_ts([EPOCH])
        with pytest.raises(TypeError):
            vtc.vector_date32_to_timestamp(v)

    def test_type_error_on_non_vector(self):
        with pytest.raises(TypeError):
            vtc.vector_date32_to_timestamp(42)

    # Leap-year round-trip
    def test_roundtrip_leap_year_feb28(self):
        d = datetime.date(2000, 2, 28)
        v = make_date32([d])
        ts = vtc.vector_date32_to_timestamp(v)
        back = vtc.vector_timestamp_to_date32(ts)
        assert pylist(back) == [d]

    def test_roundtrip_leap_year_feb29(self):
        d = datetime.date(2000, 2, 29)
        v = make_date32([d])
        ts = vtc.vector_date32_to_timestamp(v)
        back = vtc.vector_timestamp_to_date32(ts)
        assert pylist(back) == [d]

    def test_roundtrip_non_leap_year_feb28(self):
        d = datetime.date(2001, 2, 28)
        v = make_date32([d])
        ts = vtc.vector_date32_to_timestamp(v)
        back = vtc.vector_timestamp_to_date32(ts)
        assert pylist(back) == [d]

    def test_roundtrip_pre_epoch(self):
        d = datetime.date(1960, 7, 4)
        v = make_date32([d])
        ts = vtc.vector_date32_to_timestamp(v)
        back = vtc.vector_timestamp_to_date32(ts)
        assert pylist(back) == [d]


# ---------------------------------------------------------------------------
# 2. vector_timestamp_to_date32
# ---------------------------------------------------------------------------

class TestTimestampToDate32:

    def test_epoch(self):
        v = make_ts([EPOCH])
        r = vtc.vector_timestamp_to_date32(v)
        assert r.type == dn.DrakenType.DATE32
        assert pylist(r) == [D_EPOCH]

    def test_one_day_past_epoch_us(self):
        # 86_400_000_000 microseconds = 1 day
        dt = datetime.datetime(1970, 1, 2, tzinfo=datetime.timezone.utc)
        v = make_ts([dt])
        r = vtc.vector_timestamp_to_date32(v)
        assert pylist(r) == [datetime.date(1970, 1, 2)]

    def test_floor_to_day_boundary(self):
        # 12:34:56 should become the same day (time stripped)
        dt = datetime.datetime(2024, 3, 15, 12, 34, 56, tzinfo=datetime.timezone.utc)
        v = make_ts([dt])
        r = vtc.vector_timestamp_to_date32(v)
        assert pylist(r) == [datetime.date(2024, 3, 15)]

    def test_pre_epoch_floors_correctly(self):
        # 1969-12-31 23:59:59 → 1969-12-31 (not 1970-01-01)
        dt = datetime.datetime(1969, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
        v = make_ts([dt])
        r = vtc.vector_timestamp_to_date32(v)
        assert pylist(r) == [datetime.date(1969, 12, 31)]

    def test_pre_epoch_midnight(self):
        # 1969-12-31 00:00:00 → 1969-12-31
        dt = datetime.datetime(1969, 12, 31, 0, 0, 0, tzinfo=datetime.timezone.utc)
        v = make_ts([dt])
        r = vtc.vector_timestamp_to_date32(v)
        assert pylist(r) == [datetime.date(1969, 12, 31)]

    def test_unit_ms(self):
        dt = datetime.datetime(2024, 6, 15, tzinfo=datetime.timezone.utc)
        v = make_ts([dt], unit="ms")
        r = vtc.vector_timestamp_to_date32(v)
        assert pylist(r) == [datetime.date(2024, 6, 15)]

    def test_unit_s(self):
        dt = datetime.datetime(2024, 6, 15, tzinfo=datetime.timezone.utc)
        v = make_ts([dt], unit="s")
        r = vtc.vector_timestamp_to_date32(v)
        assert pylist(r) == [datetime.date(2024, 6, 15)]

    def test_null_tvl(self):
        dts = [EPOCH, None, datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)]
        v = make_ts(dts)
        r = vtc.vector_timestamp_to_date32(v)
        result = pylist(r)
        assert result[0] == D_EPOCH
        assert result[1] is None
        assert result[2] == datetime.date(2024, 1, 1)

    def test_all_null(self):
        v = make_ts([None, None])
        r = vtc.vector_timestamp_to_date32(v)
        assert pylist(r) == [None, None]

    def test_type_error_on_non_timestamp(self):
        v = make_date32([D_EPOCH])
        with pytest.raises(TypeError):
            vtc.vector_timestamp_to_date32(v)

    def test_type_error_on_non_vector(self):
        with pytest.raises(TypeError):
            vtc.vector_timestamp_to_date32("not a vector")


# ---------------------------------------------------------------------------
# 3. vector_unixtime
# ---------------------------------------------------------------------------

class TestUnixtime:

    def test_timestamp_epoch_us(self):
        v = make_ts([EPOCH])
        r = vtc.vector_unixtime(v)
        assert r.type == dn.DrakenType.INT64
        assert pylist(r) == [0]

    def test_timestamp_known_us(self):
        # 2023-01-01 00:00:00 UTC = 1672531200 unix seconds
        dt = datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
        v = make_ts([dt])
        r = vtc.vector_unixtime(v)
        assert pylist(r) == [1672531200]

    def test_timestamp_known_ms(self):
        dt = datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
        v = make_ts([dt], unit="ms")
        r = vtc.vector_unixtime(v)
        assert pylist(r) == [1672531200]

    def test_timestamp_known_s(self):
        dt = datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)
        v = make_ts([dt], unit="s")
        r = vtc.vector_unixtime(v)
        assert pylist(r) == [1672531200]

    def test_timestamp_pre_epoch(self):
        # 1969-12-31 23:59:59 = -1 unix second
        dt = datetime.datetime(1969, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
        v = make_ts([dt])
        r = vtc.vector_unixtime(v)
        assert pylist(r) == [-1]

    def test_timestamp_sub_second_truncated(self):
        # 0.5 seconds past epoch → 0 unix seconds (floor)
        dt = datetime.datetime(1970, 1, 1, 0, 0, 0, 500000, tzinfo=datetime.timezone.utc)
        v = make_ts([dt])
        r = vtc.vector_unixtime(v)
        assert pylist(r) == [0]

    def test_date32_epoch(self):
        v = make_date32([D_EPOCH])
        r = vtc.vector_unixtime(v)
        assert r.type == dn.DrakenType.INT64
        assert pylist(r) == [0]

    def test_date32_one_day(self):
        v = make_date32([datetime.date(1970, 1, 2)])
        r = vtc.vector_unixtime(v)
        assert pylist(r) == [86400]

    def test_date32_pre_epoch(self):
        v = make_date32([datetime.date(1969, 12, 31)])
        r = vtc.vector_unixtime(v)
        assert pylist(r) == [-86400]

    def test_date32_known(self):
        # 2023-01-01 = 19358 days since epoch = 1672531200 seconds
        v = make_date32([datetime.date(2023, 1, 1)])
        r = vtc.vector_unixtime(v)
        assert pylist(r) == [1672531200]

    def test_null_tvl_timestamp(self):
        v = make_ts([EPOCH, None, datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc)])
        r = vtc.vector_unixtime(v)
        result = pylist(r)
        assert result[0] == 0
        assert result[1] is None
        assert result[2] == 1672531200

    def test_null_tvl_date32(self):
        v = make_date32([D_EPOCH, None, datetime.date(1970, 1, 2)])
        r = vtc.vector_unixtime(v)
        result = pylist(r)
        assert result[0] == 0
        assert result[1] is None
        assert result[2] == 86400

    def test_type_error_on_int64(self):
        v = dn.vector_from_sequence([1, 2, 3])
        with pytest.raises(TypeError):
            vtc.vector_unixtime(v)

    def test_type_error_on_non_vector(self):
        with pytest.raises(TypeError):
            vtc.vector_unixtime(None)


# ---------------------------------------------------------------------------
# 4. vector_floor_temporal
# ---------------------------------------------------------------------------

class TestFloorTemporal:

    def _make_ts_us(self, dt_list):
        return make_ts(dt_list, unit="us")

    def _dt(self, h, m, s, us=0):
        return datetime.datetime(2024, 3, 15, h, m, s, us, tzinfo=datetime.timezone.utc)

    def test_floor_to_second(self):
        # 12:34:56.789012 → 12:34:56.000000
        v = self._make_ts_us([self._dt(12, 34, 56, 789012)])
        r = vtc.vector_floor_temporal(v, 1, "second")
        assert pylist(r) == [self._dt(12, 34, 56, 0)]

    def test_floor_to_minute(self):
        # 12:34:56 → 12:34:00
        v = self._make_ts_us([self._dt(12, 34, 56)])
        r = vtc.vector_floor_temporal(v, 1, "minute")
        assert pylist(r) == [self._dt(12, 34, 0)]

    def test_floor_to_hour(self):
        # 12:34:56 → 12:00:00
        v = self._make_ts_us([self._dt(12, 34, 56)])
        r = vtc.vector_floor_temporal(v, 1, "hour")
        assert pylist(r) == [self._dt(12, 0, 0)]

    def test_floor_to_day(self):
        # 2024-03-15 12:34:56 → 2024-03-15 00:00:00
        v = self._make_ts_us([self._dt(12, 34, 56)])
        r = vtc.vector_floor_temporal(v, 1, "day")
        assert pylist(r) == [self._dt(0, 0, 0)]

    def test_floor_5_minute_buckets(self):
        # 12:34:56 → floor to 5-minute bucket = 12:30:00
        v = self._make_ts_us([self._dt(12, 34, 56)])
        r = vtc.vector_floor_temporal(v, 5, "minute")
        assert pylist(r) == [self._dt(12, 30, 0)]

    def test_floor_5_minute_at_boundary(self):
        # 12:30:00 → 12:30:00 (already at boundary)
        v = self._make_ts_us([self._dt(12, 30, 0)])
        r = vtc.vector_floor_temporal(v, 5, "minute")
        assert pylist(r) == [self._dt(12, 30, 0)]

    def test_floor_2_hour_buckets(self):
        # 13:45:00 → floor to 2-hour bucket = 12:00:00
        v = self._make_ts_us([self._dt(13, 45, 0)])
        r = vtc.vector_floor_temporal(v, 2, "hour")
        assert pylist(r) == [self._dt(12, 0, 0)]

    def test_output_unit_preserved(self):
        v = make_ts([self._dt(12, 34, 56)], unit="ms")
        r = vtc.vector_floor_temporal(v, 1, "hour")
        assert r.type == dn.DrakenType.TIMESTAMP64
        assert r.logical_type_unit == "ms"

    def test_plural_unit_name(self):
        v = self._make_ts_us([self._dt(12, 34, 56)])
        r = vtc.vector_floor_temporal(v, 1, "minutes")
        assert pylist(r) == [self._dt(12, 34, 0)]

    def test_case_insensitive_unit(self):
        v = self._make_ts_us([self._dt(12, 34, 56)])
        r = vtc.vector_floor_temporal(v, 1, "HOUR")
        assert pylist(r) == [self._dt(12, 0, 0)]

    def test_pre_epoch_floor_to_hour(self):
        # 1969-12-31 22:34:56 UTC → floor to hour = 1969-12-31 22:00:00
        dt = datetime.datetime(1969, 12, 31, 22, 34, 56, tzinfo=datetime.timezone.utc)
        v = make_ts([dt])
        r = vtc.vector_floor_temporal(v, 1, "hour")
        expected = datetime.datetime(1969, 12, 31, 22, 0, 0, tzinfo=datetime.timezone.utc)
        assert pylist(r) == [expected]

    def test_null_tvl(self):
        dts = [self._dt(12, 34, 56), None, self._dt(7, 8, 9)]
        v = self._make_ts_us(dts)
        r = vtc.vector_floor_temporal(v, 1, "hour")
        result = pylist(r)
        assert result[0] == self._dt(12, 0, 0)
        assert result[1] is None
        assert result[2] == self._dt(7, 0, 0)

    def test_all_null(self):
        v = self._make_ts_us([None, None])
        r = vtc.vector_floor_temporal(v, 1, "hour")
        assert pylist(r) == [None, None]

    def test_value_error_on_unsupported_unit(self):
        v = self._make_ts_us([self._dt(0, 0, 0)])
        with pytest.raises(ValueError, match="unsupported unit"):
            vtc.vector_floor_temporal(v, 1, "month")

    def test_value_error_on_zero_magnitude(self):
        v = self._make_ts_us([self._dt(0, 0, 0)])
        with pytest.raises(ValueError, match="magnitude must be positive"):
            vtc.vector_floor_temporal(v, 0, "hour")

    def test_value_error_on_negative_magnitude(self):
        v = self._make_ts_us([self._dt(0, 0, 0)])
        with pytest.raises(ValueError, match="magnitude must be positive"):
            vtc.vector_floor_temporal(v, -1, "hour")

    def test_type_error_on_date32(self):
        v = make_date32([D_EPOCH])
        with pytest.raises(TypeError):
            vtc.vector_floor_temporal(v, 1, "hour")

    def test_type_error_on_non_vector(self):
        with pytest.raises(TypeError):
            vtc.vector_floor_temporal("not a vector", 1, "hour")

    def test_type_error_on_non_str_units(self):
        v = self._make_ts_us([self._dt(0, 0, 0)])
        with pytest.raises(TypeError, match="units must be a str"):
            vtc.vector_floor_temporal(v, 1, 42)


# ---------------------------------------------------------------------------
# 5. Cross-op round-trip
# ---------------------------------------------------------------------------

class TestRoundTrip:

    def test_date32_timestamp_date32_identity(self):
        dates = [
            datetime.date(1970, 1, 1),
            datetime.date(2000, 2, 28),
            datetime.date(2000, 2, 29),   # leap year
            datetime.date(2001, 2, 28),   # non-leap year
            datetime.date(2024, 12, 31),
            datetime.date(1960, 7, 4),    # pre-epoch
        ]
        v = make_date32(dates)
        ts = vtc.vector_date32_to_timestamp(v)
        back = vtc.vector_timestamp_to_date32(ts)
        assert pylist(back) == dates

    def test_unixtime_date32_consistency(self):
        # unixtime(date32(d)) == d_days * 86400
        import datetime
        d = datetime.date(2023, 1, 1)
        v = make_date32([d])
        ts = vtc.vector_date32_to_timestamp(v)
        # unixtime of the timestamp should match unixtime of the date32 directly
        via_ts   = pylist(vtc.vector_unixtime(ts))
        via_d32  = pylist(vtc.vector_unixtime(v))
        assert via_ts == via_d32

    def test_floor_then_unixtime_is_multiple(self):
        # floor to hour then unixtime must be a multiple of 3600
        dt = datetime.datetime(2024, 3, 15, 12, 34, 56, tzinfo=datetime.timezone.utc)
        v = make_ts([dt])
        floored = vtc.vector_floor_temporal(v, 1, "hour")
        unix = pylist(vtc.vector_unixtime(floored))
        assert unix[0] % 3600 == 0
