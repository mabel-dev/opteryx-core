"""
Native + parity tests for E.13 (Phase 12): temporal arithmetic cluster via
vector_temporal_arith consumer.

Loads the nanobind extension without triggering opteryx/__init__.py,
following the spec_from_file_location pattern established in E.9–E.12.

Coverage:
  vector_date_part:
    year / month / day / quarter / dayofyear / dayofweek / hour / minute / second
    TIMESTAMP64 input (unit=us), all parts
    DATE32 input — hour/minute/second always 0
    leap-year day (2024-02-29 exists, dayofyear=60)
    year boundary (2023-12-31 → year=2023, dayofyear=365)
    epoch (1970-01-01) — dayofweek = Thursday = 3 (0=Monday)
    pre-epoch timestamp (negative ticks)
    null TVL: null row → null output
    empty vector
    TypeError on non-DATE32/TIMESTAMP64 input
    ValueError on unknown part name

  vector_date_diff:
    days: 2024-01-01 to 2025-01-01 = 366 (2024 is a leap year)
    days: negative diff when end < start
    weeks, months (approx), years (approx)
    seconds/minutes/hours
    null TVL: null in either input → null output
    TypeError on non-TIMESTAMP64 input
    ValueError on unsupported part

  vector_date_trunc:
    month: 2023-06-15 12:34:56 → 2023-06-01 00:00:00
    year:  2023-06-15 → 2023-01-01 00:00:00
    quarter: 2023-05-01 → 2023-04-01 (Q2 starts April)
    week:  floor to Monday; 2023-06-14 (Wednesday) → 2023-06-12 (Monday)
    day / hour / minute / second (integer alignment)
    DATE32 input → TIMESTAMP64 in microseconds
    pre-epoch truncation
    null TVL
    D.8 invariant: output carries correct unit descriptor
    ValueError on unsupported unit

  vector_date_format:
    "%Y-%m-%d" on known date → "2023-01-15"
    "%H:%M:%S" on timestamp with time component
    null input → null output
    unsupported token raises ValueError
    DATE32 input
    TIMESTAMP64 input
    round-trip: format → parse back to same value
"""

import datetime
import glob
import importlib.util
import os

import draken.draken_native as dn
import pytest


# ---------------------------------------------------------------------------
# Load vector_temporal_arith extension
# ---------------------------------------------------------------------------

def _load_temporal_arith():
    pattern = os.path.join(
        os.path.dirname(__file__), "..", "..", "..",
        "opteryx", "compiled", "nanobind", "vector_temporal_arith*.so"
    )
    matches = glob.glob(pattern)
    if not matches:
        pytest.skip(
            "vector_temporal_arith extension not built — run make compile first",
            allow_module_level=True,
        )
    spec = importlib.util.spec_from_file_location(
        "opteryx.compiled.nanobind.vector_temporal_arith", matches[0]
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ta = _load_temporal_arith()


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------

def ts_us(values, nulls=None):
    """Sequence of datetime objects or None → TIMESTAMP64 (us)."""
    return dn.vector_timestamp_from_sequence(values, unit="us", offset_minutes=0)


def ts_const(dt, n, unit="us"):
    return dn.vector_timestamp_from_constant(dt, n, unit=unit, offset_minutes=0)


def date32(values):
    return dn.vector_date32_from_sequence(values)


def date32_const(d, n):
    return dn.vector_date32_from_constant(d, n)


def dt(year, month, day, hour=0, minute=0, second=0, tz=datetime.timezone.utc):
    return datetime.datetime(year, month, day, hour, minute, second, tzinfo=tz)


def pylist(v):
    return v.to_pylist()


# Reference datetimes
EPOCH      = dt(1970, 1, 1)           # Thursday
DATE_2023  = dt(2023, 6, 15, 12, 34, 56)
DATE_2024  = dt(2024, 1, 1)           # leap year start
DATE_LEAP  = dt(2024, 2, 29)          # leap day
DATE_2025  = dt(2025, 1, 1)
PRE_EPOCH  = dt(1969, 12, 31)

US = datetime.timezone.utc


# ===========================================================================
# vector_date_part — TIMESTAMP64
# ===========================================================================

class TestDatePartTimestamp:
    def test_year(self):
        v = ts_us([DATE_2023])
        assert pylist(ta.vector_date_part(v, "year")) == [2023]

    def test_month(self):
        v = ts_us([DATE_2023])
        assert pylist(ta.vector_date_part(v, "month")) == [6]

    def test_day(self):
        v = ts_us([DATE_2023])
        assert pylist(ta.vector_date_part(v, "day")) == [15]

    def test_quarter(self):
        v = ts_us([DATE_2023])
        assert pylist(ta.vector_date_part(v, "quarter")) == [2]

    def test_quarter_q1(self):
        v = ts_us([dt(2023, 1, 15)])
        assert pylist(ta.vector_date_part(v, "quarter")) == [1]

    def test_quarter_q4(self):
        v = ts_us([dt(2023, 10, 1)])
        assert pylist(ta.vector_date_part(v, "quarter")) == [4]

    def test_dayofyear_ordinary(self):
        # 2023-06-15: Jan=31, Feb=28, Mar=31, Apr=30, May=31 = 151 days, + 15 = 166
        v = ts_us([DATE_2023])
        result = pylist(ta.vector_date_part(v, "dayofyear"))[0]
        assert result == 166

    def test_dayofyear_leap(self):
        # 2024-02-29: Jan=31, Feb=29 = day 60
        v = ts_us([DATE_LEAP])
        result = pylist(ta.vector_date_part(v, "dayofyear"))[0]
        assert result == 60

    def test_dayofyear_year_end(self):
        v = ts_us([dt(2023, 12, 31)])
        result = pylist(ta.vector_date_part(v, "dayofyear"))[0]
        assert result == 365  # 2023 is not a leap year

    def test_dayofweek_epoch_thursday(self):
        # 1970-01-01 = Thursday, 0=Monday → dayofweek=3
        v = ts_us([EPOCH])
        assert pylist(ta.vector_date_part(v, "dayofweek")) == [3]

    def test_dayofweek_monday(self):
        # 1970-01-05 = Monday → 0
        v = ts_us([dt(1970, 1, 5)])
        assert pylist(ta.vector_date_part(v, "dayofweek")) == [0]

    def test_dayofweek_sunday(self):
        # 1970-01-04 = Sunday → 6
        v = ts_us([dt(1970, 1, 4)])
        assert pylist(ta.vector_date_part(v, "dayofweek")) == [6]

    def test_dayofweek_alias_dow(self):
        v = ts_us([EPOCH])
        assert pylist(ta.vector_date_part(v, "dow")) == [3]

    def test_hour(self):
        v = ts_us([DATE_2023])  # 12:34:56
        assert pylist(ta.vector_date_part(v, "hour")) == [12]

    def test_minute(self):
        v = ts_us([DATE_2023])
        assert pylist(ta.vector_date_part(v, "minute")) == [34]

    def test_second(self):
        v = ts_us([DATE_2023])
        assert pylist(ta.vector_date_part(v, "second")) == [56]

    def test_pre_epoch_year(self):
        v = ts_us([PRE_EPOCH])
        assert pylist(ta.vector_date_part(v, "year")) == [1969]

    def test_pre_epoch_month(self):
        v = ts_us([PRE_EPOCH])
        assert pylist(ta.vector_date_part(v, "month")) == [12]

    def test_pre_epoch_day(self):
        v = ts_us([PRE_EPOCH])
        assert pylist(ta.vector_date_part(v, "day")) == [31]

    def test_null_tvl(self):
        v = ts_us([None, DATE_2023])
        result = pylist(ta.vector_date_part(v, "year"))
        assert result[0] is None
        assert result[1] == 2023

    def test_all_null(self):
        v = ts_us([None, None])
        result = pylist(ta.vector_date_part(v, "month"))
        assert all(x is None for x in result)

    def test_empty_vector(self):
        v = ts_us([])
        result = pylist(ta.vector_date_part(v, "year"))
        assert result == []

    def test_case_insensitive_part(self):
        v = ts_us([DATE_2023])
        assert pylist(ta.vector_date_part(v, "YEAR")) == [2023]
        assert pylist(ta.vector_date_part(v, "Year")) == [2023]

    def test_unknown_part_raises(self):
        v = ts_us([DATE_2023])
        with pytest.raises(ValueError, match="unsupported part"):
            ta.vector_date_part(v, "decade")

    def test_output_type_int64(self):
        v = ts_us([DATE_2023])
        result = ta.vector_date_part(v, "year")
        assert result.type == dn.INT64

    def test_ms_unit(self):
        v = dn.vector_timestamp_from_sequence([DATE_2023], unit="ms", offset_minutes=0)
        assert pylist(ta.vector_date_part(v, "year")) == [2023]
        assert pylist(ta.vector_date_part(v, "hour")) == [12]

    def test_ns_unit(self):
        v = dn.vector_timestamp_from_sequence([DATE_2023], unit="ns", offset_minutes=0)
        assert pylist(ta.vector_date_part(v, "year")) == [2023]
        assert pylist(ta.vector_date_part(v, "second")) == [56]


# ===========================================================================
# vector_date_part — DATE32
# ===========================================================================

class TestDatePartDate32:
    def test_year(self):
        v = date32([datetime.date(2023, 6, 15)])
        assert pylist(ta.vector_date_part(v, "year")) == [2023]

    def test_month(self):
        v = date32([datetime.date(2023, 6, 15)])
        assert pylist(ta.vector_date_part(v, "month")) == [6]

    def test_day(self):
        v = date32([datetime.date(2023, 6, 15)])
        assert pylist(ta.vector_date_part(v, "day")) == [15]

    def test_hour_is_zero(self):
        v = date32([datetime.date(2023, 6, 15)])
        assert pylist(ta.vector_date_part(v, "hour")) == [0]

    def test_minute_is_zero(self):
        v = date32([datetime.date(2023, 6, 15)])
        assert pylist(ta.vector_date_part(v, "minute")) == [0]

    def test_second_is_zero(self):
        v = date32([datetime.date(2023, 6, 15)])
        assert pylist(ta.vector_date_part(v, "second")) == [0]

    def test_leap_day(self):
        v = date32([datetime.date(2024, 2, 29)])
        assert pylist(ta.vector_date_part(v, "year"))  == [2024]
        assert pylist(ta.vector_date_part(v, "month")) == [2]
        assert pylist(ta.vector_date_part(v, "day"))   == [29]

    def test_dayofweek_monday(self):
        # 2023-06-12 = Monday
        v = date32([datetime.date(2023, 6, 12)])
        assert pylist(ta.vector_date_part(v, "dayofweek")) == [0]

    def test_null_tvl(self):
        v = date32([None, datetime.date(2023, 1, 1)])
        result = pylist(ta.vector_date_part(v, "year"))
        assert result[0] is None
        assert result[1] == 2023

    def test_output_type_int64(self):
        v = date32([datetime.date(2023, 1, 1)])
        result = ta.vector_date_part(v, "year")
        assert result.type == dn.INT64


# ===========================================================================
# vector_date_part — error cases
# ===========================================================================

def test_date_part_wrong_type_raises():
    v = dn.vector_int32_from_sequence([1, 2, 3])
    with pytest.raises(TypeError):
        ta.vector_date_part(v, "year")


# ===========================================================================
# vector_date_diff
# ===========================================================================

class TestDateDiff:
    def test_days_leap_year(self):
        # 2024 is a leap year: 2024-01-01 to 2025-01-01 = 366 days
        start = ts_us([DATE_2024])
        end   = ts_us([DATE_2025])
        assert pylist(ta.vector_date_diff(start, end, "days")) == [366]

    def test_days_non_leap(self):
        # 2023-01-01 to 2024-01-01 = 365 days (2023 has no leap)
        start = ts_us([dt(2023, 1, 1)])
        end   = ts_us([dt(2024, 1, 1)])
        assert pylist(ta.vector_date_diff(start, end, "days")) == [365]

    def test_days_negative(self):
        # end < start → negative result
        start = ts_us([DATE_2025])
        end   = ts_us([DATE_2024])
        assert pylist(ta.vector_date_diff(start, end, "days")) == [-366]

    def test_seconds(self):
        start = ts_us([dt(2023, 1, 1, 0, 0, 0)])
        end   = ts_us([dt(2023, 1, 1, 1, 0, 0)])
        assert pylist(ta.vector_date_diff(start, end, "seconds")) == [3600]

    def test_minutes(self):
        start = ts_us([dt(2023, 1, 1, 0, 0, 0)])
        end   = ts_us([dt(2023, 1, 1, 1, 30, 0)])
        assert pylist(ta.vector_date_diff(start, end, "minutes")) == [90]

    def test_hours(self):
        start = ts_us([dt(2023, 1, 1, 0, 0, 0)])
        end   = ts_us([dt(2023, 1, 2, 0, 0, 0)])
        assert pylist(ta.vector_date_diff(start, end, "hours")) == [24]

    def test_weeks(self):
        start = ts_us([dt(2023, 1, 1)])
        end   = ts_us([dt(2023, 1, 22)])
        # 21 days = 3 weeks
        assert pylist(ta.vector_date_diff(start, end, "weeks")) == [3]

    def test_months_approximate(self):
        # 90 days ÷ 30 = 3 months (approx, matching old code)
        start = ts_us([dt(2023, 1, 1)])
        end   = ts_us([dt(2023, 4, 1)])  # exactly 90 days
        result = pylist(ta.vector_date_diff(start, end, "months"))[0]
        assert result == 3

    def test_years_approximate(self):
        # 2023-01-01 to 2026-01-01 = 365+366+365 = 1096 days ÷ 365 = 3 years (approx)
        start = ts_us([dt(2023, 1, 1)])
        end   = ts_us([dt(2026, 1, 1)])
        result = pylist(ta.vector_date_diff(start, end, "years"))[0]
        assert result == 3

    def test_singular_form(self):
        # "day" == "days"
        start = ts_us([DATE_2024])
        end   = ts_us([DATE_2025])
        assert pylist(ta.vector_date_diff(start, end, "day")) == [366]

    def test_null_start(self):
        start = ts_us([None])
        end   = ts_us([DATE_2025])
        result = pylist(ta.vector_date_diff(start, end, "days"))
        assert result[0] is None

    def test_null_end(self):
        start = ts_us([DATE_2024])
        end   = ts_us([None])
        result = pylist(ta.vector_date_diff(start, end, "days"))
        assert result[0] is None

    def test_both_null(self):
        start = ts_us([None])
        end   = ts_us([None])
        result = pylist(ta.vector_date_diff(start, end, "days"))
        assert result[0] is None

    def test_output_type_int64(self):
        start = ts_us([DATE_2024])
        end   = ts_us([DATE_2025])
        result = ta.vector_date_diff(start, end, "days")
        assert result.type == dn.INT64

    def test_microseconds(self):
        start = ts_us([dt(2023, 1, 1, 0, 0, 0)])
        end   = ts_us([dt(2023, 1, 1, 0, 0, 1)])  # 1 second = 1_000_000 us
        assert pylist(ta.vector_date_diff(start, end, "microseconds")) == [1_000_000]

    def test_mixed_units(self):
        # start in ms, end in us — normalised to microseconds internally
        start = dn.vector_timestamp_from_sequence([dt(2023, 1, 1)], unit="ms")
        end   = dn.vector_timestamp_from_sequence([dt(2023, 1, 2)], unit="us")
        result = pylist(ta.vector_date_diff(start, end, "days"))[0]
        assert result == 1

    def test_wrong_type_raises(self):
        bad = dn.vector_int32_from_sequence([1000000])
        good = ts_us([DATE_2024])
        with pytest.raises(TypeError):
            ta.vector_date_diff(bad, good, "days")

    def test_unsupported_part_raises(self):
        start = ts_us([DATE_2024])
        end   = ts_us([DATE_2025])
        with pytest.raises(ValueError, match="unsupported part"):
            ta.vector_date_diff(start, end, "decade")


# ===========================================================================
# vector_date_trunc
# ===========================================================================

class TestDateTrunc:
    def _trunc(self, dt_val, unit, input_unit="us"):
        v = dn.vector_timestamp_from_sequence([dt_val], unit=input_unit)
        result = ta.vector_date_trunc(v, unit)
        return result.to_pylist()[0]

    def test_month(self):
        result = self._trunc(DATE_2023, "month")
        assert result == datetime.datetime(2023, 6, 1, tzinfo=US)

    def test_year(self):
        result = self._trunc(DATE_2023, "year")
        assert result == datetime.datetime(2023, 1, 1, tzinfo=US)

    def test_quarter_q2(self):
        # June is in Q2 → truncate to April 1
        result = self._trunc(DATE_2023, "quarter")
        assert result == datetime.datetime(2023, 4, 1, tzinfo=US)

    def test_quarter_q1(self):
        result = self._trunc(dt(2023, 2, 15), "quarter")
        assert result == datetime.datetime(2023, 1, 1, tzinfo=US)

    def test_quarter_q3(self):
        result = self._trunc(dt(2023, 8, 20), "quarter")
        assert result == datetime.datetime(2023, 7, 1, tzinfo=US)

    def test_week_wednesday_to_monday(self):
        # 2023-06-14 = Wednesday → should go to 2023-06-12 (Monday)
        result = self._trunc(dt(2023, 6, 14), "week")
        assert result == datetime.datetime(2023, 6, 12, tzinfo=US)

    def test_week_already_monday(self):
        result = self._trunc(dt(2023, 6, 12), "week")
        assert result == datetime.datetime(2023, 6, 12, tzinfo=US)

    def test_week_sunday_to_prev_monday(self):
        # 2023-06-11 = Sunday → 2023-06-05 (Monday)
        result = self._trunc(dt(2023, 6, 11), "week")
        assert result == datetime.datetime(2023, 6, 5, tzinfo=US)

    def test_day(self):
        result = self._trunc(DATE_2023, "day")
        assert result == datetime.datetime(2023, 6, 15, tzinfo=US)

    def test_hour(self):
        result = self._trunc(DATE_2023, "hour")
        assert result == datetime.datetime(2023, 6, 15, 12, tzinfo=US)

    def test_minute(self):
        result = self._trunc(DATE_2023, "minute")
        assert result == datetime.datetime(2023, 6, 15, 12, 34, tzinfo=US)

    def test_second(self):
        v = dn.vector_timestamp_from_sequence([dt(2023, 6, 15, 12, 34, 56)], unit="us")
        result = ta.vector_date_trunc(v, "second")
        # Second truncation strips sub-second; for us input with no sub-second, identity
        assert result.to_pylist()[0] == datetime.datetime(2023, 6, 15, 12, 34, 56, tzinfo=US)

    def test_date32_input_month(self):
        v = date32([datetime.date(2023, 6, 15)])
        result = ta.vector_date_trunc(v, "month")
        r = result.to_pylist()[0]
        assert r == datetime.datetime(2023, 6, 1, tzinfo=US)

    def test_date32_input_year(self):
        v = date32([datetime.date(2024, 2, 29)])
        result = ta.vector_date_trunc(v, "year")
        r = result.to_pylist()[0]
        assert r == datetime.datetime(2024, 1, 1, tzinfo=US)

    def test_date32_output_is_timestamp64(self):
        v = date32([datetime.date(2023, 6, 15)])
        result = ta.vector_date_trunc(v, "month")
        assert result.type == dn.TIMESTAMP64

    def test_timestamp_output_same_unit(self):
        v = dn.vector_timestamp_from_sequence([DATE_2023], unit="ms")
        result = ta.vector_date_trunc(v, "month")
        assert result.logical_type_unit == "ms"

    def test_pre_epoch_month(self):
        pre = dt(1969, 6, 15, 12, 0, 0)
        result = self._trunc(pre, "month")
        assert result == datetime.datetime(1969, 6, 1, tzinfo=US)

    def test_pre_epoch_year(self):
        pre = dt(1969, 11, 20)
        result = self._trunc(pre, "year")
        assert result == datetime.datetime(1969, 1, 1, tzinfo=US)

    def test_null_tvl(self):
        v = dn.vector_timestamp_from_sequence([None, DATE_2023], unit="us")
        result = ta.vector_date_trunc(v, "month")
        r = result.to_pylist()
        assert r[0] is None
        assert r[1] == datetime.datetime(2023, 6, 1, tzinfo=US)

    def test_output_type_timestamp64(self):
        v = ts_us([DATE_2023])
        result = ta.vector_date_trunc(v, "year")
        assert result.type == dn.TIMESTAMP64

    def test_unsupported_unit_raises(self):
        v = ts_us([DATE_2023])
        with pytest.raises(ValueError, match="unsupported unit"):
            ta.vector_date_trunc(v, "decade")

    def test_leap_year_month_boundary(self):
        # 2024-02-15 → month trunc → 2024-02-01
        result = self._trunc(dt(2024, 2, 15), "month")
        assert result == datetime.datetime(2024, 2, 1, tzinfo=US)


# ===========================================================================
# vector_date_format
# ===========================================================================

class TestDateFormat:
    def test_iso_date_format(self):
        v = ts_us([dt(2023, 1, 15)])
        result = pylist(ta.vector_date_format(v, "%Y-%m-%d"))
        assert result == ["2023-01-15"]

    def test_time_format(self):
        v = ts_us([dt(2023, 1, 15, 12, 34, 56)])
        result = pylist(ta.vector_date_format(v, "%H:%M:%S"))
        assert result == ["12:34:56"]

    def test_datetime_format(self):
        v = ts_us([dt(2023, 1, 15, 12, 34, 56)])
        result = pylist(ta.vector_date_format(v, "%Y-%m-%d %H:%M:%S"))
        assert result == ["2023-01-15 12:34:56"]

    def test_date32_input(self):
        v = date32([datetime.date(2023, 1, 15)])
        result = pylist(ta.vector_date_format(v, "%Y-%m-%d"))
        assert result == ["2023-01-15"]

    def test_null_input_null_output(self):
        v = ts_us([None, dt(2023, 1, 15)])
        result = pylist(ta.vector_date_format(v, "%Y-%m-%d"))
        assert result[0] is None
        assert result[1] == "2023-01-15"

    def test_all_null(self):
        v = ts_us([None, None])
        result = pylist(ta.vector_date_format(v, "%Y-%m-%d"))
        assert all(x is None for x in result)

    def test_output_type_varchar(self):
        v = ts_us([dt(2023, 1, 15)])
        result = ta.vector_date_format(v, "%Y-%m-%d")
        assert result.type == dn.VARCHAR

    def test_unsupported_token_raises(self):
        v = ts_us([dt(2023, 1, 15)])
        with pytest.raises(ValueError, match="unsupported format token"):
            ta.vector_date_format(v, "%Y-%Q-%d")  # %Q is not standard

    def test_trailing_percent_raises(self):
        v = ts_us([dt(2023, 1, 15)])
        with pytest.raises(ValueError):
            ta.vector_date_format(v, "%Y-%m-%d%")

    def test_literal_percent_escaped(self):
        v = ts_us([dt(2023, 1, 15)])
        result = pylist(ta.vector_date_format(v, "100%%"))
        assert result == ["100%"]

    def test_bytes_format_accepted(self):
        v = ts_us([dt(2023, 1, 15)])
        result = pylist(ta.vector_date_format(v, b"%Y-%m-%d"))
        assert result == ["2023-01-15"]

    def test_wrong_input_type_raises(self):
        v = dn.vector_int32_from_sequence([12345])
        with pytest.raises(TypeError):
            ta.vector_date_format(v, "%Y-%m-%d")

    def test_epoch_format(self):
        v = ts_us([EPOCH])
        result = pylist(ta.vector_date_format(v, "%Y-%m-%d"))
        assert result == ["1970-01-01"]

    def test_pre_epoch_format(self):
        v = ts_us([dt(1969, 12, 31)])
        result = pylist(ta.vector_date_format(v, "%Y-%m-%d"))
        assert result == ["1969-12-31"]

    def test_empty_format(self):
        v = ts_us([dt(2023, 1, 15)])
        result = pylist(ta.vector_date_format(v, ""))
        assert result == [""]

    def test_empty_vector(self):
        v = ts_us([])
        result = pylist(ta.vector_date_format(v, "%Y-%m-%d"))
        assert result == []

    def test_round_trip_date(self):
        d = datetime.date(2024, 2, 29)
        v = date32([d])
        result = pylist(ta.vector_date_format(v, "%Y-%m-%d"))[0]
        assert datetime.date.fromisoformat(result) == d

    def test_long_format_string(self):
        # "%A, %B %d, %Y" produces e.g. "Sunday, January 15, 2023" (> 12 chars → arena slot)
        v = ts_us([dt(2023, 1, 15)])
        result = pylist(ta.vector_date_format(v, "%A, %B %d, %Y"))[0]
        # Just verify it contains the year
        assert "2023" in result
