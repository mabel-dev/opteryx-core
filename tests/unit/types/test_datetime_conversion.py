# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Unit tests for datetime conversion helper module (_datetime_conversion.py).

Tests cover:
- Epoch conversions (1970-01-01)
- Various date/datetime formats
- Round-trip conversions
- Error handling for invalid types
"""

import datetime

import pytest

from opteryx.types._datetime_conversion import (
    date_to_int64_days,
    int64_days_to_date,
    int64_us_to_datetime,
    timestamp_to_int64_us,
)


class TestTimestampToInt64Us:
    """Tests for timestamp_to_int64_us conversion function."""

    def test_int_value_returned_as_is(self):
        """int values (already in microseconds) should be returned unchanged."""
        assert timestamp_to_int64_us(0) == 0
        assert timestamp_to_int64_us(1_000_000) == 1_000_000
        assert timestamp_to_int64_us(-1_000_000) == -1_000_000

    def test_datetime_epoch(self):
        """datetime at Unix epoch (1970-01-01 00:00:00 UTC) should convert to 0."""
        dt = datetime.datetime(1970, 1, 1, 0, 0, 0)
        assert timestamp_to_int64_us(dt) == 0

    def test_datetime_one_second_after_epoch(self):
        """datetime one second after epoch should convert to 1_000_000 microseconds."""
        dt = datetime.datetime(1970, 1, 1, 0, 0, 1)
        assert timestamp_to_int64_us(dt) == 1_000_000

    def test_datetime_one_microsecond_after_epoch(self):
        """datetime one microsecond after epoch conversion (loses microsecond precision)."""
        # Python datetime.timestamp() only has microsecond precision
        dt = datetime.datetime(1970, 1, 1, 0, 0, 0, 1)
        result = timestamp_to_int64_us(dt)
        assert result == 1

    def test_datetime_year_2000(self):
        """datetime in year 2000 should convert correctly."""
        # 2000-01-01 is 10957 days after 1970-01-01
        # 10957 days * 86400 seconds/day = 946_684_800 seconds
        # 946_684_800 * 1_000_000 = 946_684_800_000_000 microseconds
        dt = datetime.datetime(2000, 1, 1, 0, 0, 0)
        result = timestamp_to_int64_us(dt)
        assert result == 946_684_800_000_000

    def test_datetime_negative_timestamp(self):
        """datetime before 1970 should convert to negative microseconds."""
        dt = datetime.datetime(1969, 12, 31, 23, 59, 59)
        result = timestamp_to_int64_us(dt)
        assert result < 0

    def test_date_epoch(self):
        """date at epoch (1970-01-01) should convert to 0."""
        d = datetime.date(1970, 1, 1)
        assert timestamp_to_int64_us(d) == 0

    def test_date_one_day_after_epoch(self):
        """date one day after epoch should convert to 86400_000_000 microseconds."""
        d = datetime.date(1970, 1, 2)
        result = timestamp_to_int64_us(d)
        # 1 day = 86400 seconds = 86_400_000_000 microseconds
        assert result == 86_400_000_000

    def test_date_year_2000(self):
        """date in year 2000 should convert correctly."""
        d = datetime.date(2000, 1, 1)
        result = timestamp_to_int64_us(d)
        # Should match the datetime conversion
        assert result == 946_684_800_000_000

    def test_invalid_type_raises_error(self):
        """invalid type should raise TypeError."""
        with pytest.raises(TypeError):
            timestamp_to_int64_us("2000-01-01")
        with pytest.raises(TypeError):
            timestamp_to_int64_us(3.14)
        with pytest.raises(TypeError):
            timestamp_to_int64_us(None)


class TestDateToInt64Days:
    """Tests for date_to_int64_days conversion function."""

    def test_int_value_returned_as_is(self):
        """int values (already in days) should be returned unchanged."""
        assert date_to_int64_days(0) == 0
        assert date_to_int64_days(365) == 365
        assert date_to_int64_days(-1) == -1

    def test_date_epoch(self):
        """date at epoch (1970-01-01) should convert to 0."""
        d = datetime.date(1970, 1, 1)
        assert date_to_int64_days(d) == 0

    def test_date_one_day_after_epoch(self):
        """date one day after epoch should convert to 1."""
        d = datetime.date(1970, 1, 2)
        assert date_to_int64_days(d) == 1

    def test_date_one_year_after_epoch(self):
        """date one year after epoch (1971-01-01) should convert to 365."""
        d = datetime.date(1971, 1, 1)
        assert date_to_int64_days(d) == 365

    def test_date_year_2000(self):
        """date in year 2000 should convert correctly."""
        # 2000-01-01 is 10957 days after 1970-01-01
        d = datetime.date(2000, 1, 1)
        assert date_to_int64_days(d) == 10957

    def test_date_before_epoch(self):
        """date before 1970 should convert to negative days."""
        d = datetime.date(1969, 12, 31)
        result = date_to_int64_days(d)
        assert result == -1

    def test_datetime_epoch(self):
        """datetime at epoch should convert to 0."""
        dt = datetime.datetime(1970, 1, 1, 12, 30, 45)
        # Date part should be 0 (time part ignored)
        assert date_to_int64_days(dt) == 0

    def test_datetime_one_day_after_epoch(self):
        """datetime one day after epoch should convert to 1."""
        dt = datetime.datetime(1970, 1, 2, 12, 30, 45)
        assert date_to_int64_days(dt) == 1

    def test_invalid_type_raises_error(self):
        """invalid type should raise TypeError."""
        with pytest.raises(TypeError):
            date_to_int64_days("1970-01-01")
        with pytest.raises(TypeError):
            date_to_int64_days(3.14)
        with pytest.raises(TypeError):
            date_to_int64_days(None)


class TestInt64UsToDatetime:
    """Tests for int64_us_to_datetime conversion function."""

    def test_zero_converts_to_epoch(self):
        """0 microseconds should convert to epoch."""
        result = int64_us_to_datetime(0)
        expected = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert result == expected

    def test_one_million_microseconds(self):
        """1_000_000 microseconds = 1 second."""
        result = int64_us_to_datetime(1_000_000)
        expected = datetime.datetime(1970, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc)
        assert result == expected

    def test_year_2000(self):
        """946_684_800_000_000 microseconds should convert to 2000-01-01."""
        result = int64_us_to_datetime(946_684_800_000_000)
        expected = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert result == expected

    def test_negative_timestamp(self):
        """negative microseconds should convert to dates before 1970."""
        result = int64_us_to_datetime(-86_400_000_000)  # 1 day before epoch
        expected = datetime.datetime(1969, 12, 31, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert result == expected

    def test_result_is_timezone_aware_utc(self):
        """result should be timezone-aware UTC."""
        result = int64_us_to_datetime(0)
        assert result.tzinfo == datetime.timezone.utc


class TestInt64DaysToDate:
    """Tests for int64_days_to_date conversion function."""

    def test_zero_converts_to_epoch(self):
        """0 days should convert to epoch."""
        result = int64_days_to_date(0)
        expected = datetime.date(1970, 1, 1)
        assert result == expected

    def test_one_day_after_epoch(self):
        """1 day should convert to 1970-01-02."""
        result = int64_days_to_date(1)
        expected = datetime.date(1970, 1, 2)
        assert result == expected

    def test_year_2000(self):
        """10957 days should convert to 2000-01-01."""
        result = int64_days_to_date(10957)
        expected = datetime.date(2000, 1, 1)
        assert result == expected

    def test_negative_days_before_epoch(self):
        """negative days should convert to dates before 1970."""
        result = int64_days_to_date(-1)
        expected = datetime.date(1969, 12, 31)
        assert result == expected


class TestRoundTripConversions:
    """Test round-trip conversions (value -> int -> value)."""

    def test_datetime_round_trip(self):
        """datetime -> int64_us -> datetime should preserve value."""
        original = datetime.datetime(2000, 6, 15, 12, 30, 45)
        us = timestamp_to_int64_us(original)
        result = int64_us_to_datetime(us)
        # Microsecond precision may be lost due to datetime.timestamp() precision
        assert result.date() == original.date()
        assert result.hour == original.hour
        assert result.minute == original.minute
        assert result.second == original.second

    def test_date_round_trip(self):
        """date -> int64_days -> date should preserve value."""
        original = datetime.date(2000, 6, 15)
        days = date_to_int64_days(original)
        result = int64_days_to_date(days)
        assert result == original

    def test_epoch_round_trip_datetime(self):
        """epoch datetime should survive round-trip."""
        original = datetime.datetime(1970, 1, 1, 0, 0, 0)
        us = timestamp_to_int64_us(original)
        result = int64_us_to_datetime(us)
        assert result == original.replace(tzinfo=datetime.timezone.utc)

    def test_epoch_round_trip_date(self):
        """epoch date should survive round-trip."""
        original = datetime.date(1970, 1, 1)
        days = date_to_int64_days(original)
        result = int64_days_to_date(days)
        assert result == original


class TestConsistency:
    """Test consistency between conversion functions."""

    def test_timestamp_date_consistency(self):
        """timestamp from date should match date conversion to microseconds."""
        d = datetime.date(2000, 1, 1)
        # Convert date to timestamp (midnight UTC)
        ts_us = timestamp_to_int64_us(d)
        # Convert date to days, then to microseconds
        days = date_to_int64_days(d)
        days_as_us = days * 86_400_000_000
        assert ts_us == days_as_us

    def test_multiple_dates_increasing_order(self):
        """consecutive dates should increase by exactly 1 day (in microseconds)."""
        d1 = datetime.date(2000, 6, 15)
        d2 = datetime.date(2000, 6, 16)
        days1 = date_to_int64_days(d1)
        days2 = date_to_int64_days(d2)
        assert days2 - days1 == 1
