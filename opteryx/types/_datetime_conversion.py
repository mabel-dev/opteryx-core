# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Datetime conversion helpers for Phase 6b.2 (NumPy-free temporal representation).

This module provides conversion functions between Python datetime types and
native int64 representations:
- Dates: int64 (days since Unix epoch: 1970-01-01)
- Timestamps: int64 (microseconds since Unix epoch: 1970-01-01 00:00:00 UTC)

All timezone-aware datetimes are assumed to be in UTC. This module does not
handle timezone conversion.
"""

import datetime
from typing import Any, Union


def timestamp_to_int64_us(value: Any) -> int:
    """
    Convert any datetime-like value to int64 microseconds since Unix epoch.

    Supports:
    - int: assumed to be microseconds since epoch (returned as-is)
    - datetime.datetime: converted to UTC microseconds since epoch
    - datetime.date: converted to midnight UTC on that date in microseconds

    Args:
        value: A datetime-like value (int, datetime.datetime, or datetime.date)

    Returns:
        int64 microseconds since 1970-01-01 00:00:00 UTC

    Raises:
        TypeError: If value is not a supported type

    Examples:
        >>> timestamp_to_int64_us(0)  # Unix epoch
        0
        >>> timestamp_to_int64_us(datetime.datetime(1970, 1, 1, 0, 0, 0))
        0
        >>> timestamp_to_int64_us(datetime.date(1970, 1, 1))
        0
    """
    if isinstance(value, int):
        # Already in microseconds, return as-is
        return value

    if isinstance(value, datetime.datetime):
        # Convert datetime to microseconds since epoch
        # Ensure datetime is timezone-aware (treat naive datetimes as UTC)
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        else:
            # Convert to UTC if in a different timezone
            value = value.astimezone(datetime.timezone.utc)
        # datetime.timestamp() returns seconds since epoch (float)
        # Multiply by 1_000_000 to get microseconds
        return int(value.timestamp() * 1_000_000)

    if isinstance(value, datetime.date):
        # Convert date to datetime at midnight UTC, then to microseconds
        dt = datetime.datetime(value.year, value.month, value.day, tzinfo=datetime.timezone.utc)
        return int(dt.timestamp() * 1_000_000)

    raise TypeError(
        f"Cannot convert {type(value).__name__} to timestamp. "
        f"Expected int, datetime.datetime, or datetime.date"
    )


def date_to_int64_days(value: Any) -> int:
    """
    Convert any date-like value to int64 days since Unix epoch.

    Supports:
    - int: assumed to be days since epoch (returned as-is)
    - datetime.datetime: day part extracted and converted to days since epoch
    - datetime.date: converted to days since epoch

    Args:
        value: A date-like value (int, datetime.datetime, or datetime.date)

    Returns:
        int64 days since 1970-01-01

    Raises:
        TypeError: If value is not a supported type

    Examples:
        >>> date_to_int64_days(0)  # Unix epoch
        0
        >>> date_to_int64_days(datetime.date(1970, 1, 1))
        0
        >>> date_to_int64_days(datetime.datetime(1970, 1, 2))
        1
    """
    if isinstance(value, int):
        # Already in days, return as-is
        return value

    if isinstance(value, datetime.datetime):
        # Extract date part and convert
        return date_to_int64_days(value.date())

    if isinstance(value, datetime.date):
        # Convert date to days since epoch (1970-01-01)
        # Reference epoch
        epoch = datetime.date(1970, 1, 1)
        delta = value - epoch
        return delta.days

    raise TypeError(
        f"Cannot convert {type(value).__name__} to date. "
        f"Expected int, datetime.datetime, or datetime.date"
    )


def int64_us_to_datetime(value: int) -> datetime.datetime:
    """
    Convert int64 microseconds since epoch to datetime.datetime (UTC).

    Args:
        value: int64 microseconds since 1970-01-01 00:00:00 UTC

    Returns:
        datetime.datetime in UTC (timezone-aware)

    Examples:
        >>> int64_us_to_datetime(0)
        datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
    """
    # Convert microseconds to seconds (datetime.fromtimestamp expects seconds)
    seconds = value / 1_000_000
    return datetime.datetime.fromtimestamp(seconds, tz=datetime.timezone.utc)


def int64_days_to_date(value: int) -> datetime.date:
    """
    Convert int64 days since epoch to datetime.date.

    Args:
        value: int64 days since 1970-01-01

    Returns:
        datetime.date

    Examples:
        >>> int64_days_to_date(0)
        datetime.date(1970, 1, 1)
        >>> int64_days_to_date(1)
        datetime.date(1970, 1, 2)
    """
    epoch = datetime.date(1970, 1, 1)
    return epoch + datetime.timedelta(days=value)


__all__ = [
    "timestamp_to_int64_us",
    "date_to_int64_days",
    "int64_us_to_datetime",
    "int64_days_to_date",
]
