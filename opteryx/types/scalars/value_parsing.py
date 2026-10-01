"""Coerce a Python value to the canonical representation for a logical type.

Parsing is a *value* concern, not a *type* concern — it does not belong as a method
on the type enum. These functions take the target `LogicalCategory` and a value and
return the coerced value. Used by CAST, literal coercion, and array-element casting.
"""

import datetime
import decimal
import re
from typing import Any, Callable

from opteryx.types.logical_type import LogicalCategory

__all__ = ["parse_value", "parser_for"]


_BOOLEAN_TRUE_STRINGS = ("true", "1", "yes", "on")
_BOOLEAN_FALSE_STRINGS = ("false", "0", "no", "off")


def _parse_boolean(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in _BOOLEAN_TRUE_STRINGS:
            return True
        if lowered in _BOOLEAN_FALSE_STRINGS:
            return False
        raise ValueError(f"Cannot parse {value!r} as boolean")
    return bool(value)


def _parse_integer(value):
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        return int(value.strip())
    return int(value)


def _parse_double(value):
    if isinstance(value, float):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        return float(value.strip())
    return float(value)


def _parse_decimal(value):
    if isinstance(value, decimal.Decimal):
        return value
    if isinstance(value, (int, float)):
        return decimal.Decimal(str(value))
    if isinstance(value, str):
        return decimal.Decimal(value.strip())
    return decimal.Decimal(value)


def _parse_varchar(value):
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _parse_blob(value):
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    # Everything else is the VARCHAR rendering, encoded — the same bytes the
    # runtime cast produces (CAST(id AS BLOB) on a column gives b'1', b'2', and
    # CAST(gravity AS BLOB) gives b'3.7'). This used to be bytes(value), whose
    # int overload builds a ZERO BUFFER OF THAT LENGTH: CAST(42 AS BLOB) folded
    # to 42 zero bytes instead of b'42', and CAST(3232235777 AS BLOB) allocated
    # ~3GB at plan time from a one-line query. Routing through _parse_varchar
    # rather than a second str() keeps one rendering rule for both targets, so
    # BLOB cannot drift from VARCHAR.
    return _parse_varchar(value).encode("utf-8")


def _parse_date(value):
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, str):
        # No .strip(). The runtime kernel's parse_iso_date (cast_string.cpp)
        # requires every character between the dashes to be a digit, so
        # '2021-02-21 ' is a hard error on a column; stripping it here let the
        # literal path quietly succeed where the column path fails. int() would
        # also swallow surrounding whitespace on each part, so the parts are
        # digit-checked rather than handed straight to int().
        parts = value.split("-")
        # isascii() as well as isdigit(): the kernel tests bytes against '0'-'9',
        # so non-ASCII digits ('٢٠٢١') are a reject there and must be here too.
        if len(parts) == 3 and all(p.isascii() and p.isdigit() for p in parts):
            return datetime.date(int(parts[0]), int(parts[1]), int(parts[2]))
    raise ValueError(f"Cannot parse {value} as date")


def _parse_time(value):
    if isinstance(value, datetime.time):
        return value
    if isinstance(value, str):
        parts = value.strip().split(":")
        if len(parts) >= 2:
            second = 0
            microsecond = 0
            if len(parts) > 2:
                sec_str, dot, frac_str = parts[2].partition(".")
                second = int(sec_str)
                if dot:
                    microsecond = int((frac_str + "000000")[:6])
            return datetime.time(int(parts[0]), int(parts[1]), second, microsecond)
    raise ValueError(f"Cannot parse {value} as time")


# Mirrors draken/core/iso_datetime.h parse_iso_timestamp, which the column
# cast uses — the two must accept exactly the same text or a literal and a
# column holding it disagree.
#
# Seconds are OPTIONAL: 'YYYY-MM-DDTHH:MM' reads back as HH:MM:00.
# Fractional seconds take any number of digits (Cloud Logging emits up to 9 and
# trims trailing zeros); past microseconds they are TRUNCATED, matching
# datetime.fromisoformat, not rounded or rejected.
# A zone suffix ('Z', '+HH:MM', '-HH:MM', '+HHMM') is only valid after a time.
_TIMESTAMP_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})"
    r"(?:[T ](?P<hour>\d{2}):(?P<minute>\d{2})(?::(?P<second>\d{2})(?:\.(?P<frac>\d+))?)?"
    r"(?:(?P<zulu>Z)|(?P<sign>[+-])(?P<oh>\d{2}):?(?P<om>\d{2}))?)?$",
    re.ASCII,  # the kernel tests bytes against '0'-'9'; '\d' alone admits '٢'
)


def _parse_timestamp(value):
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time())
    if isinstance(value, str):
        match = _TIMESTAMP_RE.match(value.strip())
        if match is None:
            raise ValueError(f"Cannot parse {value} as timestamp")
        date = datetime.date.fromisoformat(match.group("date"))
        if match.group("hour") is None:
            return datetime.datetime.combine(date, datetime.time())
        frac = match.group("frac") or ""
        result = datetime.datetime.combine(
            date,
            datetime.time(
                int(match.group("hour")),
                int(match.group("minute")),
                int(match.group("second") or 0),
                int((frac + "000000")[:6]),
            ),
        )
        # Timestamps are stored naive UTC (see LogicalCategory.TIMESTAMP docs): an
        # offset is honoured by normalising to UTC, never discarded — dropping it
        # would silently shift the value by the offset.
        if match.group("sign"):
            oh, om = int(match.group("oh")), int(match.group("om"))
            if oh > 23 or om > 59:
                raise ValueError(f"Cannot parse {value} as timestamp")
            offset = datetime.timedelta(hours=oh, minutes=om)
            result = result - offset if match.group("sign") == "+" else result + offset
        return result
    raise ValueError(f"Cannot parse {value} as timestamp")


def _parse_interval(value):
    if isinstance(value, datetime.timedelta):
        return value
    if isinstance(value, (int, float)):
        return datetime.timedelta(seconds=value)
    raise ValueError(f"Cannot parse {value} as interval")


_PARSERS: dict = {
    LogicalCategory.BOOLEAN: _parse_boolean,
    LogicalCategory.INTEGER: _parse_integer,
    LogicalCategory.FLOAT: _parse_double,
    LogicalCategory.DECIMAL: _parse_decimal,
    LogicalCategory.VARCHAR: _parse_varchar,
    LogicalCategory.NVARCHAR: _parse_varchar,
    LogicalCategory.VARIANT: _parse_varchar,
    LogicalCategory.VARBINARY: _parse_blob,
    LogicalCategory.DATE: _parse_date,
    LogicalCategory.TIME: _parse_time,
    LogicalCategory.TIMESTAMP: _parse_timestamp,
    LogicalCategory.INTERVAL: _parse_interval,
}


def parser_for(t: LogicalCategory) -> Callable[[Any], Any]:
    """Return the coercion callable for a type (identity if no parser)."""
    return _PARSERS.get(t, lambda v: v)


def parse_value(t: LogicalCategory, value: Any) -> Any:
    """Coerce `value` to the canonical Python representation for type `t`.

    Returns None for None; returns the value unchanged on parse failure or when no
    parser exists for the type (matches the historical lenient behaviour).
    """
    if value is None:
        return None
    parser = _PARSERS.get(t)
    if parser is None:
        return value
    try:
        return parser(value)
    except (ValueError, TypeError, AttributeError):
        return value
