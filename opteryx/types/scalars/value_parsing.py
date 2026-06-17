"""Coerce a Python value to the canonical representation for a logical type.

Parsing is a *value* concern, not a *type* concern — it does not belong as a method
on the type enum. These functions take the target `LogicalCategory` and a value and
return the coerced value. Used by CAST, literal coercion, and array-element casting.
"""

import datetime
import decimal
from typing import Any, Callable

from opteryx.types.logical_type import LogicalCategory

__all__ = ["parse_value", "parser_for"]


def _parse_boolean(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
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
    return bytes(value)


def _parse_date(value):
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, str):
        parts = value.strip().split("-")
        if len(parts) == 3:
            return datetime.date(int(parts[0]), int(parts[1]), int(parts[2]))
    raise ValueError(f"Cannot parse {value} as date")


def _parse_time(value):
    if isinstance(value, datetime.time):
        return value
    if isinstance(value, str):
        parts = value.strip().split(":")
        if len(parts) >= 2:
            return datetime.time(int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) > 2 else 0)
    raise ValueError(f"Cannot parse {value} as time")


def _parse_timestamp(value):
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time())
    if isinstance(value, str):
        value_str = value.strip().replace("T", " ")
        try:
            if "." in value_str:
                return datetime.datetime.strptime(value_str, "%Y-%m-%d %H:%M:%S.%f")
            return datetime.datetime.strptime(value_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return datetime.datetime.strptime(value_str.split(" ")[0], "%Y-%m-%d")
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
