# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The native value a literal of a given type holds (native plan graph P3-c,
architect rulings 2026-09-27): strings as UTF-8 bytes, temporals as their
physical ints, INTERVAL as a (months, microseconds) pair, collections as tuples.
A literal refuses any other value when it is placed (expressions._check_literal).
"""

import datetime
from typing import Any

from opteryx.compiled.planner.logical_category import LogicalCategory
from opteryx.types.timestamps._datetime_conversion import date_to_int64_days
from opteryx.types.timestamps._datetime_conversion import time_to_int64_us
from opteryx.types.timestamps._datetime_conversion import timestamp_to_int64_us


_STRING_CATEGORIES = (
    LogicalCategory.VARCHAR,
    LogicalCategory.NVARCHAR,
    LogicalCategory.VARBINARY,
)


def native_literal_value(value: Any, column_type):
    """`value` in the native form a literal of `column_type` holds (architect
    rulings 2026-09-27, P3-c): strings as UTF-8 bytes, dates / timestamps / times as
    their physical ints, collections as tuples of native elements. Anything else is
    returned as it is - the literal refuses a value that is not its type's native
    form when it is placed."""
    if value is None or column_type is None:
        return value
    category = column_type.category
    value_type = type(value)
    if category in _STRING_CATEGORIES and value_type is str:
        return value.encode("utf-8")
    if category is LogicalCategory.TIMESTAMP and value_type in (datetime.datetime, datetime.date):
        return timestamp_to_int64_us(value)
    if category is LogicalCategory.DATE and value_type in (datetime.datetime, datetime.date):
        return date_to_int64_days(value)
    if category is LogicalCategory.TIME and value_type is datetime.time:
        return time_to_int64_us(value)
    if category is LogicalCategory.ARRAY and value_type in (list, tuple):
        element = column_type.element
        return tuple(native_literal_value(item, element) for item in value)
    if category is LogicalCategory.VECTOR and value_type in (list, tuple):
        return tuple(value)
    return value


def literal_order_key(value):
    """Sort key giving IN-list values one deterministic order: a string literal
    (UTF-8 bytes) by its text, anything else by `str` - the order these lists had
    when string literals were `str`."""
    if type(value) is bytes:
        return value.decode("utf-8", "surrogateescape")
    return str(value)
