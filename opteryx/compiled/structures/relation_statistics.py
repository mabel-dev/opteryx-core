# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Lightweight helpers for converting values to int64 orderable representations.

The original Cython RelationStatistics class has been removed; only the
`to_int` helper remains because it is used by pruning logic and vector
fallbacks. This pure-Python implementation preserves the previous value
mappings and sentinel behaviour.
"""

import datetime
import math
from decimal import Decimal
from typing import Any

_INT64_HIGH_BIT = 1 << 63
NULL_FLAG = -_INT64_HIGH_BIT
MIN_SIGNED_64BIT = NULL_FLAG + 1
MAX_SIGNED_64BIT = _INT64_HIGH_BIT - 1


def _ensure_64bit_range(val: int) -> int:
    if val < MIN_SIGNED_64BIT:
        return MIN_SIGNED_64BIT
    if val > MAX_SIGNED_64BIT:
        return MAX_SIGNED_64BIT
    return val


def _encode_bytes(buf: bytes) -> int:
    """Encode up to seven bytes into a signed 64-bit integer."""
    padded = bytearray(8)
    length = min(len(buf), 7)
    padded[1 : 1 + length] = buf[:length]
    return _ensure_64bit_range(int.from_bytes(padded, byteorder="big", signed=True))


def to_int(value: Any) -> int:
    """Convert a value to a signed 64-bit integer for ordering comparisons."""
    if value is None:
        return NULL_FLAG

    # Normalise numpy/pandas scalars to Python types when possible
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass

    if isinstance(value, bool):
        value = int(value)

    if isinstance(value, int):
        return _ensure_64bit_range(value)

    if isinstance(value, float):
        if math.isinf(value):
            return MAX_SIGNED_64BIT if value > 0 else MIN_SIGNED_64BIT
        if math.isnan(value):
            return NULL_FLAG
        return _ensure_64bit_range(int(round(value)))

    if isinstance(value, datetime.datetime):
        return _ensure_64bit_range(int(round(value.timestamp())))

    if isinstance(value, datetime.date):
        # Days since epoch (1970-01-01)
        return _ensure_64bit_range(int(value.strftime("%s")))

    if isinstance(value, datetime.time):
        seconds = value.hour * 3600 + value.minute * 60 + value.second
        return _ensure_64bit_range(seconds)

    if isinstance(value, Decimal):
        return _ensure_64bit_range(int(round(value)))

    if isinstance(value, str):
        return _encode_bytes(value.encode())

    if isinstance(value, (bytes, bytearray, memoryview)):
        return _encode_bytes(bytes(value))

    return NULL_FLAG


__all__ = ["to_int", "NULL_FLAG", "MIN_SIGNED_64BIT", "MAX_SIGNED_64BIT"]
