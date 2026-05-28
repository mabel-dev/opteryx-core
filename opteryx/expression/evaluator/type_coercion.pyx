"""Type coercion utilities for Draken vector operations.

Cython migration of the former type_coercion.py. cimported by every kernel
file in this package, so every per-call saving compounds quickly.

The coercion functions take a Python literal (or single-element typed
constant vector) and return the int / bytes / etc. that downstream Draken
kernels expect.
"""

import datetime
import decimal

from draken.vectors.vector cimport Vector


cdef object _EPOCH_DATE = datetime.date(1970, 1, 1)
cdef object _EPOCH_DATETIME = datetime.datetime(1970, 1, 1)


cpdef bytes _coerce_str(value):
    if isinstance(value, Vector):
        value = None if len(value) == 0 else value[0]
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode()
    return str(value).encode()


cpdef frozenset _coerce_str_set(values):
    cdef set out = set()
    for v in values:
        out.add(_coerce_str(v))
    return frozenset(out)


cpdef double _coerce_float(value):
    if isinstance(value, decimal.Decimal):
        return float(value)
    return <double>value


cpdef frozenset _coerce_float_set(values):
    cdef set out = set()
    for v in values:
        out.add(_coerce_float(v))
    return frozenset(out)


cpdef long long _coerce_int64(value):
    if isinstance(value, Vector):
        value = None if len(value) == 0 else value[0]
    if isinstance(value, datetime.datetime):
        return <long long>(value.timestamp() * 1_000)
    if isinstance(value, datetime.date):
        return (value - _EPOCH_DATE).days
    return <long long>int(value)


cpdef frozenset _coerce_int64_set(values):
    cdef set out = set()
    for v in values:
        out.add(_coerce_int64(v))
    return frozenset(out)


cpdef long long _coerce_date32(value):
    if isinstance(value, Vector):
        value = None if len(value) == 0 else value[0]
    if isinstance(value, datetime.datetime):
        return (value.date() - _EPOCH_DATE).days
    if isinstance(value, datetime.date):
        return (value - _EPOCH_DATE).days
    return <long long>int(value)


cpdef frozenset _coerce_date32_set(values):
    cdef set out = set()
    for v in values:
        out.add(_coerce_date32(v))
    return frozenset(out)


cpdef long long _coerce_timestamp(value):
    if isinstance(value, Vector):
        value = None if len(value) == 0 else value[0]
    if isinstance(value, (bytes, bytearray, memoryview, str)):
        # Lazy: parse_timestamp_value lives in opteryx.expression.casts which
        # imports back through this package — keep the import inline to
        # break the cycle.
        from opteryx.expression.casts import parse_timestamp_value
        value = parse_timestamp_value(value)
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return <long long>((value - _EPOCH_DATETIME).total_seconds() * 1_000_000)
    if isinstance(value, datetime.date):
        return <long long>(
            (
                datetime.datetime(value.year, value.month, value.day) - _EPOCH_DATETIME
            ).total_seconds()
            * 1_000_000
        )
    return <long long>int(value)


cpdef frozenset _coerce_timestamp_set(values):
    cdef set out = set()
    for v in values:
        out.add(_coerce_timestamp(v))
    return frozenset(out)


cpdef tuple _coerce_interval(value):
    if isinstance(value, (tuple, list)) and len(value) == 2:
        return (int(value[0]), int(value[1]))
    raise TypeError(f"Cannot coerce {type(value)!r} to interval literal")


cpdef object _coerce_temporal_scalar_for_arrow(value, target_type):
    # Imported lazily for the same circular-import reason as _coerce_timestamp.
    from opteryx.expression.casts import parse_timestamp_value
    from opteryx.types import OrsoTypes

    if target_type == OrsoTypes.DATE:
        if isinstance(value, datetime.datetime):
            return value.date()
        if isinstance(value, datetime.date):
            return value
        if isinstance(value, int):
            return _EPOCH_DATE + datetime.timedelta(days=value)
        return parse_timestamp_value(value).date()

    cdef long long ivalue
    if target_type == OrsoTypes.TIMESTAMP:
        if isinstance(value, int):
            ivalue = value
            # Suspiciously-small "timestamp" that's actually days-since-epoch
            # in microseconds (legacy callers). Unwrap rather than mis-parse.
            if abs(ivalue) < 100_000_000_000 and ivalue % 1_000_000 == 0:
                return datetime.datetime(1970, 1, 1) + datetime.timedelta(
                    days=ivalue // 1_000_000
                )
        return parse_timestamp_value(value)

    return value


