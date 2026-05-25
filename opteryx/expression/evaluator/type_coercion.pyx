"""Type coercion utilities for Draken vector operations.

Cython migration of the former type_coercion.py. cimported by every kernel
file in this package, so every per-call saving compounds quickly.

The coercion functions take a Python literal (or single-element typed
constant vector) and return the int / bytes / etc. that downstream Draken
kernels expect. _is_null_as_boolvector branches on vector shape to produce
a BoolVector of "is null" flags using native Draken APIs.
"""

import datetime
import decimal

from draken.vectors.bool_vector import BoolVector
from draken.vectors.date32_vector import Date32Vector
from draken.vectors.float64_vector import Float64Vector
from draken.vectors.integer64_vector import Integer64Vector
from draken.vectors.interval_vector import IntervalVector
from draken.vectors.string_vector import StringVector
from draken.vectors.time_vector import TimeVector
from draken.vectors.timestamp_vector import TimestampVector

from draken.vectors.vector cimport Vector

from opteryx.compiled.nanobind.vector_bool_ops import (
    bool_vector_from_int8_mask,
    bool_vector_from_inverted_null_bitmap,
)


cdef object _EPOCH_DATE = datetime.date(1970, 1, 1)
cdef object _EPOCH_DATETIME = datetime.datetime(1970, 1, 1)


cpdef object _dictionary_arrow_type(vec):
    """Return the dictionary value-type for a Draken dictionary vector, else None."""
    if not type(vec).__module__.startswith("draken.vectors."):
        return None
    return getattr(vec, "dictionary_value_type", None)


cpdef bint _is_dictionary_encoded_vector(vec):
    return _dictionary_arrow_type(vec) is not None


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


# Vector classes whose null layout is a plain int8 mask (fixed-width buffer).
# StringVector and IntervalVector are intentionally excluded — their null
# layouts are handled by different branches in _is_null_as_boolvector.
cdef tuple _FIXED_BUFFER_VECTORS = (
    BoolVector,
    Integer64Vector,
    Date32Vector,
    IntervalVector,
    TimestampVector,
    TimeVector,
)


cpdef _is_null_as_boolvector(vec):
    """Produce a BoolVector flagging null entries of `vec`.

    Branches:
      - DICTIONARY-encoded: dispatch through whichever native is_null* method
        the vector implementation exposes.
      - Fixed-width buffer vectors: native int8 null mask.
      - Float64Vector: native is_null_with_nan() so NaN counts as null.
      - StringVector / fallback: native null_bitmap or is_null().
    """
    cdef Py_ssize_t n = len(vec)

    if _is_dictionary_encoded_vector(vec):
        is_null_bv = getattr(vec, "is_null_boolvector", None)
        if is_null_bv is not None:
            return is_null_bv()
        is_null_nan = getattr(vec, "is_null_with_nan", None)
        if is_null_nan is not None:
            return bool_vector_from_int8_mask(is_null_nan(), n)
        is_null = getattr(vec, "is_null", None)
        if is_null is not None:
            return bool_vector_from_int8_mask(is_null(), n)
        return BoolVector(n)

    if isinstance(vec, Float64Vector):
        return bool_vector_from_int8_mask(vec.is_null_with_nan(), n)

    if isinstance(vec, _FIXED_BUFFER_VECTORS):
        return bool_vector_from_int8_mask(vec.is_null(), n)

    nb = vec.null_bitmap()
    if nb is not None:
        return bool_vector_from_inverted_null_bitmap(nb, n)
    if getattr(vec, "null_count", 0) == 0:
        return BoolVector(n)

    if isinstance(vec, StringVector):
        is_null = getattr(vec, "is_null", None)
        if is_null is not None:
            return bool_vector_from_int8_mask(is_null(), n)

    raise TypeError(
        "Null-mask evaluation requires native Draken vector null APIs; "
        f"unsupported vector type {type(vec).__name__!r}."
    )
