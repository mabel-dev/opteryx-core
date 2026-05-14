# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: initializedcheck=False

"""Temporal (date/timestamp/interval) comparison and arithmetic dispatch.

Cython migration of the former temporal_ops.py. Callers (comparisons.pyx,
arithmetic.pyx) reach in here once per WHERE predicate evaluated on a
temporal column, so the cost saved per call multiplies by row-batch count.

The kernels themselves (vec.equals_vector, etc.) are already Draken-native;
this layer is dispatch: pick the right kernel based on the right-hand
operand's class and the requested op string.
"""

from opteryx.compiled.vector_ops import vector_in_list
from opteryx.types import OrsoTypes

from draken.vectors.bool_vector import BoolVector
from draken.vectors.date32_vector import Date32Vector
from draken.vectors.int64_vector import Int64Vector
from draken.vectors.timestamp_vector import TimestampVector
from draken.interop.vector_sequence import vector_from_sequence

from .type_coercion import (
    _coerce_date32,
    _coerce_date32_set,
    _coerce_interval,
    _coerce_timestamp,
    _coerce_timestamp_set,
)


# Microseconds per day — used to convert Date32 (days since epoch) into
# Timestamp (microseconds since epoch) for cross-type comparisons.
DEF _US_PER_DAY = 86_400_000_000


cdef _convert_date32_to_timestamp_vector(d_vec):
    """Materialize a Date32Vector as a TimestampVector for cross-type ops."""
    d_values = d_vec.to_pylist()
    cdef list ts_values = [None if d is None else (<long long>d) * _US_PER_DAY for d in d_values]
    return vector_from_sequence(ts_values)


cdef _compare_timestamp_date32_vectors(str op, ts_vec, d_vec):
    """Compare a TimestampVector against a Date32Vector via native vector ops."""
    d_as_ts = _convert_date32_to_timestamp_vector(d_vec)
    if op == "Eq":
        return ts_vec.equals_vector(d_as_ts)
    if op == "NotEq":
        return ts_vec.not_equals_vector(d_as_ts)
    if op == "Lt":
        return ts_vec.less_than_vector(d_as_ts)
    if op == "Gt":
        return ts_vec.greater_than_vector(d_as_ts)
    if op == "LtEq":
        return ts_vec.less_than_or_equals_vector(d_as_ts)
    if op == "GtEq":
        return ts_vec.greater_than_or_equals_vector(d_as_ts)
    raise NotImplementedError(f"Timestamp/Date32 comparison: unsupported op {op!r}")


cpdef _int64_temporal_compare(str op, vec, right, temporal_type):
    cdef bint is_timestamp
    cdef bint is_date

    if right is None:
        return BoolVector(len(vec))

    is_timestamp = temporal_type == OrsoTypes.TIMESTAMP
    is_date = temporal_type == OrsoTypes.DATE
    if not (is_timestamp or is_date):
        raise NotImplementedError(
            f"_int64_temporal_compare: non-temporal type {temporal_type!r}"
        )

    if isinstance(right, (list, tuple, set, frozenset)):
        if is_timestamp:
            value_set = _coerce_timestamp_set(right)
        else:
            value_set = _coerce_date32_set(right)
    elif isinstance(right, (Int64Vector, TimestampVector, Date32Vector)):
        # Physically int64, temporal semantics: native vector-vector dispatch.
        if op == "Eq":
            return vec.equals_vector(right)
        if op == "Lt":
            return vec.less_than_vector(right)
        if op == "Gt":
            return vec.greater_than_vector(right)
        if op == "LtEq":
            return vec.less_than_or_equals_vector(right)
        if op == "GtEq":
            return vec.greater_than_or_equals_vector(right)
        raise NotImplementedError(
            f"{type(right).__name__} temporal vector-vector: unsupported op {op!r}"
        )
    else:
        if is_timestamp:
            value = _coerce_timestamp(right)
        else:
            value = _coerce_date32(right)

    if op == "Eq":
        return vec.equals(value)
    if op == "Lt":
        return vec.less_than(value)
    if op == "Gt":
        return vec.greater_than(value)
    if op == "LtEq":
        return vec.less_than_or_equals(value)
    if op == "GtEq":
        return vec.greater_than_or_equals(value)
    if op == "InList":
        return vector_in_list(vec, value_set)
    raise NotImplementedError(f"Int64Vector temporal: unsupported op {op!r}")


cpdef _timestamp_compare(str op, vec, right):
    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_timestamp_set(right)
    elif isinstance(right, TimestampVector):
        if op == "Eq":
            return vec.equals_vector(right)
        if op == "NotEq":
            return vec.not_equals_vector(right)
        if op == "Lt":
            return vec.less_than_vector(right)
        if op == "Gt":
            return vec.greater_than_vector(right)
        if op == "LtEq":
            return vec.less_than_or_equals_vector(right)
        if op == "GtEq":
            return vec.greater_than_or_equals_vector(right)
        raise NotImplementedError(f"TimestampVector vector-vector: unsupported op {op!r}")
    elif isinstance(right, Date32Vector):
        return _compare_timestamp_date32_vectors(op, vec, right)
    else:
        value = _coerce_timestamp(right)
        if value is None:
            return BoolVector(len(vec))

    if op == "Eq":
        return vec.equals(value)
    if op == "Lt":
        return vec.less_than(value)
    if op == "Gt":
        return vec.greater_than(value)
    if op == "LtEq":
        return vec.less_than_or_equals(value)
    if op == "GtEq":
        return vec.greater_than_or_equals(value)
    if op == "InList":
        return vector_in_list(vec, value_set)
    raise NotImplementedError(f"TimestampVector: unsupported op {op!r}")


cpdef _date32_compare(str op, vec, right):
    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_date32_set(right)
    elif isinstance(right, Date32Vector):
        if op == "Eq":
            return vec.equals_vector(right)
        if op == "Lt":
            return vec.less_than_vector(right)
        if op == "Gt":
            return vec.greater_than_vector(right)
        if op == "LtEq":
            return vec.less_than_or_equals_vector(right)
        if op == "GtEq":
            return vec.greater_than_or_equals_vector(right)
        raise NotImplementedError(f"Date32Vector vector-vector: unsupported op {op!r}")
    elif isinstance(right, TimestampVector):
        return _compare_timestamp_date32_vectors(op, right, vec)
    else:
        value = _coerce_date32(right)

    if op == "Eq":
        return vec.equals(value)
    if op == "Lt":
        return vec.less_than(value)
    if op == "Gt":
        return vec.greater_than(value)
    if op == "LtEq":
        return vec.less_than_or_equals(value)
    if op == "GtEq":
        return vec.greater_than_or_equals(value)
    if op == "InList":
        return vector_in_list(vec, value_set)
    raise NotImplementedError(f"Date32Vector: unsupported op {op!r}")


cpdef _interval_compare(str op, vec, right):
    if right is None:
        return BoolVector(len(vec))

    literal = _coerce_interval(right)
    if op == "Eq":
        return vec.equals(literal)
    if op == "Lt":
        return vec.less_than(literal)
    if op == "Gt":
        return vec.greater_than(literal)
    if op == "LtEq":
        return vec.less_than_or_equals(literal)
    if op == "GtEq":
        return vec.greater_than_or_equals(literal)
    raise NotImplementedError(f"IntervalVector: unsupported op {op!r}")


cpdef _date_minus_date_draken(left_vec, right_vec):
    # Date/timestamp subtraction needs a native IntervalVector constructor,
    # which is not yet wired through. Fail explicitly rather than masking it.
    raise NotImplementedError(
        "Date/timestamp subtraction requires a native IntervalVector constructor; "
        "Arrow-backed interval materialization has been removed."
    )


# Intervals are detected by class name to avoid a hard dependency on the
# IntervalVector type at module import time.
cdef frozenset _INTERVAL_TYPES = frozenset(("IntervalVector",))


cpdef _date_interval_op_draken(left_vec, right_vec, str op):
    from opteryx.expression.intervals import _as_interval_vector

    cdef int signum = 1 if op == "Plus" else -1

    if type(right_vec).__name__ in _INTERVAL_TYPES:
        date_vec = left_vec
        interval_vec = _as_interval_vector(right_vec)
    else:
        date_vec = right_vec
        interval_vec = _as_interval_vector(left_vec)

    return interval_vec.apply_to_temporal(date_vec, signum)
