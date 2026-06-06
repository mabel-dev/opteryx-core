"""Temporal (date/timestamp/interval) comparison and arithmetic dispatch.

Cython migration of the former temporal_ops.py. Callers (comparisons.pyx,
arithmetic.pyx) reach in here once per WHERE predicate evaluated on a
temporal column, so the cost saved per call multiplies by row-batch count.

The kernels themselves (vec.equals_vector, etc.) are already Draken-native;
this layer is dispatch: pick the right kernel based on the right-hand
operand's class and the requested op string.
"""

from libc.stdint cimport int16_t

from opteryx.compiled.nanobind.vector_misc import vector_in_list
from opteryx.types import SqlType

from draken.vectors.bool_vector import BoolVector
from draken.vectors.vector import Vector as _TemporalVector
import draken.draken_native as _draken_native
from draken.draken_native import vector_from_sequence



# Microseconds per day — used to convert Date32 (days since epoch) into
# Timestamp (microseconds since epoch) for cross-type comparisons.
DEF _US_PER_DAY = 86_400_000_000


cdef _convert_date32_to_timestamp_vector(d_vec):
    """Materialize a Date32Vector as a TimestampVector for cross-type ops."""
    d_values = d_vec.to_pylist()
    cdef list ts_values = [None if d is None else (<long long>d) * _US_PER_DAY for d in d_values]
    return vector_from_sequence(ts_values)


cdef _compare_timestamp_date32_vectors(int op_code, ts_vec, d_vec):
    """Compare a TimestampVector against a Date32Vector via native vector ops."""
    d_as_ts = _convert_date32_to_timestamp_vector(d_vec)
    return ts_vec._compare_vector(d_as_ts, _DRAKEN_CMP_OP[op_code])


cdef _int64_temporal_compare(int op_code, vec, right, int16_t temporal_type):
    """Compare an int64-backed temporal vector against `right`.

    `temporal_type` is a BCTypeCode integer (BC_TYPE_DATE=1, BC_TYPE_TIMESTAMP=2).
    """
    cdef bint is_timestamp
    cdef bint is_date

    if right is None:
        return BoolVector(len(vec))

    is_timestamp = temporal_type == BC_TYPE_TIMESTAMP
    is_date = temporal_type == BC_TYPE_DATE
    if not (is_timestamp or is_date):
        raise NotImplementedError(
            f"_int64_temporal_compare: non-temporal type {temporal_type!r}"
        )

    if isinstance(right, (list, tuple, set, frozenset)):
        if is_timestamp:
            value_set = _coerce_timestamp_set(right)
        else:
            value_set = _coerce_date32_set(right)
    elif isinstance(right, _TemporalVector):
        return vec._compare_vector(right, _DRAKEN_CMP_OP[op_code])
    else:
        if is_timestamp:
            value = _coerce_timestamp(right)
        else:
            value = _coerce_date32(right)

    if op_code == OP_IN_LIST:
        return vector_in_list(vec, value_set)
    return vec._compare_scalar(value, _DRAKEN_CMP_OP[op_code])


cdef _timestamp_compare(int op_code, vec, right):
    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_timestamp_set(right)
    elif isinstance(right, _TemporalVector) and right.type == _draken_native.TIMESTAMP64:
        return vec._compare_vector(right, _DRAKEN_CMP_OP[op_code])
    elif isinstance(right, _TemporalVector) and right.type == _draken_native.DATE32:
        return _compare_timestamp_date32_vectors(op_code, vec, right)
    else:
        value = _coerce_timestamp(right)
        if value is None:
            return BoolVector(len(vec))

    if op_code == OP_IN_LIST:
        return vector_in_list(vec, value_set)
    return vec._compare_scalar(value, _DRAKEN_CMP_OP[op_code])


cdef _date32_compare(int op_code, vec, right):
    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_date32_set(right)
    elif isinstance(right, _TemporalVector) and right.type == _draken_native.DATE32:
        return vec._compare_vector_op(right, _DRAKEN_CMP_OP[op_code])
    elif isinstance(right, _TemporalVector) and right.type == _draken_native.TIMESTAMP64:
        return _compare_timestamp_date32_vectors(op_code, right, vec)
    else:
        value = _coerce_date32(right)

    if op_code == OP_IN_LIST:
        return vector_in_list(vec, value_set)
    return vec._compare_scalar(value, _DRAKEN_CMP_OP[op_code])


cdef _interval_compare(int op_code, vec, right):
    if right is None:
        return BoolVector(len(vec))

    literal = _coerce_interval(right)
    if op_code == OP_EQ:
        return vec.equals(literal)
    if op_code == OP_LT:
        return vec.less_than(literal)
    if op_code == OP_GT:
        return vec.greater_than(literal)
    if op_code == OP_LT_EQ:
        return vec.less_than_or_equals(literal)
    if op_code == OP_GT_EQ:
        return vec.greater_than_or_equals(literal)
    raise NotImplementedError(f"IntervalVector: unsupported op (code {op_code})")


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
