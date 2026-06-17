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
from opteryx.types.logical_type import LogicalCategory

from datetime import datetime as _datetime
from datetime import time as _time
from datetime import timezone as _timezone

from draken.vectors.bool_vector import BoolVector
from draken.vectors.vector import Vector as _TemporalVector
import draken.draken_native as _draken_native
from draken.draken_native import vector_timestamp_from_sequence


cdef _convert_date32_to_timestamp_vector(d_vec, str unit):
    """Materialize a Date32Vector as a TIMESTAMP64 vector for cross-type ops.

    Each DATE is promoted to a UTC timestamp at midnight (DuckDB semantics),
    in the same storage `unit` as the timestamp side so `compare_vector` (which
    rejects cross-unit comparison) sees matching units. The dates arrive from
    `to_pylist()` as `datetime.date`; combine each with midnight and let the
    native factory do the calendar→instant conversion.
    """
    d_values = d_vec.to_pylist()
    cdef list ts_values = [
        None if d is None
        else _datetime.combine(d, _time.min, tzinfo=_timezone.utc)
        for d in d_values
    ]
    return vector_timestamp_from_sequence(ts_values, unit, 0)


cdef _compare_timestamp_date32_vectors(int op_code, ts_vec, d_vec):
    """Compare a TimestampVector against a Date32Vector via native vector ops."""
    cdef str unit = ts_vec._nb.logical_type_unit
    d_as_ts = _convert_date32_to_timestamp_vector(d_vec, unit)
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


# NOTE: `_unwrap_nb` is defined in arithmetic.pyx and shared across the single
# _impl translation unit (both are `include`d by _impl.pyx). Do not redefine it
# here — that produces a duplicate-symbol compile error.


cpdef _date_minus_date_draken(left_vec, right_vec):
    """date/timestamp − date/timestamp → INTERVAL (µs delta, months=0).

    Matches the engine operator_map (DATE−DATE / TIMESTAMP−TIMESTAMP → INTERVAL).
    Delegates to the native temporal_minus_temporal kernel; returns a nanobind
    INTERVAL Vector (the executor re-wraps via BC_RESULT_NEEDS_NB_WRAP).
    """
    cdef object left_nb = _unwrap_nb(left_vec)
    cdef object right_nb = _unwrap_nb(right_vec)
    return left_nb.temporal_minus_temporal(right_nb)


cpdef _date_interval_op_draken(left_vec, right_vec, str op):
    """date/timestamp ± interval → TIMESTAMP (calendar month day-clamping).

    Operands may arrive in either order; the interval side is identified by its
    DRAKEN_INTERVAL physical type. Delegates to the native apply_to_temporal
    kernel on the temporal vector. Returns a nanobind TIMESTAMP64 Vector.
    """
    import draken.draken_native as _dn

    cdef int signum = 1 if op == "Plus" else -1
    cdef object left_nb = _unwrap_nb(left_vec)
    cdef object right_nb = _unwrap_nb(right_vec)

    if right_nb.type == _dn.INTERVAL:
        return left_nb.apply_to_temporal(right_nb, signum)
    return right_nb.apply_to_temporal(left_nb, signum)


cpdef _interval_interval_op_draken(left_vec, right_vec, str op):
    """interval ± interval → INTERVAL (component-wise months/µs).

    Delegates to the native interval_add / interval_sub kernels. Returns a
    nanobind INTERVAL Vector.
    """
    cdef object left_nb = _unwrap_nb(left_vec)
    cdef object right_nb = _unwrap_nb(right_vec)
    if op == "Plus":
        return left_nb.interval_add(right_nb)
    return left_nb.interval_sub(right_nb)
