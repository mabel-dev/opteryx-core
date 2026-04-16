"""Temporal (date/timestamp/interval) comparison operations."""

import datetime

from opteryx.compiled.vector_ops import vector_in_list
from opteryx.exceptions import ColumnReferencedBeforeEvaluationError

from .function_execution import _is_draken_vector, apply_bounded_function

# Encoding type identifiers for interval detection
_INTERVAL_TYPES = frozenset(("IntervalVector",))
from .type_coercion import (
    _coerce_date32,
    _coerce_date32_set,
    _coerce_float,
    _coerce_float_set,
    _coerce_int64,
    _coerce_int64_set,
    _coerce_interval,
    _coerce_str,
    _coerce_str_set,
    _coerce_temporal_scalar_for_arrow,
    _coerce_timestamp,
    _coerce_timestamp_set,
    _constant_scalar_value,
    _dictionary_arrow_type,
    _dictionary_compare_vector,
    _is_constant_vector_like,
    _is_dictionary_encoded_vector,
    _is_null_as_boolvector,
    _is_typed_constant_encoded_vector,
)

_EPOCH_DATE = datetime.date(1970, 1, 1)
_EPOCH_DATETIME = datetime.datetime(1970, 1, 1)

_NEGATED_OPS = {
    "NotEq": "Eq",
    "NotInList": "InList",
    "NotLike": "Like",
    "NotILike": "ILike",
    "NotRLike": "RLike",
    "NotInStr": "InStr",
    "NotIInStr": "IInStr",
}


def _int64_temporal_compare(op: str, vec, right, temporal_type):
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector
    from opteryx.types import OrsoTypes

    if right is None:
        return BoolVector(len(vec))

    if temporal_type == OrsoTypes.TIMESTAMP:
        coerce = _coerce_timestamp
    elif temporal_type == OrsoTypes.DATE:
        coerce = _coerce_date32
    else:
        return _int64_compare(op, vec, right)

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = frozenset(coerce(value) for value in right)
    elif right.__class__.__name__ == "Int64Vector":
        vec_ops = {
            "Eq": vec.equals_vector,
            "Lt": vec.less_than_vector,
            "Gt": vec.greater_than_vector,
            "LtEq": vec.less_than_or_equals_vector,
            "GtEq": vec.greater_than_or_equals_vector,
        }
        fn = vec_ops.get(op)
        if fn is None:
            raise NotImplementedError(f"Int64Vector temporal vector-vector: unsupported op {op!r}")
        return fn(right)
    else:
        value = coerce(right)

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


def _timestamp_compare(op: str, vec, right):
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_timestamp_set(right)
    elif right.__class__.__name__ == "TimestampVector":
        vec_ops = {
            "Eq": vec.equals_vector,
            "NotEq": vec.not_equals_vector,
            "Lt": vec.less_than_vector,
            "Gt": vec.greater_than_vector,
            "LtEq": vec.less_than_or_equals_vector,
            "GtEq": vec.greater_than_or_equals_vector,
        }
        fn = vec_ops.get(op)
        if fn is None:
            raise NotImplementedError(f"TimestampVector vector-vector: unsupported op {op!r}")
        return fn(right)
    elif right.__class__.__name__ == "Date32Vector":
        import pyarrow as _pa_local

        from opteryx.compiled.draken.interop.arrow import vector_from_arrow as _vfa

        ts_right = _vfa(right.to_arrow().cast(_pa_local.timestamp("us")))
        return _timestamp_compare(op, vec, ts_right)
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


def _date32_compare(op: str, vec, right):
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_date32_set(right)
    elif right.__class__.__name__ == "Date32Vector":
        vec_ops = {
            "Eq": vec.equals_vector,
            "Lt": vec.less_than_vector,
            "Gt": vec.greater_than_vector,
            "LtEq": vec.less_than_or_equals_vector,
            "GtEq": vec.greater_than_or_equals_vector,
        }
        fn = vec_ops.get(op)
        if fn is None:
            raise NotImplementedError(f"Date32Vector vector-vector: unsupported op {op!r}")
        return fn(right)
    elif right.__class__.__name__ == "TimestampVector":
        import pyarrow as _pa_local

        from opteryx.compiled.draken.interop.arrow import vector_from_arrow as _vfa

        ts_left = _vfa(vec.to_arrow().cast(_pa_local.timestamp("us")))
        return _timestamp_compare(op, ts_left, right)
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


def _interval_compare(op: str, vec, right):
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector

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


def _date_minus_date_draken(left_vec, right_vec):
    from opteryx.compiled.draken.interop.arrow import vector_from_arrow
    from opteryx.expression.intervals import _intervals_to_month_day_nano

    # Native vector subtraction
    if left_vec.__class__.__name__ == "Date32Vector":
        if right_vec.__class__.__name__ == "Date32Vector":
            diff_us = left_vec.subtract_date32_vector(right_vec)
        else:  # TimestampVector
            diff_us = left_vec.subtract_timestamp_vector(right_vec)
    else:  # TimestampVector
        if right_vec.__class__.__name__ == "TimestampVector":
            diff_us = left_vec.subtract_timestamp_vector(right_vec)
        else:  # Date32Vector
            diff_us = left_vec.subtract_date32_vector(right_vec)

    # Convert Int64Vector microseconds to IntervalVector (months, microseconds)
    # Nulls are already handled by the subtraction methods via bitmap
    rows = [(0, d) if d is not None else None for d in diff_us.to_pylist()]
    return vector_from_arrow(_intervals_to_month_day_nano(rows))


def _date_interval_op_draken(left_vec, right_vec, op):
    from opteryx.expression.intervals import _as_interval_vector

    signum = 1 if op == "Plus" else -1

    if right_vec.__class__.__name__ in _INTERVAL_TYPES:
        date_vec, interval_vec = left_vec, _as_interval_vector(right_vec)
    else:
        date_vec, interval_vec = right_vec, _as_interval_vector(left_vec)

    return interval_vec.apply_to_temporal(date_vec, signum)
