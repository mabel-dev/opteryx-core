"""Temporal (date/timestamp/interval) comparison operations."""

import datetime

from opteryx.compiled.vector_ops import vector_in_list
from opteryx.exceptions import ColumnReferencedBeforeEvaluationError

from .function_execution import apply_bounded_function, is_draken_vector

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


def _compare_nullable_temporal(op: str, left, right):
    if left is None or right is None:
        return None
    if op == "Eq":
        return left == right
    if op == "NotEq":
        return left != right
    if op == "Lt":
        return left < right
    if op == "Gt":
        return left > right
    if op == "LtEq":
        return left <= right
    if op == "GtEq":
        return left >= right
    raise NotImplementedError(f"Temporal compare: unsupported op {op!r}")


def _convert_date32_to_timestamp_vector(d_vec):
    """Convert Date32Vector to TimestampVector for cross-type comparison.

    Date32 values (days since epoch) are converted to Timestamp values (microseconds since epoch)
    by multiplying by 86,400,000,000 (microseconds per day).

    This materializes the Date32 vector once, rather than materializing both vectors separately
    during comparison.
    """
    from draken.interop.vector_sequence import vector_from_sequence

    # Convert each Date32 value (days) to Timestamp value (microseconds)
    d_values = d_vec.to_pylist()
    ts_values = [None if d is None else int(d) * 86_400_000_000 for d in d_values]
    return vector_from_sequence(ts_values)


def _compare_timestamp_date32_vectors(op: str, ts_vec, d_vec):
    """Compare TimestampVector with Date32Vector using native vector comparison.

    Converts Date32Vector to TimestampVector, then uses native equals_vector/less_than_vector/etc.
    """
    d_as_ts = _convert_date32_to_timestamp_vector(d_vec)

    # Use native vector comparison operators
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


def _int64_temporal_compare(op: str, vec, right, temporal_type):
    from draken.vectors.bool_vector import BoolVector
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
    elif right.__class__.__name__ in ("Int64Vector", "TimestampVector", "Date32Vector"):
        # Handle vector-vector temporal comparisons
        # TimestampVector and Date32Vector are physically int64 with temporal semantics
        vec_ops = {
            "Eq": vec.equals_vector,
            "Lt": vec.less_than_vector,
            "Gt": vec.greater_than_vector,
            "LtEq": vec.less_than_or_equals_vector,
            "GtEq": vec.greater_than_or_equals_vector,
        }
        fn = vec_ops.get(op)
        if fn is None:
            raise NotImplementedError(f"{right.__class__.__name__} temporal vector-vector: unsupported op {op!r}")
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
    from draken.vectors.bool_vector import BoolVector

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


def _date32_compare(op: str, vec, right):
    from draken.vectors.bool_vector import BoolVector

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


def _interval_compare(op: str, vec, right):
    from draken.vectors.bool_vector import BoolVector

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

    raise NotImplementedError(
        "Date/timestamp subtraction requires a native IntervalVector constructor; "
        "Arrow-backed interval materialization has been removed."
    )


def _date_interval_op_draken(left_vec, right_vec, op):
    from opteryx.expression.intervals import _as_interval_vector

    signum = 1 if op == "Plus" else -1

    if right_vec.__class__.__name__ in _INTERVAL_TYPES:
        date_vec, interval_vec = left_vec, _as_interval_vector(right_vec)
    else:
        date_vec, interval_vec = right_vec, _as_interval_vector(left_vec)

    return interval_vec.apply_to_temporal(date_vec, signum)
