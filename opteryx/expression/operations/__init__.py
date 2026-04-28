"""Filter operations dispatcher - Draken-native only.

All array inputs must be Draken vectors. No numpy/pyarrow conversion or fallbacks.
Null handling is native to Draken vector operations.
If you get AttributeError, your input isn't Draken - that's a bug upstream.
"""

import datetime

from draken.interop.vector_sequence import vector_from_sequence
from opteryx.compiled.vector_ops import vector_contains
from opteryx.expression.evaluator.comparisons import draken_compare
from opteryx.expression.operations import (
    array_ops,
    comparisons,
    list_ops,
    special_ops,
    string_matching,
)
from opteryx.expression.operations.fastpath_constant import (
    constant_fastpath,
    has_constant_candidate,
    supports_constant_fastpath,
)
from opteryx.expression.operations.fastpath_dictionary import (
    has_dictionary_candidate,
    supports_dictionary_fastpath,
    supports_dictionary_numeric_fastpath,
)
from opteryx.expression.operations.fastpath_telemetry import (
    get_fastpath_telemetry,
    record_constant_fastpath_fallback,
    record_constant_fastpath_hit,
    reset_fastpath_telemetry,
)
from opteryx.types import OrsoTypes
from opteryx.types._datetime_conversion import (
    date_to_int64_days,
    int64_days_to_date,
    int64_us_to_datetime,
    timestamp_to_int64_us,
)

# Operators that should skip null compression during filtering
_SKIP_COMPRESSION_OPS = frozenset(
    (
        "InList",
        "NotInList",
        "AnyOpEq",
        "AnyOpNotEq",
        "AnyOpGt",
        "AnyOpGtEq",
        "AnyOpLt",
        "AnyOpLtEq",
        "AnyOpLike",
        "AnyOpNotLike",
        "AnyOpILike",
        "AnyOpNotILike",
        "AnyOpRLike",
        "AnyOpNotRLike",
        "AllOpEq",
        "AllOpNotEq",
        "AtArrow",
        "ArrayContainsAll",
    )
)


def reset_dict_expr_telemetry():
    """Reset telemetry counters."""
    reset_fastpath_telemetry()


def get_dict_expr_telemetry():
    """Get telemetry snapshot."""
    return get_fastpath_telemetry()


def _coerce_temporal_scalar(value, source_type, target_type):
    """Normalize temporal scalars to the backing Python type for `target_type`."""

    if target_type == OrsoTypes.DATE:
        if isinstance(value, datetime.datetime):
            value = value.date()
        if isinstance(value, datetime.date):
            return date_to_int64_days(value)
        if isinstance(value, int):
            if source_type == OrsoTypes.TIMESTAMP:
                return date_to_int64_days(int64_us_to_datetime(value).date())
            return date_to_int64_days(int64_days_to_date(value))
        return date_to_int64_days(OrsoTypes.DATE.parse(value))

    if target_type == OrsoTypes.TIMESTAMP:
        if isinstance(value, datetime.datetime):
            if value.tzinfo is not None:
                value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return timestamp_to_int64_us(value)
        if isinstance(value, datetime.date):
            return timestamp_to_int64_us(value)
        if isinstance(value, int):
            if source_type == OrsoTypes.DATE:
                return timestamp_to_int64_us(int64_days_to_date(value))
            return value
        return timestamp_to_int64_us(OrsoTypes.TIMESTAMP.parse(value))

    return value


def to_temporal_array(values, source_type, target_type):
    """Coerce values to a Draken temporal vector without Arrow/Numpy conversion."""
    if values.__class__.__module__.startswith("draken.vectors."):
        values = values.to_pylist()
    elif not isinstance(values, (list, tuple)):
        values = [values]

    coerced = [
        None if value is None else _coerce_temporal_scalar(value, source_type, target_type)
        for value in values
    ]

    source_vec = vector_from_sequence(coerced, dtype=OrsoTypes.INTEGER)
    if target_type == OrsoTypes.DATE:
        from draken.vectors.date32_vector import from_int64_vector as _from_int64

        return _from_int64(source_vec)
    if target_type == OrsoTypes.TIMESTAMP:
        from draken.vectors.timestamp_vector import (
            from_int64_vector as _from_int64,
        )

        return _from_int64(source_vec, timestamp_unit="us")
    return source_vec


def filter_operations(left_arr, left_type, operator, right_arr, right_type):
    """Execute filter operation with appropriate fast path.

    All inputs must be Draken vectors. Null handling is native to Draken.
    """
    from opteryx.exceptions import IncompatibleTypesError

    # Empty arrays return empty result
    if len(left_arr) == 0 or len(right_arr) == 0:
        from draken.vectors.bool_vector import BoolVector

        return BoolVector.from_scalar(None, 0)

    # Fast path for constant-encoded vectors
    if has_constant_candidate(left_arr):
        return _inner_filter_operations(left_arr, operator, right_arr)

    # Type coercion: DECIMAL + INTEGER
    if left_type == OrsoTypes.DECIMAL and right_type == OrsoTypes.INTEGER:
        right_type = OrsoTypes.DOUBLE
    elif right_type == OrsoTypes.DECIMAL and left_type == OrsoTypes.INTEGER:
        left_type = OrsoTypes.DOUBLE

    # Temporal type coercions - reject INT vs temporal comparisons (no implicit conversion)
    temporal_types = {OrsoTypes.DATE, OrsoTypes.TIMESTAMP}
    if (
        OrsoTypes.INTEGER in (left_type, right_type)
        or left_type in temporal_types
        or right_type in temporal_types
    ):
        # Reject implicit INTEGER to temporal conversions
        if left_type == OrsoTypes.INTEGER and right_type in temporal_types:
            raise IncompatibleTypesError(
                message="Ambiguous comparison: INTEGER = TIMESTAMP/DATE. "
                "Provide a TIMESTAMP or DATE column instead. "
                "To convert an INTEGER to TIMESTAMP, use an explicit cast with a unit: "
                "`::TIMESTAMP[ms]`, `::TIMESTAMP[s]`, or `::TIMESTAMP[us]`."
            )
        if right_type == OrsoTypes.INTEGER and left_type in temporal_types:
            raise IncompatibleTypesError(
                message="Ambiguous comparison: TIMESTAMP/DATE = INTEGER. "
                "Provide a TIMESTAMP or DATE column instead. "
                "To convert an INTEGER to TIMESTAMP, use an explicit cast with a unit: "
                "`::TIMESTAMP[ms]`, `::TIMESTAMP[s]`, or `::TIMESTAMP[us]`."
            )

        left_source_type = left_type
        right_source_type = right_type
        left_target_type = left_type
        right_target_type = right_type

        if {left_type, right_type} == temporal_types:
            left_target_type = right_target_type = OrsoTypes.TIMESTAMP

        left_arr = to_temporal_array(left_arr, left_source_type, left_target_type)
        right_arr = to_temporal_array(right_arr, right_source_type, right_target_type)
        left_type = left_target_type
        right_type = right_target_type
        return draken_compare(operator, left_arr, right_arr, left_type, right_type)

    # Handle interval operations
    if OrsoTypes.INTERVAL in (left_type, right_type):
        from opteryx.expression.intervals import INTERVAL_KERNELS

        function = INTERVAL_KERNELS.get((left_type, right_type, operator))
        if function is None:
            from opteryx.exceptions import UnsupportedTypeError

            raise UnsupportedTypeError(
                f"Cannot perform {operator.upper()} on {left_type} and {right_type}."
            )
        return function(left_arr, left_type, right_arr, right_type, operator)

    # Dispatch to appropriate operation handler
    return _inner_filter_operations(left_arr, operator, right_arr)


def _inner_filter_operations(arr, operator, value):
    """Dispatch filter operation to appropriate handler."""
    raw_arr = arr

    # Normalize scalar values for non-array operations
    if not operator.startswith(("AnyOp", "AllOp")):
        try:
            if len(value) == 1:
                value = value[0]
                try:
                    value = value.item()
                except AttributeError:
                    pass
        except TypeError:
            pass

    dict_candidate = has_dictionary_candidate(raw_arr)
    constant_candidate = has_constant_candidate(raw_arr)

    # Constant-encoded fastpath
    if constant_candidate:
        if not supports_constant_fastpath(operator):
            raise NotImplementedError(
                f"Constant motor path does not support operator `{operator}`."
            )
        fast = constant_fastpath(raw_arr, operator, value)
        if fast is not None:
            record_constant_fastpath_hit()
            return fast
        record_constant_fastpath_fallback()
        raise RuntimeError(f"Constant fastpath failed for `{operator}`.")

    # InStr fast path
    if operator in ("InStr", "NotInStr", "IInStr", "NotIInStr"):
        from opteryx.expression.operations.fastpath_dictionary import dictionary_fastpath
        from opteryx.expression.operations.fastpath_telemetry import record_dict_fastpath_hit

        ignore_case = operator in ("IInStr", "NotIInStr")
        negate = operator in ("NotInStr", "NotIInStr")
        raw_value = value[0] if hasattr(value, "__len__") and len(value) == 1 else value
        needle = raw_value if isinstance(raw_value, bytes) else str(raw_value).encode("utf-8")

        if dict_candidate:
            fast = dictionary_fastpath(raw_arr, operator, raw_value)
            if fast is not None:
                record_dict_fastpath_hit()
                return fast

        result = raw_arr.contains(needle, ignore_case=ignore_case)
        return result.not_vector() if negate else result

    # Dictionary-encoded fastpath
    if dict_candidate and supports_dictionary_fastpath(operator):
        from opteryx.expression.operations.fastpath_dictionary import dictionary_fastpath
        from opteryx.expression.operations.fastpath_telemetry import record_dict_fastpath_hit

        fast = dictionary_fastpath(raw_arr, operator, value)
        if fast is not None:
            record_dict_fastpath_hit()
            return fast

    # Dispatch by operator type
    if operator in ("Eq", "Equal"):
        return comparisons.equal(raw_arr, value, dict_candidate=dict_candidate)
    elif operator in ("NotEq", "NotEqual"):
        return comparisons.not_equal(raw_arr, value, dict_candidate=dict_candidate)
    elif operator in ("Lt", "LessThan"):
        return comparisons.less_than(raw_arr, value, dict_candidate=dict_candidate)
    elif operator in ("Gt", "GreaterThan"):
        return comparisons.greater_than(raw_arr, value, dict_candidate=dict_candidate)
    elif operator in ("LtEq", "LessThanOrEqual"):
        return comparisons.less_than_or_equal(raw_arr, value, dict_candidate=dict_candidate)
    elif operator in ("GtEq", "GreaterThanOrEqual"):
        return comparisons.greater_than_or_equal(raw_arr, value, dict_candidate=dict_candidate)
    elif operator in ("InList",):
        return list_ops.in_list(raw_arr, value, dict_candidate=dict_candidate)
    elif operator in ("NotInList",):
        return list_ops.not_in_list(raw_arr, value, dict_candidate=dict_candidate)
    elif operator.startswith("Like"):
        return string_matching.like_match(raw_arr, value, operator)
    elif operator.startswith("RLike"):
        return string_matching.rlike_match(raw_arr, value, operator)
    elif operator.startswith("AnyOp"):
        return array_ops.any_op(raw_arr, value, operator)
    elif operator.startswith("AllOp"):
        return array_ops.all_op(raw_arr, value, operator)
    elif operator.startswith("ArrayContains"):
        return vector_contains(raw_arr, value)
    elif operator in ("AtArrow",):
        return special_ops.at_arrow(raw_arr, value)
    else:
        raise NotImplementedError(f"Filter operation `{operator}` not implemented.")
