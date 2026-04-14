"""Filter operations dispatcher - Draken-native only.

All array inputs must be Draken vectors. No numpy/pyarrow conversion or fallbacks.
Null handling is native to Draken vector operations.
If you get AttributeError, your input isn't Draken - that's a bug upstream.
"""

from opteryx.compiled.vector_ops import vector_contains
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
from opteryx.expression.operations.type_coercion import to_temporal_array
from opteryx.types import OrsoTypes

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


def filter_operations(left_arr, left_type, operator, right_arr, right_type):
    """Execute filter operation with appropriate fast path.

    All inputs must be Draken vectors. Null handling is native to Draken.
    """
    # Empty arrays return empty result
    if len(left_arr) == 0 or len(right_arr) == 0:
        from opteryx.compiled.draken.vectors.bool_vector import BoolVector

        return BoolVector.from_scalar(None, 0)

    # Fast path for constant-encoded vectors
    if has_constant_candidate(left_arr):
        return _inner_filter_operations(left_arr, operator, right_arr)

    # Type coercion: DECIMAL + INTEGER
    if left_type == OrsoTypes.DECIMAL and right_type == OrsoTypes.INTEGER:
        right_type = OrsoTypes.DOUBLE
    elif right_type == OrsoTypes.DECIMAL and left_type == OrsoTypes.INTEGER:
        left_type = OrsoTypes.DOUBLE

    # Temporal type coercions
    if (
        OrsoTypes.TIMESTAMP in (left_type, right_type) or OrsoTypes.DATE in (left_type, right_type)
    ) and OrsoTypes.INTEGER in (left_type, right_type):
        if left_type == OrsoTypes.INTEGER:
            target_type = OrsoTypes.DATE if right_type == OrsoTypes.DATE else OrsoTypes.TIMESTAMP
            left_arr = to_temporal_array(left_arr, left_type, target_type)
            left_type = target_type
        if right_type == OrsoTypes.INTEGER:
            target_type = OrsoTypes.DATE if left_type == OrsoTypes.DATE else OrsoTypes.TIMESTAMP
            right_arr = to_temporal_array(right_arr, right_type, target_type)
            right_type = target_type

    if {left_type, right_type} == {OrsoTypes.DATE, OrsoTypes.TIMESTAMP}:
        left_arr = to_temporal_array(left_arr, left_type, OrsoTypes.TIMESTAMP)
        right_arr = to_temporal_array(right_arr, right_type, OrsoTypes.TIMESTAMP)
        left_type = right_type = OrsoTypes.TIMESTAMP

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
