"""
Filter operations package: fast paths for expression evaluation.

This package provides optimized filter operations (comparisons, list matching,
pattern matching, etc.) with specialized fast paths for:
- Constant-encoded vectors
- Dictionary-encoded vectors
- Array and string operations

Main entry points:
- filter_operations() - main dispatch for filter evaluation
- reset_fastpath_telemetry() - reset performance metrics
- get_fastpath_telemetry() - get current performance metrics
"""

import numpy
import pyarrow
from opteryx.types import OrsoTypes

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
    """Reset telemetry counters (backward compat wrapper)."""
    reset_fastpath_telemetry()


def get_dict_expr_telemetry():
    """Get telemetry snapshot (backward compat wrapper)."""
    return get_fastpath_telemetry()


def _to_arrow_if_needed(arr):
    """Convert to Arrow if array has to_arrow method."""
    if hasattr(arr, "to_arrow") and not isinstance(arr, (pyarrow.Array, pyarrow.ChunkedArray)):
        return arr.to_arrow()
    return arr


def filter_operations(left_arr, left_type, operator, right_arr, right_type):
    """
    Execute filter operation with appropriate fast path.

    Wraps filter evaluation to correctly handle null semantics, coercing type
    mismatches, and selecting optimized implementations based on vector encoding.

    Returns:
        Array with tri-state boolean (true/false/None). For filtering, None is
        treated as false. For display, None is preserved.
    """
    if len(left_arr) == 0 or len(right_arr) == 0:
        return numpy.empty(0, dtype=bool)

    # Fast path for constant-encoded vectors
    if has_constant_candidate(left_arr):
        return _inner_filter_operations(left_arr, operator, right_arr)

    # INTEGERS and DECIMALS don't play nicely so we cast the INTS to DOUBLES
    if left_type == OrsoTypes.DECIMAL and right_type == OrsoTypes.INTEGER:
        right_arr = pyarrow.compute.cast(right_arr, pyarrow.float64())
        right_type = OrsoTypes.DOUBLE
    elif right_type == OrsoTypes.DECIMAL and left_type == OrsoTypes.INTEGER:
        left_arr = pyarrow.compute.cast(left_arr, pyarrow.float64())
        left_type = OrsoTypes.DOUBLE

    morsel_size = len(left_arr)
    compressed = False

    if operator not in _SKIP_COMPRESSION_OPS:
        # Optimize by removing nulls for non-null-sensitive operations
        from pyarrow import compute

        left_null_positions = compute.is_null(left_arr, nan_is_null=True)

        if len(right_arr) > 1:
            right_null_positions = compute.is_null(right_arr, nan_is_null=True)
            null_positions = numpy.logical_or(left_null_positions, right_null_positions)
        elif len(right_arr) == 1 and right_arr[0] is None:
            return pyarrow.nulls(morsel_size, type=pyarrow.bool_())
        else:
            null_positions = left_null_positions.to_numpy(False)

        # Early exit if all values are null
        if null_positions.all():
            return pyarrow.nulls(morsel_size, type=pyarrow.bool_())

        if null_positions.any():
            valid_positions = ~null_positions
            compressed = True

            if isinstance(left_arr, numpy.ndarray):
                left_arr = left_arr.compress(valid_positions)
            else:
                left_arr = compute.filter(left_arr, valid_positions)

            if len(right_arr) > 1:
                if isinstance(right_arr, numpy.ndarray):
                    right_arr = right_arr.compress(valid_positions)
                else:
                    right_arr = compute.filter(right_arr, valid_positions)

    # Handle temporal type coercions
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

        results_mask = function(left_arr, left_type, right_arr, right_type, operator)
    else:
        # Dispatch to appropriate operation handler
        results_mask = _inner_filter_operations(left_arr, operator, right_arr)

    # Restore nulls if we compressed the data
    if compressed:
        full_result = numpy.full(morsel_size, None, dtype=object)
        numpy.place(full_result, valid_positions, results_mask)
        return pyarrow.array(full_result, type=pyarrow.bool_())

    return results_mask


def _inner_filter_operations(arr, operator, value):
    """
    Dispatch filter operation to appropriate handler.

    Handles constant encoding, dictionary encoding, and regular operations.
    """
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
                if isinstance(value, (tuple, list)):
                    value = pyarrow.array(value)
        except TypeError:
            pass

    dict_candidate = has_dictionary_candidate(raw_arr)
    constant_candidate = has_constant_candidate(raw_arr)
    numeric_dict_candidate = dict_candidate and supports_dictionary_numeric_fastpath(operator)

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

    # InStr fast path: use StringVector.contains() directly, bypassing Arrow conversion.
    # Handles dict-encoded, dense Draken, and Arrow arrays uniformly.
    if operator in ("InStr", "NotInStr", "IInStr", "NotIInStr"):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow
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

        # Convert to StringVector if needed
        if hasattr(raw_arr, "contains"):
            vec = raw_arr
        elif hasattr(raw_arr, "to_arrow"):
            vec = vector_from_arrow(raw_arr.to_arrow())
        elif isinstance(raw_arr, (pyarrow.Array, pyarrow.ChunkedArray)):
            vec = vector_from_arrow(raw_arr)
        else:
            vec = vector_from_arrow(pyarrow.array(list(raw_arr), type=pyarrow.string()))

        result = vector_contains(vec, needle, ignore_case)
        if negate:
            result = result.not_vector()
        return result

    # Convert to Arrow if needed for regular operations
    if hasattr(raw_arr, "to_arrow") and not isinstance(
        raw_arr, (pyarrow.Array, pyarrow.ChunkedArray)
    ):
        arr = raw_arr.to_arrow()
    else:
        arr = raw_arr

    # Check if dictionary fastpath is available for this operator
    if dict_candidate and not supports_dictionary_fastpath(operator) and not numeric_dict_candidate:
        raise NotImplementedError(f"Dictionary motor path does not support operator `{operator}`.")

    # Dispatch to operation handlers
    if operator == "Eq":
        return comparisons.equal(arr, value, dict_candidate)
    if operator == "NotEq":
        return comparisons.not_equal(arr, value, dict_candidate)
    if operator == "Lt":
        return comparisons.less_than(arr, value, numeric_dict_candidate)
    if operator == "Gt":
        return comparisons.greater_than(arr, value, numeric_dict_candidate)
    if operator == "LtEq":
        return comparisons.less_than_or_equal(arr, value, numeric_dict_candidate)
    if operator == "GtEq":
        return comparisons.greater_than_or_equal(arr, value, numeric_dict_candidate)
    if operator == "InList":
        return list_ops.in_list(arr, value, dict_candidate)
    if operator == "NotInList":
        return list_ops.not_in_list(arr, value, dict_candidate)
    if operator == "Like":
        return string_matching.like(arr, value, dict_candidate)
    if operator == "NotLike":
        return string_matching.not_like(arr, value, dict_candidate)
    if operator == "ILike":
        return string_matching.ilike(arr, value, dict_candidate)
    if operator == "NotILike":
        return string_matching.not_ilike(arr, value, dict_candidate)
    if operator == "RLike":
        return string_matching.rlike(arr, value, dict_candidate)
    if operator == "NotRLike":
        return string_matching.not_rlike(arr, value, dict_candidate)
    if operator == "AnyOpEq":
        return array_ops.anyop_eq(literal=arr[0], column=value)
    if operator == "AnyOpNotEq":
        return array_ops.anyop_not_eq(literal=arr[0], column=value)
    if operator == "AnyOpGt":
        return array_ops.anyop_greater_than(arr[0], value)
    if operator == "AnyOpLt":
        return array_ops.anyop_less_than(arr[0], value)
    if operator == "AnyOpGtEq":
        return array_ops.anyop_greater_than_or_equal(arr[0], value)
    if operator == "AnyOpLtEq":
        return array_ops.anyop_less_than_or_equal(arr[0], value)
    if operator == "AllOpEq":
        return array_ops.allop_eq(arr[0], value)
    if operator == "AllOpNotEq":
        return array_ops.allop_not_eq(arr[0], value)
    if operator == "AnyOpILike":
        return array_ops.anyop_ilike(arr, value)
    if operator == "AnyOpLike":
        return array_ops.anyop_like(arr, value)
    if operator == "AnyOpNotLike":
        return array_ops.anyop_not_like(arr, value)
    if operator == "AnyOpNotILike":
        return array_ops.anyop_not_ilike(arr, value)
    if operator == "AtQuestion":
        return special_ops.json_path_exists(arr, value)
    if operator == "AtArrow":
        return array_ops.array_contains_any(arr, value)
    if operator == "ArrayContainsAll":
        return array_ops.array_contains_all(arr, value)

    raise NotImplementedError(f"Operator {operator} is not implemented!")


__all__ = [
    "filter_operations",
    "_inner_filter_operations",
    "reset_dict_expr_telemetry",
    "get_dict_expr_telemetry",
    "reset_fastpath_telemetry",
    "get_fastpath_telemetry",
]
