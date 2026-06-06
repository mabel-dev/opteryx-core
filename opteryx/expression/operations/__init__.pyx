# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: initializedcheck=False

"""Filter operations dispatcher - Draken-native only.

All array inputs must be Draken vectors. No numpy/pyarrow conversion or
fallbacks. Null handling is native to Draken vector operations. If you get
AttributeError, your input isn't Draken — that's a bug upstream.

Consolidation: the leaf modules (comparisons / list_ops / array_ops /
string_matching / special_ops / fastpath_*) are textually included into
this umbrella so the package compiles to a single .so. All cdef/cpdef
helpers from those files live in this module's namespace.
"""

import datetime

from draken.vectors.bool_vector import BoolVector
import draken.draken_native as _draken_native
from opteryx.compiled.vector_ops import (
    vector_like,
    vector_rlike,
)
from opteryx.compiled.nanobind.vector_misc import vector_in_list
from opteryx.compiled.nanobind.vector_string_search import vector_contains
# Phase 4: draken_compare (string-keyed) deleted; use draken_compare_int with
# bind-time-resolved op_code. The shim below stays plan-time-only.
from opteryx.expression.evaluator.comparisons import draken_compare_int
from opteryx.expression.evaluator._impl import _OP_CODE as _COMPARE_OP_CODE
from opteryx.third_party import yyjson
from opteryx.types import SqlType
from opteryx.types._datetime_conversion import (
    date_to_int64_days,
    int64_days_to_date,
    int64_us_to_datetime,
    timestamp_to_int64_us,
)

# Leaf includes — the operator wrappers (comparisons / list_ops /
# string_matching) route through the uniform Draken vector path. array_ops and
# special_ops are independent and follow.
include "comparisons.pyx"
include "list_ops.pyx"
include "string_matching.pyx"
include "array_ops.pyx"
include "special_ops.pyx"


# Operators that should skip null compression during filtering.
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


def _coerce_temporal_scalar(value, source_type, target_type):
    """Normalize temporal scalars to the backing Python type for `target_type`."""

    if target_type == SqlType.DATE:
        if isinstance(value, datetime.datetime):
            value = value.date()
        if isinstance(value, datetime.date):
            return date_to_int64_days(value)
        if isinstance(value, int):
            if source_type == SqlType.TIMESTAMP:
                return date_to_int64_days(int64_us_to_datetime(value).date())
            return date_to_int64_days(int64_days_to_date(value))
        return date_to_int64_days(SqlType.DATE.parse(value))

    if target_type == SqlType.TIMESTAMP:
        if isinstance(value, datetime.datetime):
            if value.tzinfo is not None:
                value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return timestamp_to_int64_us(value)
        if isinstance(value, datetime.date):
            return timestamp_to_int64_us(value)
        if isinstance(value, int):
            if source_type == SqlType.DATE:
                return timestamp_to_int64_us(int64_days_to_date(value))
            return value
        return timestamp_to_int64_us(SqlType.TIMESTAMP.parse(value))

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

    source_vec = _draken_native.vector_from_sequence(coerced)
    if target_type == SqlType.DATE:
        return _draken_native.vector_reinterpret_as_date32(source_vec)
    if target_type == SqlType.TIMESTAMP:
        return _draken_native.vector_reinterpret_as_timestamp64(source_vec)
    return source_vec


def filter_operations(left_arr, left_type, operator, right_arr, right_type):
    """Execute filter operation with appropriate fast path.

    All inputs must be Draken vectors. Null handling is native to Draken.
    """
    from opteryx.exceptions import IncompatibleTypesError

    # Empty arrays return empty result
    if len(left_arr) == 0 or len(right_arr) == 0:
        return BoolVector.from_scalar(None, 0)

    # Type coercion: DECIMAL + INTEGER
    if left_type == SqlType.DECIMAL and right_type == SqlType.INTEGER:
        right_type = SqlType.DOUBLE
    elif right_type == SqlType.DECIMAL and left_type == SqlType.INTEGER:
        left_type = SqlType.DOUBLE

    # Temporal type coercions — reject INT vs temporal comparisons.
    temporal_types = {SqlType.DATE, SqlType.TIMESTAMP}
    if (
        SqlType.INTEGER in (left_type, right_type)
        or left_type in temporal_types
        or right_type in temporal_types
    ):
        if left_type == SqlType.INTEGER and right_type in temporal_types:
            raise IncompatibleTypesError(
                message="Ambiguous comparison: INTEGER = TIMESTAMP/DATE. "
                "Provide a TIMESTAMP or DATE column instead. "
                "To convert an INTEGER to TIMESTAMP, use an explicit cast with a unit: "
                "`::TIMESTAMP[ms]`, `::TIMESTAMP[s]`, or `::TIMESTAMP[us]`."
            )
        if right_type == SqlType.INTEGER and left_type in temporal_types:
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
            left_target_type = right_target_type = SqlType.TIMESTAMP

        left_arr = to_temporal_array(left_arr, left_source_type, left_target_type)
        right_arr = to_temporal_array(right_arr, right_source_type, right_target_type)
        left_type = left_target_type
        right_type = right_target_type
        # Plan-time call: resolve op-string and BCTypeCode, then dispatch to
        # the int-keyed kernel. Hot path (BC_COMPARE) resolves at bind time.
        _op_code = _COMPARE_OP_CODE.get(operator, 0)
        if _op_code == 0:
            raise NotImplementedError(
                f"operations: unknown comparison op {operator!r}"
            )
        # BCTypeCode: 0=NONE, 1=DATE, 2=TIMESTAMP
        _lc = 1 if left_type == SqlType.DATE else (2 if left_type == SqlType.TIMESTAMP else 0)
        _rc = 1 if right_type == SqlType.DATE else (2 if right_type == SqlType.TIMESTAMP else 0)
        return draken_compare_int(_op_code, left_arr, right_arr, _lc, _rc)

    # Handle interval operations
    if SqlType.INTERVAL in (left_type, right_type):
        from opteryx.expression.intervals import INTERVAL_KERNELS

        function = INTERVAL_KERNELS.get((left_type, right_type, operator))
        if function is None:
            from opteryx.exceptions import UnsupportedTypeError

            raise UnsupportedTypeError(
                f"Cannot perform {operator.upper()} on {left_type} and {right_type}."
            )
        return function(left_arr, left_type, right_arr, right_type, operator)

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

    # InStr — uniform substring match via the Draken vector kernel.
    if operator in ("InStr", "NotInStr", "IInStr", "NotIInStr"):
        ignore_case = operator in ("IInStr", "NotIInStr")
        negate = operator in ("NotInStr", "NotIInStr")
        raw_value = value[0] if hasattr(value, "__len__") and len(value) == 1 else value
        needle = raw_value if isinstance(raw_value, bytes) else str(raw_value).encode("utf-8")
        result = raw_arr.contains(needle, ignore_case=ignore_case)
        return result.not_vector() if negate else result

    # Dispatch by operator type — symbols are in-scope via the leaf includes.
    if operator in ("Eq", "Equal"):
        return equal(raw_arr, value)
    elif operator in ("NotEq", "NotEqual"):
        return not_equal(raw_arr, value)
    elif operator in ("Lt", "LessThan"):
        return less_than(raw_arr, value)
    elif operator in ("Gt", "GreaterThan"):
        return greater_than(raw_arr, value)
    elif operator in ("LtEq", "LessThanOrEqual"):
        return less_than_or_equal(raw_arr, value)
    elif operator in ("GtEq", "GreaterThanOrEqual"):
        return greater_than_or_equal(raw_arr, value)
    elif operator in ("InList",):
        return in_list(raw_arr, value)
    elif operator in ("NotInList",):
        return not_in_list(raw_arr, value)
    elif operator.startswith("Like"):
        return like_match(raw_arr, value, operator)
    elif operator.startswith("RLike"):
        return rlike_match(raw_arr, value, operator)
    elif operator.startswith("ArrayContains"):
        from draken.draken_native import vector_from_string_sequence as _vfss
        return vector_contains(raw_arr, _vfss([str(value)]))
    else:
        # AnyOp* / AllOp* / AtArrow are dispatched via
        # evaluator.comparisons.draken_compare before they can reach here.
        raise NotImplementedError(f"Filter operation `{operator}` not implemented.")
