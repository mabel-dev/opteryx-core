"""String comparison operations."""

import datetime

from opteryx.exceptions import ColumnReferencedBeforeEvaluationError
from opteryx.compiled.vector_ops import vector_like, vector_rlike, vector_contains
from opteryx.compiled.vector_ops import vector_in_list

from .function_execution import _is_draken_vector
from .function_execution import apply_bounded_function
from .type_coercion import _coerce_date32
from .type_coercion import _coerce_date32_set
from .type_coercion import _coerce_float
from .type_coercion import _coerce_float_set
from .type_coercion import _coerce_int64
from .type_coercion import _coerce_int64_set
from .type_coercion import _coerce_interval
from .type_coercion import _coerce_str
from .type_coercion import _coerce_str_set
from .type_coercion import _coerce_temporal_scalar_for_arrow
from .type_coercion import _coerce_timestamp
from .type_coercion import _coerce_timestamp_set
from .type_coercion import _constant_scalar_value
from .type_coercion import _dictionary_arrow_type
from .type_coercion import _dictionary_compare_vector
from .type_coercion import _is_constant_vector_like
from .type_coercion import _is_dictionary_encoded_vector
from .type_coercion import _is_null_as_boolvector
from .type_coercion import _is_typed_constant_encoded_vector

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


def _string_compare(op: str, vec, right):
    from opteryx.compiled.draken.vectors.bool_vector import BoolVector

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_str_set(right)
    elif right.__class__.__name__ == "StringVector":
        raise NotImplementedError("StringVector column-column comparisons not yet supported")
    else:
        value_bytes = _coerce_str(right)

    if op == "Eq":
        return vec.equals(value_bytes)
    if op == "Lt":
        return vec.less_than(value_bytes)
    if op == "Gt":
        return vec.greater_than(value_bytes)
    if op == "LtEq":
        return vec.less_than_or_equals(value_bytes)
    if op == "GtEq":
        return vec.greater_than_or_equals(value_bytes)
    if op == "InList":
        return vector_in_list(vec, value_set)
    if op == "Like":
        return vector_like(vec, value_bytes, False)
    if op == "ILike":
        return vector_like(vec, value_bytes, True)
    if op == "RLike":
        return vector_rlike(vec, value_bytes)
    if op == "InStr":
        return vector_contains(vec, value_bytes, False)
    if op == "IInStr":
        return vector_contains(vec, value_bytes, True)
    raise NotImplementedError(f"StringVector: unsupported op {op!r}")


def _string_anyop_like(vec, patterns, ignore_case: bool):
    pat_list = patterns if isinstance(patterns, (list, tuple)) else [patterns]
    result = None
    for p in pat_list:
        if p is None:
            continue
        pat_bytes = p if isinstance(p, bytes) else str(p).encode()
        mask = vector_like(vec, pat_bytes, ignore_case)
        result = mask if result is None else result.or_vector(mask)
    if result is None:
        from opteryx.compiled.draken.vectors.bool_vector import BoolVector

        return BoolVector(len(vec))
    return result
