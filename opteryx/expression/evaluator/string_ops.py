"""String comparison operations."""

import datetime
import decimal

import numpy
import pyarrow as _pa
from opteryx.exceptions import ColumnReferencedBeforeEvaluationError

from .function_execution import _is_draken_vector
from .function_execution import apply_bounded_function
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
    _is_dictionary_encoded_vector,
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
        return vec.in_list(value_set)
    if op == "Like":
        return vec.like(value_bytes, False)
    if op == "ILike":
        return vec.like(value_bytes, True)
    if op == "RLike":
        return vec.rlike(value_bytes)
    if op == "InStr":
        return vec.contains(value_bytes, False)
    if op == "IInStr":
        return vec.contains(value_bytes, True)
    raise NotImplementedError(f"StringVector: unsupported op {op!r}")



def _string_anyop_like(vec, patterns, ignore_case: bool):
    pat_list = patterns if isinstance(patterns, (list, tuple)) else [patterns]
    result = None
    for p in pat_list:
        if p is None:
            continue
        pat_bytes = p if isinstance(p, bytes) else str(p).encode()
        mask = vec.like(pat_bytes, ignore_case)
        result = mask if result is None else result.or_vector(mask)
    if result is None:
        from opteryx.compiled.draken.vectors.bool_vector import BoolVector

        return BoolVector(len(vec))
    return result


