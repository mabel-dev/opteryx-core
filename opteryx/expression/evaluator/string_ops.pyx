"""String comparison operations.

Cython migration of the former string_ops.py. Called from comparisons.pyx
once per WHERE predicate evaluated on a string column.

The kernels themselves (vector_like / vector_rlike / vector_in_list /
vec.equals etc.) are Draken-native; this layer normalises the right-hand
operand into the byte form the kernels expect and dispatches by op name.
"""

from opteryx.compiled.vector_ops import (
    build_in_list_carchar,
    vector_contains,
    vector_in_list,
    vector_like,
    vector_rlike,
)

from draken.vectors.bool_vector import BoolVector
from draken.vectors.string_vector import StringVector



cdef _string_compare(int op_code, vec, right):
    cdef bytes value_bytes
    cdef object value_set = None

    if right is None:
        return BoolVector(len(vec))

    if isinstance(right, (list, tuple, set, frozenset)):
        value_set = _coerce_str_set(right)
    elif isinstance(right, StringVector):
        if _is_constant_vector_like(right):
            value_bytes = _coerce_str(right)
        else:
            raise NotImplementedError(
                "StringVector column-column comparisons not yet supported"
            )
    else:
        value_bytes = _coerce_str(right)

    if op_code <= OP_GT_EQ:
        return vec._compare_scalar(value_bytes, _DRAKEN_CMP_OP[op_code])
    if op_code == OP_IN_LIST:
        return vector_in_list(vec, build_in_list_carchar(value_set))
    if op_code == OP_LIKE:
        return vector_like(vec, value_bytes, False)
    if op_code == OP_ILIKE:
        return vector_like(vec, value_bytes, True)
    if op_code == OP_RLIKE:
        return vector_rlike(vec, value_bytes)
    if op_code == OP_IN_STR:
        return vector_contains(vec, value_bytes, False)
    if op_code == OP_I_IN_STR:
        return vector_contains(vec, value_bytes, True)
    raise NotImplementedError(f"StringVector: unsupported op (code {op_code})")


cpdef _string_anyop_like(vec, patterns, bint ignore_case):
    cdef list pat_list
    cdef bytes pat_bytes
    cdef object result = None
    cdef object mask

    if isinstance(patterns, (list, tuple)):
        pat_list = list(patterns)
    else:
        pat_list = [patterns]

    for p in pat_list:
        if p is None:
            continue
        if isinstance(p, bytes):
            pat_bytes = p
        else:
            pat_bytes = str(p).encode()
        mask = vector_like(vec, pat_bytes, ignore_case)
        if result is None:
            result = mask
        else:
            result = result.or_vector(mask)

    if result is None:
        return BoolVector(len(vec))
    return result
