# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: initializedcheck=False

"""String comparison operations.

Cython migration of the former string_ops.py. Called from comparisons.pyx
once per WHERE predicate evaluated on a string column.

The kernels themselves (vector_like / vector_rlike / vector_in_list /
vec.equals etc.) are Draken-native; this layer normalises the right-hand
operand into the byte form the kernels expect and dispatches by op name.
"""

from opteryx.compiled.vector_ops import vector_contains, vector_in_list, vector_like, vector_rlike

from draken.vectors.bool_vector import BoolVector
from draken.vectors.string_vector import StringVector

from .type_coercion import _coerce_str, _coerce_str_set, _is_constant_vector_like


cpdef _string_compare(str op, vec, right):
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
