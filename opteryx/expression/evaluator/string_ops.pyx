"""String comparison operations.

Cython migration of the former string_ops.py. Called from comparisons.pyx
once per WHERE predicate evaluated on a string column.

The kernels themselves (vector_like / vector_rlike / vector_in_list /
vec.equals etc.) are Draken-native; this layer normalises the right-hand
operand into the byte form the kernels expect and dispatches by op name.
"""

from opteryx.compiled.vector_ops import (
    vector_like,
    vector_rlike,
)
from opteryx.compiled.nanobind.vector_misc import vector_in_list as _raw_string_in_list
from opteryx.compiled.nanobind.vector_string_search import vector_contains as _raw_string_contains

from draken.vectors.bool_vector import BoolVector
from draken.vectors.string_vector import StringVector
from draken.vectors.vector import Vector as _DrakenVectorBase

def _string_in_list(vec, literals, negate=False):
    nb_vec = vec._nb if isinstance(vec, _DrakenVectorBase) else vec
    result = _raw_string_in_list(nb_vec, literals, negate)
    if not isinstance(result, BoolVector):
        result = BoolVector(result)
    return result

def _string_contains(haystack, needle, ignore_case=False):
    nb_h = haystack._nb if isinstance(haystack, _DrakenVectorBase) else haystack
    nb_n = needle._nb if isinstance(needle, _DrakenVectorBase) else needle
    result = _raw_string_contains(nb_h, nb_n, ignore_case)
    if not isinstance(result, BoolVector):
        result = BoolVector(result)
    return result



cdef _string_compare(int op_code, vec, right):
    if right is None:
        return BoolVector(len(vec))

    # InList: `right` is a Python collection of literals (the only legitimate
    # non-vector RHS — it predates the carchar/perfect-hash set wrappers that
    # draken_compare builds upstream).
    if op_code == OP_IN_LIST:
        return _string_in_list(vec, _coerce_str_set(right))

    # Eq / NotEq / Lt / Gt / LtEq / GtEq: vector-to-vector. `right` is a wrapped
    # literal (or a column); the *_vector kernels walk both operands together
    # and handle every layout (constant/dict/dense) internally.
    if op_code <= OP_GT_EQ:
        if op_code == OP_EQ:
            return vec.equals_vector(right)
        if op_code == OP_NOT_EQ:
            return vec.not_equals_vector(right)
        if op_code == OP_LT:
            return vec.less_than_vector(right)
        if op_code == OP_GT:
            return vec.greater_than_vector(right)
        if op_code == OP_LT_EQ:
            return vec.less_than_or_equals_vector(right)
        if op_code == OP_GT_EQ:
            return vec.greater_than_or_equals_vector(right)

    # LIKE / RLIKE / InStr family: vector-to-scalar. The pattern arrives wrapped
    # as a StringVector; the kernels enforce the single-pattern shape rule
    # (data_length == 1) and read the pattern bytes from the arena.
    if op_code == OP_LIKE:
        return vector_like(vec, right, False)
    if op_code == OP_ILIKE:
        return vector_like(vec, right, True)
    if op_code == OP_RLIKE:
        return vector_rlike(vec, right)
    if op_code == OP_IN_STR:
        return _string_contains(vec, right, False)
    if op_code == OP_I_IN_STR:
        return _string_contains(vec, right, True)
    raise NotImplementedError(f"StringVector: unsupported op (code {op_code})")


cpdef _string_anyop_like(vec, patterns, bint ignore_case):
    cdef list pat_list
    cdef object result = None
    cdef object mask
    cdef object needle

    if isinstance(patterns, (list, tuple)):
        pat_list = list(patterns)
    else:
        pat_list = [patterns]

    for p in pat_list:
        if p is None:
            continue
        # AnyOp iterates individual patterns; wrap each as a 1-row constant
        # StringVector so the single-pattern kernel can read it.
        needle = StringVector.from_constant(p, 1)
        mask = vector_like(vec, needle, ignore_case)
        if result is None:
            result = mask
        else:
            result = result.or_vector(mask)

    if result is None:
        return BoolVector(len(vec))
    return result
