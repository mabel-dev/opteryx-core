"""Unary operations (IS NULL / IS TRUE / IS FALSE / IS EMPTY / BitwiseNot).

Thin Cython dispatch over the underlying Draken / vector_ops kernels. All
`values` inputs must be Draken vectors; an AttributeError surfaces if not.
"""

from opteryx.compiled.vector_ops import (
    vector_bitwise_not,
    vector_string_is_empty,
    vector_string_is_not_empty,
)


cpdef _is_null(values):
    return values.is_null()


cpdef _is_not_null(values):
    return values.is_null().not_vector()


cpdef _is_true(values):
    return values.equals(True)


cpdef _is_false(values):
    return values.equals(False)


cpdef _is_not_true(values):
    return values.equals(True).not_vector()


cpdef _is_not_false(values):
    return values.equals(False).not_vector()


cpdef _bitwise_not(values):
    return vector_bitwise_not(values)


cpdef _is_empty(values):
    return vector_string_is_empty(values)


cpdef _is_not_empty(values):
    return vector_string_is_not_empty(values)


UNARY_OPERATIONS = {
    "IsNull":      _is_null,
    "IsNotFalse":  _is_not_false,
    "IsNotNull":   _is_not_null,
    "IsNotTrue":   _is_not_true,
    "IsTrue":      _is_true,
    "IsFalse":     _is_false,
    "IsEmpty":     _is_empty,
    "IsNotEmpty":  _is_not_empty,
    "BitwiseNot":  _bitwise_not,
}
