"""
Unary operations (IS NULL, IS TRUE, IS FALSE)
"""


def _is_null(values):
    """Check for null values. Input must be Draken vector."""
    return values.is_null()


def _is_not_null(values):
    """Check for non-null values. Input must be Draken vector."""
    return values.is_null().not_vector()


def _is_true(values):
    """Check for TRUE values in boolean vector. Input must be Draken BoolVector."""
    return values.equals(True)


def _is_false(values):
    """Check for FALSE values in boolean vector. Input must be Draken BoolVector."""
    return values.equals(False)


def _is_not_true(values):
    """Check for NOT TRUE values. Input must be Draken BoolVector."""
    return values.equals(True).not_vector()


def _is_not_false(values):
    """Check for NOT FALSE values. Input must be Draken BoolVector."""
    return values.equals(False).not_vector()


def _bitwise_not(values):
    """Bitwise complement of integer values. Input must be Draken Int64Vector."""
    from opteryx.compiled.vector_ops import vector_bitwise_not

    return vector_bitwise_not(values)


def _is_empty(values):
    """Check for empty strings. Input must be StringVector."""
    from opteryx.compiled.vector_ops import vector_string_is_empty

    return vector_string_is_empty(values)


def _is_not_empty(values):
    """Check for non-empty strings. Input must be StringVector."""
    from opteryx.compiled.vector_ops import vector_string_is_not_empty

    return vector_string_is_not_empty(values)


UNARY_OPERATIONS = {
    "IsNull": _is_null,
    "IsNotFalse": _is_not_false,
    "IsNotNull": _is_not_null,
    "IsNotTrue": _is_not_true,
    "IsTrue": _is_true,
    "IsFalse": _is_false,
    "IsEmpty": _is_empty,
    "IsNotEmpty": _is_not_empty,
    "BitwiseNot": _bitwise_not,
}
