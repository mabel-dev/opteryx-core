# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Logical and control flow function kernels.

Includes:
- Null handling: COALESCE, IFNULL, IFNOTNULL, NULLIF
- Conditional logic: CASE, IIF
- Array membership: ARRAY_CONTAINS

Note: Binary logical operators (And, Or, Xor, Not) are handled as binary_operators and
logical operators respectively.
"""

from opteryx.exceptions import IncompatibleTypesError


def array_contains(array, item):
    """does array contain item"""
    if array is None:
        return False
    return item in set(array)


def if_null(values, replacements):
    """
    Replace null values in the input array with corresponding values from the replacement array.
    IIF(IS_NULL(values), replacements, values)
    """
    from opteryx.compiled.nanobind.vector_selection_concat import vector_iif
    from opteryx.compiled.nanobind.vector_bool_ops import bool_vector_from_int8_mask
    from draken.vectors.vector import Vector as _ShimVector

    if not values.__class__.__module__.startswith("draken.vectors."):
        raise TypeError(f"IFNULL expects Draken vector input, got {type(values).__name__}.")
    if not replacements.__class__.__module__.startswith("draken.vectors."):
        raise TypeError(f"IFNULL expects Draken vector replacement, got {type(replacements).__name__}.")

    # Draken vector — is_null() returns int8_t[::1] memoryview
    n = len(values)
    null_mask = values.is_null()  # int8_t[::1]: 1 = null, 0 = not null
    null_boolvec = bool_vector_from_int8_mask(null_mask, n)

    # vector_iif is a nanobind kernel: it unwraps raw draken_native Vectors. The
    # non-nb function-call path hands us Cython shims (draken.vectors.vector), so
    # unwrap to ._nb before the call and re-wrap the raw result in a shim — the
    # convention every non-nb function follows (see null_if).
    result = vector_iif(null_boolvec, replacements._nb, values._nb)
    return _ShimVector(result)


def if_not_null(values, replacements):
    """
    Replace a value only if it is NOT null.

    For each element:
        if value is NOT null → use replacement
        if value IS null    → keep the original null
    IIF(IS_NOT_NULL(values), replacements, values)
    """
    from array import array as _array

    from opteryx.compiled.nanobind.vector_selection_concat import vector_iif
    from opteryx.compiled.nanobind.vector_bool_ops import bool_vector_from_int8_mask
    from draken.vectors.vector import Vector as _ShimVector

    if not values.__class__.__module__.startswith("draken.vectors."):
        raise TypeError(f"IFNOTNULL expects Draken vector input, got {type(values).__name__}.")
    if not replacements.__class__.__module__.startswith("draken.vectors."):
        raise TypeError(
            f"IFNOTNULL expects Draken vector replacement, got {type(replacements).__name__}."
        )

    # Draken vector — is_null() returns int8_t[::1] memoryview
    n = len(values)
    null_mask = values.is_null()  # int8_t[::1]: 1 = null, 0 = not null
    # Invert: 1 = not null (true → use replacement), 0 = null (false → keep null)
    inv_mask = _array("b", [0 if b else 1 for b in null_mask])
    not_null_boolvec = bool_vector_from_int8_mask(inv_mask, n)

    # vector_iif is a nanobind kernel: unwrap shims to ._nb and re-wrap the raw
    # result (see if_null / null_if for the non-nb function convention).
    result = vector_iif(not_null_boolvec, replacements._nb, values._nb)
    return _ShimVector(result)


def null_if(col1, col2):
    """
    Returns null if col1 equals col2, otherwise returns col1.
    """

    # Convert Draken vectors to Python lists
    col1_list = col1.to_pylist()
    col2_list = col2.to_pylist()
    n = len(col1_list)

    # Validate type compatibility on first non-null pair
    from opteryx.types import PYTHON_TO_SQL_MAP, LogicalCategory, find_compatible_type

    def _first_non_null_type(lst):
        for item in lst:
            if item is not None:
                return PYTHON_TO_SQL_MAP.get(type(item))
        return LogicalCategory.NULL

    col1_type = _first_non_null_type(col1_list)
    col2_type = _first_non_null_type(col2_list)

    if col1_type not in (LogicalCategory.NULL, None) and col2_type not in (
        LogicalCategory.NULL,
        None,
    ):
        compatible = find_compatible_type([col1_type, col2_type])
        if compatible is None:
            raise IncompatibleTypesError(
                left_type=col1_type,
                right_type=col2_type,
                message=f"`NULLIF` called with input arrays of different types, {col1_type} and {col2_type}.",
            )

    # Element-wise: None where equal, else col1.
    if len(col2_list) == 1:
        eq_val = col2_list[0]
        result_list = [None if c1 == eq_val else c1 for c1 in col1_list]
    else:
        result_list = [None if c1 == c2 else c1 for c1, c2 in zip(col1_list, col2_list)]

    # NULLIF returns "same type as value" (col1). Build a typed Draken Vector —
    # returning a raw Python list produces a column that fails to wrap (and a
    # heap-corrupting type confusion when the null-bearing result is consumed by
    # string kernels). vector_from_sequence dispatches on col1's physical
    # DrakenType, so the result carries col1's type and null rows are real nulls.
    #
    # Wrap in the Cython shim Vector (draken.vectors.vector): the executor stores
    # this Python-function result without an NB_WRAP step, and a shim is what every
    # downstream consumer expects — from_vectors keeps it as-is, and an nb-function
    # parent unwraps it via ._nb. A raw nanobind Vector here would be cast as a shim
    # by an nb-parent and crash.
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.vectors.vector import Vector as _ShimVector

    return _ShimVector(vector_from_sequence(result_list, dtype=col1.type))
