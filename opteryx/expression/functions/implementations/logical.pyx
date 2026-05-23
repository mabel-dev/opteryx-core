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

    if not values.__class__.__module__.startswith("draken.vectors."):
        raise TypeError(f"IFNULL expects Draken vector input, got {type(values).__name__}.")
    if not replacements.__class__.__module__.startswith("draken.vectors."):
        raise TypeError(f"IFNULL expects Draken vector replacement, got {type(replacements).__name__}.")

    # Draken vector — is_null() returns int8_t[::1] memoryview
    n = len(values)
    null_mask = values.is_null()  # int8_t[::1]: 1 = null, 0 = not null
    null_boolvec = bool_vector_from_int8_mask(null_mask, n)
    return vector_iif(null_boolvec, replacements, values)


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
    return vector_iif(not_null_boolvec, replacements, values)


def null_if(col1, col2):
    """
    Returns null if col1 equals col2, otherwise returns col1.
    """

    # Convert Draken vectors to Python lists
    col1_list = col1.to_pylist()
    col2_list = col2.to_pylist()
    n = len(col1_list)

    # Validate type compatibility on first non-null pair
    from opteryx.types import PYTHON_TO_ORSO_MAP, OrsoTypes, find_compatible_type

    def _first_non_null_type(lst):
        for item in lst:
            if item is not None:
                return PYTHON_TO_ORSO_MAP.get(type(item), OrsoTypes._MISSING_TYPE)
        return OrsoTypes.NULL

    col1_type = _first_non_null_type(col1_list)
    col2_type = _first_non_null_type(col2_list)

    if col1_type not in (OrsoTypes.NULL, OrsoTypes._MISSING_TYPE) and col2_type not in (
        OrsoTypes.NULL,
        OrsoTypes._MISSING_TYPE,
    ):
        compatible = find_compatible_type([col1_type, col2_type])
        if compatible is None or compatible == OrsoTypes._MISSING_TYPE:
            raise IncompatibleTypesError(
                left_type=col1_type,
                right_type=col2_type,
                message=f"`NULLIF` called with input arrays of different types, {col1_type} and {col2_type}.",
            )

    # Element-wise: return None where equal, else col1
    if len(col2_list) == 1:
        eq_val = col2_list[0]
        return [None if c1 == eq_val else c1 for c1 in col1_list]

    return [None if c1 == c2 else c1 for c1, c2 in zip(col1_list, col2_list)]
