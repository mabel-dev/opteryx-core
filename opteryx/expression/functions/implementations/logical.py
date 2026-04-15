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


# Draken vector class names — used to guard against PyArrow arrays that also
# expose .is_null() but return a PyArrow BooleanArray instead of int8_t[::1].
_DRAKEN_VECTOR_CLASSES = frozenset(
    {
        "StringVector",
        "Int64Vector",
        "IntegerVector",
        "Float64Vector",
        "BoolVector",
        "TimestampVector",
        "Date32Vector",
        "IntervalVector",
        "ArrayVector",
        "VectorVector",
        "DecimalVector",
    }
)


def if_null(values, replacements):
    """
    Replace null values in the input array with corresponding values from the replacement array.
    IIF(IS_NULL(values), replacements, values)
    """
    from opteryx.compiled.vector_ops import vector_iif
    from opteryx.compiled.vector_ops.function_definitions import bool_vector_from_int8_mask

    # Convert PyArrow arrays to Draken vectors so the fast path can be used
    if type(values).__name__ not in _DRAKEN_VECTOR_CLASSES and hasattr(values, "type"):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        values = vector_from_arrow(
            values.combine_chunks() if hasattr(values, "combine_chunks") else values
        )
    if (
        type(replacements).__name__ not in _DRAKEN_VECTOR_CLASSES
        and hasattr(replacements, "type")
        and not isinstance(replacements, type)
    ):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        replacements = vector_from_arrow(
            replacements.combine_chunks()
            if hasattr(replacements, "combine_chunks")
            else replacements
        )

    # Fast path: native Draken vector — is_null() returns int8_t[::1] memoryview
    if type(values).__name__ in _DRAKEN_VECTOR_CLASSES:
        n = len(values)
        null_mask = values.is_null()  # int8_t[::1]: 1 = null, 0 = not null
        null_boolvec = bool_vector_from_int8_mask(null_mask, n)
        return vector_iif(null_boolvec, replacements, values)

    # Fallback: Python iterable (should rarely be reached in production)
    vals = (
        list(values)
        if hasattr(values, "__iter__") and not isinstance(values, (str, bytes))
        else [values]
    )
    n = len(vals)

    if hasattr(replacements, "to_pylist"):
        repls = replacements.to_pylist()
    elif hasattr(replacements, "__iter__") and not isinstance(replacements, (str, bytes)):
        repls = list(replacements)
    else:
        repls = [replacements]

    if len(repls) == 1:
        r = repls[0]
        return [r if v is None else v for v in vals]

    return [r if v is None else v for v, r in zip(vals, repls)]


def if_not_null(values, replacements):
    """
    Replace a value only if it is NOT null.

    For each element:
        if value is NOT null → use replacement
        if value IS null    → keep the original null
    IIF(IS_NOT_NULL(values), replacements, values)
    """
    from array import array as _array

    from opteryx.compiled.vector_ops import vector_iif
    from opteryx.compiled.vector_ops.function_definitions import bool_vector_from_int8_mask

    # Convert PyArrow arrays to Draken vectors so the fast path can be used
    if type(values).__name__ not in _DRAKEN_VECTOR_CLASSES and hasattr(values, "type"):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        values = vector_from_arrow(
            values.combine_chunks() if hasattr(values, "combine_chunks") else values
        )
    if (
        type(replacements).__name__ not in _DRAKEN_VECTOR_CLASSES
        and hasattr(replacements, "type")
        and not isinstance(replacements, type)
    ):
        from opteryx.compiled.draken.interop.arrow import vector_from_arrow

        replacements = vector_from_arrow(
            replacements.combine_chunks()
            if hasattr(replacements, "combine_chunks")
            else replacements
        )

    # Fast path: native Draken vector — is_null() returns int8_t[::1] memoryview
    if type(values).__name__ in _DRAKEN_VECTOR_CLASSES:
        n = len(values)
        null_mask = values.is_null()  # int8_t[::1]: 1 = null, 0 = not null
        # Invert: 1 = not null (true → use replacement), 0 = null (false → keep null)
        inv_mask = _array("b", [0 if b else 1 for b in null_mask])
        not_null_boolvec = bool_vector_from_int8_mask(inv_mask, n)
        return vector_iif(not_null_boolvec, replacements, values)

    # Fallback: Python iterable
    vals = (
        list(values)
        if hasattr(values, "__iter__") and not isinstance(values, (str, bytes))
        else [values]
    )
    n = len(vals)

    if hasattr(replacements, "to_pylist"):
        repls = replacements.to_pylist()
    elif hasattr(replacements, "__iter__") and not isinstance(replacements, (str, bytes)):
        repls = list(replacements)
    else:
        repls = [replacements]

    if len(repls) == 1:
        r = repls[0]
        return [r if v is not None else None for v in vals]

    return [r if v is not None else None for v, r in zip(vals, repls)]


def null_if(col1, col2):
    """
    Returns null if col1 equals col2, otherwise returns col1.
    """

    # Normalise both operands to Python lists for comparison
    def _to_pylist(v):
        if hasattr(v, "to_pylist"):
            return v.to_pylist()
        if hasattr(v, "to_arrow"):
            return v.to_arrow().to_pylist()
        if isinstance(v, list):
            return v
        return list(v) if hasattr(v, "__iter__") and not isinstance(v, (str, bytes)) else [v]

    col1_list = _to_pylist(col1)
    col2_list = _to_pylist(col2)
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
