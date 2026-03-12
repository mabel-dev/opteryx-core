# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Logical and control flow function kernels.

Includes:
- Null handling: COALESCE, IFNULL, IFNOTNULL, NULLIF
- Conditional logic: CASE, IIF
- Array membership: ARRAY_CONTAINS

Note: Binary logical operators (And, Or, Xor, Not) are handled as binary_operators and
logical operators respectively.
"""

import numpy
import pyarrow

from opteryx.exceptions import IncompatibleTypesError


def array_contains(array, item):
    """does array contain item"""
    if array is None:
        return False
    return item in set(array)


def if_null(values, replacements):
    """
    Replace null values in the input array with corresponding values from the replacement array.
    """
    from opteryx.expression.unary_operations import _is_null

    # Broadcast scalar replacement to a 1-element numpy array so the length
    # checks below work uniformly regardless of caller.
    if not hasattr(replacements, "__len__") and not hasattr(replacements, "to_numpy"):
        replacements = numpy.array([replacements])

    is_null_mask = _is_null(values)

    if hasattr(replacements, "to_numpy"):
        replacements = replacements.to_numpy(zero_copy_only=False)
    if hasattr(values, "to_numpy"):
        values = values.to_numpy(zero_copy_only=False)

    if len(replacements) == 1:
        if isinstance(replacements, numpy.ndarray):
            replacement = replacements[0]
            if hasattr(is_null_mask, "tolist"):
                is_null_mask = is_null_mask.tolist()
            if isinstance(replacement, (list, tuple)) or values.dtype == object:
                return numpy.array(
                    [
                        replacement if is_null else values[i]
                        for i, is_null in enumerate(is_null_mask)
                    ],
                    dtype=object,
                )
            return numpy.array(
                [replacement if is_null else values[i] for i, is_null in enumerate(is_null_mask)],
                dtype=values.dtype,
            )

        if values.dtype == object or isinstance(replacements[0], (list, tuple)):
            return numpy.array(
                [
                    replacements[0] if is_null else values[i]
                    for i, is_null in enumerate(
                        is_null_mask.tolist() if hasattr(is_null_mask, "tolist") else is_null_mask
                    )
                ],
                dtype=object,
            )

        replacements = numpy.full(values.shape, replacements[0], dtype=values.dtype)

    target_type = numpy.promote_types(values.dtype, replacements.dtype)
    return numpy.where(is_null_mask, replacements, values).astype(target_type)


def if_not_null(values: numpy.ndarray, replacements: numpy.ndarray) -> numpy.ndarray:
    """
    Replace a value only if it is not null.

    For each element:
        if value is NOT null → use replacement
        if value IS null → keep the original null
    """
    from opteryx.expression.unary_operations import _is_not_null

    if hasattr(replacements, "to_numpy"):
        replacements = replacements.to_numpy(zero_copy_only=False)
    if hasattr(values, "to_numpy"):
        values = values.to_numpy(zero_copy_only=False)

    is_not_null_mask = _is_not_null(values)
    target_type = numpy.promote_types(values.dtype, replacements.dtype)
    return numpy.where(is_not_null_mask, replacements, values).astype(target_type)


def null_if(col1, col2):
    """
    Returns null if col1 equals col2, otherwise returns col1.
    """
    if isinstance(col1, pyarrow.Array):
        col1 = col1.to_numpy(False)
    if isinstance(col1, list):
        col1 = col1.array(col1)
    if isinstance(col2, pyarrow.Array):
        col2 = col2.to_numpy(False)
    if isinstance(col2, list):
        col2 = col2.array(col2)

    from orso.types import PYTHON_TO_ORSO_MAP
    from orso.types import OrsoTypes
    from orso.types import find_compatible_type

    def get_first_non_null_type(array):
        for item in array:
            if item is not None:
                return PYTHON_TO_ORSO_MAP.get(type(item), OrsoTypes._MISSING_TYPE)
        return OrsoTypes.NULL

    col1_type = get_first_non_null_type(col1.tolist())
    col2_type = get_first_non_null_type(col2.tolist())

    if find_compatible_type([col1_type, col2_type], None) is None:
        raise IncompatibleTypesError(
            left_type=col1_type,
            right_type=col2_type,
            message=f"`NULLIF` called with input arrays of different types, {col1_type} and {col2_type}.",
        )

    mask = col1 == col2
    return numpy.where(mask, None, col1)
