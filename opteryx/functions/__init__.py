# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
SQL Functions Module

This module provides kernel implementations for SQL functions available in
Opteryx queries. Functions are registered in the catalog via
opteryx.expression.functions.native_function_registrar and dispatched through
the expression evaluator (opteryx.expression.evaluator).

This module contains:
- Kernel callables used by the catalog registrar
- Utility helpers (iterate wrappers, null-handling combinators)
- Re-exports of implementations from expression/functions/implementations/

Adding New Functions:
1. Implement the kernel in the appropriate implementations/ module
2. Register it in native_function_registrar.py
3. Add tests in tests/functions/
"""

import datetime
import decimal
import inspect
import time

import numpy
import orjson
import pyarrow
from orso.types import OrsoTypes
from pyarrow import ArrowNotImplementedError
from pyarrow import compute

import opteryx
from opteryx.compiled.vector_ops import vector_contains_all
from opteryx.compiled.vector_ops import vector_contains_any
from opteryx.compiled.vector_ops import vector_encode_utf8 as to_blob
from opteryx.exceptions import FunctionExecutionError
from opteryx.exceptions import IncorrectTypeError
from opteryx.expression.casts import cast
from opteryx.expression.casts import cast_to_blob
from opteryx.expression.casts import cast_to_double
from opteryx.expression.casts import cast_to_int
from opteryx.expression.casts import cast_to_varchar
from opteryx.expression.casts import try_cast
from opteryx.expression.functions.implementations.text import _initcap
from opteryx.expression.functions.implementations.text import _md5
from opteryx.expression.functions.implementations.text import _replace
from opteryx.expression.functions.implementations.text import _sha1
from opteryx.expression.functions.implementations.text import _sha256
from opteryx.expression.functions.implementations.text import _sha512
from opteryx.expression.functions.implementations.text import _soundex
from opteryx.expression.functions.implementations.text import _string_slice_left
from opteryx.expression.functions.implementations.text import _string_slice_right
from opteryx.expression.functions.implementations.text import to_lower
from opteryx.expression.functions.implementations.text import to_upper
from opteryx.expression.functions.implementations.text import vector_lengther
from opteryx.third_party.cyan4973.xxhash import hash_bytes
from opteryx.utils import dates

# to_lower, to_upper imported from opteryx.expression.functions.implementations.text


def vector_encode_utf8(arr):
    try:
        # vector_encode_utf8 is fast but brittle
        return to_blob(arr)
    except Exception:
        return [None if s is None else str(s).encode() for s in arr]


def _get_string(array, key):
    key = key[0]
    return pyarrow.array(
        [None if i != i else str(i) for i in (item.get(key) for item in array)],
        type=pyarrow.string(),
    )


def cast_varchar(arr):
    if len(arr) > 0 and all(i is None or isinstance(i, dict) for i in arr[:100]):
        return [json_dumps(n).decode() if n is not None else None for n in arr]
    return compute.cast(arr, "string")


def cast_blob(arr):
    """
    Checks if the first 100 elements of arr are either None or bytes.
    If true, returns the original array. Otherwise, converts all elements
    to strings and then encodes them to bytes.

    Parameters:
    arr (list): The input list to be checked and potentially converted.

    Returns:
    list: The original list if all elements in the first 100 are None or bytes,
          otherwise a new list with all elements converted to bytes.
    """
    if len(arr) > 0 and all(i is None or isinstance(i, bytes) for i in arr[:100]):
        return arr
    return [None if a is None else str(a).encode() for a in arr]


def fixed_value_function(function, context):
    from orso.types import OrsoTypes

    if function in ("VERSION",):
        return OrsoTypes.VARCHAR, opteryx.__version__
    if function in ("NOW", "UTC_TIMESTAMP"):
        return OrsoTypes.TIMESTAMP, numpy.datetime64(context.execution_context.connected_at, "us")
    if function in ("CURRENT_TIME",):
        # CURRENT_TIME is an alias for NOW, so we return the same value
        return OrsoTypes.TIME, context.execution_context.connected_at.time()
    if function in ("CURRENT_TIMESTAMP",):
        # CURRENT_TIMESTAMP is an alias for NOW, so we return the same value
        return OrsoTypes.TIMESTAMP, numpy.datetime64(context.execution_context.connected_at, "us")
    if function in ("CURRENT_DATE", "TODAY"):
        return OrsoTypes.DATE, numpy.datetime64(context.execution_context.connected_at.date())
    if function in ("YESTERDAY",):
        return OrsoTypes.DATE, numpy.datetime64(
            context.execution_context.connected_at.date() - datetime.timedelta(days=1), "D"
        )
    if function == "CONNECTION_ID":
        return OrsoTypes.INTEGER, context.execution_context.query_id
    if function == "DATABASE":
        return OrsoTypes.VARCHAR, context.execution_context.schema or "DEFAULT"
    if function == "USER":
        return OrsoTypes.VARCHAR, context.execution_context.user or "ANONYMOUS"
    if function == "PI":
        return OrsoTypes.DOUBLE, 3.14159265358979323846264338327950288419716939937510
    if function == "PHI":
        # the golden ratio
        return OrsoTypes.DOUBLE, 1.61803398874989484820458683436563811772030917980576
    if function == "E":
        # eulers number
        return OrsoTypes.DOUBLE, 2.71828182845904523536028747135266249775724709369995
    if function == "UTC_TIMESTAMP":
        # UTC timestamp
        return OrsoTypes.TIMESTAMP, numpy.datetime64(datetime.datetime.now(datetime.UTC), "us")
    if function == "UNIXTIME":
        # We should only ever get here if the function is called without parameters
        return OrsoTypes.INTEGER, context.execution_context.connected_at.timestamp()
    if function == "YEAR":
        return OrsoTypes.INTEGER, context.execution_context.connected_at.year
    if function == "MONTH":
        return OrsoTypes.INTEGER, context.execution_context.connected_at.month
    if function == "DAY":
        return OrsoTypes.INTEGER, context.execution_context.connected_at.day
    if function == "HOUR":
        return OrsoTypes.INTEGER, context.execution_context.connected_at.hour
    if function == "MINUTE":
        return OrsoTypes.INTEGER, context.execution_context.connected_at.minute
    if function == "SECOND":
        return OrsoTypes.INTEGER, context.execution_context.connected_at.second
    return None, None


def safe(func, *parms, **kwargs):
    """execute a function, return None if fails"""
    try:
        return func(*parms, **kwargs)
    except (
        ValueError,
        IndexError,
        TypeError,
        ArrowNotImplementedError,
        AttributeError,
        decimal.InvalidOperation,
    ) as e:
        return None


# Cast functions have been moved to opteryx.expression.casts
# They are imported at the top of this file for backward compatibility


def _iterate_single_parameter(func):
    def _inner(array):
        return pyarrow.array(list(map(func, array)))

    return _inner


def _sort(func):
    def _inner(array):
        return pyarrow.array([func(item) for item in array])

    return _inner


def _iterate_double_parameter(func):
    """
    for functions called FUNCTION(field, literal)
    """

    def _inner(array, literal):
        if isinstance(array, str):
            array = [array]
        return pyarrow.array(func(item, literal[index]) for index, item in enumerate(array))

    return _inner


def _iterate_double_parameter_swapped(func):
    """
    for functions called FUNCTION(literal, field) when planner supplies
    arrays as (literal_values, field_values).
    """

    def _inner(array, literal):
        if isinstance(array, str):
            array = [array]
        return pyarrow.array(func(literal[index], item) for index, item in enumerate(array))

    return _inner


def _coalesce(*arrays):
    """
    Element-wise coalesce function for multiple numpy arrays.
    Selects the first non-None item in each row across the input arrays.

    Parameters:
        arrays: tuple of numpy arrays

    Returns:
        numpy array with coalesced values
    """
    # Start with an array full of None values
    result = numpy.array(arrays[0], dtype=object)

    mask = result == None

    for arr in arrays[1:]:
        mask = numpy.array([None if value != value else value for value in result]) == None
        numpy.copyto(result, arr, where=mask)

    return result


def select_values(boolean_arrays, value_arrays):
    """
    Build a result array based on boolean conditions and corresponding value arrays.

    Parameters:
    - boolean_arrays: List[np.ndarray], list of boolean arrays representing conditions.
    - value_arrays: List[np.ndarray], list of arrays with values corresponding to each condition.

    Returns:
    - np.ndarray: Result array with selected values or False where no condition is met.
    """
    # Ensure the input lists are not empty and have the same length
    if not boolean_arrays or not value_arrays or len(boolean_arrays) != len(value_arrays):
        raise ValueError("Input lists must be non-empty and of the same length.")

    # Initialize the result array with False, assuming no condition will be met
    result = numpy.full(len(boolean_arrays[0]), None)

    # Iterate over pairs of boolean and value arrays
    for condition, values in zip(reversed(boolean_arrays), reversed(value_arrays)):
        # Update the result array where the condition is True
        numpy.putmask(result, condition, values)

    return result


def sleep(x):
    time.sleep(x[0] / 1000)  # Sleep for x[0] milliseconds
    return x[0]


def is_function(name: str) -> bool:
    """
    Check if the given name is a valid function name.
    """
    from opteryx.expression.functions import get_catalog

    return get_catalog().get_definition(name.upper()) is not None


def functions() -> list[str]:
    """
    Return a list of all available function names.
    """
    from opteryx.expression.functions import get_catalog

    return [f.name for f in get_catalog().list_functions()]
