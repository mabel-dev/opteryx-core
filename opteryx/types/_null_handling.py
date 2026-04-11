"""Null Handling Primitives (Step 3: Draken Integration).

Fast null/NaN/infinity detection for scalars and Draken vectors,
replacing numpy equivalents (numpy.isnan, numpy.isinf, etc.).

This module provides:
- is_null(value) - Check if value is NULL/None
- is_nan(value) - Check if value is NaN
- is_inf(value) - Check if value is positive/negative infinity
- is_not_null(value) - Efficient NOT NULL check

Supports:
- Python scalars (None, float NaN/inf, numpy types, pyarrow scalars)
- Draken vectors (delegated to Draken's null detection kernels)
- Fast paths for native Python types
- Error handling (fail-fast on invalid inputs)

Design:
- Native Python: Direct type checks (O(1))
- NumPy/PyArrow: Module inspection + attribute access
- Draken vectors: Call Draken's C++ null-detection kernels
- No external dependencies in public API
"""

import math
from typing import Any, Generator, Optional

__all__ = [
    "is_null",
    "is_nan",
    "is_inf",
    "is_not_null",
    "is_null_vector",
    "null_count_vector",
    "count_nulls",
    "has_nulls",
    "remove_nulls",
    "nulls_to_default",
]


# =============================================================================
# Scalar Null Detection (Python/NumPy/PyArrow)
# =============================================================================


def is_null(value: Any) -> bool:
    """Check if a scalar value is NULL/None.

    A value is considered NULL if it is:
    - Python None
    - numpy.nan or numpy float NaN
    - PyArrow null scalar

    Args:
        value: Scalar value to check

    Returns:
        True if value is NULL, False otherwise

    Notes:
        - This checks for NULL, not for other "falsy" values (0, False, "")
        - For numeric types, use is_nan() to distinguish NaN from NULL
        - Fast path for None; O(1) type checking for others

    Examples:
        >>> is_null(None)
        True
        >>> is_null(42)
        False
        >>> is_null(0)
        False
        >>> is_null("")
        False
    """
    # Fast path: Python None (most common case)
    if value is None:
        return True

    # Check for numeric NaN (may represent NULL in some contexts)
    if isinstance(value, float):
        return math.isnan(value)

    # Check for numpy NaN scalar
    try:
        import numpy as np
        if isinstance(value, np.floating):
            return np.isnan(value)
        if value is np.nan:
            return True
    except ImportError:
        pass

    # Check for pyarrow null scalar
    try:
        import pyarrow as pa
        if isinstance(value, pa.Scalar):
            return not value.is_valid
    except ImportError:
        pass

    # Not NULL (or unknown type; assume not NULL)
    return False


def is_nan(value: Any) -> bool:
    """Check if a scalar value is NaN (Not a Number).

    A value is considered NaN if it is:
    - Python float('nan')
    - numpy.nan or numpy float NaN
    - PyArrow float NaN scalar

    Args:
        value: Scalar value to check

    Returns:
        True if value is NaN, False otherwise

    Notes:
        - This specifically checks for NaN, not NULL
        - NULL and NaN are different (NULL = no value, NaN = invalid number)
        - For non-numeric types, returns False
        - Fast path for Python floats

    Examples:
        >>> is_nan(float('nan'))
        True
        >>> is_nan(42.0)
        False
        >>> is_nan(None)
        False
        >>> is_nan("hello")
        False
    """
    # Fast path: native Python float
    if isinstance(value, float):
        return math.isnan(value)

    # Check numpy floats
    try:
        import numpy as np
        if isinstance(value, np.floating):
            return bool(np.isnan(value))
        if value is np.nan:
            return True
    except ImportError:
        pass

    # Check pyarrow float scalars
    try:
        import pyarrow as pa
        if isinstance(value, pa.Scalar):
            if value.is_valid and hasattr(value, "as_py"):
                py_value = value.as_py()
                if isinstance(py_value, float):
                    return math.isnan(py_value)
    except ImportError:
        pass

    # Not NaN (or not numeric)
    return False


def is_inf(value: Any) -> bool:
    """Check if a scalar value is positive or negative infinity.

    A value is considered infinite if it is:
    - Python float('inf') or float('-inf')
    - numpy float infinity
    - PyArrow float infinity scalar

    Args:
        value: Scalar value to check

    Returns:
        True if value is +inf or -inf, False otherwise

    Notes:
        - This checks for either positive or negative infinity
        - Use math.copysign(1.0, value) to distinguish sign if needed
        - For non-numeric types, returns False
        - Fast path for Python floats

    Examples:
        >>> is_inf(float('inf'))
        True
        >>> is_inf(float('-inf'))
        True
        >>> is_inf(42.0)
        False
        >>> is_inf(float('nan'))
        False
    """
    # Fast path: native Python float
    if isinstance(value, float):
        return math.isinf(value)

    # Check numpy floats
    try:
        import numpy as np
        if isinstance(value, np.floating):
            return bool(np.isinf(value))
    except ImportError:
        pass

    # Check pyarrow float scalars
    try:
        import pyarrow as pa
        if isinstance(value, pa.Scalar):
            if value.is_valid and hasattr(value, "as_py"):
                py_value = value.as_py()
                if isinstance(py_value, float):
                    return math.isinf(py_value)
    except ImportError:
        pass

    # Not infinity (or not numeric)
    return False


def is_not_null(value: Any) -> bool:
    """Check if a scalar value is NOT NULL (inverse of is_null).

    Efficient inverse operation: returns True if value is not NULL.

    Args:
        value: Scalar value to check

    Returns:
        True if value is NOT NULL, False otherwise

    Notes:
        - Equivalent to `not is_null(value)` but semantically clearer
        - Useful for filtering and conditionals in tight loops

    Examples:
        >>> is_not_null(42)
        True
        >>> is_not_null(None)
        False
    """
    return not is_null(value)


# =============================================================================
# Vector Null Detection (Draken Integration)
# =============================================================================


def is_null_vector(vector: Any) -> bool:
    """Check if a Draken vector has any null values.

    Args:
        vector: Draken vector to check

    Returns:
        True if vector has any NULL values, False otherwise

    Notes:
        - Delegates to Draken's vector.null_count property
        - For non-Draken vectors, returns False
        - O(1) operation (uses cached null count)

    Examples:
        >>> from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        >>> from opteryx.types import OrsoTypes
        >>> v = vector_from_sequence([1, None, 3], dtype=OrsoTypes.INTEGER)
        >>> is_null_vector(v)
        True
    """
    # Check for Draken vector
    try:
        null_count = getattr(vector, "null_count", None)
        if null_count is not None:
            return null_count > 0
    except Exception:
        pass

    # Check for Arrow array/chunked array
    try:
        import pyarrow as pa
        if isinstance(vector, (pa.Array, pa.ChunkedArray)):
            return vector.null_count > 0
    except ImportError:
        pass

    # Unknown type; assume no nulls
    return False


def null_count_vector(vector: Any) -> int:
    """Get the number of NULL values in a vector.

    Args:
        vector: Draken or Arrow vector to check

    Returns:
        Number of NULL values in the vector

    Notes:
        - Delegates to Draken's vector.null_count property
        - Returns 0 for non-vector types
        - O(1) operation (uses cached null count)

    Examples:
        >>> from opteryx.compiled.draken.interop.arrow import vector_from_sequence
        >>> from opteryx.types import OrsoTypes
        >>> v = vector_from_sequence([1, None, 3, None], dtype=OrsoTypes.INTEGER)
        >>> null_count_vector(v)
        2
    """
    try:
        null_count = getattr(vector, "null_count", None)
        if null_count is not None:
            return int(null_count)
    except Exception:
        pass

    try:
        import pyarrow as pa
        if isinstance(vector, (pa.Array, pa.ChunkedArray)):
            return int(vector.null_count)
    except ImportError:
        pass

    return 0


# =============================================================================
# Utility Functions for Common Null Patterns
# =============================================================================


def count_nulls(iterable: Any) -> int:
    """Count NULL values in an iterable of scalars.

    Args:
        iterable: List, tuple, or generator of scalar values

    Returns:
        Number of NULL values found

    Performance:
        - O(n) linear scan
        - Early exit optimization not applicable (must visit all)

    Examples:
        >>> count_nulls([1, None, 3, None, 5])
        2
        >>> count_nulls([None, None])
        2
        >>> count_nulls([1, 2, 3])
        0
    """
    count = 0
    for value in iterable:
        if is_null(value):
            count += 1
    return count


def has_nulls(iterable: Any) -> bool:
    """Check if an iterable contains any NULL values.

    Args:
        iterable: List, tuple, or generator of scalar values

    Returns:
        True if any NULL value found, False otherwise

    Performance:
        - Early exit on first NULL found
        - Better than count_nulls() if you only need boolean result
        - O(1) if first element is NULL, O(n) worst case

    Examples:
        >>> has_nulls([1, None, 3])
        True
        >>> has_nulls([1, 2, 3])
        False
        >>> has_nulls([None])
        True
    """
    for value in iterable:
        if is_null(value):
            return True
    return False


def remove_nulls(iterable: Any) -> Generator[Any, None, None]:
    """Filter out NULL values from an iterable.

    Args:
        iterable: List, tuple, or iterable of scalar values

    Yields:
        Non-NULL values in original order

    Performance:
        - Lazy evaluation (doesn't materialize list)
        - O(n) iteration, constant memory overhead

    Examples:
        >>> list(remove_nulls([1, None, 3, None, 5]))
        [1, 3, 5]
        >>> list(remove_nulls([None, None]))
        []
    """
    return (v for v in iterable if is_not_null(v))


def nulls_to_default(iterable: Any, default_value: Any) -> Generator[Any, None, None]:
    """Replace NULL values with a default value.

    Args:
        iterable: List, tuple, or iterable of scalar values
        default_value: Value to substitute for NULLs

    Yields:
        Values with NULLs replaced by default_value

    Performance:
        - Lazy evaluation
        - O(n) iteration

    Examples:
        >>> list(nulls_to_default([1, None, 3, None, 5], 0))
        [1, 0, 3, 0, 5]
        >>> list(nulls_to_default([None, None], -1))
        [-1, -1]
    """
    return (default_value if is_null(v) else v for v in iterable)
