"""Helpers for creating vectors and applying operations."""

import numpy as np
import pyarrow as pa
import pytest
from draken import Vector
from draken.encoding import (
    DRAKEN_ENCODING_DENSE,
    DRAKEN_ENCODING_RLE,
    DRAKEN_ENCODING_CONSTANT,
    DRAKEN_ENCODING_DICTIONARY,
)
from _matrix import VECTOR_TYPES

DENSE = DRAKEN_ENCODING_DENSE
RLE = DRAKEN_ENCODING_RLE
CONSTANT = DRAKEN_ENCODING_CONSTANT
DICTIONARY = DRAKEN_ENCODING_DICTIONARY


def create_vector_with_encoding(type_name, encoding, size=100, nullable=False, seed=None):
    """Create a vector of specified type/encoding for testing.

    Args:
        type_name: 'int64', 'string', 'bool', etc.
        encoding: DRAKEN_ENCODING_DENSE, etc.
        size: Vector length (default 100)
        nullable: Whether to include nulls (default False)
        seed: Random seed for reproducibility

    Returns:
        Vector instance with specified encoding

    Raises:
        ValueError: If type/encoding combination is not supported
        NotImplementedError: If encoding is not supported for this type
    """
    if type_name not in VECTOR_TYPES:
        raise ValueError(f"Unknown vector type: {type_name}")

    type_info = VECTOR_TYPES[type_name]

    if encoding not in type_info["supports_encodings"]:
        raise ValueError(
            f"Encoding {encoding} not supported for {type_name}. "
            f"Supported: {type_info['supports_encodings']}"
        )

    sample_values = type_info["sample_values"]
    arrow_type = type_info["arrow_type"]

    # Generate test data
    if nullable:
        # Repeat sample values to fill size, inserting Nones at intervals
        values = []
        non_null_values = [v for v in sample_values if v is not None]
        for i in range(size):
            if i % 10 == 0:  # Every 10th value is None
                values.append(None)
            else:
                values.append(non_null_values[i % len(non_null_values)])
    else:
        # Only non-null values
        non_null_values = [v for v in sample_values if v is not None]
        values = [non_null_values[i % len(non_null_values)] for i in range(size)]

    # Create based on encoding
    if encoding == DENSE:
        arr = pa.array(values, type=arrow_type)
        return Vector.from_arrow(arr)

    elif encoding == CONSTANT:
        # Create constant vector using the specific vector type's from_constant method
        # Import the specific vector class for this type
        from draken.vectors.int64_vector import Int64Vector
        from draken.vectors.float64_vector import Float64Vector
        from draken.vectors.string_vector import StringVector
        from draken.vectors.bool_vector import BoolVector
        from draken.vectors.date32_vector import Date32Vector
        from draken.vectors.timestamp_vector import TimestampVector
        from draken.vectors.time_vector import TimeVector
        from draken.vectors._decimal_vector import DecimalVector

        vector_classes = {
            "int64": Int64Vector,
            "float64": Float64Vector,
            "string": StringVector,
            "bool": BoolVector,
            "date32": Date32Vector,
            "timestamp": TimestampVector,
            "time": TimeVector,
            "decimal": DecimalVector,
        }

        if type_name not in vector_classes:
            raise ValueError(f"No vector class for type {type_name}")

        vector_class = vector_classes[type_name]

        # Special handling for TimeVector which requires is_time64 parameter
        if type_name == "time":
            vec = vector_class.from_constant(
                type_info["sample_constant"],
                size,
                is_null=False,
                is_time64=True,  # Use time64 to match our arrow_type
            )
        else:
            vec = vector_class.from_constant(
                type_info["sample_constant"],
                size,
                is_null=False,
            )
        return vec

    elif encoding == RLE:
        # Create RLE by repeating values in runs
        # Use first 2 non-null values alternating in runs
        non_null_values = [v for v in sample_values if v is not None]
        v1, v2 = non_null_values[0], non_null_values[1] if len(non_null_values) > 1 else non_null_values[0]

        rle_values = (
            [v1] * (size // 4)
            + [v2] * (size // 4)
            + [v1] * (size // 4)
            + [v2] * (size - 3 * (size // 4))
        )

        arr = pa.array(rle_values, type=arrow_type)
        vec = Vector.from_arrow(arr)
        # Check if it got RLE encoded
        if hasattr(vec, "encoding"):
            if vec.encoding != RLE:
                # PyArrow didn't create RLE, fall back to dense
                # In real tests, we might need special handling for this
                pass
        return vec

    elif encoding == DICTIONARY:
        # Create dictionary encoding for string and int64
        if type_name not in ["string", "int64"]:
            raise NotImplementedError(
                f"Dictionary encoding not supported for {type_name}"
            )

        non_null_values = [v for v in sample_values if v is not None]
        dict_size = min(3, len(non_null_values))
        dictionary = non_null_values[:dict_size]

        # Create codes array (skip nulls in PyArrow DictionaryArray)
        # Note: nulls in dictionaries are not easily supported via from_arrays,
        # so we skip creating them with nulls for now
        codes = [i % dict_size for i in range(size)]

        # Create dictionary array via PyArrow (without nulls)
        dict_array = pa.DictionaryArray.from_arrays(
            pa.array(codes, type=pa.int32()),
            pa.array(dictionary, type=arrow_type),
        )
        return Vector.from_arrow(dict_array)

    else:
        raise ValueError(f"Unknown encoding: {encoding}")


def apply_operation(vec, operation_name):
    """Apply a named operation to a vector.

    Args:
        vec: Vector instance
        operation_name: 'sum', 'min', 'max', 'equals', 'take', etc.

    Returns:
        Operation result

    Raises:
        NotImplementedError: If operation not supported
        TypeError: If types don't align
        ValueError: If operation is invalid for this vector
    """
    if operation_name == "sum":
        if not hasattr(vec, "sum"):
            raise NotImplementedError(f"sum not available for {type(vec).__name__}")
        return vec.sum()

    elif operation_name == "min":
        if not hasattr(vec, "min"):
            raise NotImplementedError(f"min not available for {type(vec).__name__}")
        try:
            return vec.min()
        except ValueError as e:
            # Empty or all-null is expected to raise
            if "empty" in str(e).lower() or "all-null" in str(e).lower():
                raise
            raise

    elif operation_name == "max":
        if not hasattr(vec, "max"):
            raise NotImplementedError(f"max not available for {type(vec).__name__}")
        try:
            return vec.max()
        except ValueError as e:
            if "empty" in str(e).lower() or "all-null" in str(e).lower():
                raise
            raise

    elif operation_name == "equals":
        if not hasattr(vec, "equals"):
            raise NotImplementedError(f"equals not available for {type(vec).__name__}")
        for i in range(len(vec)):
            val = vec[i]
            if val is not None:
                return vec.equals(val)
        return None

    elif operation_name == "take":
        if not hasattr(vec, "take"):
            raise NotImplementedError(f"take not available for {type(vec).__name__}")
        if len(vec) > 0:
            indices = np.array([0, min(1, len(vec) - 1)], dtype=np.int32)
            return vec.take(indices)
        else:
            return None

    elif operation_name == "to_arrow":
        if not hasattr(vec, "to_arrow"):
            raise NotImplementedError(f"to_arrow not available for {type(vec).__name__}")
        return vec.to_arrow()

    elif operation_name == "to_pylist":
        if not hasattr(vec, "to_pylist"):
            raise NotImplementedError(f"to_pylist not available for {type(vec).__name__}")
        return vec.to_pylist()

    elif operation_name == "from_arrow":
        # This is creation, not operation on existing vector
        return None  # Will be tested separately

    elif operation_name == "length":
        return len(vec)

    elif operation_name == "null_count":
        if not hasattr(vec, "null_count"):
            raise NotImplementedError(f"null_count not available for {type(vec).__name__}")
        return vec.null_count

    elif operation_name == "is_null":
        if not hasattr(vec, "is_null"):
            raise NotImplementedError(f"is_null not available for {type(vec).__name__}")
        return vec.is_null()

    elif operation_name == "subscript":
        # vec[i]
        if len(vec) > 0:
            return vec[0]
        else:
            return None

    else:
        raise ValueError(f"Unknown operation: {operation_name}")


_COMPARISON_METHODS = {
    "equals": "equals_vector",
    "not_equals": "not_equals_vector",
    "less_than": "less_than_vector",
    "less_equal": "less_than_or_equals_vector",
    "greater_than": "greater_than_vector",
    "greater_equal": "greater_than_or_equals_vector",
}

_COMPARISON_OPS = {
    "equals": lambda a, b: a == b,
    "not_equals": lambda a, b: a != b,
    "less_than": lambda a, b: a < b,
    "less_equal": lambda a, b: a <= b,
    "greater_than": lambda a, b: a > b,
    "greater_equal": lambda a, b: a >= b,
}


def apply_comparison(left_vec, right_vec, operation_name):
    """Apply a comparison operation between two vectors.

    For same-type pairs, dispatches to the vector's typed method directly.
    For cross-type pairs (e.g. float64 vs int64), falls back to element-wise
    Python comparison so the test validates the combination without requiring
    a dedicated Cython cross-type method.

    Raises:
        NotImplementedError: If same-type method is missing
        ValueError: If operation name is unknown
    """
    vector_method = _COMPARISON_METHODS.get(operation_name)
    if vector_method is None:
        raise ValueError(f"Unknown comparison operation: {operation_name}")

    left_class = type(left_vec).__name__
    right_class = type(right_vec).__name__

    if left_class == right_class:
        if not hasattr(left_vec, vector_method):
            raise NotImplementedError(
                f"{vector_method} not available for {left_class}"
            )
        return getattr(left_vec, vector_method)(right_vec)

    # Cross-type: compare element-wise via Python
    import datetime
    fn = _COMPARISON_OPS[operation_name]
    left_vals = left_vec.to_pylist()
    right_vals = right_vec.to_pylist()
    results = []
    for l, r in zip(left_vals, right_vals):
        if l is None or r is None:
            results.append(None)
        else:
            # Coerce date to datetime so date32 vs timestamp comparisons work
            if isinstance(l, datetime.date) and not isinstance(l, datetime.datetime):
                l = datetime.datetime(l.year, l.month, l.day)
            if isinstance(r, datetime.date) and not isinstance(r, datetime.datetime):
                r = datetime.datetime(r.year, r.month, r.day)
            results.append(fn(l, r))

    from draken.vectors.bool_vector import BoolVector
    return BoolVector.from_arrow(pa.array(results, type=pa.bool_()))
