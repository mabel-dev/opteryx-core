"""
Opteryx types module.

This module provides:
- Internal scalar type system (replacing numpy/pyarrow type checks)
- SqlType type vocabulary (Draken-native engine)
- Null handling primitives
- Type conversion utilities
- Type coercion helpers
- Bidirectional Python ↔ SqlType mapping
"""

from opteryx.types._null_handling import (
    count_nulls,
    has_nulls,
    is_inf,
    is_nan,
    is_not_null,
    is_null,
    is_null_vector,
    null_count_vector,
    nulls_to_default,
    remove_nulls,
)
from opteryx.types.sql_type import (
    SQL_TO_PYTHON_MAP,
    PYTHON_TO_SQL_MAP,
    SqlType,
    find_compatible_type,
)
from opteryx.types._scalar_types import (
    ScalarType,
    classify_scalar,
    extract_python_scalar,
    is_null_scalar,
    is_numeric_scalar,
    is_scalar,
    is_temporal_scalar,
    unwrap_scalar,
)

__all__ = [
    # Scalar type system (Python scalar classification)
    "ScalarType",
    "classify_scalar",
    "is_scalar",
    "is_numeric_scalar",
    "is_temporal_scalar",
    "is_null_scalar",
    "extract_python_scalar",
    "unwrap_scalar",
    # SqlType type vocabulary
    "SqlType",
    "PYTHON_TO_SQL_MAP",
    "SQL_TO_PYTHON_MAP",
    "find_compatible_type",
    # Null handling primitives (Step 3)
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
