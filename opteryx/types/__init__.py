"""
Opteryx types module.

This module provides:
- Internal scalar type system (replacing numpy/pyarrow type checks)
- Inlined OrsoTypes type system (replacing orso.types dependency)
- Null handling primitives
- Type conversion utilities
- Type coercion helpers
- Bidirectional type mapping (Python ↔ Orso)
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
from opteryx.types._orso_types import (
    ORSO_TO_PYTHON_MAP,
    PYTHON_TO_ORSO_MAP,
    OrsoTypes,
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
    # OrsoTypes type system (inlined from orso)
    "OrsoTypes",
    "PYTHON_TO_ORSO_MAP",
    "ORSO_TO_PYTHON_MAP",
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
