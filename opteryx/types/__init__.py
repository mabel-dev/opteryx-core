"""
Opteryx types module.

This module provides:
- Internal scalar type system (replacing numpy/pyarrow type checks)
- LogicalCategory type vocabulary (Draken-native engine)
- Null handling primitives
- Type conversion utilities
- Type coercion helpers
- Bidirectional Python ↔ type mapping
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
from opteryx.types.logical_type import (
    PYTHON_TO_SQL_MAP,
    SQL_TO_PYTHON_MAP,
    LogicalCategory,
    find_compatible_type,
)
from opteryx.types.vector_types import (
    get_vector_source_identifier,
    is_numeric_vector_type,
    node_is_constant_embed_call,
    node_is_literal_numeric_vector,
    node_is_numeric_vector,
    node_is_vector_query_expression,
    resolve_node_type,
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
    # type vocabulary
    "LogicalCategory",
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
    # Vector type helpers
    "get_vector_source_identifier",
    "is_numeric_vector_type",
    "node_is_constant_embed_call",
    "node_is_literal_numeric_vector",
    "node_is_numeric_vector",
    "node_is_vector_query_expression",
    "resolve_node_type",
]
