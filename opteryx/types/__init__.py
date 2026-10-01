"""
Opteryx types module.

This module provides:
- LogicalCategory type vocabulary (Draken-native engine)
- Null handling primitives
- Type conversion utilities
- Type coercion helpers
- Bidirectional Python ↔ type mapping
"""

from opteryx.types.logical_type import (
    PYTHON_TO_SQL_MAP,
    SQL_TO_PYTHON_MAP,
    ColumnType,
    LogicalCategory,
    column_type_from_vector,
    find_compatible_type,
    is_legal_widen,
    morsel_column_types,
    parse_column_type,
    serialize_column_type,
)
from opteryx.types.scalars._null_handling import (
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

__all__ = [
    # type vocabulary
    "ColumnType",
    "LogicalCategory",
    "PYTHON_TO_SQL_MAP",
    "SQL_TO_PYTHON_MAP",
    "column_type_from_vector",
    "find_compatible_type",
    "is_legal_widen",
    "morsel_column_types",
    "parse_column_type",
    "serialize_column_type",
    # Null handling primitives
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
