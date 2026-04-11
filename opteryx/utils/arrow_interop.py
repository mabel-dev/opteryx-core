"""
Converters module - utilities for converting between PyArrow and Opteryx formats.

This module provides conversion functions that replace functionality previously
provided by the orso package (now eradicated).
"""

from typing import Any, Generator, List, Optional, Tuple

import pyarrow
from pyarrow import types as arrow_types

from opteryx.types import OrsoTypes
from opteryx.types.schema import FlatColumn, RelationSchema

__all__ = ["from_arrow"]


def _arrow_type_to_orso_type(arrow_type: pyarrow.DataType) -> OrsoTypes:
    """Convert PyArrow type to OrsoTypes enum value.

    Args:
        arrow_type: PyArrow data type

    Returns:
        Corresponding OrsoTypes value
    """
    if arrow_types.is_null(arrow_type):
        return OrsoTypes.NULL_TYPE
    elif arrow_types.is_boolean(arrow_type):
        return OrsoTypes.BOOLEAN
    elif arrow_types.is_int8(arrow_type):
        return OrsoTypes.BYTE
    elif arrow_types.is_int16(arrow_type):
        return OrsoTypes.SHORT
    elif arrow_types.is_int32(arrow_type):
        return OrsoTypes.INTEGER
    elif arrow_types.is_int64(arrow_type):
        return OrsoTypes.LONG
    elif arrow_types.is_float32(arrow_type):
        return OrsoTypes.FLOAT
    elif arrow_types.is_float64(arrow_type):
        return OrsoTypes.DOUBLE
    elif arrow_types.is_decimal(arrow_type):
        return OrsoTypes.DECIMAL
    elif arrow_types.is_large_string(arrow_type) or arrow_types.is_unicode(arrow_type):
        return OrsoTypes.VARCHAR
    elif arrow_types.is_string(arrow_type) or arrow_types.is_large_unicode(arrow_type):
        return OrsoTypes.VARCHAR
    elif arrow_types.is_binary(arrow_type) or arrow_types.is_large_binary(arrow_type):
        return OrsoTypes.BLOB
    elif arrow_types.is_date(arrow_type):
        return OrsoTypes.DATE
    elif arrow_types.is_time(arrow_type):
        return OrsoTypes.TIME
    elif arrow_types.is_timestamp(arrow_type):
        return OrsoTypes.TIMESTAMP
    elif arrow_types.is_duration(arrow_type):
        return OrsoTypes.INTERVAL
    elif arrow_types.is_dictionary(arrow_type):
        # Dictionary types resolve to their value type
        return _arrow_type_to_orso_type(arrow_type.value_type)
    elif arrow_types.is_list(arrow_type) or arrow_types.is_large_list(arrow_type):
        return OrsoTypes.ARRAY
    elif arrow_types.is_struct(arrow_type):
        return OrsoTypes.STRUCT
    else:
        # Default fallback for unmapped types
        return OrsoTypes.VARCHAR


def from_arrow(
    arrow_tables: Generator[pyarrow.Table, None, None],
) -> Tuple[List[Tuple[Any, ...]], Optional[RelationSchema]]:
    """Convert PyArrow tables to rows and schema.

    This function processes a generator of PyArrow tables and converts them into:
    1. A list of row tuples suitable for iteration by the cursor
    2. A RelationSchema representing the table structure

    Args:
        arrow_tables: Generator/iterable yielding pyarrow.Table objects

    Returns:
        Tuple of (rows, schema) where:
        - rows: List of tuples, each representing a row
        - schema: RelationSchema representing the table schema, or None if no tables
    """
    rows: List[Tuple[Any, ...]] = []
    schema: Optional[RelationSchema] = None

    for table in arrow_tables:
        if schema is None:
            # Extract schema from first table
            columns = []
            for field in table.schema:
                orso_type = _arrow_type_to_orso_type(field.type)
                columns.append(FlatColumn(name=field.name, type=orso_type))
            schema = RelationSchema(name="table", columns=columns)

        # Convert table rows to tuples
        # Use to_pylist() for efficient conversion to Python objects
        for row_dict in table.to_pylist():
            if isinstance(row_dict, dict):
                # Extract values in column order
                row_values = tuple(row_dict.values())
            else:
                # Fallback for non-dict rows (shouldn't happen with PyArrow)
                row_values = tuple(row_dict) if hasattr(row_dict, "__iter__") else (row_dict,)
            rows.append(row_values)

    return rows, schema
