"""
Convert rugo parquet metadata schemas to orso RelationSchema format.
"""

import re
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

ORSO_TYPE_ALIASES = {
    "float": "double",
    "float32": "double",
    "float64": "double",
    "int8": "integer",
    "int16": "integer",
    "int32": "integer",
    "int64": "integer",
    "bool": "boolean",
    "byte_array": "blob",
    "fixed_len_byte_array": "blob",
    "utf8": "varchar",
    "string": "varchar",
}

PARQUET_LOGICAL_TYPE_MAP = {
    "string": OrsoTypes.VARCHAR,
    "utf8": OrsoTypes.VARCHAR,
    "varchar": OrsoTypes.VARCHAR,
    "date": OrsoTypes.DATE,
    "date32[day]": OrsoTypes.DATE,
    "json": OrsoTypes.JSONB,
    "jsonb": OrsoTypes.JSONB,
    "struct": OrsoTypes.JSONB,
    "boolean": OrsoTypes.BOOLEAN,
    "binary": OrsoTypes.BLOB,
    "byte_array": OrsoTypes.BLOB,
    "fixed_len_byte_array": OrsoTypes.BLOB,
    "uint_8": OrsoTypes.INTEGER,
    "uint_16": OrsoTypes.INTEGER,
    "uint_32": OrsoTypes.INTEGER,
    "uint_64": OrsoTypes.INTEGER,
}

PARQUET_LOGICAL_COMPLEX_PREFIXES = {
    "array": OrsoTypes.ARRAY,
    "decimal": OrsoTypes.DECIMAL,
    "time": OrsoTypes.TIME,
    "timestamp": OrsoTypes.TIMESTAMP,
}

PARQUET_PHYSICAL_TYPE_MAP = {
    "int8": OrsoTypes.INTEGER,
    "int16": OrsoTypes.INTEGER,
    "int32": OrsoTypes.INTEGER,
    "int64": OrsoTypes.INTEGER,
    "float": OrsoTypes.DOUBLE,
    "float32": OrsoTypes.DOUBLE,
    "float64": OrsoTypes.DOUBLE,
    "double": OrsoTypes.DOUBLE,
    "byte_array": OrsoTypes.BLOB,
    "fixed_len_byte_array": OrsoTypes.BLOB,
    "boolean": OrsoTypes.BOOLEAN,
}

JSONL_TYPE_MAP = {
    "int64": OrsoTypes.INTEGER,
    "double": OrsoTypes.DOUBLE,
    "bytes": OrsoTypes.BLOB,
    "boolean": OrsoTypes.BOOLEAN,
    "null": OrsoTypes.BLOB,  # Default null to varchar
    "object": OrsoTypes.JSONB,
}

JSONL_ARRAY_INNER_TYPE_ALIASES = {
    "int64": "integer",
    "int32": "integer",
    "int16": "integer",
    "int8": "integer",
    "integer": "integer",
    "double": "double",
    "float": "double",
    "bytes": "blob",
    "string": "blob",
    "varchar": "blob",
    "boolean": "boolean",
    "object": "jsonb",
}


def _normalize_orso_type_aliases(type_name: str) -> str:
    normalized = type_name.lower()
    for source, target in ORSO_TYPE_ALIASES.items():
        normalized = re.sub(rf"(?<![a-z0-9_]){re.escape(source)}(?![a-z0-9_])", target, normalized)
    return normalized


def _map_parquet_type_to_orso(
    parquet_type: Optional[str], logical_type: Optional[str] = None
) -> str:
    """
    Map parquet physical and logical types to orso types.

    Args:
        parquet_type: Physical parquet type (e.g., "int64", "byte_array", "float64")
        logical_type: Logical parquet type if available (e.g., "STRING", "TIMESTAMP_MILLIS")

    Returns:
        Orso type string
    """
    # If we have a logical type, use it for more precise mapping
    if logical_type:
        logical_lower = logical_type.lower()

        if logical_lower in PARQUET_LOGICAL_TYPE_MAP:
            return PARQUET_LOGICAL_TYPE_MAP[logical_lower]

        if logical_lower.startswith("time") and not logical_lower.startswith("timestamp"):
            return PARQUET_LOGICAL_COMPLEX_PREFIXES["time"]
        if logical_lower.startswith("timestamp") or "timestamp" in logical_lower:
            return PARQUET_LOGICAL_COMPLEX_PREFIXES["timestamp"]

        if logical_lower.startswith(("array", "decimal")):
            normalized_logical = _normalize_orso_type_aliases(logical_lower)
            _type, _length, _precision, _scale, _element_type = OrsoTypes.from_name(
                normalized_logical
            )
            _type._length = _length
            _type._precision = _precision
            _type._scale = _scale
            _type._element_type = _element_type
            # print(f"DEBUG: mapped {logical_lower} to {_type} with element_type {_element_type}")
            return _type

    # Fall back to physical type mapping
    physical_lower = parquet_type.lower() if parquet_type else ""

    if physical_lower in PARQUET_PHYSICAL_TYPE_MAP:
        return PARQUET_PHYSICAL_TYPE_MAP[physical_lower]

    # Default to VARCHAR for unknown types
    return OrsoTypes.VARCHAR


def _columns_from_metadata(metadata: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    schema_columns = metadata.get("schema_columns")
    if schema_columns:
        return schema_columns

    return _fallback_schema_columns(metadata)


def _fallback_schema_columns(metadata: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    row_groups = metadata.get("row_groups") or []
    if not row_groups:
        return []

    first_row_group = row_groups[0]
    columns_meta = first_row_group.get("columns") or []

    columns: Dict[str, Dict[str, Any]] = {}
    for col_metadata in columns_meta:
        col_name = col_metadata.get("name")
        if not col_name:
            continue

        logical_type = col_metadata.get("logical_type")
        physical_type = col_metadata.get("physical_type") or ""

        top_name = col_name.split(".", 1)[0]
        if top_name != col_name:
            if top_name in columns:
                continue
            columns[top_name] = {
                "name": top_name,
                "physical_type": "struct",
                "logical_type": "json",
                "nullable": True,
            }
            continue

        columns[col_name] = {
            "name": col_name,
            "physical_type": physical_type,
            "logical_type": logical_type,
            "nullable": bool(col_metadata.get("null_count", 0)),
        }

    return columns.values()


def rugo_to_orso_schema(
    rugo_metadata: Dict[str, Any], schema_name: str = "parquet_schema"
) -> RelationSchema:
    """
    Convert rugo parquet metadata to an orso RelationSchema.

    Args:
        rugo_metadata: The metadata dictionary returned by rugo.parquet.read_metadata()
        schema_name: Name for the resulting schema (default: "parquet_schema")

    Returns:
        OrsoRelationSchema object

    Raises:
        ValueError: If the metadata format is invalid
    """
    if not isinstance(rugo_metadata, dict):
        raise ValueError("rugo_metadata must be a dictionary")

    if not rugo_metadata.get("schema_columns") and not rugo_metadata.get("row_groups"):
        raise ValueError("rugo_metadata must contain 'schema_columns' or 'row_groups'")

    columns = []
    for entry in _columns_from_metadata(rugo_metadata):
        name = entry.get("name")
        if not name:
            continue

        physical_type = entry.get("physical_type")
        logical_type = entry.get("logical_type")
        nullable = bool(entry.get("nullable", True))

        orso_type = _map_parquet_type_to_orso(physical_type, logical_type)
        # print(f"DEBUG: creating FlatColumn for {name}, type={orso_type}, element_type={getattr(orso_type, '_element_type', 'MISSING')}")
        columns.append(
            FlatColumn(
                name=name,
                type=orso_type,
                nullable=nullable,
                precision=orso_type._precision,
                scale=orso_type._scale,
                length=orso_type._length,
                element_type=orso_type._element_type,
            )
        )

    if not columns:
        raise ValueError("No columns could be derived from rugo metadata")

    # Create and populate the RelationSchema
    schema = RelationSchema(name=schema_name)

    # Add all columns to the schema
    schema.columns.extend(columns)

    # Add row count estimate if available
    if "num_rows" in rugo_metadata:
        schema.row_count_estimate = rugo_metadata["num_rows"]

    return schema


def extract_schema_only(
    rugo_metadata: Dict[str, Any], schema_name: str = "parquet_schema"
) -> Dict[str, str]:
    """
    Extract just the column name to type mapping from rugo metadata.

    Args:
        rugo_metadata: The metadata dictionary returned by rugo.parquet.read_metadata()
        schema_name: Name for the schema (included in result for completeness)

    Returns:
        Dictionary with schema name and column type mappings
    """
    column_types = {}
    for entry in _columns_from_metadata(rugo_metadata):
        name = entry.get("name")
        if not name:
            continue
        physical = entry.get("physical_type")
        logical = entry.get("logical_type")
        column_types[name] = _map_parquet_type_to_orso(physical, logical)

    return {
        "schema_name": schema_name,
        "columns": column_types,
        "row_count": rugo_metadata.get("num_rows"),
    }


def _map_jsonl_type_to_orso(jsonl_type: str) -> str:
    """
    Map JSON lines type to orso type.

    Args:
        jsonl_type: JSON lines type (e.g., "int64", "double", "string", "boolean")

    Returns:
        Orso type string
    """
    jt = jsonl_type.lower()
    # Direct simple types
    if jt in JSONL_TYPE_MAP:
        return JSONL_TYPE_MAP[jt]

    # array or array<elem> -> use OrsoTypes.from_name to parse element type
    if jt.startswith("array"):
        # Normalize inner element type names so OrsoTypes.from_name accepts them
        # supports forms like 'array<int64>' produced by get_jsonl_schema
        if jt.startswith("array<") and jt.endswith(">"):
            inner = jt[jt.find("<") + 1 : -1].strip()
            normalized_inner = JSONL_ARRAY_INNER_TYPE_ALIASES.get(inner.lower(), inner.lower())
            normalized = f"array<{normalized_inner}>"
        else:
            normalized = jt

        try:
            _type, _length, _precision, _scale, _element_type = OrsoTypes.from_name(normalized)
            return _type
        except ValueError:
            return OrsoTypes.BLOB

    return OrsoTypes.BLOB


def jsonl_to_orso_schema(
    jsonl_schema: List[Dict[str, Any]], schema_name: str = "jsonl_schema"
) -> RelationSchema:
    """
    Convert JSON lines schema to an orso RelationSchema.

    Args:
        jsonl_schema: The schema list returned by opteryx.compiled.rugo.jsonl.get_jsonl_schema()
        schema_name: Name for the resulting schema (default: "jsonl_schema")

    Returns:
        OrsoRelationSchema object

    Raises:
        ValueError: If the schema format is invalid
    """
    if not isinstance(jsonl_schema, list):
        raise ValueError("jsonl_schema must be a list")

    if not jsonl_schema:
        raise ValueError("jsonl_schema cannot be empty")

    columns = []
    for entry in jsonl_schema:
        name = entry.get("name")
        if not name:
            continue

        jsonl_type = entry.get("type", "string")
        nullable = bool(entry.get("nullable", True))

        orso_type = _map_jsonl_type_to_orso(jsonl_type)
        columns.append(FlatColumn(name=name, type=orso_type, nullable=nullable))

    if not columns:
        raise ValueError("No columns could be derived from jsonl schema")

    # Create and populate the RelationSchema
    schema = RelationSchema(name=schema_name)

    # Add all columns to the schema
    schema.columns.extend(columns)

    return schema
