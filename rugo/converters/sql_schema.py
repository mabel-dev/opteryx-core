"""
Convert rugo parquet metadata schemas to RelationSchema format.
"""

import re
from typing import Any, Dict, Iterable, List, Optional

from opteryx.types.logical_type import LogicalCategory
from opteryx.types.schema import SchemaColumn, RelationSchema

SQL_TYPE_ALIASES = {
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
    "string": LogicalCategory.VARCHAR,
    "utf8": LogicalCategory.VARCHAR,
    "varchar": LogicalCategory.VARCHAR,
    "date": LogicalCategory.DATE,
    "date32[day]": LogicalCategory.DATE,
    "json": LogicalCategory.NVARCHAR,
    "jsonb": LogicalCategory.NVARCHAR,
    "struct": LogicalCategory.NVARCHAR,
    "boolean": LogicalCategory.BOOLEAN,
    "binary": LogicalCategory.VARBINARY,
    "byte_array": LogicalCategory.VARBINARY,
    "fixed_len_byte_array": LogicalCategory.VARBINARY,
    "uint_8": LogicalCategory.INTEGER,
    "uint_16": LogicalCategory.INTEGER,
    "uint_32": LogicalCategory.INTEGER,
    "uint_64": LogicalCategory.INTEGER,
}

PARQUET_LOGICAL_COMPLEX_PREFIXES = {
    "array": LogicalCategory.ARRAY,
    "decimal": LogicalCategory.DECIMAL,
    "time": LogicalCategory.TIME,
    "timestamp": LogicalCategory.TIMESTAMP,
}

PARQUET_PHYSICAL_TYPE_MAP = {
    "int8": LogicalCategory.INTEGER,
    "int16": LogicalCategory.INTEGER,
    "int32": LogicalCategory.INTEGER,
    "int64": LogicalCategory.INTEGER,
    "float": LogicalCategory.FLOAT,
    "float32": LogicalCategory.FLOAT,
    "float64": LogicalCategory.FLOAT,
    "double": LogicalCategory.FLOAT,
    "byte_array": LogicalCategory.VARBINARY,
    "fixed_len_byte_array": LogicalCategory.VARBINARY,
    "boolean": LogicalCategory.BOOLEAN,
}

JSONL_TYPE_MAP = {
    "int64": LogicalCategory.INTEGER,
    "double": LogicalCategory.FLOAT,
    "bytes": LogicalCategory.VARBINARY,
    "boolean": LogicalCategory.BOOLEAN,
    "null": LogicalCategory.VARBINARY,  # Default null to varbinary
    "object": LogicalCategory.NVARCHAR,
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


def _normalize_sql_type_aliases(type_name: str) -> str:
    normalized = type_name.lower()
    for source, target in SQL_TYPE_ALIASES.items():
        normalized = re.sub(rf"(?<![a-z0-9_]){re.escape(source)}(?![a-z0-9_])", target, normalized)
    return normalized


def _map_parquet_type_to_sql(
    parquet_type: Optional[str], logical_type: Optional[str] = None
) -> str:
    """
    Map parquet physical and logical types to SQL types.

    Args:
        parquet_type: Physical parquet type (e.g., "int64", "byte_array", "float64")
        logical_type: Logical parquet type if available (e.g., "STRING", "TIMESTAMP_MILLIS")

    Returns:
        SQL type string
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
            from opteryx.types.logical_type import parse_column_type
            normalized_logical = _normalize_sql_type_aliases(logical_lower)
            return parse_column_type(normalized_logical).category

    # Fall back to physical type mapping
    physical_lower = parquet_type.lower() if parquet_type else ""

    if physical_lower in PARQUET_PHYSICAL_TYPE_MAP:
        return PARQUET_PHYSICAL_TYPE_MAP[physical_lower]

    # Default to VARCHAR for unknown types
    return LogicalCategory.VARCHAR


def _columns_from_metadata(metadata: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    schema_columns = metadata.get("schema_columns")
    if schema_columns:
        return schema_columns

    return _fallback_schema_columns(metadata)


def _parse_decimal_params(logical_type: Optional[str]) -> tuple:
    """
    Extract precision and scale from a decimal logical type string.

    Handles formats: 'decimal(15,2)', 'DECIMAL(38,10)', 'decimal'.

    Returns:
        (precision, scale) as ints, or (None, None) if not parseable.
    """
    if not logical_type:
        return None, None
    lt = logical_type.lower()
    if not lt.startswith("decimal"):
        return None, None
    lb = lt.find("(")
    rb = lt.find(")", lb + 1) if lb >= 0 else -1
    if lb < 0 or rb <= lb:
        return None, None
    parts = lt[lb + 1 : rb].split(",")
    try:
        precision = int(parts[0].strip())
    except (ValueError, IndexError):
        precision = None
    try:
        scale = int(parts[1].strip())
    except (ValueError, IndexError):
        scale = None
    return precision, scale


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

        col_entry: Dict[str, Any] = {
            "name": col_name,
            "physical_type": physical_type,
            "logical_type": logical_type,
            "nullable": bool(col_metadata.get("null_count", 0)),
        }
        # Propagate precision/scale from decimal logical type (e.g. "decimal(15,2)")
        _prec, _scale = _parse_decimal_params(logical_type)
        if _prec is not None:
            col_entry["precision"] = _prec
        if _scale is not None:
            col_entry["scale"] = _scale
        columns[col_name] = col_entry

    return columns.values()


def rugo_to_relation_schema(
    rugo_metadata, schema_name: str = "parquet_schema"
) -> RelationSchema:
    """
    Convert a typed rugo ParquetMetadata to an RelationSchema.

    Args:
        rugo_metadata: The ParquetMetadata returned by rugo.parquet_reader.read_metadata()
                       (typed object with .num_rows and .schema_columns of parquet SchemaColumn fields).
        schema_name: Name for the resulting schema (default: "parquet_schema")

    Returns:
        SqlRelationSchema object

    Raises:
        ValueError: If no columns can be derived from the metadata
    """
    schema_columns = rugo_metadata.schema_columns
    if not schema_columns:
        raise ValueError("rugo_metadata must contain schema_columns")

    columns = []
    for entry in schema_columns:
        name = entry.name
        if not name:
            continue
        if name.startswith("arrow_schema."):
            name = name[len("arrow_schema."):]

        physical_type = entry.physical_type
        logical_type = entry.logical_type
        nullable = bool(entry.nullable)

        sql_type = _map_parquet_type_to_sql(physical_type, logical_type)

        # precision/scale parsed from the decimal logical type string
        # (e.g. "decimal(15,2)"); schema metadata carries no explicit fields.
        precision, scale = _parse_decimal_params(logical_type)

        from opteryx.types import logical_type as _lt
        from opteryx.types.logical_type import _CATEGORY_TO_CANONICAL
        if sql_type == LogicalCategory.DECIMAL and precision is not None and scale is not None:
            _ct = _lt.DECIMAL(precision, scale)
        else:
            _ct = _CATEGORY_TO_CANONICAL.get(sql_type)
        columns.append(SchemaColumn(name=name, column_type=_ct, nullable=nullable))

    if not columns:
        raise ValueError("No columns could be derived from rugo metadata")

    schema = RelationSchema(name=schema_name)
    schema.columns.extend(columns)
    schema.row_count_estimate = rugo_metadata.num_rows

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
        column_types[name] = _map_parquet_type_to_sql(physical, logical)

    return {
        "schema_name": schema_name,
        "columns": column_types,
        "row_count": rugo_metadata.get("num_rows"),
    }


def _map_jsonl_type_to_sql(jsonl_type: str) -> str:
    """
    Map JSON lines type to sql type.

    Args:
        jsonl_type: JSON lines type (e.g., "int64", "double", "string", "boolean")

    Returns:
        SQL type string
    """
    jt = jsonl_type.lower()
    # Direct simple types
    if jt in JSONL_TYPE_MAP:
        return JSONL_TYPE_MAP[jt]

    # array or array<elem> -> use LogicalCategory.from_name to parse element type
    if jt.startswith("array"):
        # Normalize inner element type names so LogicalCategory.from_name accepts them
        # supports forms like 'array<int64>' produced by get_jsonl_schema
        if jt.startswith("array<") and jt.endswith(">"):
            inner = jt[jt.find("<") + 1 : -1].strip()
            normalized_inner = JSONL_ARRAY_INNER_TYPE_ALIASES.get(inner.lower(), inner.lower())
            normalized = f"array<{normalized_inner}>"
        else:
            normalized = jt

        from opteryx.types.logical_type import parse_column_type
        try:
            return parse_column_type(normalized).category
        except ValueError:
            return LogicalCategory.VARBINARY

    return LogicalCategory.VARBINARY


def jsonl_to_sql_schema(
    jsonl_schema: List[Dict[str, Any]], schema_name: str = "jsonl_schema"
) -> RelationSchema:
    """
    Convert JSON lines schema to an RelationSchema.

    Args:
        jsonl_schema: The schema list returned by rugo.jsonl.get_jsonl_schema()
        schema_name: Name for the resulting schema (default: "jsonl_schema")

    Returns:
        SqlRelationSchema object

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

        sql_type = _map_jsonl_type_to_sql(jsonl_type)
        from opteryx.types.logical_type import _CATEGORY_TO_CANONICAL
        columns.append(SchemaColumn(name=name, column_type=_CATEGORY_TO_CANONICAL.get(sql_type), nullable=nullable))

    if not columns:
        raise ValueError("No columns could be derived from jsonl schema")

    # Create and populate the RelationSchema
    schema = RelationSchema(name=schema_name)

    # Add all columns to the schema
    schema.columns.extend(columns)

    return schema
