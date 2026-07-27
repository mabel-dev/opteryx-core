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


# Parquet integer width/signedness → the exact ColumnType to declare.
#
# LogicalCategory stays a single INTEGER member (CLAUDE.md §14: the category is a
# derived projection, not a place to encode width) — the width lives where it
# belongs, in ColumnType.physical.
#
# Declaring the TRUE width matters because the scan produces vectors at that
# width (E33 exact-width integers). If the schema said INT64 while the scan
# handed back INT32, every consumer keyed on the declared type would be working
# from a lie — most visibly `_coerce_literal_physical`, which re-materializes a
# comparison literal at the column's physical type so draken_compare_dv's
# identical-type guard can fire. Declaring INT64 there left narrow columns
# unable to use the c-native compare at all.
#
# Keys are the rugo footer's logical-type strings (metadata.cpp emits lowercase
# "int8".."uint64" from the modern IntType annotation) plus the underscored
# legacy ConvertedType spellings.
_INTEGER_LOGICAL_WIDTHS = {
    "int8": "INT8",
    "int16": "INT16",
    "int32": "INT32",
    "int64": "INT64",
    "uint8": "UINT8",
    "uint16": "UINT16",
    "uint32": "UINT32",
    "uint64": "UINT64",
    "uint_8": "UINT8",
    "uint_16": "UINT16",
    "uint_32": "UINT32",
    "uint_64": "UINT64",
    "int_8": "INT8",
    "int_16": "INT16",
    "int_32": "INT32",
    "int_64": "INT64",
}

# Fallback when a column carries no integer annotation: the physical type states
# the width exactly. A bare parquet int32 IS a 32-bit signed column — that is what
# PyArrow and parquet-mr emit for int32 — so it must not be widened here.
_INTEGER_PHYSICAL_WIDTHS = {
    "int8": "INT8",
    "int16": "INT16",
    "int32": "INT32",
    "int64": "INT64",
}


def _integer_column_type(physical_type: Optional[str], logical_type: Optional[str]):
    """The exact ColumnType for an integer column, honouring declared width and
    signedness. Falls back to INT64 only when neither the annotation nor the
    physical type identifies a width."""
    from opteryx.types import logical_type as _lt

    if logical_type:
        name = _INTEGER_LOGICAL_WIDTHS.get(logical_type.lower())
        if name is not None:
            return getattr(_lt, name)
    if physical_type:
        name = _INTEGER_PHYSICAL_WIDTHS.get(physical_type.lower())
        if name is not None:
            return getattr(_lt, name)
    return _lt.INT64


def _normalize_sql_type_aliases(type_name: str) -> str:
    normalized = type_name.lower()
    for source, target in SQL_TYPE_ALIASES.items():
        normalized = re.sub(rf"(?<![a-z0-9_]){re.escape(source)}(?![a-z0-9_])", target, normalized)
    return normalized


# Parquet logical-type strings carry a unit/width the SQL type grammar does not:
# "timestamp[us]", "time32[ms]", "date32[day]". parse_column_type knows only the
# bare names.
_PARQUET_UNIT_SUFFIX = re.compile(r"\[[^\]]*\]")
_PARQUET_WIDTH_ALIASES = {
    "time32": "time",
    "time64": "time",
    "date32": "date",
    "date64": "date",
}


def _parse_timestamp_unit(logical_type: Optional[str]):
    """Extract the TimestampUnit from a parquet timestamp logical string.

    Handles "timestamp[us]" and "array<timestamp[ms]>" alike — the unit is read
    wherever it sits. Returns None when absent/unparseable, which callers treat as
    the microsecond default (parquet's own default and the canonical ColumnType's).

    This exists because the unit is REAL data: a `timestamp[ms]` column whose unit
    is dropped decodes 1704164645000 as microseconds → 1970-01-20 instead of
    2024-01-02. Silently wrong dates, not a rounding detail.
    """
    from opteryx.types.logical_type import TimestampUnit

    if not logical_type:
        return None
    match = re.search(r"timestamp\[([a-z]+)\]", logical_type.lower())
    if not match:
        return None
    return {
        "s": TimestampUnit.SECONDS,
        "ms": TimestampUnit.MILLISECONDS,
        "us": TimestampUnit.MICROSECONDS,
        "ns": TimestampUnit.NANOSECONDS,
    }.get(match.group(1))


def _normalize_parquet_type_string(type_name: str) -> str:
    """Parquet logical-type string → a string parse_column_type accepts.

    Strips the unit/width suffix ("timestamp[us]" → "timestamp") and collapses
    width-tagged temporal names ("time32" → "time"). Dropping the unit is not a
    new loss: a SCALAR timestamp column already resolves through
    _CATEGORY_TO_CANONICAL to the canonical TIMESTAMP (unit=us) regardless of the
    file's unit, so this keeps array elements consistent with scalars rather than
    inventing a second convention. (If per-column units are ever honoured, both
    paths need it — not just this one.)
    """
    normalized = _PARQUET_UNIT_SUFFIX.sub("", type_name.lower())
    for source, target in _PARQUET_WIDTH_ALIASES.items():
        normalized = re.sub(
            rf"(?<![a-z0-9_]){re.escape(source)}(?![a-z0-9_])", target, normalized
        )
    return _normalize_sql_type_aliases(normalized)


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

        # Container types are matched BEFORE the scalar temporal checks below.
        # Those checks match on a SUBSTRING ("timestamp" in ...), so an
        # `array<timestamp[us]>` used to resolve to a bare TIMESTAMP — the ARRAY
        # container silently dropped, leaving the declared type (TIMESTAMP) at odds
        # with the runtime vector (genuinely ARRAY). That made every ARRAY-typed
        # function reject the column at bind time ("SORT arg1: expected ARRAY, got
        # TIMESTAMP"). An element type is the ELEMENT's business — parse the whole
        # string and let the container win.
        if logical_lower.startswith(("array", "decimal")):
            from opteryx.types.logical_type import parse_column_type
            return parse_column_type(_normalize_parquet_type_string(logical_lower)).category

        if logical_lower.startswith("time") and not logical_lower.startswith("timestamp"):
            return PARQUET_LOGICAL_COMPLEX_PREFIXES["time"]
        if logical_lower.startswith("timestamp") or "timestamp" in logical_lower:
            return PARQUET_LOGICAL_COMPLEX_PREFIXES["timestamp"]

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
        _ts_unit = _parse_timestamp_unit(logical_type)
        if sql_type == LogicalCategory.DECIMAL and precision is not None and scale is not None:
            _ct = _lt.DECIMAL(precision, scale)
        elif sql_type == LogicalCategory.TIMESTAMP and _ts_unit is not None:
            # Honour the FILE's unit. _CATEGORY_TO_CANONICAL would hand back the
            # canonical TIMESTAMP (unit=us) for every timestamp column, so a
            # `timestamp[ms]`/`[s]`/`[ns]` column decoded its raw int64 as
            # microseconds and rendered a wildly wrong date (2024 → 1970), or
            # overflowed outright for ns. The unit is data, not a formatting hint.
            _ct = _lt.TIMESTAMP(_ts_unit)
        elif sql_type == LogicalCategory.ARRAY:
            # ARRAY carries its element type in the logical string (e.g.
            # "array<int64>"); _CATEGORY_TO_CANONICAL has no ARRAY entry because
            # an ARRAY ColumnType is invalid without an `element`. Parse the full
            # type so `list[i]` (MapAccess) can resolve the element ColumnType.
            # Same normalizer as the category mapping above — they must agree, or a
            # column typed ARRAY there would raise here.
            from opteryx.types.logical_type import parse_column_type
            _ct = (
                parse_column_type(_normalize_parquet_type_string(logical_type))
                if logical_type
                else None
            )
            # The normalizer strips the unit to make the string parseable, so an
            # array<timestamp[ms]> element comes back as the canonical TIMESTAMP
            # (us). Re-attach the file's real unit — same reason as the scalar
            # branch above; the parquet reader retags the ARRAY's child with it
            # (parquet_read.pyx's _sp_array_ts_unit_map).
            if (
                _ct is not None
                and _ts_unit is not None
                and _ct.element is not None
                and _ct.element.category == LogicalCategory.TIMESTAMP
            ):
                _ct = _lt.ARRAY(_lt.TIMESTAMP(_ts_unit))
        elif sql_type == LogicalCategory.INTEGER:
            # Declare the column's REAL width — _CATEGORY_TO_CANONICAL would hand
            # back INT64 for every integer, contradicting the exact-width vector
            # the scan produces. See _integer_column_type.
            _ct = _integer_column_type(physical_type, logical_type)
        else:
            _ct = _CATEGORY_TO_CANONICAL.get(sql_type)
        from opteryx.types.schema import mint_column_identity
        columns.append(SchemaColumn(name=name, column_type=_ct, nullable=nullable, identity=mint_column_identity(schema_name, name)))

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
        from opteryx.types.schema import mint_column_identity
        columns.append(SchemaColumn(name=name, column_type=_CATEGORY_TO_CANONICAL.get(sql_type), nullable=nullable, identity=mint_column_identity(schema_name, name)))

    if not columns:
        raise ValueError("No columns could be derived from jsonl schema")

    # Create and populate the RelationSchema
    schema = RelationSchema(name=schema_name)

    # Add all columns to the schema
    schema.columns.extend(columns)

    return schema
