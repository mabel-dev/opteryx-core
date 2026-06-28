"""Helpers for exporting a generated type catalog from runtime metadata."""

from __future__ import annotations

import json
from collections import OrderedDict
from collections import defaultdict
from pathlib import Path
from typing import Any

from opteryx.connectors._rugo_schema import JSONL_ARRAY_INNER_TYPE_ALIASES
from opteryx.connectors._rugo_schema import JSONL_TYPE_MAP
from opteryx.connectors._rugo_schema import PARQUET_LOGICAL_COMPLEX_PREFIXES
from opteryx.connectors._rugo_schema import PARQUET_LOGICAL_TYPE_MAP
from opteryx.connectors._rugo_schema import PARQUET_PHYSICAL_TYPE_MAP
from opteryx.types.logical_type import LogicalCategory
from opteryx.types.logical_type import _NUMERIC_TYPES
from opteryx.types.logical_type import _TEMPORAL_TYPES

# SQL alias groups: alternate spellings accepted in SQL DDL/casts.
# Source of truth: _SQL_NAME_ALIASES in opteryx/types/logical_type.py
_SQL_ALIASES: dict[str, list[str]] = {
    "integer": ["bigint", "int"],
    "float": ["double"],
    "varchar": ["string", "text"],
    "varbinary": ["blob", "bytes"],
    "nvarchar": [],
    "boolean": ["bool"],
}

# Static metadata per canonical type name (lower-case).
# Keys:
#   description     — one-paragraph summary shown at the top of the page
#   example         — literal that can appear in SQL (shown in a code block)
#   min/max         — for numeric types with hard bounds
#   string_formats  — list of accepted string formats (for temporal types)
#   cast_to         — list of {type, example, note?} dicts: how to CAST TO this type
#   comparable_with — list of type names that can be compared with this type
#   arithmetic      — list of {expr, result, desc} showing arithmetic results
#   limitations     — list of plain-text strings documenting known gaps
#   notes           — additional detail (shown as a Notes section)
_TYPE_METADATA: dict[str, dict[str, Any]] = {
    "null": {
        "description": "The absence of a value. NULL is not a type you declare — it appears when a column has no value or an expression produces no result.",
        "notes": "NULL is never equal to anything, including itself. Use `IS NULL` or `IS NOT NULL` to test for nulls. NULL propagates through arithmetic and most functions: `1 + NULL` is NULL.",
        "comparable_with": [],
        "limitations": [
            "You cannot CAST to NULL.",
            "NULL = NULL is always false; use IS NULL instead.",
        ],
    },
    "boolean": {
        "description": "A logical TRUE or FALSE value.",
        "example": "TRUE",
        "cast_to": [
            {"type": "from INTEGER", "example": "1::BOOLEAN", "note": "0 → FALSE, any non-zero → TRUE"},
            {"type": "from FLOAT",   "example": "1.0::BOOLEAN", "note": "0.0 → FALSE, any non-zero → TRUE"},
            {"type": "from VARCHAR", "example": "'true'::BOOLEAN", "note": "Exact matches only (case-insensitive): `true`/`false`, `1`/`0`, `yes`/`no`, `on`/`off`. Any other value raises an error — including empty string, `'null'`, and `'none'`."},
        ],
        "comparable_with": ["BOOLEAN"],
        "limitations": [
            "BOOLEAN cannot be compared to numeric types directly. Cast first: `col::INTEGER = 1`.",
            "Casting an unrecognised string to BOOLEAN raises an error, not NULL. There is no silent fallback.",
        ],
    },
    "integer": {
        "description": "Signed 64-bit integer. Write `INTEGER`, `INT`, or `BIGINT` in SQL — they are all equivalent.",
        "example": "42",
        "min": -9223372036854775808,
        "max": 9223372036854775807,
        "cast_to": [
            {"type": "from FLOAT",     "example": "3.9::INTEGER",     "note": "Truncates toward zero — 3.9 becomes 3, -3.9 becomes -3"},
            {"type": "from BOOLEAN",   "example": "TRUE::INTEGER",    "note": "TRUE → 1, FALSE → 0"},
            {"type": "from VARCHAR",   "example": "'42'::INTEGER",    "note": "String must contain only digits with an optional leading minus sign"},
            {"type": "from TIMESTAMP", "example": "ts_col::INTEGER",  "note": "Returns microseconds since the Unix epoch (1970-01-01 00:00:00 UTC)"},
            {"type": "from DATE",      "example": "date_col::INTEGER","note": "Returns days since the Unix epoch"},
        ],
        "comparable_with": ["INTEGER", "FLOAT", "DECIMAL"],
        "limitations": [
            "Cannot compare INTEGER to VARCHAR or temporal types — cast first.",
            "Overflow is not detected at runtime: values outside the ±9,223,372,036,854,775,807 range wrap silently.",
        ],
    },
    "float": {
        "description": "64-bit IEEE 754 double-precision floating-point number. Write `FLOAT` or `DOUBLE` in SQL — they are equivalent.",
        "example": "3.14",
        "min": -1.7976931348623157e+308,
        "max": 1.7976931348623157e+308,
        "cast_to": [
            {"type": "from INTEGER", "example": "42::FLOAT",       "note": "Exact for values up to 2^53; larger integers lose precision"},
            {"type": "from BOOLEAN", "example": "TRUE::FLOAT",     "note": "TRUE → 1.0, FALSE → 0.0"},
            {"type": "from VARCHAR", "example": "'3.14'::FLOAT",   "note": "Accepts decimal notation and scientific notation (e.g. '1.5e3')"},
            {"type": "from DECIMAL", "example": "d_col::FLOAT",    "note": "May lose precision for high-scale decimals"},
        ],
        "comparable_with": ["FLOAT", "INTEGER", "DECIMAL"],
        "notes": "NaN sorts highest (appears after all real numbers). -0.0 and 0.0 compare as equal. Infinity is representable but not directly writable as a literal.",
        "limitations": [
            "Floating-point arithmetic is inexact. Use DECIMAL for financial calculations.",
            "NaN comparisons: NaN = NaN is FALSE in SQL; NaN appears at the top when sorting.",
        ],
    },
    "decimal": {
        "description": (
            "Exact fixed-point number with declared precision and scale: `DECIMAL(precision, scale)`. "
            "Precision is the total number of significant digits (1–38); scale is the number of digits after the decimal point (0–precision). "
            "For example, `DECIMAL(10, 2)` holds values up to 99999999.99."
        ),
        "example": "1.23::DECIMAL(10,2)",
        "cast_to": [
            {"type": "from INTEGER", "example": "42::DECIMAL(10,2)",     "note": "Exact — no precision loss for integers within range"},
            {"type": "from FLOAT",   "example": "3.14::DECIMAL(10,4)",   "note": "Rounded to declared scale; floating-point representation may introduce noise"},
            {"type": "from VARCHAR", "example": "'1.23'::DECIMAL(10,2)", "note": "String must be a valid decimal literal"},
        ],
        "comparable_with": ["DECIMAL", "INTEGER", "FLOAT"],
        "notes": (
            "Precision 1–18 uses 64-bit integer storage. Precision 19–38 uses 128-bit integer storage. "
            "Precision above 38 is not supported. "
            "Arithmetic result precision follows SQL Server rules: "
            "addition/subtraction scales as `max(s1,s2)` with precision `max(p1-s1, p2-s2) + max(s1,s2) + 1`; "
            "multiplication gives `p1+p2` precision and `s1+s2` scale."
        ),
        "limitations": [
            "SUM, AVG, and MEDIAN do not support DECIMAL columns — cast to FLOAT first: `SUM(col::FLOAT)`.",
            "DECIMAL columns from Parquet files are read correctly but aggregate functions reject them.",
        ],
    },
    "date": {
        "description": "A calendar date with no time component. Stored as the number of days since 1970-01-01.",
        "example": "'2024-01-01'::DATE",
        "string_formats": [
            {"format": "YYYY-MM-DD", "example": "'2024-01-15'::DATE", "note": "Only this format is accepted — ISO 8601 date"},
        ],
        "cast_to": [
            {"type": "from VARCHAR",    "example": "'2024-01-15'::DATE",        "note": "String must be in YYYY-MM-DD format"},
            {"type": "from TIMESTAMP",  "example": "ts_col::DATE",               "note": "Truncates the time component; returns the date portion only"},
        ],
        "comparable_with": ["DATE", "TIMESTAMP"],
        "arithmetic": [
            {"expr": "date_col + INTERVAL '7' DAY",   "result": "TIMESTAMP", "desc": "Add a duration to a date"},
            {"expr": "date_col - INTERVAL '1' MONTH",  "result": "TIMESTAMP", "desc": "Subtract a duration"},
            {"expr": "date_col - other_date",          "result": "INTERVAL",  "desc": "Difference between two dates"},
        ],
        "limitations": [
            "You cannot cast an integer to DATE directly. To convert a Unix epoch value, cast to TIMESTAMP first then to DATE: `FROM_UNIXTIME(n)::DATE`.",
            "Only YYYY-MM-DD string format is accepted. Formats like MM/DD/YYYY or DD-MM-YYYY will fail.",
        ],
    },
    "time": {
        "description": "A time of day with no date component. Stores hours, minutes, seconds, and microseconds.",
        "example": "'12:30:00'::TIME",
        "string_formats": [
            {"format": "HH:MM",    "example": "'09:30'::TIME",    "note": "Hour and minute only; seconds default to 0"},
            {"format": "HH:MM:SS", "example": "'09:30:45'::TIME", "note": "Hour, minute, and second"},
        ],
        "cast_to": [
            {"type": "from VARCHAR", "example": "'09:30:45'::TIME", "note": "String must be HH:MM or HH:MM:SS"},
        ],
        "comparable_with": ["TIME"],
        "limitations": [
            "TIME cannot be compared to DATE or TIMESTAMP.",
            "No timezone support — TIME is always local/naive.",
            "You cannot cast an integer to TIME.",
        ],
    },
    "timestamp": {
        "description": (
            "A date and time value. The default scale is microseconds. "
            "Use `TIMESTAMP[s]`, `TIMESTAMP[ms]`, `TIMESTAMP[us]`, `TIMESTAMP[ns]`, or `TIMESTAMP[d]` "
            "to declare a specific scale — this matters when casting integer epoch columns."
        ),
        "example": "'2024-01-01 12:00:00'::TIMESTAMP",
        "string_formats": [
            {"format": "YYYY-MM-DD",                  "example": "'2024-01-15'::TIMESTAMP",              "note": "Date only — time defaults to 00:00:00"},
            {"format": "YYYY-MM-DD HH:MM:SS",         "example": "'2024-01-15 09:30:00'::TIMESTAMP",    "note": "Date and time separated by a space"},
            {"format": "YYYY-MM-DDTHH:MM:SS",         "example": "'2024-01-15T09:30:00'::TIMESTAMP",    "note": "ISO 8601 with T separator"},
            {"format": "YYYY-MM-DDTHH:MM:SS.ffffff",  "example": "'2024-01-15T09:30:00.123456'::TIMESTAMP", "note": "With microseconds"},
        ],
        "cast_to": [
            {"type": "from VARCHAR",  "example": "'2024-01-15 09:30:00'::TIMESTAMP",   "note": "Accepts the string formats listed above"},
            {"type": "from DATE",     "example": "date_col::TIMESTAMP",                 "note": "Fills time as midnight (00:00:00)"},
            {"type": "from INTEGER (seconds)",      "example": "epoch_col::TIMESTAMP[s]",  "note": "Seconds since Unix epoch"},
            {"type": "from INTEGER (milliseconds)", "example": "epoch_col::TIMESTAMP[ms]", "note": "Milliseconds since Unix epoch"},
            {"type": "from INTEGER (microseconds)", "example": "epoch_col::TIMESTAMP[us]", "note": "Microseconds since Unix epoch (default scale)"},
            {"type": "from INTEGER (nanoseconds)",  "example": "epoch_col::TIMESTAMP[ns]", "note": "Nanoseconds since Unix epoch"},
        ],
        "comparable_with": ["TIMESTAMP", "DATE"],
        "arithmetic": [
            {"expr": "ts_col + INTERVAL '1' HOUR",      "result": "TIMESTAMP", "desc": "Add a duration"},
            {"expr": "ts_col - INTERVAL '30' MINUTE",   "result": "TIMESTAMP", "desc": "Subtract a duration"},
            {"expr": "ts_col - other_ts",               "result": "INTERVAL",  "desc": "Difference between two timestamps"},
        ],
        "notes": (
            "All scales are stored as INT64. "
            "At the default microsecond scale, the representable range is approximately 1677-09-21 to 2262-04-11. "
            "Timezone information is not stored — all timestamps are naive (no offset). "
            "String parsing accepts a space or T as the date/time separator. "
            "Timezone suffixes (Z, +01:00) in strings are ignored — only the local time portion is parsed."
        ),
        "limitations": [
            "Timezone offsets in string literals are silently ignored. Convert to UTC before storing if timezone matters.",
            "`1::TIMESTAMP` is not valid — you must specify the scale: `1::TIMESTAMP[s]`.",
            "Timestamps outside 1677–2262 are not representable at microsecond scale.",
        ],
    },
    "interval": {
        "description": (
            "A duration or period of time. Written as `INTERVAL 'value' UNIT` where UNIT is one of "
            "`DAY`, `MONTH`, `YEAR`, `HOUR`, `MINUTE`, `SECOND`, or `MICROSECOND`."
        ),
        "example": "INTERVAL '7' DAY",
        "cast_to": [],
        "comparable_with": ["INTERVAL"],
        "arithmetic": [
            {"expr": "INTERVAL '1' DAY + INTERVAL '2' HOUR", "result": "INTERVAL", "desc": "Add two intervals"},
            {"expr": "date_col + INTERVAL '1' MONTH",        "result": "TIMESTAMP", "desc": "Shift a date forward"},
            {"expr": "ts_col - INTERVAL '30' MINUTE",        "result": "TIMESTAMP", "desc": "Shift a timestamp back"},
        ],
        "notes": (
            "Sub-month components (days, hours, minutes, seconds, microseconds) are stored as a microsecond count. "
            "Month and year components are stored separately and applied calendar-accurately during arithmetic. "
            "You cannot mix month-based and sub-month intervals in a single expression."
        ),
        "limitations": [
            "There is no INTERVAL literal that combines months and days in one expression (e.g. '1 month 3 days' is not supported). Use separate additions.",
            "INTERVAL cannot be cast to or from other types.",
        ],
    },
    "varchar": {
        "description": (
            "A variable-length ASCII text string. Use VARCHAR for columns that contain only ASCII characters. "
            "For text with accented characters, emoji, or any non-ASCII content, use NVARCHAR instead."
        ),
        "example": "'hello world'",
        "cast_to": [
            {"type": "from INTEGER",   "example": "42::VARCHAR",                    "note": "Decimal string representation"},
            {"type": "from FLOAT",     "example": "3.14::VARCHAR",                  "note": "Decimal notation"},
            {"type": "from BOOLEAN",   "example": "TRUE::VARCHAR",                  "note": "'true' or 'false'"},
            {"type": "from DATE",      "example": "date_col::VARCHAR",               "note": "'YYYY-MM-DD'"},
            {"type": "from TIMESTAMP", "example": "ts_col::VARCHAR",                 "note": "ISO 8601 string representation"},
        ],
        "comparable_with": ["VARCHAR", "NVARCHAR", "VARBINARY"],
        "notes": "Supports `LIKE` (case-sensitive), `ILIKE` (case-insensitive), and `RLIKE` (regular expression) pattern matching.",
        "limitations": [
            "Non-ASCII bytes stored in a VARCHAR column produce undefined behaviour — use NVARCHAR for Unicode.",
        ],
    },
    "nvarchar": {
        "description": (
            "A variable-length UTF-8 encoded text string. Use NVARCHAR for any text that may contain "
            "non-ASCII characters. JSON columns are stored as NVARCHAR."
        ),
        "example": "'héllo wörld'",
        "cast_to": [
            {"type": "from VARCHAR",   "example": "ascii_col::NVARCHAR", "note": "Validates UTF-8; fails if the bytes are not valid UTF-8"},
            {"type": "from VARBINARY", "example": "bin_col::NVARCHAR",   "note": "Interprets raw bytes as UTF-8; fails on invalid sequences"},
        ],
        "comparable_with": ["NVARCHAR", "VARCHAR"],
        "notes": "Supports `LIKE`, `ILIKE`, and `RLIKE` pattern matching. String functions that operate on character positions (e.g. SUBSTRING) count Unicode code points, not bytes.",
        "limitations": [
            "Casting from VARBINARY will fail if the bytes are not valid UTF-8.",
            "There is no structured STRUCT or JSONB type. JSON data lands as NVARCHAR — use `->` and `->>` to navigate it.",
        ],
    },
    "varbinary": {
        "description": "Raw binary data (arbitrary bytes). Use for hashes, encoded payloads, or any non-text binary content.",
        "example": "HEX_DECODE('deadbeef')",
        "cast_to": [
            {"type": "from VARCHAR",  "example": "'hello'::VARBINARY", "note": "Treats the string bytes directly as binary"},
            {"type": "from NVARCHAR", "example": "utf8_col::VARBINARY","note": "Returns the raw UTF-8 byte sequence"},
        ],
        "comparable_with": ["VARBINARY", "VARCHAR"],
        "limitations": [
            "There is no binary literal syntax. Use HEX_DECODE(), BASE64_DECODE(), or cast from a hex string.",
        ],
    },
    "variant": {
        "description": (
            "A semi-structured type for heterogeneous or schema-on-read data. "
            "VARIANT values are typically produced by reading JSON columns. "
            "Use the `->` operator to extract a field as NVARCHAR, or `->>` to extract as plain text."
        ),
        "cast_to": [],
        "comparable_with": [],
        "notes": "VARIANT is produced automatically when reading JSON/NVARCHAR columns that contain structured data. Use `->` and `->>` operators to navigate the structure.",
        "limitations": [
            "You cannot CAST any value to VARIANT — it is read-only at the SQL level.",
            "VARIANT values cannot be compared with = or <. Extract a field first.",
            "VARIANT columns cannot be used in GROUP BY, ORDER BY, or JOIN conditions directly — extract and cast first.",
        ],
    },
    "array": {
        "description": (
            "An ordered sequence of elements, all of the same type. "
            "Array columns appear when reading Parquet or JSONL files that contain repeated/array fields. "
            "The element type is declared as `ARRAY<type>` (e.g. `ARRAY<INTEGER>`, `ARRAY<VARCHAR>`)."
        ),
        "cast_to": [],
        "comparable_with": [],
        "notes": "Individual elements are accessed with subscript notation: `arr[0]` returns the first element (zero-indexed).",
        "limitations": [
            "There is no standalone array literal syntax. `SELECT [1, 2, 3]` is not valid.",
            "Arrays cannot be used in WHERE clauses or JOIN conditions directly. Use UNNEST or element access.",
            "You cannot CAST a scalar value to ARRAY.",
        ],
    },
    "vector": {
        "description": (
            "A fixed-length vector of FP16 (half-precision) floating-point values. "
            "Used for similarity search and ML embedding workloads. "
            "Declared as `VECTOR(n)` where n is the number of dimensions."
        ),
        "cast_to": [
            {"type": "from ARRAY<FLOAT>", "example": "float_array_col::VECTOR(384)", "note": "Quantizes each element to FP16"},
        ],
        "comparable_with": [],
        "notes": "Similarity search uses dedicated functions such as `COSINE_DISTANCE(a, b)` and `COSINE_SIMILARITY(a, b)`. Standard comparison operators are not supported on VECTOR.",
        "limitations": [
            "Vector columns cannot be used with standard comparison operators (=, <, >, etc.).",
            "The dimension count must match between vectors in any operation.",
        ],
    },
}

_FAMILY: dict[LogicalCategory, str] = {
    LogicalCategory.INTEGER: "numeric",
    LogicalCategory.FLOAT: "numeric",
    LogicalCategory.DECIMAL: "numeric",
    LogicalCategory.DATE: "temporal",
    LogicalCategory.TIME: "temporal",
    LogicalCategory.TIMESTAMP: "temporal",
    LogicalCategory.INTERVAL: "interval",
    LogicalCategory.VARCHAR: "text",
    LogicalCategory.NVARCHAR: "text",
    LogicalCategory.VARBINARY: "binary",
    LogicalCategory.BOOLEAN: "boolean",
    LogicalCategory.ARRAY: "nested",
    LogicalCategory.VARIANT: "nested",
    LogicalCategory.VECTOR: "vector",
    LogicalCategory.NULL: "null",
}


def _type_id(cat: LogicalCategory) -> str:
    return cat.value.lower()


def _storage_mapping_groups(
    mapping: dict[str, LogicalCategory],
) -> dict[str, list[str]]:
    grouped: defaultdict[str, set[str]] = defaultdict(set)
    for spelling, cat in mapping.items():
        type_id = _type_id(cat)
        grouped[type_id].add(spelling)
    return {key: sorted(values) for key, values in grouped.items()}


def export_type_catalog() -> OrderedDict[str, dict[str, Any]]:
    parquet_physical = _storage_mapping_groups(PARQUET_PHYSICAL_TYPE_MAP)
    parquet_logical = _storage_mapping_groups(PARQUET_LOGICAL_TYPE_MAP)
    jsonl = _storage_mapping_groups(JSONL_TYPE_MAP)

    exported: dict[str, dict[str, Any]] = {}
    for cat in LogicalCategory:
        type_id = _type_id(cat)
        aliases = _SQL_ALIASES.get(type_id, [])
        metadata = _TYPE_METADATA.get(type_id, {})

        entry: dict[str, Any] = {
            "canonical_name": cat.value,
            "aliases": aliases,
            "accepted_spellings": sorted({type_id, *aliases}),
            "family": _FAMILY.get(cat, "other"),
            "flags": {
                "numeric": cat in _NUMERIC_TYPES,
                "temporal": cat in _TEMPORAL_TYPES,
                "collection": cat in {LogicalCategory.ARRAY, LogicalCategory.VARIANT, LogicalCategory.NVARCHAR, LogicalCategory.VECTOR},
                "parameterized": cat in {LogicalCategory.DECIMAL, LogicalCategory.ARRAY},
            },
            "metadata": metadata,
            "parameterized_forms": ["DECIMAL(precision,scale)"] if cat == LogicalCategory.DECIMAL else (["ARRAY<type>"] if cat == LogicalCategory.ARRAY else []),
            "ingestion_mappings": {
                "parquet_physical": parquet_physical.get(type_id, []),
                "parquet_logical": parquet_logical.get(type_id, []),
                "jsonl": jsonl.get(type_id, []),
            },
        }

        if cat == LogicalCategory.ARRAY:
            entry["ingestion_mappings"]["parquet_logical_patterns"] = ["array<...>"]
            entry["ingestion_mappings"]["jsonl_patterns"] = ["array<...>"]
            entry["element_type_aliases"] = sorted(
                set(JSONL_ARRAY_INNER_TYPE_ALIASES.values())
                | {"integer", "float", "varbinary", "boolean", "nvarchar"}
            )
        elif cat == LogicalCategory.DECIMAL:
            entry["ingestion_mappings"]["parquet_logical_patterns"] = ["decimal(...)"]
        elif cat == LogicalCategory.TIME:
            entry["ingestion_mappings"]["parquet_logical_patterns"] = [
                pattern + "..."
                for pattern, mapped_cat in PARQUET_LOGICAL_COMPLEX_PREFIXES.items()
                if mapped_cat == LogicalCategory.TIME
            ]
        elif cat == LogicalCategory.TIMESTAMP:
            entry["ingestion_mappings"]["parquet_logical_patterns"] = [
                pattern + "..."
                for pattern, mapped_cat in PARQUET_LOGICAL_COMPLEX_PREFIXES.items()
                if mapped_cat == LogicalCategory.TIMESTAMP
            ]

        exported[type_id] = entry

    ordered = OrderedDict()
    for name in sorted(exported):
        ordered[name] = exported[name]
    return ordered


def write_type_catalog(path: str | Path) -> None:
    output_path = Path(path)
    output_path.write_text(
        json.dumps(export_type_catalog(), indent=4) + "\n",
        encoding="utf8",
    )
