"""
Parquet logical types must survive the schema mapping: the ARRAY container, and the
timestamp unit. Two distinct bugs, both in _rugo_schema, both silent.

A parquet ARRAY column must be typed ARRAY, whatever its element type is.

rugo reports a list column's logical type as one string carrying the element:
"array<timestamp[us]>", "array<int64>", "array<time32[ms]>". _map_parquet_type_to_sql
matched the SCALAR temporal cases first, and matched them on a SUBSTRING:

    if logical_lower.startswith("timestamp") or "timestamp" in logical_lower:

so "array<timestamp[us]>" resolved to a bare TIMESTAMP — the ARRAY container silently
dropped. The declared column type (TIMESTAMP) then disagreed with the runtime vector
(genuinely DRAKEN_ARRAY), and every ARRAY-typed function rejected the column at bind
time: `SORT(ts)` → "SORT arg1 ('ts'): expected ARRAY, got TIMESTAMP". Nothing failed
loudly at the point of the mistake — the schema just quietly described the wrong type.

These are pure unit tests of the mapping (no file IO): the substring hazard lives in
the string mapping, so that is where it is pinned.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.connectors._rugo_schema import (
    _map_parquet_type_to_sql,
    _normalize_parquet_type_string,
    _parse_timestamp_unit,
)
from opteryx.types.logical_type import LogicalCategory, TimestampUnit, parse_column_type


# (physical_type, logical_type) -> expected category.
# The array<temporal> rows are the regression; the scalar rows pin that fixing it
# did not break the scalar temporal mapping the reorder moved past.
ARRAY_CASES = [
    ("list", "array<timestamp[us]>"),
    ("list", "array<timestamp[ms]>"),
    ("list", "array<time32[ms]>"),
    ("list", "array<time64[us]>"),
    ("list", "array<date32[day]>"),
    ("list", "array<int64>"),
    ("list", "array<varchar>"),
    ("list", "array<decimal(9,2)>"),
]

SCALAR_CASES = [
    ("int64", "timestamp[us]", LogicalCategory.TIMESTAMP),
    ("int64", "timestamp[ms]", LogicalCategory.TIMESTAMP),
    ("int32", "time32[ms]", LogicalCategory.TIME),
    ("int32", "date32[day]", LogicalCategory.DATE),
    ("int64", "int64", LogicalCategory.INTEGER),
    ("byte_array", "string", LogicalCategory.VARCHAR),
    ("boolean", "boolean", LogicalCategory.BOOLEAN),
]


@pytest.mark.parametrize("physical, logical", ARRAY_CASES)
def test_array_logical_type_maps_to_array(physical, logical):
    """An array<...> logical type is ARRAY regardless of its element type."""
    assert _map_parquet_type_to_sql(physical, logical) == LogicalCategory.ARRAY


@pytest.mark.parametrize("physical, logical, expected", SCALAR_CASES)
def test_scalar_logical_types_unchanged(physical, logical, expected):
    """Scalar mappings must be untouched by the array reorder."""
    assert _map_parquet_type_to_sql(physical, logical) == expected


@pytest.mark.parametrize("physical, logical", ARRAY_CASES)
def test_array_logical_type_is_parseable_with_an_element(physical, logical):
    """The category alone is not enough: rugo_to_relation_schema re-parses the same
    string into a full ColumnType, and an ARRAY ColumnType is invalid without an
    element. If the normalizer and the mapping ever disagree, a column typed ARRAY
    by the mapping raises here instead — so pin that they agree."""
    column_type = parse_column_type(_normalize_parquet_type_string(logical))
    assert column_type.category == LogicalCategory.ARRAY
    assert column_type.element is not None


def test_array_of_timestamp_keeps_its_element_type():
    """The specific regression: container AND element both survive."""
    column_type = parse_column_type(_normalize_parquet_type_string("array<timestamp[us]>"))
    assert column_type.category == LogicalCategory.ARRAY
    assert column_type.element.category == LogicalCategory.TIMESTAMP


def test_unit_suffix_stripping():
    """Parquet's unit/width suffixes are not part of the SQL type grammar."""
    assert _normalize_parquet_type_string("array<timestamp[us]>") == "array<timestamp>"
    assert _normalize_parquet_type_string("timestamp[us]") == "timestamp"
    assert _normalize_parquet_type_string("array<time32[ms]>") == "array<time>"
    assert _normalize_parquet_type_string("date32[day]") == "date"
    # decimal's (p,s) is part of the grammar and must survive
    assert _normalize_parquet_type_string("array<decimal(9,2)>") == "array<decimal(9,2)>"


# ---------------------------------------------------------------------------
# Timestamp unit. Separate bug, same file: _map_parquet_type_to_sql returns only a
# CATEGORY, and rugo_to_relation_schema then resolved TIMESTAMP through
# _CATEGORY_TO_CANONICAL — which is always unit=us. So a `timestamp[ms]` column
# decoded its raw int64 as microseconds: 1704164645000 → 1970-01-20, not
# 2024-01-02. Silently wrong DATES (and an outright overflow for ns), for scalar
# timestamp columns as much as for array elements. The unit is data, not
# formatting, so it is pinned here.
# ---------------------------------------------------------------------------

UNIT_CASES = [
    ("timestamp[s]", TimestampUnit.SECONDS),
    ("timestamp[ms]", TimestampUnit.MILLISECONDS),
    ("timestamp[us]", TimestampUnit.MICROSECONDS),
    ("timestamp[ns]", TimestampUnit.NANOSECONDS),
    ("array<timestamp[s]>", TimestampUnit.SECONDS),
    ("array<timestamp[ms]>", TimestampUnit.MILLISECONDS),
    ("array<timestamp[us]>", TimestampUnit.MICROSECONDS),
    ("array<timestamp[ns]>", TimestampUnit.NANOSECONDS),
]


@pytest.mark.parametrize("logical, expected_unit", UNIT_CASES)
def test_timestamp_unit_is_read_from_the_logical_string(logical, expected_unit):
    """The file's unit must survive, scalar or array element alike."""
    assert _parse_timestamp_unit(logical) == expected_unit


@pytest.mark.parametrize(
    "logical", ["int64", "array<int64>", "decimal(9,2)", "string", "", None]
)
def test_non_timestamp_types_have_no_unit(logical):
    """None (→ the microsecond default) for anything that is not a timestamp."""
    assert _parse_timestamp_unit(logical) is None


def test_timestamp_unit_survives_into_the_schema_column_type():
    """End-to-end through the ColumnType builders the schema path uses: a
    non-microsecond unit must reach the ColumnType, scalar and element alike."""
    from opteryx.types import logical_type as _lt

    scalar = _lt.TIMESTAMP(_parse_timestamp_unit("timestamp[ms]"))
    assert scalar.logical.unit == TimestampUnit.MILLISECONDS

    array = _lt.ARRAY(_lt.TIMESTAMP(_parse_timestamp_unit("array<timestamp[ns]>")))
    assert array.category == LogicalCategory.ARRAY
    assert array.element.logical.unit == TimestampUnit.NANOSECONDS


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
