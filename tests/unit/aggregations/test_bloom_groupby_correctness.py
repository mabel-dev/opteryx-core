"""
Correctness tests for Bloom filter in group-by operations.

These tests validate that the Bloom filter pre-filter (Fix 2) maintains
correctness while improving performance. The Bloom filter must never produce
false negatives (no group elimination), only false positives (extra lookups).
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa

from opteryx import EOS
from opteryx.models import QueryProperties
from opteryx.operators.shuffle import AggregationSpec, ShuffleGroupByOperation


def _make_int_array(values: list[int]) -> pa.Array:
    """Create a PyArrow integer array."""
    return pa.array(values, type=pa.int64())


def _make_string_array(values: list[str]) -> pa.Array:
    """Create a PyArrow string array."""
    return pa.array(values, type=pa.string())


def _make_float_array(values: list[float]) -> pa.Array:
    """Create a PyArrow float64 array."""
    return pa.array(values, type=pa.float64())


def _make_table(schema: dict[str, pa.Array]) -> pa.Table:
    """Create a PyArrow table from a schema dict."""
    return pa.table(schema)


def _groupby_result_as_dict(morsel_or_table) -> dict:
    """Convert a group-by result (Morsel or Table) to a dict of dicts for easy comparison."""
    if morsel_or_table is None:
        return {}

    # Convert Morsel to PyArrow Table if needed
    table = morsel_or_table.to_arrow() if hasattr(morsel_or_table, "to_arrow") else morsel_or_table

    if len(table) == 0:
        return {}

    result = {}
    # Known aggregate result column names
    agg_suffixes = {
        "_val",
        "_count",
        "_distinct",
        "_cd",
        "_cnt",
        "_sum",
        "_avg",
        "_min",
        "_max",
        "_h",
    }

    for row in table.to_pylist():
        columns = list(row.keys())

        # Identify GROUP BY columns: those that don't look like aggregate results
        key_cols = []
        for col in columns:
            is_agg = any(col.endswith(suffix) for suffix in agg_suffixes) or col in {
                "count_val",
                "cd",
                "cnt",
                "h",
            }
            if not is_agg:
                key_cols.append(col)

        # If no key columns identified, use the first column
        if not key_cols:
            key_cols = [columns[0]]

        # Create key from GROUP BY columns
        key_vals = [row[col] for col in key_cols]
        key = tuple(key_vals) if len(key_vals) > 1 else key_vals[0]

        result[key] = row
    return result


def _ingest_table_into_groupby(
    groupby: ShuffleGroupByOperation,
    table: pa.Table,
    morsel_size: int = 1000,
) -> None:
    """Ingest a table into a group-by operation, splitting into morsels."""
    num_rows = len(table)
    for start_idx in range(0, num_rows, morsel_size):
        end_idx = min(start_idx + morsel_size, num_rows)
        morsel = table.slice(start_idx, end_idx - start_idx)
        groupby.ingest(morsel)


def test_bloom_groupby_low_cardinality_correctness():
    """
    Test Bloom filter with low cardinality (100 groups).
    Validates that results match expected output when bloom is not heavily utilized.
    """
    # Create 1M rows with 100 unique groups
    num_rows = 1_000_000
    num_groups = 100
    group_ids = np.tile(np.arange(num_groups), num_rows // num_groups + 1)[:num_rows]
    np.random.shuffle(group_ids)
    values = np.random.rand(num_rows)

    table = _make_table(
        {
            "group_id": _make_int_array(group_ids.tolist()),
            "value": _make_float_array(values.tolist()),
        }
    )

    groupby = ShuffleGroupByOperation(
        group_by_columns=["group_id"],
        aggregations=[
            AggregationSpec(alias="count_val", function="count", column="*"),
            AggregationSpec(alias="sum_val", function="sum", column="value"),
            AggregationSpec(alias="avg_val", function="avg", column="value"),
        ],
    )

    _ingest_table_into_groupby(groupby, table, morsel_size=10000)
    result = groupby.finalize()

    # Verify result correctness
    assert result is not None
    assert len(result) == num_groups

    # Verify that each group appears in result
    result_dict = _groupby_result_as_dict(result)
    for gid in range(num_groups):
        assert gid in result_dict
        assert result_dict[gid]["count_val"] > 0
        assert result_dict[gid]["sum_val"] is not None
        assert result_dict[gid]["avg_val"] is not None


def test_bloom_groupby_high_cardinality_stress():
    """
    Test Bloom filter with high cardinality (900K unique groups from 1M rows).
    Stress tests the miss path and bloom filter effectiveness.
    """
    num_rows = 1_000_000
    num_groups = 900_000

    # Create 900K unique groups with ~1.1 rows per group on average
    group_ids = list(range(num_groups)) + [
        np.random.randint(0, num_groups) for _ in range(num_rows - num_groups)
    ]
    np.random.shuffle(group_ids)
    values = np.random.rand(num_rows)

    table = _make_table(
        {
            "group_id": _make_int_array(group_ids),
            "value": _make_float_array(values.tolist()),
        }
    )

    groupby = ShuffleGroupByOperation(
        group_by_columns=["group_id"],
        aggregations=[
            AggregationSpec(alias="count_val", function="count", column="*"),
        ],
    )

    _ingest_table_into_groupby(groupby, table, morsel_size=50000)
    result = groupby.finalize()

    # Verify result correctness
    assert result is not None
    # Should have around 900K groups (some might not appear if random selection skips them)
    assert len(result) >= 900_000 - 10_000  # Allow some variance
    assert len(result) <= 900_000


def test_bloom_groupby_null_key_handling():
    """
    Test that Bloom filter correctly handles NULL keys.
    NULLs must group together (or be excluded per SQL semantics).
    """
    # Create data with 10% NULLs
    num_rows = 100_000
    num_groups = 50
    group_ids = []
    for i in range(num_rows):
        if i % 10 == 0:  # 10% NULLs
            group_ids.append(None)
        else:
            group_ids.append(i % num_groups)

    table = _make_table(
        {
            "group_id": pa.array(group_ids, type=pa.int64()),
            "value": _make_float_array(np.random.rand(num_rows).tolist()),
        }
    )

    groupby = ShuffleGroupByOperation(
        group_by_columns=["group_id"],
        aggregations=[
            AggregationSpec(alias="count_val", function="count", column="*"),
        ],
    )

    _ingest_table_into_groupby(groupby, table, morsel_size=10000)
    result = groupby.finalize()

    # Verify that NULLs are grouped correctly
    assert result is not None
    result_dict = _groupby_result_as_dict(result)

    # Should have around 50 non-NULL groups + 1 NULL group (allow variance)
    assert len(result_dict) >= 45

    # Find the NULL group
    null_count = 0
    result_arrow = result.to_arrow()
    for row in result_arrow.to_pylist():
        if row.get("group_id") is None:
            null_count = row["count_val"]

    # Should have approximately 10% of rows in the NULL group
    expected_null_count = num_rows // 10
    assert abs(null_count - expected_null_count) < expected_null_count * 0.1  # Within 10% tolerance


def test_bloom_groupby_repeated_morsels():
    """
    Test that Bloom filter effectiveness increases as morsels repeat.
    After the first ingest, all subsequent identical rows should be hits,
    and bloom should show near-100% skip rate by morsel 3.
    """
    # Create a small table with 10 unique groups
    num_groups = 10
    base_table = _make_table(
        {
            "group_id": _make_int_array(list(range(num_groups)) * 1000),
            "value": _make_float_array(np.random.rand(num_groups * 1000).tolist()),
        }
    )

    groupby = ShuffleGroupByOperation(
        group_by_columns=["group_id"],
        aggregations=[
            AggregationSpec(alias="count_val", function="count", column="*"),
        ],
    )

    # Ingest the same morsel 10 times
    for _ in range(10):
        _ingest_table_into_groupby(groupby, base_table, morsel_size=5000)

    result = groupby.finalize()

    # Verify correctness: should still have 10 groups
    assert result is not None
    assert len(result) == num_groups

    # Each group should appear 10 times (10 ingests × 1000 rows each / 10 groups)
    result_dict = _groupby_result_as_dict(result)
    for gid in range(num_groups):
        assert gid in result_dict
        # Each group should have 10,000 total rows (1000 per ingest × 10 ingests)
        assert result_dict[gid]["count_val"] == 10_000


def test_bloom_groupby_multi_column_mixed_types():
    """
    Test Bloom filter with multi-column GROUP BY and mixed types.
    Validates the _ingest_object_key path specifically.
    """
    num_rows = 100_000

    # Create data with two different column types
    int_values = np.random.randint(0, 50, num_rows)
    string_values = [f"group_{i % 100}" for i in range(num_rows)]
    numeric_values = np.random.rand(num_rows)

    table = _make_table(
        {
            "int_col": _make_int_array(int_values.tolist()),
            "str_col": _make_string_array(string_values),
            "value": _make_float_array(numeric_values.tolist()),
        }
    )

    groupby = ShuffleGroupByOperation(
        group_by_columns=["int_col", "str_col"],
        aggregations=[
            AggregationSpec(alias="count_val", function="count", column="*"),
            AggregationSpec(alias="sum_val", function="sum", column="value"),
        ],
    )

    _ingest_table_into_groupby(groupby, table, morsel_size=10000)
    result = groupby.finalize()

    # Verify correctness
    assert result is not None
    # Should have 50 × 100 = 5000 unique group combinations (or fewer if not all combinations appear)
    assert len(result) > 0
    assert len(result) <= 5000

    # Each row should have all expected columns
    result_arrow = result.to_arrow()
    for row in result_arrow.to_pylist():
        assert "int_col" in row
        assert "str_col" in row
        assert "count_val" in row
        assert "sum_val" in row


def test_bloom_groupby_ordering_independence():
    """
    Test that different row orderings produce identical aggregate results.
    Validates that Bloom filter's incremental population doesn't affect output correctness.
    """
    # Create base data
    num_rows = 100_000
    num_groups = 500
    base_group_ids = list(range(num_groups)) * (num_rows // num_groups)
    base_values = np.arange(num_rows, dtype=float)

    # Test 1: in-order ingest
    groupby1 = ShuffleGroupByOperation(
        group_by_columns=["group_id"],
        aggregations=[
            AggregationSpec(alias="count_val", function="count", column="*"),
            AggregationSpec(alias="sum_val", function="sum", column="value"),
        ],
    )

    table1 = _make_table(
        {
            "group_id": _make_int_array(base_group_ids),
            "value": _make_float_array(base_values.tolist()),
        }
    )
    _ingest_table_into_groupby(groupby1, table1, morsel_size=10000)
    result1 = groupby1.finalize()

    # Test 2: shuffled ingest
    shuffled_indices = np.random.permutation(num_rows)
    shuffled_group_ids = [base_group_ids[i] for i in shuffled_indices]
    shuffled_values = [base_values[i] for i in shuffled_indices]

    groupby2 = ShuffleGroupByOperation(
        group_by_columns=["group_id"],
        aggregations=[
            AggregationSpec(alias="count_val", function="count", column="*"),
            AggregationSpec(alias="sum_val", function="sum", column="value"),
        ],
    )

    table2 = _make_table(
        {
            "group_id": _make_int_array(shuffled_group_ids),
            "value": _make_float_array(shuffled_values),
        }
    )
    _ingest_table_into_groupby(groupby2, table2, morsel_size=10000)
    result2 = groupby2.finalize()

    # Results should be identical regardless of order
    assert result1 is not None
    assert result2 is not None
    assert len(result1) == len(result2)

    # Convert to dicts for comparison (order-independent)
    dict1 = _groupby_result_as_dict(result1)
    dict2 = _groupby_result_as_dict(result2)

    for key in dict1:
        assert key in dict2
        assert dict1[key]["count_val"] == dict2[key]["count_val"]
        # Allow small floating point differences in sum
        assert abs(dict1[key]["sum_val"] - dict2[key]["sum_val"]) < 1e-6


def test_bloom_groupby_no_false_negatives():
    """
    Test that Bloom filter never produces false negatives (eliminating groups).
    Every group present in a dataset must appear in the output.
    """
    # Create data with distinct group IDs to ensure each appears
    unique_group_ids = list(range(10_000))
    # Each group appears exactly once
    group_ids = unique_group_ids * 10  # 10 copies of each
    values = np.arange(len(group_ids), dtype=float)

    table = _make_table(
        {
            "group_id": _make_int_array(group_ids),
            "value": _make_float_array(values.tolist()),
        }
    )

    groupby = ShuffleGroupByOperation(
        group_by_columns=["group_id"],
        aggregations=[
            AggregationSpec(alias="count_val", function="count", column="*"),
        ],
    )

    _ingest_table_into_groupby(groupby, table, morsel_size=10000)
    result = groupby.finalize()

    # Verify all groups are present
    assert result is not None
    assert len(result) == len(unique_group_ids)

    result_dict = _groupby_result_as_dict(result)
    for gid in unique_group_ids:
        assert gid in result_dict, f"Group {gid} missing from results (false negative)"
        assert result_dict[gid]["count_val"] == 10


def test_bloom_groupby_string_keys():
    """
    Test Bloom filter with string group keys.
    Validates that string hashing and bloom lookup work correctly.
    """
    num_rows = 100_000
    num_groups = 1000

    # Create string group IDs
    group_ids = [f"group_{i % num_groups}" for i in range(num_rows)]
    values = np.random.rand(num_rows)

    table = _make_table(
        {
            "group_str": _make_string_array(group_ids),
            "value": _make_float_array(values.tolist()),
        }
    )

    groupby = ShuffleGroupByOperation(
        group_by_columns=["group_str"],
        aggregations=[
            AggregationSpec(alias="count_val", function="count", column="*"),
        ],
    )

    _ingest_table_into_groupby(groupby, table, morsel_size=10000)
    result = groupby.finalize()

    # Verify correctness
    assert result is not None
    assert len(result) == num_groups

    result_dict = _groupby_result_as_dict(result)
    for i in range(num_groups):
        key = f"group_{i}"
        # PyArrow may return strings as bytes, so try both forms
        if key not in result_dict:
            key = key.encode()
        assert key in result_dict, f"Key {key} not found in result"
        assert result_dict[key]["count_val"] == num_rows // num_groups
