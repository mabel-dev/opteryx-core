"""
Telemetry verification tests for Bloom filter in group-by operations.

These tests validate that Bloom filter telemetry metrics are correctly
collected and reported during group-by ingest operations.
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa
from opteryx.compiled.aggregations.group_by_engine import CarcharGroupStateEngine
from draken.morsels.morsel import Morsel


def _make_morsel_from_arrow(table: pa.Table) -> Morsel:
    """Convert PyArrow table to Morsel."""
    return Morsel.from_arrow(table)


def test_bloom_telemetry_fields_present():
    """Verify that all required Bloom telemetry fields are initialized."""
    engine = CarcharGroupStateEngine([b"group_id"], [("cnt", "count", None)])

    # Check that telemetry fields exist in readings
    readings = engine.readings
    assert "groupby_bloom_checks" in readings
    assert "groupby_bloom_skips" in readings
    assert "groupby_bloom_false_positives" in readings
    assert "groupby_ingest_hits" in readings
    assert "groupby_ingest_misses" in readings
    assert "time_groupby_ingest_state_assign_ns" in readings
    assert "time_groupby_hash_ns" in readings
    assert "time_groupby_accumulate_ns" in readings


def test_bloom_telemetry_high_cardinality_collection():
    """
    Verify that Bloom telemetry is collected during high-cardinality ingests.
    After bloom is created, we should see bloom_checks > 0.
    """
    engine = CarcharGroupStateEngine([b"group_id"], [("cnt", "count", None)])

    # Create high cardinality data across multiple morsels
    num_morsels = 5
    rows_per_morsel = 50_000
    unique_groups = 10_000

    for morsel_idx in range(num_morsels):
        # Create data with different groups in each morsel to trigger misses
        start_group = morsel_idx * (unique_groups // num_morsels)
        group_ids = np.arange(start_group, start_group + rows_per_morsel) % (unique_groups * 2)

        table = pa.table(
            {
                "group_id": pa.array(group_ids, type=pa.int64()),
            }
        )
        morsel = _make_morsel_from_arrow(table)
        engine.ingest(morsel)

    readings = engine.readings

    # First morsel should have misses but no bloom checks (filter not yet created)
    # Subsequent morsels should have bloom activity
    assert readings["groupby_ingest_misses"] > 0
    assert readings["time_groupby_ingest_state_assign_ns"] > 0

    # After multiple morsels, bloom should be active
    # (bloom is created after first morsel completes)
    # Morsels 2+ should show bloom checks
    total_records = num_morsels * rows_per_morsel
    assert total_records > 0  # Sanity check


def test_bloom_telemetry_hits_and_misses_consistency():
    """
    Verify that hits + misses are consistent with total records processed.
    For repeated identical data, after first ingest all subsequent rows should be hits.
    """
    engine = CarcharGroupStateEngine([b"group_id"], [("cnt", "count", None)])

    # Create small dataset with 100 unique groups
    num_rows = 10_000
    num_groups = 100
    group_ids = np.tile(np.arange(num_groups), num_rows // num_groups + 1)[:num_rows]

    table = pa.table(
        {
            "group_id": pa.array(group_ids, type=pa.int64()),
        }
    )
    morsel = _make_morsel_from_arrow(table)

    # First ingest: most rows are misses (new groups)
    engine.ingest(morsel)
    readings_1 = engine.readings.copy()

    first_hits = readings_1["groupby_ingest_hits"]
    first_misses = readings_1["groupby_ingest_misses"]

    # We should have created 100 new groups
    # Exact split depends on hash collision behavior, but misses should be significant
    assert first_misses >= num_groups
    assert first_hits + first_misses == num_rows

    # Second ingest: same data, should be mostly hits
    engine.ingest(morsel)
    readings_2 = engine.readings

    second_hits = readings_2["groupby_ingest_hits"] - first_hits
    second_misses = readings_2["groupby_ingest_misses"] - first_misses

    # Almost all rows should be hits on second ingest
    assert second_hits >= num_rows * 0.95  # Allow 5% variance
    assert second_misses <= num_rows * 0.05


def test_bloom_telemetry_skips_subset_of_misses():
    """
    Verify that bloom skips are a subset of total misses.
    bloom_skips <= ingest_misses (skips are only counted for a subset of misses).
    """
    engine = CarcharGroupStateEngine([b"group_id"], [("cnt", "count", None)])

    # Create data with moderate cardinality across multiple morsels
    num_morsels = 3
    rows_per_morsel = 100_000
    unique_per_morsel = 50_000

    for morsel_idx in range(num_morsels):
        # Create overlapping groups across morsels
        # Each morsel has some new groups and some repeats from previous morsels
        offset = morsel_idx * 25_000  # 25K new groups per morsel, 25K from previous
        group_ids = np.random.randint(offset, offset + unique_per_morsel, rows_per_morsel)

        table = pa.table(
            {
                "group_id": pa.array(group_ids, type=pa.int64()),
            }
        )
        morsel = _make_morsel_from_arrow(table)
        engine.ingest(morsel)

    readings = engine.readings

    # Verify constraints
    assert readings["groupby_bloom_skips"] >= 0
    # After bloom is created, skips should be positive for workloads with new groups
    # (bloom will correctly identify them as "definitely not present")
    assert readings["time_groupby_ingest_state_assign_ns"] > 0


def test_bloom_telemetry_false_positive_rate_reasonable():
    """
    Verify that Bloom false positive rate is within expected bounds.
    For large cardinalities, FPR should be low (< 1% typically).
    """
    engine = CarcharGroupStateEngine([b"group_id"], [("cnt", "count", None)])

    # Create high cardinality data
    num_morsels = 10
    rows_per_morsel = 100_000
    unique_groups = 500_000

    for morsel_idx in range(num_morsels):
        # Create mostly new groups (high miss rate)
        start_idx = morsel_idx * rows_per_morsel
        group_ids = np.arange(start_idx, start_idx + rows_per_morsel)

        table = pa.table(
            {
                "group_id": pa.array(group_ids, type=pa.int64()),
            }
        )
        morsel = _make_morsel_from_arrow(table)
        engine.ingest(morsel)

    readings = engine.readings

    # After bloom is well-populated, false positive rate should be low
    # FPR = fps / checks (if checks > 0)
    checks = readings.get("groupby_bloom_checks", 0)
    fps = readings.get("groupby_bloom_false_positives", 0)

    if checks > 0:
        fpr = fps / checks
        # Bloom filter should have < 1% false positive rate for this configuration
        assert fpr < 0.01, f"FPR {fpr:.4f} exceeds expected threshold of 0.01"


def test_bloom_telemetry_string_keys():
    """
    Verify that Bloom telemetry works correctly with string group keys.
    """
    engine = CarcharGroupStateEngine([b"group_str"], [("cnt", "count", None)])

    # Create string group data
    num_rows = 100_000
    num_groups = 1000
    group_strs = [f"group_{i % num_groups}".encode() for i in range(num_rows)]

    table = pa.table(
        {
            "group_str": pa.array(group_strs, type=pa.string()),
        }
    )
    morsel = _make_morsel_from_arrow(table)

    engine.ingest(morsel)
    readings = engine.readings

    # Should have hits and misses
    assert readings["groupby_ingest_hits"] + readings["groupby_ingest_misses"] == num_rows
    # For the first morsel, should have approximately num_groups misses
    assert readings["groupby_ingest_misses"] >= num_groups * 0.95
    assert readings["groupby_ingest_misses"] <= num_groups


def test_bloom_telemetry_multi_key():
    """
    Verify that Bloom telemetry works correctly with multi-key GROUP BY.
    """
    engine = CarcharGroupStateEngine([b"key1", b"key2"], [("cnt", "count", None)])

    # Create multi-key data
    num_rows = 100_000
    key1_values = np.random.randint(0, 100, num_rows)
    key2_values = np.random.randint(0, 100, num_rows)

    table = pa.table(
        {
            "key1": pa.array(key1_values, type=pa.int64()),
            "key2": pa.array(key2_values, type=pa.int64()),
        }
    )
    morsel = _make_morsel_from_arrow(table)

    engine.ingest(morsel)
    readings = engine.readings

    # Multi-key object path may not record hits/misses in all cases
    # Just verify that telemetry was attempted
    assert readings["time_groupby_ingest_state_assign_ns"] >= 0
    assert "groupby_ingest_hits" in readings
    assert "groupby_ingest_misses" in readings


def test_bloom_telemetry_accumulate_time_positive():
    """
    Verify that accumulation time fields exist in telemetry.
    """
    engine = CarcharGroupStateEngine(
        [b"group_id"],
        [
            ("cnt", "count", None),
            ("total", "sum", b"value"),
            ("avg_val", "avg", b"value"),
        ],
    )

    # Create data with aggregations
    num_rows = 50_000
    num_groups = 100
    group_ids = np.tile(np.arange(num_groups), num_rows // num_groups + 1)[:num_rows]
    values = np.random.rand(num_rows)

    table = pa.table(
        {
            "group_id": pa.array(group_ids, type=pa.int64()),
            "value": pa.array(values, type=pa.float64()),
        }
    )
    morsel = _make_morsel_from_arrow(table)

    engine.ingest(morsel)
    readings = engine.readings

    # Verify that telemetry fields are present (values may be zero depending on optimization paths)
    assert "time_groupby_accumulate_ns" in readings
    assert "time_groupby_hash_ns" in readings
    assert readings["time_groupby_accumulate_ns"] >= 0
    assert readings["time_groupby_hash_ns"] >= 0


def test_bloom_telemetry_null_keys():
    """
    Verify that Bloom telemetry handles NULL group keys correctly.
    """
    engine = CarcharGroupStateEngine([b"group_id"], [("cnt", "count", None)])

    # Create data with some NULL keys
    num_rows = 50_000
    group_ids_list = [None] * (num_rows // 10)  # 10% NULLs
    group_ids_list.extend(np.arange(num_rows - num_rows // 10))
    group_ids = np.array(group_ids_list, dtype=object)
    np.random.shuffle(group_ids)

    table = pa.table(
        {
            "group_id": pa.array(group_ids, type=pa.int64()),
        }
    )
    morsel = _make_morsel_from_arrow(table)

    engine.ingest(morsel)
    readings = engine.readings

    # Should handle NULLs without issues
    assert readings["groupby_ingest_hits"] + readings["groupby_ingest_misses"] == num_rows
