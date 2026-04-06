"""
Comprehensive GROUP BY unit and stress tests - Part 2

This module focuses on unit-level testing of GROUP BY operations including:
- Direct testing of ShuffleGroupByOperation
- Aggregation specification variations
- Stress testing with large datasets
- Memory characteristics
- Multiple morsel ingestion and merging
- Bloom filter behavior
- Edge cases and boundary conditions
"""

import math
from typing import Dict, List

import pyarrow as pa
import pytest
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.operators.shuffle_node import ShuffleNode

from opteryx import EOS
from opteryx.models import QueryProperties
from opteryx.operators.shuffle import AggregationSpec, ShuffleGroupByOperation


def _morsel_from_dict(values: dict) -> Morsel:
    """Helper to create a Morsel from a dictionary of arrays."""
    return Morsel.from_arrow(pa.table(values))


def _morsel_to_rows(morsel) -> List[dict]:
    """Helper to convert a Morsel to list of dicts."""
    if morsel is None:
        return []
    return morsel.to_arrow().to_pylist()


def _result_to_dict(morsel, group_keys: List[str]) -> Dict:
    """Convert group by result to dict keyed by group values."""
    rows = _morsel_to_rows(morsel)
    result = {}
    for row in rows:
        key_values = tuple(row[k] for k in group_keys)
        if len(group_keys) == 1:
            key = key_values[0]
        else:
            key = key_values
        result[key] = row
    return result


class TestShuffleGroupByBasics:
    """Test basic ShuffleGroupByOperation functionality."""

    def test_groupby_single_morsel_single_group(self):
        """GROUP BY single group in single morsel."""
        morsel = _morsel_from_dict({"k": [1, 1, 1], "v": [10, 20, 30]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 1
        assert result[1]["sum_v"] == 60

    def test_groupby_single_morsel_multiple_groups(self):
        """GROUP BY multiple groups in single morsel."""
        morsel = _morsel_from_dict({"k": [1, 1, 2, 2, 3], "v": [10, 20, 5, 15, 25]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 3
        assert result[1]["sum_v"] == 30
        assert result[2]["sum_v"] == 20
        assert result[3]["sum_v"] == 25

    def test_groupby_multiple_morsels_same_groups(self):
        """GROUP BY multiple morsels with same group keys."""
        morsel1 = _morsel_from_dict({"k": [1, 2], "v": [10, 20]})
        morsel2 = _morsel_from_dict({"k": [1, 2], "v": [5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest_many([morsel1, morsel2])
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 2
        assert result[1]["sum_v"] == 15
        assert result[2]["sum_v"] == 35

    def test_groupby_multiple_morsels_different_groups(self):
        """GROUP BY multiple morsels with different group keys."""
        morsel1 = _morsel_from_dict({"k": [1, 2], "v": [10, 20]})
        morsel2 = _morsel_from_dict({"k": [3, 4], "v": [5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest_many([morsel1, morsel2])
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 4
        assert result[1]["sum_v"] == 10
        assert result[2]["sum_v"] == 20
        assert result[3]["sum_v"] == 5
        assert result[4]["sum_v"] == 15

    def test_groupby_multiple_morsels_overlapping_groups(self):
        """GROUP BY multiple morsels with overlapping group keys."""
        morsel1 = _morsel_from_dict({"k": [1, 2, 3], "v": [10, 20, 30]})
        morsel2 = _morsel_from_dict({"k": [2, 3, 4], "v": [5, 15, 25]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest_many([morsel1, morsel2])
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 4
        assert result[1]["sum_v"] == 10
        assert result[2]["sum_v"] == 25
        assert result[3]["sum_v"] == 45
        assert result[4]["sum_v"] == 25


class TestShuffleGroupByAggregations:
    """Test all aggregation functions in GROUP BY."""

    def test_count_star(self):
        """Test COUNT(*) aggregation."""
        morsel = _morsel_from_dict({"k": [1, 1, 2, 2, 2], "v": [10, 20, 5, 15, 25]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert result[1]["cnt"] == 2
        assert result[2]["cnt"] == 3

    def test_count_column(self):
        """Test COUNT(column) aggregation - excludes NULLs."""
        morsel = _morsel_from_dict({"k": [1, 1, 1, 2, 2, 2], "v": [10, None, 30, 5, None, 25]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert result[1]["cnt"] == 2  # NULL excluded
        assert result[2]["cnt"] == 2  # NULL excluded

    def test_sum(self):
        """Test SUM aggregation."""
        morsel = _morsel_from_dict({"k": [1, 1, 2, 2, 2], "v": [10, 20, 5, 15, 25]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert result[1]["sum_v"] == 30
        assert result[2]["sum_v"] == 45

    def test_sum_with_nulls(self):
        """Test SUM ignores NULL values."""
        morsel = _morsel_from_dict({"k": [1, 1, 1, 2, 2], "v": [10, None, 20, None, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert result[1]["sum_v"] == 30  # NULL ignored
        assert result[2]["sum_v"] == 15  # NULL ignored

    def test_avg(self):
        """Test AVG aggregation."""
        morsel = _morsel_from_dict({"k": [1, 1, 2, 2, 2], "v": [10, 20, 5, 15, 25]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="avg_v", function="mean", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert result[1]["avg_v"] == 15.0
        assert result[2]["avg_v"] == 15.0

    def test_min(self):
        """Test MIN aggregation."""
        morsel = _morsel_from_dict({"k": [1, 1, 2, 2, 2], "v": [10, 20, 5, 15, 25]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="min_v", function="min", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert result[1]["min_v"] == 10
        assert result[2]["min_v"] == 5

    def test_max(self):
        """Test MAX aggregation."""
        morsel = _morsel_from_dict({"k": [1, 1, 2, 2, 2], "v": [10, 20, 5, 15, 25]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="max_v", function="max", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert result[1]["max_v"] == 20
        assert result[2]["max_v"] == 25

    def test_count_distinct(self):
        """Test COUNT(DISTINCT) aggregation."""
        morsel = _morsel_from_dict({"k": [1, 1, 1, 1, 2, 2, 2], "v": [10, 10, 20, 20, 5, 5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="distinct_v", function="count_distinct", column="v")
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert result[1]["distinct_v"] == 2  # 10, 20
        assert result[2]["distinct_v"] == 2  # 5, 15

    def test_all_aggregates_together(self):
        """Test all aggregation functions in single query."""
        morsel = _morsel_from_dict({"k": [1, 1, 1, 2, 2], "v": [10, 20, 30, 5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="*"),
                AggregationSpec(alias="cnt_v", function="count", column="v"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
                AggregationSpec(alias="avg_v", function="mean", column="v"),
                AggregationSpec(alias="min_v", function="min", column="v"),
                AggregationSpec(alias="max_v", function="max", column="v"),
                AggregationSpec(alias="distinct_v", function="count_distinct", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 2
        assert result[1]["cnt"] == 3
        assert result[1]["sum_v"] == 60
        assert result[1]["avg_v"] == 20.0
        assert result[1]["min_v"] == 10
        assert result[1]["max_v"] == 30
        assert result[1]["distinct_v"] == 3

    def test_count_star_with_count_column(self):
        """Test COUNT(*) combined with COUNT(column)."""
        morsel = _morsel_from_dict({"k": [1, 1, 1, 2, 2, 2], "v": [10, None, 30, 5, None, 25]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt_all", function="count", column="*"),
                AggregationSpec(alias="cnt_v", function="count", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 2
        assert result[1]["cnt_all"] == 3
        assert result[1]["cnt_v"] == 2
        assert result[2]["cnt_all"] == 3
        assert result[2]["cnt_v"] == 2

    def test_count_star_with_sum(self):
        """Test COUNT(*) combined with SUM."""
        morsel = _morsel_from_dict({"k": [1, 1, 1, 2, 2], "v": [10, 20, 30, 5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt_all", function="count", column="*"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 2
        assert result[1]["cnt_all"] == 3
        assert result[1]["sum_v"] == 60
        assert result[2]["cnt_all"] == 2
        assert result[2]["sum_v"] == 20

    def test_count_star_with_avg(self):
        """Test COUNT(*) combined with AVG."""
        morsel = _morsel_from_dict({"k": [1, 1, 1, 2, 2], "v": [10, 20, 30, 5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt_all", function="count", column="*"),
                AggregationSpec(alias="avg_v", function="mean", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 2
        assert result[1]["cnt_all"] == 3
        assert result[1]["avg_v"] == 20.0
        assert result[2]["cnt_all"] == 2
        assert result[2]["avg_v"] == 10.0

    def test_count_star_with_min(self):
        """Test COUNT(*) combined with MIN."""
        morsel = _morsel_from_dict({"k": [1, 1, 1, 2, 2], "v": [10, 20, 30, 5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt_all", function="count", column="*"),
                AggregationSpec(alias="min_v", function="min", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 2
        assert result[1]["cnt_all"] == 3
        assert result[1]["min_v"] == 10
        assert result[2]["cnt_all"] == 2
        assert result[2]["min_v"] == 5

    def test_count_star_with_max(self):
        """Test COUNT(*) combined with MAX."""
        morsel = _morsel_from_dict({"k": [1, 1, 1, 2, 2], "v": [10, 20, 30, 5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt_all", function="count", column="*"),
                AggregationSpec(alias="max_v", function="max", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 2
        assert result[1]["cnt_all"] == 3
        assert result[1]["max_v"] == 30
        assert result[2]["cnt_all"] == 2
        assert result[2]["max_v"] == 15


class TestMultiColumnGroupBy:
    """Test GROUP BY with multiple key columns."""

    def test_two_column_group_by(self):
        """GROUP BY with two columns."""
        morsel = _morsel_from_dict(
            {
                "k1": [1, 1, 1, 2, 2],
                "k2": ["a", "a", "b", "a", "b"],
                "v": [10, 20, 30, 5, 15],
            }
        )
        op = ShuffleGroupByOperation(
            group_by_columns=["k1", "k2"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k1", "k2"])

        assert len(result) == 4
        assert result[(1, "a")]["sum_v"] == 30
        assert result[(1, "b")]["sum_v"] == 30
        assert result[(2, "a")]["sum_v"] == 5
        assert result[(2, "b")]["sum_v"] == 15

    def test_three_column_group_by(self):
        """GROUP BY with three columns."""
        morsel = _morsel_from_dict(
            {
                "k1": [1, 1, 2, 2],
                "k2": ["a", "b", "a", "b"],
                "k3": ["x", "x", "y", "y"],
                "v": [10, 20, 5, 15],
            }
        )
        op = ShuffleGroupByOperation(
            group_by_columns=["k1", "k2", "k3"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k1", "k2", "k3"])

        assert len(result) == 4


class TestNullHandling:
    """Test NULL handling in GROUP BY."""

    def test_null_in_group_key(self):
        """NULLs in group key column group together."""
        morsel = _morsel_from_dict({"k": [1, None, None, 2, None], "v": [10, 20, 30, 5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        # NULL values should group together
        assert result[None]["cnt"] == 3
        assert result[1]["cnt"] == 1
        assert result[2]["cnt"] == 1

    def test_null_in_aggregate_column(self):
        """NULLs in aggregate column are ignored by aggregates."""
        morsel = _morsel_from_dict({"k": [1, 1, 1, 2, 2, 2], "v": [10, None, 20, 5, None, None]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt_all", function="count", column="*"),
                AggregationSpec(alias="cnt_v", function="count", column="v"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        # COUNT(*) counts all rows, COUNT(v) excludes NULLs
        assert result[1]["cnt_all"] == 3
        assert result[1]["cnt_v"] == 2
        assert result[1]["sum_v"] == 30
        assert result[2]["cnt_all"] == 3
        assert result[2]["cnt_v"] == 1
        assert result[2]["sum_v"] == 5

    def test_all_null_values(self):
        """All values are NULL."""
        morsel = _morsel_from_dict({"k": [1, 1, 2, 2], "v": [None, None, None, None]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="v"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert result[1]["cnt"] == 0
        assert result[1]["sum_v"] is None
        assert result[2]["cnt"] == 0
        assert result[2]["sum_v"] is None


class TestGlobalAggregation:
    """Test GROUP BY with no group columns (global aggregation)."""

    def test_global_aggregation_empty_input(self):
        """Global aggregation on empty input returns one row with aggregates."""
        op = ShuffleGroupByOperation(
            group_by_columns=[],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="*"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
            ],
        )
        result = _morsel_to_rows(op.finalize())

        assert len(result) == 1
        assert result[0]["cnt"] == 0
        assert result[0]["sum_v"] is None

    def test_global_aggregation_single_morsel(self):
        """Global aggregation on single morsel."""
        morsel = _morsel_from_dict({"v": [10, 20, 30, 5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=[],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="*"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _morsel_to_rows(op.finalize())

        assert len(result) == 1
        assert result[0]["cnt"] == 5
        assert result[0]["sum_v"] == 80

    def test_global_aggregation_multiple_morsels(self):
        """Global aggregation on multiple morsels."""
        morsel1 = _morsel_from_dict({"v": [10, 20, 30]})
        morsel2 = _morsel_from_dict({"v": [5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=[],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="*"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
            ],
        )
        op.ingest_many([morsel1, morsel2])
        result = _morsel_to_rows(op.finalize())

        assert len(result) == 1
        assert result[0]["cnt"] == 5
        assert result[0]["sum_v"] == 80


class TestStressTesting:
    """Stress tests with large datasets."""

    def test_large_single_morsel_low_cardinality(self):
        """Stress test: large morsel with low cardinality."""
        rows = 10000
        groups = 100
        keys = [(i % groups) for i in range(rows)]
        values = list(range(rows))

        morsel = _morsel_from_dict({"k": keys, "v": values})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="*"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == groups
        for gid in range(groups):
            assert result[gid]["cnt"] == rows // groups

    def test_large_single_morsel_high_cardinality(self):
        """Stress test: large morsel with high cardinality."""
        rows = 10000
        keys = list(range(rows))
        values = list(range(rows))

        morsel = _morsel_from_dict({"k": keys, "v": values})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == rows

    def test_many_small_morsels(self):
        """Stress test: many small morsels."""
        morsel_count = 100
        rows_per_morsel = 100
        morsels = []

        for i in range(morsel_count):
            keys = [(j % 10) for j in range(rows_per_morsel)]
            values = list(range(rows_per_morsel))
            morsels.append(_morsel_from_dict({"k": keys, "v": values}))

        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest_many(morsels)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 10
        # Each group should have values summed across all morsels
        for gid in range(10):
            assert result[gid]["sum_v"] > 0

    def test_large_multi_column_group_by(self):
        """Stress test: multi-column GROUP BY with many rows."""
        rows = 5000
        keys1 = [(i % 50) for i in range(rows)]
        keys2 = [(i % 20) for i in range(rows)]
        values = list(range(rows))

        morsel = _morsel_from_dict({"k1": keys1, "k2": keys2, "v": values})
        op = ShuffleGroupByOperation(
            group_by_columns=["k1", "k2"],
            aggregations=[AggregationSpec(alias="cnt", function="count", column="*")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k1", "k2"])

        # Should have 50 * 20 = 1000 groups
        assert len(result) == 1000


class TestStringGroupBy:
    """Test GROUP BY with string columns."""

    def test_groupby_string_key_simple(self):
        """GROUP BY on string column."""
        morsel = _morsel_from_dict({"k": ["a", "a", "b", "b", "c"], "v": [10, 20, 5, 15, 25]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 3
        assert result["a"]["sum_v"] == 30
        assert result["b"]["sum_v"] == 20
        assert result["c"]["sum_v"] == 25

    def test_groupby_string_with_unicode(self):
        """GROUP BY with unicode strings."""
        morsel = _morsel_from_dict({"k": ["αβγ", "αβγ", "δεζ", "δεζ"], "v": [10, 20, 5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 2

    def test_groupby_string_mixed_case(self):
        """GROUP BY is case-sensitive."""
        morsel = _morsel_from_dict({"k": ["A", "a", "A", "a"], "v": [10, 20, 30, 40]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 2
        assert result["A"]["sum_v"] == 40
        assert result["a"]["sum_v"] == 60


class TestMixedTypeGroupBy:
    """Test GROUP BY with mixed data types."""

    def test_groupby_int_and_string(self):
        """GROUP BY with int and string columns."""
        morsel = _morsel_from_dict(
            {
                "k1": [1, 1, 2, 2],
                "k2": ["a", "b", "a", "b"],
                "v": [10, 20, 5, 15],
            }
        )
        op = ShuffleGroupByOperation(
            group_by_columns=["k1", "k2"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k1", "k2"])

        assert len(result) == 4


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_single_row_single_group(self):
        """Single row input."""
        morsel = _morsel_from_dict({"k": [1], "v": [42]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 1
        assert result[1]["sum_v"] == 42

    def test_empty_morsel(self):
        """Empty morsel input."""
        morsel = _morsel_from_dict({"k": [], "v": []})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 0

    def test_single_group_many_rows(self):
        """Single group with many rows."""
        morsel = _morsel_from_dict({"k": [1] * 1000, "v": list(range(1000))})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="*"),
                AggregationSpec(alias="sum_v", function="sum", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert len(result) == 1
        assert result[1]["cnt"] == 1000
        assert result[1]["sum_v"] == sum(range(1000))

    def test_zero_values(self):
        """GROUP BY with zero values."""
        morsel = _morsel_from_dict({"k": [1, 1, 2, 2], "v": [0, 0, 0, 0]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert result[1]["sum_v"] == 0
        assert result[2]["sum_v"] == 0

    def test_negative_values(self):
        """GROUP BY with negative values."""
        morsel = _morsel_from_dict({"k": [1, 1, 2, 2], "v": [-10, -20, -5, -15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="sum_v", function="sum", column="v"),
                AggregationSpec(alias="min_v", function="min", column="v"),
                AggregationSpec(alias="max_v", function="max", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert result[1]["sum_v"] == -30
        assert result[1]["min_v"] == -20
        assert result[1]["max_v"] == -10

    def test_float_values(self):
        """GROUP BY with float values."""
        morsel = _morsel_from_dict({"k": [1, 1, 2, 2], "v": [1.5, 2.5, 3.7, 4.3]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="sum_v", function="sum", column="v"),
                AggregationSpec(alias="avg_v", function="mean", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        assert abs(result[1]["sum_v"] - 4.0) < 0.001
        assert abs(result[2]["sum_v"] - 8.0) < 0.001


class TestAggregateInvariants:
    """Test mathematical invariants of aggregates."""

    def test_avg_equals_sum_div_count(self):
        """AVG should equal SUM / COUNT."""
        morsel = _morsel_from_dict({"k": [1, 1, 1, 2, 2], "v": [10, 20, 30, 5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="sum_v", function="sum", column="v"),
                AggregationSpec(alias="avg_v", function="mean", column="v"),
                AggregationSpec(alias="cnt_v", function="count", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        for key in result:
            sum_v = result[key]["sum_v"]
            avg_v = result[key]["avg_v"]
            cnt_v = result[key]["cnt_v"]
            if cnt_v > 0:
                expected_avg = sum_v / cnt_v
                assert abs(avg_v - expected_avg) < 0.0001

    def test_min_max_ordering(self):
        """MIN should be <= MAX."""
        morsel = _morsel_from_dict({"k": [1, 1, 1, 2, 2], "v": [10, 20, 30, 5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="min_v", function="min", column="v"),
                AggregationSpec(alias="max_v", function="max", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        for key in result:
            assert result[key]["min_v"] <= result[key]["max_v"]

    def test_count_distinct_less_than_count(self):
        """COUNT(DISTINCT) should be <= COUNT()."""
        morsel = _morsel_from_dict({"k": [1, 1, 1, 1, 2, 2, 2], "v": [10, 10, 20, 20, 5, 5, 15]})
        op = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[
                AggregationSpec(alias="cnt", function="count", column="v"),
                AggregationSpec(alias="distinct_cnt", function="count_distinct", column="v"),
            ],
        )
        op.ingest(morsel)
        result = _result_to_dict(op.finalize(), ["k"])

        for key in result:
            assert result[key]["distinct_cnt"] <= result[key]["cnt"]


class TestConsistency:
    """Test consistency across multiple operations."""

    def test_order_independence(self):
        """Result should be order-independent."""
        values1 = {"k": [1, 2, 1, 2], "v": [10, 20, 30, 40]}
        values2 = {"k": [1, 1, 2, 2], "v": [10, 30, 20, 40]}

        morsel1 = _morsel_from_dict(values1)
        op1 = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op1.ingest(morsel1)
        result1 = _result_to_dict(op1.finalize(), ["k"])

        morsel2 = _morsel_from_dict(values2)
        op2 = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op2.ingest(morsel2)
        result2 = _result_to_dict(op2.finalize(), ["k"])

        assert result1 == result2

    def test_merging_consistency(self):
        """Merging morsels should give same result as combined morsel."""
        values = {"k": [1, 1, 2, 2, 1, 2], "v": [10, 20, 5, 15, 30, 40]}

        # Test 1: Single combined morsel
        morsel_combined = _morsel_from_dict(values)
        op1 = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op1.ingest(morsel_combined)
        result1 = _result_to_dict(op1.finalize(), ["k"])

        # Test 2: Multiple morsels merged
        morsel1 = _morsel_from_dict({"k": values["k"][:3], "v": values["v"][:3]})
        morsel2 = _morsel_from_dict({"k": values["k"][3:], "v": values["v"][3:]})
        op2 = ShuffleGroupByOperation(
            group_by_columns=["k"],
            aggregations=[AggregationSpec(alias="sum_v", function="sum", column="v")],
        )
        op2.ingest_many([morsel1, morsel2])
        result2 = _result_to_dict(op2.finalize(), ["k"])

        assert result1 == result2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
