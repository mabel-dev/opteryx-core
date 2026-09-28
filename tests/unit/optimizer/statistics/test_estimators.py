"""
Comprehensive tests for statistics estimation primitives.

Tests cover:
- Value-range intersection (the join's key narrowing) and width (the range
  interpolation a comparison's selectivity is computed by)
- Column and relation statistics as the native StatisticsStore holds them
- CardinalityEstimator for GROUP BY and JOINs

The statistics are native (src/cpp/planner/stats_store.hpp): statistics are
seeded into a query's StatisticsStore as INPUTS (StatisticsInput) and read back
through its accessors, keyed by node id and column identity.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

# Importing opteryx.planner.optimizer (the package) first resolves the
# pre-existing import cycle a compiled planner module hits when imported first.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.compiled.planner.plan_graph import EdgeRole
from opteryx.compiled.planner.statistics import RANGE_FALLBACK_SELECTIVITY
from opteryx.compiled.planner.statistics import StatisticsInput
from opteryx.compiled.structures.expressions import Comparison
from opteryx.compiled.structures.expressions import Literal
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.compiled.structures.plan_steps import JoinStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.plan_context import PlanContext
from opteryx.types.logical_type import FLOAT64
from opteryx.types.logical_type import INT64


class _Seeded:
    """One node's statistics seeded into a fresh query's store: `columns` maps a
    column name to its statistics fields; each column is minted in the query's
    ColumnTable (statistics are keyed by opaque identity, never by name)."""

    def __init__(self, columns, row_count_estimate=10000, column_type=INT64):
        self.plan_context = PlanContext()
        self.column = {
            name: self.plan_context.columns.relation_column("t", name, column_type=column_type)
            for name in columns
        }
        plan = LogicalPlan(self.plan_context)
        self.nid = plan.add_node(ScanStep())
        self.store = self.plan_context.statistics
        self.store.seed(
            self.nid,
            StatisticsInput(
                self.plan_context.columns,
                row_count_estimate=row_count_estimate,
                column_stats={self.column[name].identity: fields for name, fields in columns.items()},
            ),
        )

    def identity(self, name):
        return self.column[name].identity


def _range_selectivity(value_range, literal, column_type=INT64):
    """The selectivity of `x < literal` against a column whose value range is
    `value_range` (no histogram): (literal - lower) / (upper - lower) - the
    range's WIDTH is the divisor - or the fallback when the width is unknown."""
    seeded = _Seeded({"x": {"value_range": value_range}}, column_type=column_type)
    arena = seeded.plan_context.expressions
    identifier = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="x", arena=arena)
    identifier.schema_column = seeded.column["x"]
    predicate = Comparison(
        value="Lt", left=identifier, right=Literal(value=literal, type=column_type, arena=arena), arena=arena
    )
    return seeded.store.estimate_selectivity(seeded.nid, predicate)


def _intersect(left_range, right_range):
    """The value range an inner equi-join publishes for its key: the two keys'
    ranges intersected (the join's key narrowing, `intersect_join_keys`)."""
    plan_context = PlanContext()
    left_key = plan_context.columns.relation_column("l", "k", column_type=INT64).identity
    right_key = plan_context.columns.relation_column("r", "k", column_type=INT64).identity
    join = JoinStep()
    join.type = "inner"
    join.left_columns = [left_key]
    join.right_columns = [right_key]
    plan = LogicalPlan(plan_context)
    left_nid = plan.add_node(ScanStep())
    right_nid = plan.add_node(ScanStep())
    join_nid = plan.add_node(join)
    plan.add_edge(left_nid, join_nid, EdgeRole.LEFT)
    plan.add_edge(right_nid, join_nid, EdgeRole.RIGHT)
    store = plan_context.statistics
    for nid, key, value_range in ((left_nid, left_key, left_range), (right_nid, right_key, right_range)):
        store.seed(
            nid,
            StatisticsInput(
                plan_context.columns, row_count_estimate=1000, column_stats={key: {"value_range": value_range}}
            ),
        )
    store.compute(plan, join_nid)
    # an inner join narrows BOTH keys to the intersection
    assert store.value_range(join_nid, left_key) == store.value_range(join_nid, right_key)
    return store.value_range(join_nid, left_key)


class TestColumnRange:
    """Tests for a column's value range."""

    def test_range_creation(self):
        """Test creating a range with bounds."""
        seeded = _Seeded({"x": {"value_range": (10, 100)}})
        assert seeded.store.value_range(seeded.nid, seeded.identity("x")) == (10, 100)

    def test_range_open_ended_lower(self):
        """Test range with open lower bound."""
        seeded = _Seeded({"x": {"value_range": (None, 100)}})
        lower, upper = seeded.store.value_range(seeded.nid, seeded.identity("x"))
        assert lower is None
        assert upper == 100

    def test_range_open_ended_upper(self):
        """Test range with open upper bound."""
        seeded = _Seeded({"x": {"value_range": (10, None)}})
        lower, upper = seeded.store.value_range(seeded.nid, seeded.identity("x"))
        assert lower == 10
        assert upper is None

    def test_range_width_calculation(self):
        """Test width calculation for numeric ranges: width 90, so a probe 45
        above the lower bound selects half."""
        assert _range_selectivity((10, 100), 55) == 0.5

    def test_range_width_with_floats(self):
        """Test width calculation with float bounds: width 10.0."""
        assert _range_selectivity((10.5, 20.5), 15.5, column_type=FLOAT64) == 0.5

    def test_range_width_with_no_bounds(self):
        """Test width is unknown when bounds are missing: the estimate falls back."""
        assert _range_selectivity(None, 55) == RANGE_FALLBACK_SELECTIVITY
        assert _range_selectivity((10, None), 55) == RANGE_FALLBACK_SELECTIVITY
        assert _range_selectivity((None, 100), 55) == RANGE_FALLBACK_SELECTIVITY

    def test_range_width_negative(self):
        """Test width with negative numbers: width 90."""
        assert _range_selectivity((-100, -10), -55) == 0.5

    def test_range_width_crossing_zero(self):
        """Test width for range that crosses zero: width 100."""
        assert _range_selectivity((-50, 50), 0) == 0.5

    def test_range_intersection_both_bounded(self):
        """Test intersection of two fully bounded ranges."""
        assert _intersect((10, 100), (50, 150)) == (50, 100)

    def test_range_intersection_no_overlap(self):
        """Test intersection of non-overlapping ranges."""
        # Invalid range (lower > upper)
        assert _intersect((10, 50), (60, 100)) == (60, 50)

    def test_range_intersection_one_open_lower(self):
        """Test intersection when one range has open lower bound."""
        assert _intersect((None, 100), (50, 150)) == (50, 100)

    def test_range_intersection_one_open_upper(self):
        """Test intersection when one range has open upper bound."""
        assert _intersect((10, None), (50, 150)) == (50, 150)

    def test_range_intersection_both_open(self):
        """Test intersection when both ranges are open on same side."""
        assert _intersect((None, 100), (None, 150)) == (None, 100)

    def test_range_intersection_identical(self):
        """Test intersection of identical ranges."""
        assert _intersect((10, 100), (10, 100)) == (10, 100)


class TestColumnStatistics:
    """Tests for a column's statistics."""

    def test_column_statistics_creation(self):
        """Test creating column statistics.

        The column's name and type are the ColumnTable's, not the statistics'
        (the original `column_name == "age"` / `data_type == "int"` assertions
        have no native statistics equivalent)."""
        seeded = _Seeded({"age": {"distinct_count": 100, "value_range": (0, 120)}})
        assert seeded.store.distinct_count(seeded.nid, seeded.identity("age")) == 100
        assert seeded.store.value_range(seeded.nid, seeded.identity("age")) == (0, 120)


class TestRelationStatistics:
    """Tests for a relation's statistics."""

    def test_relation_statistics_creation(self):
        """Test creating relation statistics."""
        seeded = _Seeded({"age": {"distinct_count": 100, "value_range": (0, 120)}, "name": {}})

        assert seeded.store.row_count(seeded.nid) == 10000
        assert seeded.store.has_column(seeded.nid, seeded.identity("age"))
        assert seeded.store.has_column(seeded.nid, seeded.identity("name"))

    def test_relation_statistics_get_column(self):
        """Test retrieving column statistics."""
        seeded = _Seeded({"age": {"distinct_count": 100}})
        assert seeded.store.has_column(seeded.nid, seeded.identity("age"))
        assert seeded.store.distinct_count(seeded.nid, seeded.identity("age")) == 100

    def test_relation_statistics_get_nonexistent_column(self):
        """Test retrieving nonexistent column."""
        seeded = _Seeded({})
        age = seeded.plan_context.columns.relation_column("t", "age").identity
        assert not seeded.store.has_column(seeded.nid, age)
        assert seeded.store.distinct_count(seeded.nid, age) is None

class TestCardinalityFunctions:
    """Tests for the pure cardinality functions in cost_estimation."""

    def test_estimate_after_filter_basic(self):
        from opteryx.planner.cost_estimation import estimate_after_filter
        assert estimate_after_filter(1000, 0.1) == 100
        assert estimate_after_filter(1000, 1.0) == 1000
        assert estimate_after_filter(1000, 0.0) == 1  # floored at 1
        assert estimate_after_filter(0, 0.5) == 1     # floored at 1

    def test_estimate_after_filter_rejects_negative(self):
        from opteryx.planner.cost_estimation import estimate_after_filter
        with pytest.raises(ValueError):
            estimate_after_filter(-1, 0.5)
        with pytest.raises(ValueError):
            estimate_after_filter(100, -0.1)

    def test_estimate_group_by_cardinality_known_ndvs(self):
        from opteryx.planner.cost_estimation import estimate_group_by_cardinality
        # 100 input rows, two group keys with NDV 3 and 4 -> 12, capped by input.
        assert estimate_group_by_cardinality(100, [3, 4]) == 12
        # Cap at input rows when product exceeds it.
        assert estimate_group_by_cardinality(10, [50, 50]) == 10

    def test_estimate_group_by_cardinality_unknown_ndvs(self):
        from opteryx.planner.cost_estimation import estimate_group_by_cardinality
        # A key with unknown NDV makes the estimate the input row count -- the
        # only sound cap (a grouped aggregate can never emit more rows than it
        # consumes). The old input_rows // 2 per missing key was a fabrication.
        assert estimate_group_by_cardinality(100, [None]) == 100
        # One unknown key poisons the product: known NDVs cannot bound the
        # combination count when any key's contribution is unknown.
        assert estimate_group_by_cardinality(100, [3, None]) == 100
        assert estimate_group_by_cardinality(100, []) == 1
        assert estimate_group_by_cardinality(0, [10]) == 1
