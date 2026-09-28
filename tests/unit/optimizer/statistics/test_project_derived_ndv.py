# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A Project must carry NDV across a distinctness-preserving CAST.

``Project`` used to be a pure pass-through in the statistics refresh, so a
COMPUTED join key arrived at the join with no statistics at all. The join's key
classes (``equi_key_classes``, src/cpp/planner/statistics_refresh.hpp) then fell
through to their domain-size stand-in -- the smaller side's PRE-filter row
count -- and used that as the divisor in ``|L| x |R| / tdom``.

Measured on the live catalog: ``home.network.netflow JOIN home.network.dns ON
CAST(src_addr AS VARCHAR) = client`` divided by 278,985 (the dns table's row count)
for a key with ~5,000 distinct values, estimating 462,275 rows for a join that emits
2,295,861,762. ``src_addr``'s measured NDV of 10,087 was sitting in the scan
statistics one node below, unread.

The cast is injective -- distinct UINT32s render as distinct dotted quads -- so the
derived column's NDV *is* the source's, and stays MEASURED. Casts that can collapse
two values onto one must carry nothing rather than a bound: column statistics have
no provenance field, so ``equi_key_classes`` would read a bound as a counted value.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

# Importing opteryx.planner.optimizer (the package) first resolves the
# pre-existing import cycle a compiled planner module hits when imported first.
import opteryx.planner.optimizer  # noqa: F401
from opteryx.compiled.planner.plan_graph import EdgeRole
from opteryx.compiled.planner.statistics import StatisticsInput
from opteryx.compiled.structures.expressions import Cast
from opteryx.compiled.structures.expressions import Literal
from opteryx.compiled.structures.expressions import LogicalColumn
from opteryx.compiled.structures.plan_steps import JoinStep
from opteryx.compiled.structures.plan_steps import ProjectStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.expression import NodeType
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.plan_context import PlanContext
from opteryx.types import logical_type as types

# The physical types the fixtures name, as the column types the refresh reads
# through each expression's bound column.
_TYPES = {
    "INT8": types.INT8,
    "INT16": types.INT16,
    "INT32": types.INT32,
    "INT64": types.INT64,
    "UINT8": types.UINT8,
    "UINT32": types.UINT32,
    "UINT64": types.UINT64,
    "FLOAT32": types.FLOAT32,
    "FLOAT64": types.FLOAT64,
    "VARCHAR": types.VARCHAR,
    "TIMESTAMP": types.TIMESTAMP(),
    "DECIMAL128": types.DECIMAL(38, 2),
}


class _Query:
    """One query: its context and plan. The source and derived columns are
    minted per test (each test's types differ) in its ColumnTable."""

    def __init__(self):
        self.plan_context = PlanContext()
        self.plan = LogicalPlan(self.plan_context)
        self.source = None
        self.derived = None

    def column(self, relation, name, physical_name):
        return self.plan_context.columns.relation_column(relation, name, column_type=_TYPES[physical_name])


def _cast_column(query, source_physical, target_physical, target_spelling=None, fmt=None):
    """CAST(src_addr AS <target>), bound to a derived column typed <target>."""
    arena = query.plan_context.expressions
    query.source = query.column("netflow", "src_addr", source_physical)
    query.derived = query.column("$project", "derived", target_physical)
    source = LogicalColumn(node_type=NodeType.IDENTIFIER, source_column="src_addr", arena=arena)
    source.schema_column = query.source
    return Cast(
        value=target_spelling if target_spelling is not None else target_physical,
        left=source,
        format=None if fmt is None else Literal(value=fmt, type=types.VARCHAR, arena=arena),
        schema_column=query.derived,
        arena=arena,
    )


def _child_stats(query, distinct_count=10_087, null_fraction=0.25, metric=False):
    rows = {"row_count_metric": 1_486_781} if metric else {"row_count_estimate": 1_486_781}
    return StatisticsInput(
        query.plan_context.columns,
        base_row_count=4_048_894,
        column_stats={
            query.source.identity: {"distinct_count": distinct_count, "null_fraction": null_fraction}
        },
        **rows,
    )


class _Stats:
    """The Project node's statistics, read from the store."""

    def __init__(self, query, nid):
        self.store = query.plan_context.statistics
        self.nid = nid
        self.row_count = self.store.row_count(nid)
        self.row_count_is_metric = self.store.row_count_metric(nid) is not None
        self.domain_row_count = self.store.domain_row_count(nid)

    def has_column(self, column):
        return self.store.has_column(self.nid, column.identity)

    def distinct_count(self, column):
        return self.store.distinct_count(self.nid, column.identity)

    def null_fraction(self, column):
        return self.store.null_fraction(self.nid, column.identity)


def _run(query, cast_column, child=None):
    """The Project node's native propagator run alone over a seeded child."""
    child = child if child is not None else _child_stats(query)
    child_nid = query.plan.add_node(ScanStep())
    project_nid = query.plan.add_node(ProjectStep(columns=[cast_column]))
    query.plan.add_edge(child_nid, project_nid)
    store = query.plan_context.statistics
    store.seed(child_nid, child)
    store.compute(query.plan, project_nid)
    return _Stats(query, project_nid)


def test_integer_to_varchar_cast_carries_the_source_ndv():
    """The reported defect: CAST(src_addr AS VARCHAR) is injective."""
    query = _Query()
    stats = _run(query, _cast_column(query, "UINT32", "VARCHAR"))
    assert stats.distinct_count(query.derived) == 10_087
    assert stats.null_fraction(query.derived) == 0.25


def test_the_source_column_still_passes_through():
    query = _Query()
    stats = _run(query, _cast_column(query, "UINT32", "VARCHAR"))
    assert stats.distinct_count(query.source) == 10_087


def test_row_count_provenance_and_domain_size_are_untouched():
    """A projection changes no row counts. Rebuilding without base_row_count
    would shrink the very stand-in this function exists to stop being reached."""
    query = _Query()
    stats = _run(query, _cast_column(query, "UINT32", "VARCHAR"))
    assert stats.row_count == 1_486_781
    assert not stats.row_count_is_metric
    assert stats.domain_row_count == 4_048_894

    query = _Query()
    cast = _cast_column(query, "UINT32", "VARCHAR")
    metric = _run(query, cast, child=_child_stats(query, metric=True))
    assert metric.row_count_is_metric
    assert metric.domain_row_count == 4_048_894


def test_derived_column_takes_no_range_or_histogram():
    """'10.0.0.9' and '10.0.0.10' sort the opposite way round to the integers
    behind them, so the source's ordering statistics do not describe the cast."""
    query = _Query()
    stats = _run(query, _cast_column(query, "UINT32", "VARCHAR"))
    assert stats.has_column(query.derived)
    # neither bound is known: the store reports no range at all
    assert stats.store.value_range(stats.nid, query.derived.identity) is None
    assert stats.store.has_histogram(stats.nid, query.derived.identity) is False
    assert stats.store.column_total_bytes(stats.nid, query.derived.identity) is None


@pytest.mark.parametrize(
    "source_physical, target_physical",
    [
        ("INT32", "INT64"),     # value-preserving widening
        ("UINT32", "UINT64"),
        ("UINT8", "INT16"),
        ("INT64", "VARCHAR"),   # integer rendering
        ("INT8", "VARCHAR"),
    ],
)
def test_distinctness_preserving_casts_carry_ndv(source_physical, target_physical):
    query = _Query()
    stats = _run(query, _cast_column(query, source_physical, target_physical))
    assert stats.distinct_count(query.derived) == 10_087


@pytest.mark.parametrize(
    "source_physical, target_physical, why",
    [
        ("INT64", "INT32", "narrowing wraps distinct values onto one"),
        ("INT64", "FLOAT64", "2^53 integers collide in a double"),
        ("FLOAT64", "VARCHAR", "0.0 and -0.0 are one value rendered two ways"),
        ("FLOAT32", "VARCHAR", "same, plus the NaN spellings"),
        ("VARCHAR", "INT64", "a parse maps every unparseable input onto one outcome"),
        ("VARCHAR", "VARCHAR", "identity on a string is not in either family"),
        ("TIMESTAMP", "VARCHAR", "sub-unit truncation collapses instants"),
        ("DECIMAL128", "VARCHAR", "scale normalisation collapses values"),
    ],
)
def test_collapsing_casts_carry_nothing(source_physical, target_physical, why):
    """Not even as a bound. A function can only REDUCE distinct values, so the
    source NDV is an upper bound -- and a bound written into `distinct_count`
    is read as MEASURED by `equi_key_classes`, which is the stand-in problem
    one level down."""
    query = _Query()
    stats = _run(query, _cast_column(query, source_physical, target_physical))
    assert not stats.has_column(query.derived), why


def test_try_cast_carries_nothing():
    """TRY_ exists precisely to collapse every failure onto NULL."""
    query = _Query()
    stats = _run(query, _cast_column(query, "UINT32", "VARCHAR", target_spelling="TRY_VARCHAR"))
    assert not stats.has_column(query.derived)


def test_format_cast_carries_nothing():
    """A FORMAT pattern can render two distinct values identically."""
    query = _Query()
    stats = _run(query, _cast_column(query, "UINT32", "VARCHAR", fmt="%Y"))
    assert not stats.has_column(query.derived)


def test_source_without_a_distinct_count_invents_nothing():
    query = _Query()
    cast = _cast_column(query, "UINT32", "VARCHAR")
    stats = _run(query, cast, child=_child_stats(query, distinct_count=None))
    assert not stats.has_column(query.derived)


def test_the_join_divisor_stops_being_the_relation_size():
    """End-to-end through the join's key classes: the whole point of the fix.

    Without the derived NDV the left side reports nothing, tdom falls back to
    min(domain_row_count) = 283,839 and the estimate is ~5,000x low.
    """
    query = _Query()
    left = _run(query, _cast_column(query, "UINT32", "VARCHAR"))
    right_key = query.column("dns", "client", "VARCHAR")

    join = JoinStep()
    join.type = "inner"
    join.left_columns = [query.derived.identity]
    join.right_columns = [right_key.identity]
    right_nid = query.plan.add_node(ScanStep())
    join_nid = query.plan.add_node(join)
    query.plan.add_edge(left.nid, join_nid, EdgeRole.LEFT)
    query.plan.add_edge(right_nid, join_nid, EdgeRole.RIGHT)
    store = query.plan_context.statistics
    store.seed(
        right_nid,
        StatisticsInput(
            query.plan_context.columns,
            row_count_estimate=86_743,
            base_row_count=283_839,
            column_stats={right_key.identity: {"distinct_count": 55, "null_fraction": 0.0}},
        ),
    )
    store.compute(query.plan, join_nid)

    def inner_estimate(divisor):
        # the native inner estimate for one key class: the left side's rows
        # discounted by its key's 0.25 null fraction, the right's by none
        return int(1_486_781 * (1.0 - 0.25) * 86_743 * (1.0 / divisor) * 1.0)

    # The divisor `key_selectivity` applies is max(left.ndv, right.ndv). The key
    # classes' KeyStats (each side's own NDV, its provenance) are internal to the
    # native join propagator, so the divisor is observed through the estimate it
    # produces.
    assert store.row_count(join_nid) == inner_estimate(10_087), (
        "tdom must be max(10087, 55), not the dns row count"
    )
    # The domain-size stand-in (min domain rows = 283,839) would divide ~28x
    # harder: the estimate must be the measured-NDV one, above it.
    assert store.row_count(join_nid) > inner_estimate(283_839)
