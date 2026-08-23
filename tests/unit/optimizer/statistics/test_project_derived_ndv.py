# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A Project must carry NDV across a distinctness-preserving CAST.

``Project`` used to be a pure pass-through in the statistics refresh, so a
COMPUTED join key arrived at the join with no statistics at all. ``_equi_key_classes``
then fell through to its domain-size stand-in -- the smaller side's PRE-filter row
count -- and used that as the divisor in ``|L| x |R| / tdom``.

Measured on the live catalog: ``home.network.netflow JOIN home.network.dns ON
CAST(src_addr AS VARCHAR) = client`` divided by 278,985 (the dns table's row count)
for a key with ~5,000 distinct values, estimating 462,275 rows for a join that emits
2,295,861,762. ``src_addr``'s measured NDV of 10,087 was sitting in the scan
statistics one node below, unread.

The cast is injective -- distinct UINT32s render as distinct dotted quads -- so the
derived column's NDV *is* the source's, and stays MEASURED. Casts that can collapse
two values onto one must carry nothing rather than a bound: ``ColumnStatistics`` has
no provenance field, so ``_equi_key_classes`` would read a bound as a counted value.
"""

import os
import sys
from types import SimpleNamespace

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import pytest

from opteryx.expression import NodeType
from opteryx.planner.optimizer.statistics import ColumnStatistics
from opteryx.planner.optimizer.statistics import RelationStatistics
from opteryx.planner.optimizer.statistics_refresh import _equi_key_classes
from opteryx.planner.optimizer.statistics_refresh import _project_stats
from opteryx.planner.cost_estimation.join_cardinality import NdvProvenance

_SRC = b"tes_src_00000001"
_DERIVED = b"tes_drv_00000002"
_RIGHT_KEY = b"tes_rk__00000003"


def _typed(identity, physical_name, name="c"):
    """A node carrying the schema_column.column_type.physical.name chain the
    statistics refresh reads types through."""
    return SimpleNamespace(
        schema_column=SimpleNamespace(
            identity=identity,
            name=name,
            column_type=SimpleNamespace(physical=SimpleNamespace(name=physical_name)),
        )
    )


def _cast_column(source_physical, target_physical, target_spelling=None, fmt=None):
    node = _typed(_DERIVED, target_physical, name="derived")
    node.node_type = NodeType.CAST
    node.value = target_spelling if target_spelling is not None else target_physical
    node.format = fmt
    source = _typed(_SRC, source_physical, name="src_addr")
    source.node_type = NodeType.IDENTIFIER
    node.left = source
    return node


def _child_stats(distinct_count=10_087, null_fraction=0.25, source_physical="UINT32"):
    return RelationStatistics(
        row_count_estimate=1_486_781,
        base_row_count=4_048_894,
        columns={
            _SRC: ColumnStatistics(
                column_name="src_addr",
                data_type=source_physical,
                distinct_count=distinct_count,
                null_fraction=null_fraction,
            )
        },
    )


def _project(*columns):
    return SimpleNamespace(columns=list(columns))


def _run(cast_column, child=None):
    child = child if child is not None else _child_stats()
    return _project_stats(_project(cast_column), [(child, "")])


def test_integer_to_varchar_cast_carries_the_source_ndv():
    """The reported defect: CAST(src_addr AS VARCHAR) is injective."""
    stats = _run(_cast_column("UINT32", "VARCHAR"))
    derived = stats.columns[_DERIVED]
    assert derived.distinct_count == 10_087
    assert derived.null_fraction == 0.25


def test_the_source_column_still_passes_through():
    stats = _run(_cast_column("UINT32", "VARCHAR"))
    assert stats.columns[_SRC].distinct_count == 10_087


def test_row_count_provenance_and_domain_size_are_untouched():
    """A projection changes no row counts. Rebuilding without base_row_count
    would shrink the very stand-in this function exists to stop being reached."""
    stats = _run(_cast_column("UINT32", "VARCHAR"))
    assert stats.row_count == 1_486_781
    assert not stats.row_count_is_metric
    assert stats.domain_row_count == 4_048_894

    metric_child = RelationStatistics(
        row_count_metric=1_486_781,
        base_row_count=4_048_894,
        columns=_child_stats().columns,
    )
    metric = _run(_cast_column("UINT32", "VARCHAR"), child=metric_child)
    assert metric.row_count_is_metric
    assert metric.domain_row_count == 4_048_894


def test_derived_column_takes_no_range_or_histogram():
    """'10.0.0.9' and '10.0.0.10' sort the opposite way round to the integers
    behind them, so the source's ordering statistics do not describe the cast."""
    stats = _run(_cast_column("UINT32", "VARCHAR"))
    derived = stats.columns[_DERIVED]
    assert derived.value_range.lower_bound is None
    assert derived.value_range.upper_bound is None
    assert derived.histogram is None
    assert derived.total_bytes is None


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
    stats = _run(_cast_column(source_physical, target_physical))
    assert stats.columns[_DERIVED].distinct_count == 10_087


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
    is read as MEASURED by `_equi_key_classes`, which is the stand-in problem
    one level down."""
    stats = _run(_cast_column(source_physical, target_physical))
    assert _DERIVED not in stats.columns, why


def test_try_cast_carries_nothing():
    """TRY_ exists precisely to collapse every failure onto NULL."""
    stats = _run(_cast_column("UINT32", "VARCHAR", target_spelling="TRY_VARCHAR"))
    assert _DERIVED not in stats.columns


def test_format_cast_carries_nothing():
    """A FORMAT pattern can render two distinct values identically."""
    stats = _run(_cast_column("UINT32", "VARCHAR", fmt="%Y"))
    assert _DERIVED not in stats.columns


def test_source_without_a_distinct_count_invents_nothing():
    stats = _run(_cast_column("UINT32", "VARCHAR"), child=_child_stats(distinct_count=None))
    assert _DERIVED not in stats.columns


def test_the_join_divisor_stops_being_the_relation_size():
    """End-to-end through `_equi_key_classes`: the whole point of the fix.

    Without the derived NDV the left side reports nothing, tdom falls back to
    min(domain_row_count) = 283,839 and the estimate is ~5,000x low.
    """
    left = _run(_cast_column("UINT32", "VARCHAR"))
    right = RelationStatistics(
        row_count_estimate=86_743,
        base_row_count=283_839,
        columns={
            _RIGHT_KEY: ColumnStatistics(
                column_name="client", data_type="VARCHAR", distinct_count=55, null_fraction=0.0
            )
        },
    )
    (left_key, right_key), = _equi_key_classes([_DERIVED], [_RIGHT_KEY], left, right)

    assert left_key.ndv == 10_087, "tdom must be max(10087, 55), not the dns row count"
    assert right_key.ndv == 10_087
    assert left_key.ndv_provenance is NdvProvenance.MEASURED
    assert left_key.ndv < 283_839
