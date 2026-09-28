# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The range-derived NDV fallback — the dataless NDV a scan's base statistics
carry when the manifest has no sketch (native: statistics_refresh.hpp
manifest_column_stats -> estimate_range_cardinality).

Un-ANALYZE'd relations (plain parquet — the norm) have no KMV sketches, so
``estimate_cardinality`` returns None and, before this fallback existed, every
group-key NDV downstream was unknown. ``estimate_group_by_cardinality`` then
fabricated ``input_rows // 2`` per unknown key, which saturated its input-row
cap and told the planner a GROUP BY reduces nothing — the direct cause of
TPC-DS Q39's self-join of a grouped CTE being "estimated" at 8.5 billion rows
and refused by the result-size guard.

The fallback estimates from what a manifest DOES always carry: per-file row
counts and min/max bounds. Integer bounds imply at most ``max - min + 1``
distinct values; other numeric columns contribute ``rows // 2``; files merge
by range overlap so two files covering the same span count it once. The
worked examples below are the architect's own (2026-08-20 ruling).
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.compiled.structures.plan_steps import ExitStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
from opteryx.planner.plan_context import PlanContext
from opteryx.types import logical_type as _lt
from opteryx.types.schema import RelationSchema
from tests.manifests import FileSpec
from tests.manifests import build_manifest

# Bound columns are minted by a query's ColumnTable; these tests share one.
_PLAN_CONTEXT = PlanContext()


def _schema(*cols):
    return RelationSchema(
        name="t",
        columns=[
            _PLAN_CONTEXT.columns.relation_column(
                "t",
                name,
                column_type=column_type,
            )
            for name, column_type in cols
        ],
    )


def _distinct_count(manifest, name):
    """The NDV the statistics refresh gives column `name` of a Scan over
    `manifest` (the scan's base statistics, keyed by the column's identity)."""
    plan = LogicalPlan(_PLAN_CONTEXT)
    scan = plan.add_node(ScanStep(relation="t", schema=manifest.schema, manifest=manifest))
    plan.add_edge(scan, plan.add_node(ExitStep()))
    refresh_statistics(plan, _PLAN_CONTEXT)
    identity = manifest.schema.find_column(name).identity
    return _PLAN_CONTEXT.statistics.distinct_count(scan, identity)


def _file(rows, bounds_by_position):
    """A file with `rows` rows and {position: (lo, hi)} bounds."""
    return FileSpec(
        file_path="f",
        file_format="PARQUET",
        record_count=rows,
        file_size_in_bytes=0,
        lower_bounds={pos: lo for pos, (lo, _hi) in bounds_by_position.items()},
        upper_bounds={pos: hi for pos, (_lo, hi) in bounds_by_position.items()},
    )


def test_single_file_integer_span():
    # Architect's example: 1M rows, ints 0..100 -> the range can hold 101
    # distinct values, and NDV can never exceed that.
    manifest = build_manifest(_schema(("k", _lt.INT64)), [_file(1_000_000, {0: (0, 100)})])
    assert _distinct_count(manifest, "k") == 101


def test_integer_span_capped_at_file_rows():
    # 10 rows spanning 0..1000000: at most 10 distinct values can exist.
    manifest = build_manifest(_schema(("k", _lt.INT64)), [_file(10, {0: (0, 1_000_000)})])
    assert _distinct_count(manifest, "k") == 10


def test_single_file_float_is_half_the_rows():
    manifest = build_manifest(
        _schema(("k", _lt.FLOAT64)), [_file(1_000_000, {0: (0.0, 100.0)})]
    )
    assert _distinct_count(manifest, "k") == 500_000


def test_integer_overlap_merge():
    # Architect's example: file 1 spans 0..100; file 2 spans 20..120, so 20%
    # of file 2's range is new territory and only that fraction of its
    # estimate accrues: 101 + 0.2 * 101 = ~121.
    manifest = build_manifest(
        _schema(("k", _lt.INT64)),
        [_file(1_000_000, {0: (0, 100)}), _file(1_000_000, {0: (20, 120)})],
    )
    assert _distinct_count(manifest, "k") == 121


def test_float_overlap_merge():
    # Architect's example: 500,000 + 20% of 500,000 = 600,000.
    manifest = build_manifest(
        _schema(("k", _lt.FLOAT64)),
        [_file(1_000_000, {0: (0.0, 100.0)}), _file(1_000_000, {0: (20.0, 120.0)})],
    )
    assert _distinct_count(manifest, "k") == 600_000


def test_fully_covered_file_adds_nothing():
    # File 2's range sits entirely inside file 1's: same values, counted once.
    manifest = build_manifest(
        _schema(("k", _lt.INT64)),
        [_file(1000, {0: (0, 999)}), _file(1000, {0: (100, 200)})],
    )
    assert _distinct_count(manifest, "k") == 1000


def test_non_numeric_without_footer_ndv_is_unknown():
    # Strings get an estimate ONLY from a real footer distinct_count. A
    # fabricated stand-in (the old total // 2) gave enum-like VARCHARs an NDV
    # of half the relation, drove equality selectivity to ~0, and cost TPC-DS
    # Q85 a 660x slower plan. Unknown stays unknown.
    manifest = build_manifest(
        _schema(("k", _lt.VARCHAR)),
        [_file(1000, {0: (b"aaa", b"zzz")}), _file(1000, {0: (b"aaa", b"mmm")})],
    )
    assert _distinct_count(manifest, "k") is None


def test_missing_bounds_on_one_file_poisons_the_merge():
    # A file resolving no NDV (no footer count, no numeric bounds) would be
    # counted zero times or twice by a partial merge — the column is unknown.
    manifest = build_manifest(
        _schema(("k", _lt.INT64)),
        [_file(1000, {0: (0, 100)}), _file(1000, {})],
    )
    assert _distinct_count(manifest, "k") is None


def _footer_file(rows, ndv, lo, hi):
    """A file whose column 0 carries a footer-style EXACT distinct count and
    bounds - what a parquet footer's Statistics give the estimator (an exact
    per-file distinct count is the footer NDV in manifest_estimates.hpp)."""
    return FileSpec(
        file_path="f",
        file_format="PARQUET",
        record_count=rows,
        file_size_in_bytes=0,
        lower_bounds={0: lo},
        upper_bounds={0: hi},
        distinct_value_counts={0: (ndv, True)},
    )


def test_footer_distinct_count_is_preferred_for_strings():
    # rugo-written files carry a real hash-derived Statistics.distinct_count.
    # Every file spans the same enum domain -> overlapping ranges take the max:
    # exactly the true NDV (cd_marital_status: every row group is D..W, ndv 5).
    manifest = build_manifest(
        _schema(("k", _lt.VARCHAR)),
        [
            _footer_file(1000, ndv=5, lo=b"D", hi=b"W"),
            _footer_file(1000, ndv=5, lo=b"D", hi=b"W"),
        ],
    )
    assert _distinct_count(manifest, "k") == 5


def test_footer_distinct_count_disjoint_string_ranges_sum():
    manifest = build_manifest(
        _schema(("k", _lt.VARCHAR)),
        [
            _footer_file(1000, ndv=100, lo=b"aaa", hi=b"mmm"),
            _footer_file(1000, ndv=200, lo=b"nnn", hi=b"zzz"),
        ],
    )
    assert _distinct_count(manifest, "k") == 300


def test_footer_distinct_count_beats_the_integer_span():
    # A sparse int key: span says 1,000,001 possible values, the footer KNOWS
    # there are 42. The real count wins.
    manifest = build_manifest(
        _schema(("k", _lt.INT64)),
        [_footer_file(1000, ndv=42, lo=0, hi=1_000_000)],
    )
    assert _distinct_count(manifest, "k") == 42


def test_unknown_record_count_returns_none():
    # Unknown is not zero: no denominator, no estimate — the caller must treat
    # the NDV as unknown rather than adopt a fabrication.
    manifest = build_manifest(_schema(("k", _lt.INT64)), [_file(None, {0: (0, 100)})])
    assert _distinct_count(manifest, "k") is None


def test_result_never_exceeds_total_rows():
    manifest = build_manifest(
        _schema(("k", _lt.INT64)),
        [_file(10, {0: (0, 5)}), _file(10, {0: (1_000_000, 2_000_000)})],
    )
    estimate = _distinct_count(manifest, "k")
    assert 1 <= estimate <= 20


@pytest.mark.skipif(
    not os.path.isdir("testdata/tpch_001"),
    reason="testdata/tpch_001 not populated",
)
def test_parquet_scan_statistics_carry_range_derived_ndv():
    """End to end: a plain parquet relation (no ANALYZE, no KMV) now reports
    per-column NDV estimates in Scan.statistics — int keys from their bounds
    span, strings from the total // 2 fallback."""
    import opteryx

    session = opteryx.session()
    sql = "EXPLAIN SELECT n_regionkey FROM testdata.tpch_001.nation GROUP BY n_regionkey"
    rows = []
    for morsel in session.execute_to_morsels(sql):
        rows.extend(zip(*[morsel.column(n).to_pylist() for n in morsel.column_names]))
    agg = next(r for r in rows if b"Aggregate" in r[0])
    # n_regionkey spans 0..4 across 25 rows: the group-by estimate is ~5, not
    # the old NDV-less saturation at the full input row count.
    assert agg[2] <= 6, f"group-by estimate did not use range-derived NDV: {agg}"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
