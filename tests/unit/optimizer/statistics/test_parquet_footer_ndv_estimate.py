# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A parquet footer's NDV for costing: AggColumnStat::distinct_estimate.

rugo's footer aggregate merges per-row-group distinct counts as a FLOOR -
overlapping row groups take the max - so a key spread across every row group
reported about one row group's row count. On a 1e8-distinct H2O join key that
was 65,536, which flipped j5's build side; on TPC-H SF100 it made Q21 decline
its anti-join exchange. The estimate inverts each row group's count under
uniform sampling for the domain it implies and reports the file's expected
count, bounded by the sum of the row groups' counts and by the floor.

Files are written by rugo's own writer at small row groups, and the NDV is the
one the statistics refresh gives the scan - the same path as a real query.
"""

import os
import random
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import draken.draken_native as dn
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector
from rugo.parquet import write_parquet

from opteryx.compiled.structures.plan_steps import ExitStep
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.optimizer.statistics_refresh import refresh_statistics
from opteryx.planner.plan_context import PlanContext

ROWS_PER_GROUP = 1024
ROW_GROUPS = 32
ROWS = ROWS_PER_GROUP * ROW_GROUPS


def _dataset(tmp_path, values):
    """A one-file parquet dataset of int64 column `k`, ROWS_PER_GROUP-row groups."""
    directory = tmp_path / "ds"
    directory.mkdir()
    morsel = Morsel.from_vectors(["k"], [Vector(dn.vector_int64_from_sequence(values))])
    (directory / "part-000.parquet").write_bytes(
        write_parquet(morsel, max_rows_per_row_group=ROWS_PER_GROUP)
    )
    return str(directory)


def _footer_and_ndv(dataset):
    """(the file's footer statistics for `k`, the scan's NDV for `k`)."""
    from opteryx.connectors import connector_factory

    table = connector_factory(dataset, telemetry=None).table_engine(dataset)
    described, manifest = table.get_dataset_metadata()
    plan_context = PlanContext()
    manifest.schema = plan_context.columns.bind_relation(described, dataset)
    plan = LogicalPlan(plan_context)
    scan = plan.add_node(ScanStep(relation=dataset, schema=manifest.schema, manifest=manifest))
    plan.add_edge(scan, plan.add_node(ExitStep()))
    refresh_statistics(plan, plan_context)
    identity = manifest.schema.find_column("k").identity
    return manifest.native.cell(0, 0)["footer"], plan_context.statistics.distinct_count(scan, identity)


def test_high_cardinality_key_is_not_pinned_at_a_row_group(tmp_path):
    # Every value distinct, in random order: each row group is all-distinct and
    # every range overlaps, so the floor is one row group's count. That floor
    # used to BE the NDV.
    values = list(range(ROWS))
    random.Random(7).shuffle(values)
    footer, ndv = _footer_and_ndv(_dataset(tmp_path, values))
    assert footer["distinct_count"] == ROWS_PER_GROUP, footer
    assert footer["distinct_estimate"] == ROWS, footer
    assert ndv == ROWS


def test_mid_cardinality_key_recovers_its_count(tmp_path):
    # The l_suppkey shape: rows drawn from a domain much larger than a row
    # group, so each group is nearly but not entirely distinct.
    rng = random.Random(11)
    values = [rng.randrange(20_000) for _ in range(ROWS)]
    true_ndv = len(set(values))
    footer, ndv = _footer_and_ndv(_dataset(tmp_path, values))
    assert footer["distinct_count"] < ROWS_PER_GROUP
    assert abs(ndv - true_ndv) <= 0.05 * true_ndv, (ndv, true_ndv)


def test_low_cardinality_key_stays_exact(tmp_path):
    # An enum: every row group holds all ten values; the estimate must not
    # inflate it (the floor IS the count here).
    values = [k % 10 for k in range(ROWS)]
    footer, ndv = _footer_and_ndv(_dataset(tmp_path, values))
    assert footer["distinct_count"] == 10
    assert ndv == 10


def test_sorted_key_keeps_the_exact_disjoint_sum(tmp_path):
    # Sorted values: row-group ranges are disjoint, so the floor is the exact
    # sum (the TPC-H l_orderkey shape) and the estimate must not move off it.
    values = [k // 4 for k in range(ROWS)]
    footer, ndv = _footer_and_ndv(_dataset(tmp_path, values))
    assert footer["distinct_count"] == ROWS // 4
    assert ndv == ROWS // 4


def test_sorted_key_straddling_row_groups_is_exact(tmp_path):
    # Runs of three over 1024-row groups: most boundaries split a run, so
    # adjacent row groups TOUCH at one value. Taking the max there (the old
    # rule) floored TPC-H SF100 l_orderkey at a quarter of its count; touching
    # ranges share at most that value, so the floor sums them less one.
    values = [k // 3 for k in range(ROWS)]
    footer, ndv = _footer_and_ndv(_dataset(tmp_path, values))
    assert footer["distinct_count"] == len(set(values)), footer
    assert ndv == len(set(values))


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
