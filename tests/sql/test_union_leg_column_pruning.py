"""A scan beneath a UNION reads the columns its leg uses, not every column it has.

    SELECT count(*) FROM (SELECT cs_item_sk FROM catalog_sales
                          UNION ALL SELECT ss_item_sk FROM store_sales) u

scanned 34 and 23 columns. ProjectionPushdown keeps a fallback for a leg whose Project
was pruned away (a bare `SELECT *`): its identities are independent of the union's, so it
supplies its first N schema columns, N the union's width. The width was read from a
record nothing ever made (the line that would have stored it sat in a block `Union` is
not eligible for), defaulted to the scan's OWN full schema, and `len(columns) < width`
was therefore always true: every Scan, Subquery and CTE reference under a union - however
deep in its leg - was reset to all its columns. TPC-DS Q04's year_total body read 375 MB
where its three branches read 107 MB.

The fallback now applies only to a leg ROOT (no Project, Aggregate, Join... between the
relation and its Union) and takes its width from that Union.

Scan widths are read off the optimized plan; results are checked against Python from the
raw `$planets` rows.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
import opteryx.planner.optimizer as optimizer


@pytest.fixture
def optimized_plans(monkeypatch):
    """Every plan the optimizer returns (the main plan, and any shared body)."""
    plans = []
    original = optimizer.OptimizerVisitor.optimize

    def capture(self, plan):
        optimized = original(self, plan)
        plans.append(optimized)
        return optimized

    monkeypatch.setattr(optimizer.OptimizerVisitor, "optimize", capture)
    return plans


def run(statement):
    """Every row of `statement`, as tuples in column order."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(statement):
        morsel.materialize()
        names = list(morsel.column_names)
        out.extend(zip(*(morsel.column(name).to_pylist() for name in names)))
    return out


def scan_widths(plans):
    return sorted(
        len(list(node.columns or []))
        for plan in plans
        for _, node in plan.nodes(True)
        if node.node_type.name == "Scan"
    )


PLANETS = run("SELECT * FROM $planets")
FULL = len(PLANETS[0])  # every column of $planets


def test_a_single_column_leg_reads_a_single_column(optimized_plans):
    assert run("SELECT id FROM $planets UNION ALL SELECT id FROM $planets WHERE id < 3")
    assert scan_widths(optimized_plans) == [1, 1]
    assert FULL > 2, "the fixture must have columns to prune"


def test_a_leg_reads_its_where_columns_too(optimized_plans):
    run("SELECT id FROM $planets WHERE mass > 1 UNION ALL SELECT id FROM $planets WHERE gravity > 1")
    assert scan_widths(optimized_plans) == [2, 2]


def test_scans_under_a_join_in_a_leg_are_pruned(optimized_plans):
    run(
        "SELECT a.id, b.name FROM $planets AS a INNER JOIN $planets AS b ON a.id = b.id "
        "UNION ALL SELECT id, name FROM $planets"
    )
    widths = scan_widths(optimized_plans)
    assert max(widths) <= 2 and len(widths) == 3, widths


def test_scans_under_an_aggregate_in_a_leg_are_pruned(optimized_plans):
    leg = (
        "SELECT a.number_of_moons, SUM(b.mass) FROM $planets AS a INNER JOIN $planets AS b "
        "ON a.id = b.id GROUP BY a.number_of_moons"
    )
    sums = {}
    for moons, mass in run("SELECT number_of_moons, mass FROM $planets"):
        sums[moons] = sums.get(moons, 0) + mass
    optimized_plans.clear()  # the oracle's own plan is not the one under test
    result = run(f"{leg} UNION ALL {leg}")
    assert sorted((moons, round(total, 6)) for moons, total in result) == sorted(
        [(moons, round(total, 6)) for moons, total in sums.items()] * 2
    )
    assert scan_widths(optimized_plans) == [2, 2, 2, 2]


def test_a_bare_select_star_leg_keeps_every_column(optimized_plans):
    """The case the fallback exists for: no Project, so the leg supplies its own width."""
    result = run("SELECT * FROM $planets UNION ALL SELECT * FROM $planets")
    assert sorted(result) == sorted(PLANETS + PLANETS)
    assert scan_widths(optimized_plans) == [FULL, FULL]


def test_a_bare_leg_with_a_where_keeps_every_column(optimized_plans):
    result = run("SELECT * FROM $planets WHERE id < 4 UNION ALL SELECT * FROM $planets WHERE id > 7")
    assert sorted(result) == sorted([r for r in PLANETS if r[0] < 4] + [r for r in PLANETS if r[0] > 7])
    assert scan_widths(optimized_plans) == [FULL, FULL]


def test_a_bare_derived_table_leg_is_not_narrower_than_the_union(optimized_plans):
    result = run(
        "SELECT * FROM (SELECT id, name FROM $planets) AS s UNION ALL SELECT id, name FROM $planets WHERE id < 3"
    )
    expected = [(r[0], r[1]) for r in PLANETS] + [(r[0], r[1]) for r in PLANETS if r[0] < 3]
    assert sorted(result) == sorted(expected)


def test_positional_alignment_is_by_position_not_by_name():
    result = run("SELECT id, id + 1 FROM $planets UNION ALL SELECT id + 10, id FROM $planets")
    ids = [r[0] for r in PLANETS]
    assert sorted(result) == sorted([(i, i + 1) for i in ids] + [(i + 10, i) for i in ids])


def test_the_outer_query_reading_some_columns_of_a_wider_union():
    result = run("SELECT b FROM (SELECT id AS a, name AS b FROM $planets UNION ALL SELECT id, name FROM $planets) u WHERE a < 3")
    names = [r[1] for r in PLANETS if r[0] < 3]
    assert sorted(result) == sorted([(n,) for n in names] * 2)


def test_a_nested_union_keeps_each_unions_own_width(optimized_plans):
    result = run(
        "SELECT id FROM $planets UNION ALL "
        "(SELECT id FROM $planets UNION ALL SELECT id FROM $planets)"
    )
    assert len(result) == 3 * len(PLANETS)
    assert scan_widths(optimized_plans) == [1, 1, 1]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
