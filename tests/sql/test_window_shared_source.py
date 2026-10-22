"""A window over a relation that does work reads it ONCE, not as a rebuilt copy.

    SELECT p.name, COUNT(*) c, SUM(COUNT(*)) OVER (PARTITION BY p.name) t
    FROM $planets p JOIN testdata.satellites s ON ... GROUP BY p.name

`window_to_join` lowers an aggregate window to `source JOIN aggregate(source)`, and the
plan has one consumer per node, so the second use of the source was a full COPY of its
sub-plan: the join and the aggregate ran twice (TPC-DS Q47/Q57: the window cost +22 ms
over its source against DuckDB's +1.2). A window input that is a Subquery boundary over a
join / aggregate / set operation is now carved into a shared body - the engine's
materialize-once rail - and both uses read it through a MaterializedCteRef.

Three things had to change with it, and each is pinned here:

* the body is rewritten and ordered like any shared body (plan_rewriter), including a
  window inside a shared body;
* a grouped relation keeps its HAVING filter inside the boundary, above the Project, so
  the stripped body is HEADED by a Filter. Predicate pushdown restores a predicate it
  cannot place through the chain of nodes above it, a Filter at the head has none, and the
  predicate was silently dropped: `HAVING SUM(id) > 4` stopped applying;
* a filter above every reference is hoisted into the body (shared_cte), after GroupJoin
  has annotated the join with the ids of those filter nodes; removing the filter left the
  compiler holding a node that was not in the plan.

Not shared: a window over a bare table, or any body with no compute step. Measured: no
faster, and a copy keeps a partition-key filter pushing into both scans.

Expected values are computed in Python from the raw rows, not from the engine. The plan
shape is read from EXPLAIN ANALYZE, whose tree is indented by depth; a shared body is
executed once and does not appear in it - its references do.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx


def rows(statement):
    """Every row of `statement`, as tuples in column order."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(statement):
        morsel.materialize()
        names = list(morsel.column_names)
        out.extend(zip(*(morsel.column(name).to_pylist() for name in names)))
    return out


def operators(statement):
    """The EXPLAIN ANALYZE operator names, in plan order, up to the OPTIMIZATIONS section."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels("EXPLAIN ANALYZE " + statement):
        morsel.materialize()
        for tree in morsel.column("tree").to_pylist():
            tree = tree.decode() if isinstance(tree, bytes) else tree
            if tree == "OPTIMIZATIONS":
                return out
            out.append(tree.lstrip(" │├└─"))
    return out


PLANETS = rows("SELECT id, number_of_moons FROM $planets")  # [(id, moons)]
JOINED_GROUPED = (
    "SELECT p.name AS planet, COUNT(*) AS c, SUM(COUNT(*)) OVER (PARTITION BY p.name) AS t "
    "FROM $planets AS p INNER JOIN testdata.satellites AS s ON s.planetId = p.id GROUP BY p.name"
)


def test_a_window_over_a_grouped_join_reads_its_source_once():
    plan = operators(JOINED_GROUPED)
    assert plan.count("Cross Join") + sum(1 for op in plan if op.startswith("Inner Join")) == 1, plan
    references = [op for op in plan if op.startswith("CTE Reference")]
    assert len(references) == 2, plan  # the join's outer leg and the aggregate's input: one shared result, read twice
    assert not [op for op in plan if "Reader" in op], (
        "the join and the aggregate run inside the shared body, once; none of the main plan's scans may remain",
        plan,
    )
    assert sorted(rows(JOINED_GROUPED)) == [
        ("Earth", 1, 1), ("Jupiter", 67, 67), ("Mars", 2, 2), ("Neptune", 14, 14),
        ("Pluto", 5, 5), ("Saturn", 61, 61), ("Uranus", 27, 27),
    ]


def test_a_window_over_a_bare_table_is_still_copied_not_shared():
    plan = operators("SELECT id, COUNT(*) OVER (PARTITION BY number_of_moons) AS c FROM $planets")
    assert not [op for op in plan if op.startswith("CTE Reference")], plan
    assert len([op for op in plan if "Reader" in op]) == 2, plan  # the copy


def test_a_having_filter_inside_the_shared_grouped_relation_still_applies():
    """The stripped body is headed by the HAVING Filter. Every group kept by HAVING is in
    the window's total; every group it removes is in neither."""
    statement = (
        "SELECT number_of_moons, SUM(id) AS s, SUM(SUM(id)) OVER () AS t "
        "FROM $planets GROUP BY number_of_moons HAVING SUM(id) > 4 ORDER BY number_of_moons"
    )
    sums = {}
    for planet_id, moons in PLANETS:
        sums[moons] = sums.get(moons, 0) + planet_id
    kept = sorted((moons, s) for moons, s in sums.items() if s > 4)
    total = sum(s for _, s in kept)
    assert rows(statement) == [(moons, s, total) for moons, s in kept]
    assert len(kept) < len(sums), "the fixture must contain groups HAVING removes"


def test_a_partition_key_filter_above_the_window_is_hoisted_into_the_shared_body():
    """Equal filters above both references move into the body; GroupJoin had annotated the
    join with the ids of those filter nodes, so the annotation has to follow them."""
    statement = f"SELECT * FROM ({JOINED_GROUPED}) AS x WHERE planet = 'Jupiter'"
    assert rows(statement) == [("Jupiter", 67, 67)]
    plan = operators(statement)
    assert len([op for op in plan if op.startswith("CTE Reference")]) == 2, plan
    assert not [op for op in plan if op == "Filter" or op.startswith("Filter ")], plan  # hoisted off both references


def test_a_filter_on_a_non_key_column_above_the_window_is_correct():
    statement = f"SELECT * FROM ({JOINED_GROUPED}) AS x WHERE c > 20 ORDER BY planet"
    assert rows(statement) == [("Jupiter", 67, 67), ("Saturn", 61, 61), ("Uranus", 27, 27)]


def test_two_partition_specs_over_one_shared_source():
    """Two windows, two CTEs, ONE shared source: both aggregate legs read the same body."""
    statement = (
        "SELECT p.name AS planet, COUNT(*) AS c, "
        "SUM(COUNT(*)) OVER (PARTITION BY p.name) AS by_planet, SUM(COUNT(*)) OVER () AS overall "
        "FROM $planets AS p INNER JOIN testdata.satellites AS s ON s.planetId = p.id GROUP BY p.name"
    )
    got = sorted(rows(statement))
    counts = {planet: c for planet, c, _ in rows(JOINED_GROUPED)}
    overall = sum(counts.values())
    assert got == sorted((planet, c, c, overall) for planet, c in counts.items())
    references = [op for op in operators(statement) if op.startswith("CTE Reference")]
    assert len(references) == 3, references  # the outer leg plus one per aggregate leg


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
