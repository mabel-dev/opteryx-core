"""
Tests for GroupJoinStrategy — the plan-shape half of the groupjoin
(docs/GROUPJOIN_DESIGN.md). Result correctness is pinned by
tests/tools/sqllogictest/tests/results/groupjoin_01.slt, which passes with the rule
on and off; these assert that the rule actually FIRED (or declined, with its reason)
on those shapes, so the SLT cannot pass by silently running the old plan.

Decisions are read from EXPLAIN's OPTIMIZATIONS records ("groupjoin | applied ..." /
"groupjoin | declined: ...").
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import DiskConnector

opteryx.register_workspace("testdata", DiskConnector)

R = "(VALUES (1,'a'),(1,'b'),(2,'a'),(3,'c'),(NULL,'d'),(NULL,'e')) AS r(k,g)"
S = "(VALUES (1,10),(1,NULL),(2,5),(4,7),(NULL,3)) AS s(k,x)"


def groupjoin_decisions(sql):
    sess = opteryx.session()
    out = []
    for morsel in sess.execute_to_morsels("EXPLAIN " + sql):
        trees = morsel.column("tree").to_pylist()
        details = morsel.column("details").to_pylist()
        for tree, detail in zip(trees, details):
            tree = tree.decode() if isinstance(tree, bytes) else str(tree)
            if "groupjoin" in tree:
                out.append(str(detail))
    sess.close()
    return out


def test_organic_left_on_build_key_fires():
    # Needs statistics: the groupjoin builds the preserved leg only when the join
    # planner exchanged the legs (left outer join exchange) — see the next test.
    d = groupjoin_decisions(
        "SELECT p.id, COUNT(*), COUNT(s.id) FROM $planets AS p "
        "LEFT JOIN testdata.satellites AS s ON p.id = s.planetId GROUP BY p.id"
    )
    assert len(d) == 1 and d[0].startswith("applied organic (left outer"), d


def test_left_without_exchange_defers_to_the_join_build_side():
    d = groupjoin_decisions(
        f"SELECT r.k, COUNT(*), SUM(s.x) FROM {R} LEFT JOIN {S} ON r.k = s.k GROUP BY r.k"
    )
    assert d == ["declined: the join chose to build the other leg"], d


def test_organic_inner_on_build_key_fires():
    d = groupjoin_decisions(
        f"SELECT r.k, COUNT(*), SUM(s.x) FROM {R} INNER JOIN {S} ON r.k = s.k GROUP BY r.k"
    )
    assert len(d) == 1 and d[0].startswith("applied organic (inner"), d


def test_correlated_scalar_takes_outer_leg_as_build():
    d = groupjoin_decisions(
        f"SELECT r.k, r.g FROM {R} WHERE 5 < (SELECT SUM(s.x) FROM {S} WHERE s.k = r.k)"
    )
    assert any(x.startswith("applied outer leg as build") for x in d), d


def test_window_aggregate_takes_outer_leg_as_build():
    d = groupjoin_decisions(
        "SELECT name, COUNT(*) OVER (PARTITION BY number_of_moons) AS c FROM $planets"
    )
    assert any(x.startswith("applied outer leg as build") for x in d), d


def test_group_by_non_key_build_column_fuses_per_row():
    d = groupjoin_decisions(
        f"SELECT r.g, COUNT(*) FROM {R} INNER JOIN {S} ON r.k = s.k GROUP BY r.g"
    )
    assert len(d) == 1 and "per row + GROUP BY" in d[0], d


def test_group_by_both_sides_declines():
    d = groupjoin_decisions(
        f"SELECT r.g, s.x, COUNT(*) FROM {R} INNER JOIN {S} ON r.k = s.k GROUP BY r.g, s.x"
    )
    assert d == ["declined: GROUP BY mixes both sides of the join"], d


def test_avg_under_per_row_declines():
    d = groupjoin_decisions(
        f"SELECT r.g, AVG(s.x) FROM {R} INNER JOIN {S} ON r.k = s.k GROUP BY r.g"
    )
    assert d == ["declined: AVG under per-row emit (needs a sum/count pair above)"], d


def test_distinct_aggregate_declines():
    d = groupjoin_decisions(
        f"SELECT r.k, COUNT(DISTINCT s.x) FROM {R} LEFT JOIN {S} ON r.k = s.k GROUP BY r.k"
    )
    assert d == ["declined: DISTINCT aggregate"], d


def test_aggregate_over_build_side_declines():
    d = groupjoin_decisions(
        f"SELECT r.k, MAX(r.g) FROM {R} LEFT JOIN {S} ON r.k = s.k GROUP BY r.k"
    )
    assert d == ["declined: MAX reads the build side"], d


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
