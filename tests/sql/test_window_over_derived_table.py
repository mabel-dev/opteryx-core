"""A window over a DERIVED TABLE is a window over one relation, however many tables its body joins.

    SELECT item_sk, MAX(w) OVER (PARTITION BY item_sk ORDER BY d_date ROWS ...)
    FROM (SELECT ... FROM a FULL OUTER JOIN b ON ...) x

was refused with "Window functions over multiple joined tables are not yet
supported" while the identical body written as a CTE ran. The logical planner's
`_find_base_scan` counted every Scan in the plan, including the ones below the
derived table's Subquery boundary; `window_to_join._source_relation` — the rule
that runs after resolution — already stopped at that boundary. TPC-DS Q51 is
written this way.

The refusal still stands where it is true: a join sitting DIRECTLY under the window
(two relations at the window's own scope) has no single relation name to rebuild the
outer leg of the rewrite from.

Expected values are computed in Python from the raw join rows, not from the engine's
own window path.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError


def rows(statement):
    """Every row of `statement`, as tuples in column order."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(statement):
        morsel.materialize()
        names = list(morsel.column_names)
        out.extend(zip(*(morsel.column(name).to_pylist() for name in names)))
    return out


# Each `a` planet pairs with every planet sharing its id % 3, so a partition on `planet` has several rows.
JOIN_BODY = "SELECT a.id AS planet, b.id AS moon FROM $planets AS a INNER JOIN $planets AS b ON a.id % 3 = b.id % 3"


def _per_planet_max_moon():
    joined = rows(JOIN_BODY)
    best = {}
    for planet, moon in joined:
        best[planet] = max(best.get(planet, moon), moon)
    return sorted((planet, moon, best[planet]) for planet, moon in joined)


def test_partitioned_window_over_a_derived_join():
    got = sorted(
        rows(f"SELECT planet, moon, MAX(moon) OVER (PARTITION BY planet) AS m FROM ({JOIN_BODY}) AS x")
    )
    assert got == _per_planet_max_moon()
    assert len(got) > 0


def test_derived_table_matches_the_same_body_as_a_cte():
    derived = sorted(
        rows(f"SELECT planet, moon, MAX(moon) OVER (PARTITION BY planet) AS m FROM ({JOIN_BODY}) AS x")
    )
    cte = sorted(
        rows(f"WITH x AS ({JOIN_BODY}) SELECT planet, moon, MAX(moon) OVER (PARTITION BY planet) AS m FROM x")
    )
    assert derived == cte


def test_framed_window_over_a_derived_join():
    """The Q51 shape: a running maximum, PARTITION BY plus ORDER BY plus a frame."""
    got = sorted(
        rows(
            "SELECT planet, moon, MAX(moon) OVER (PARTITION BY planet ORDER BY moon "
            f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM ({JOIN_BODY}) AS x"
        )
    )
    # moon is unique within a planet, so the running maximum is the moon itself.
    assert got == sorted((planet, moon, moon) for planet, moon in rows(JOIN_BODY))


def test_a_join_directly_under_the_window_is_still_refused():
    with pytest.raises(UnsupportedSyntaxError) as raised:
        rows(
            "SELECT a.id, MAX(b.id) OVER (PARTITION BY a.id) FROM $planets AS a "
            "INNER JOIN $planets AS b ON a.id % 3 = b.id % 3"
        )
    assert "multiple joined tables" in str(raised.value), raised.value


def test_two_derived_tables_joined_under_the_window_are_still_refused():
    with pytest.raises(UnsupportedSyntaxError) as raised:
        rows(
            "SELECT a.planet, MAX(a.moon) OVER (PARTITION BY a.planet) "
            f"FROM ({JOIN_BODY}) AS a INNER JOIN (SELECT id FROM $planets) AS b ON a.planet = b.id"
        )
    assert "multiple joined tables" in str(raised.value), raised.value


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
