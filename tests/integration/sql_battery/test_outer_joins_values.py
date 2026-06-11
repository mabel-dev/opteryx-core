"""
Regression tests for outer joins with VALUES clauses.

These tests verify that LEFT JOIN, RIGHT JOIN, and FULL OUTER JOIN work correctly
with inline VALUES tables, ensuring NULL handling is correct.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from tests.helpers import execute_and_get_arrow
import pytest


def test_left_join_with_values():
    """Test LEFT JOIN preserves all rows from left table, with NULL for non-matches."""
    table = execute_and_get_arrow("""
        SELECT a.x, b.y
        FROM (VALUES (1), (2), (3)) AS a(x)
        LEFT JOIN (VALUES (2), (3), (4)) AS b(y) ON a.x = b.y
        ORDER BY a.x
    """)

    actual = table.to_pydict()
    expected = {'a.x': [1, 2, 3], 'b.y': [None, 2, 3]}
    assert actual == expected, f"Expected {expected}, got {actual}"


def test_right_join_with_values():
    """Test RIGHT JOIN preserves all rows from right table, with NULL for non-matches."""
    table = execute_and_get_arrow("""
        SELECT a.x, b.y
        FROM (VALUES (1), (2), (3)) AS a(x)
        RIGHT JOIN (VALUES (2), (3), (4)) AS b(y) ON a.x = b.y
        ORDER BY b.y
    """)

    actual = table.to_pydict()
    expected = {'a.x': [2, 3, None], 'b.y': [2, 3, 4]}
    assert actual == expected, f"Expected {expected}, got {actual}"


def test_full_outer_join_with_values():
    """Test FULL OUTER JOIN preserves all rows from both tables."""
    table = execute_and_get_arrow("""
        SELECT a.x, b.y
        FROM (VALUES (1), (2), (3)) AS a(x)
        FULL OUTER JOIN (VALUES (2), (3), (4)) AS b(y) ON a.x = b.y
        ORDER BY a.x, b.y
    """)

    actual = table.to_pydict()

    # Should have: (1, None), (2, 2), (3, 3), (None, 4)
    assert 1 in actual['a.x'], "Expected x=1 in result"
    assert 4 in actual['b.y'], "Expected y=4 in result"
    assert None in actual['a.x'], "Expected NULL in x column"
    assert None in actual['b.y'], "Expected NULL in y column"

    # Verify we have exactly 4 rows (one for each unique value)
    assert len(actual['a.x']) == 4, f"Expected 4 rows, got {len(actual['a.x'])}"


def test_right_outer_join_distinct_with_nulls():
    """
    Test DISTINCT with NULL handling in RIGHT OUTER JOIN.

    This verifies that DISTINCT correctly handles NULL values when they appear
    in the result of an outer join.
    """
    # First verify which planets have satellites
    satellites_table = execute_and_get_arrow("SELECT DISTINCT planetId FROM testdata.satellites ORDER BY planetId")
    planets_with_satellites = satellites_table.to_pydict()

    # Right outer join should include all planets (even those without satellites)
    result_table = execute_and_get_arrow("""
        SELECT DISTINCT planetId
        FROM testdata.satellites
        RIGHT OUTER JOIN $planets ON testdata.satellites.planetId = $planets.id
        ORDER BY planetId
    """)

    actual = result_table.to_pydict()

    # Should have more distinct values than just planets with satellites
    # (because it includes NULLs for planets without satellites)
    assert len(actual['planetId']) >= len(planets_with_satellites['planetId']), \
        "RIGHT OUTER JOIN should include planets without satellites"

    # Should have at least one NULL (for planets without satellites)
    assert None in actual['planetId'], \
        "Expected NULL for planets without satellites"

@pytest.mark.parametrize("limit_clause", ["", "LIMIT 1000"])
def test_right_join_preserved_right_one_to_many(limit_clause):
    """Regression: RIGHT OUTER JOIN where the preserved (right) side matches many
    probe rows (1:N) used to SIGBUS in align_tables — the left index buffer carried
    -1 for unmatched preserved-right rows, but the left-column gather assumed all
    indices were non-negative. Asserts a stable, correct row count with and without
    a LIMIT (the crash is independent of LIMIT handling).

    satellites RIGHT JOIN planets: planets preserved, joined 1:N (each planet has
    many satellites). 177 satellites all match a planet; Mercury and Venus have no
    satellites and are preserved with a NULL left side → 179 rows.
    """
    import opteryx

    session = opteryx.session()
    morsels = list(session.execute_to_morsels(f"""
        SELECT p.name
        FROM testdata.satellites AS sat
        RIGHT JOIN testdata.planets AS p ON p.id = sat.planetId
        {limit_clause}
    """))
    rows = sum(m.num_rows for m in morsels)
    assert rows == 179, f"Expected 179 rows, got {rows}"


def test_right_join_preserved_right_unmatched_names_present():
    """Regression companion: the preserved-right (planet) name must survive on
    unmatched rows even though the left (satellite) side is NULL."""
    import opteryx

    session = opteryx.session()
    morsels = list(session.execute_to_morsels("""
        SELECT p.name AS planet, sat.id AS sat_id
        FROM testdata.satellites AS sat
        RIGHT JOIN testdata.planets AS p ON p.id = sat.planetId
        WHERE sat.id IS NULL
        ORDER BY p.name
    """))

    planets = []
    sat_ids = []
    for m in morsels:
        planet_col = m.column("planet")
        sat_col = m.column("sat_id")
        for i in range(m.num_rows):
            planets.append(planet_col[i])
            sat_ids.append(sat_col[i])

    assert planets == ["Mercury", "Venus"], planets
    assert sat_ids == [None, None], sat_ids


if __name__ == "__main__":
    test_left_join_with_values()
    test_right_join_with_values()
    test_full_outer_join_with_values()
    test_right_outer_join_distinct_with_nulls()
    test_right_join_preserved_right_one_to_many("")
    test_right_join_preserved_right_one_to_many("LIMIT 1000")
    test_right_join_preserved_right_unmatched_names_present()
    print("All tests passed.")
