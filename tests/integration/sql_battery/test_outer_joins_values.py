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

if __name__ == "__main__":
    test_left_join_with_values()
    test_right_join_with_values()
    test_full_outer_join_with_values()
    test_right_outer_join_distinct_with_nulls()
    print("All tests passed.")
