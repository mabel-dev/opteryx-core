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


def test_left_join_on_clause_literal_conjunct_preserves_unmatched_rows():
    """Regression: a LEFT OUTER JOIN whose ON clause combines a real join-key
    equality with a literal-comparison conjunct on the build (right) side used
    to return ZERO rows instead of preserving every left-side row.

    predicate_pushdown's ON-clause extraction (`_is_collectable`/`_inner`) pulls
    `s.planetId=1` out of the ON clause and, for LEFT OUTER, was dumping it as a
    post-join Filter — indistinguishable from a genuine WHERE predicate. Since
    WHERE applies after the join, `NULL (build) op literal` filtered out every
    unmatched preserved row, collapsing LEFT OUTER into INNER. The conjunct must
    instead act as a pre-join filter on the build side (a match-candidate
    filter), so unmatched left rows are still preserved with NULL build columns.
    """
    import opteryx

    session = opteryx.session()

    # No satellite has planetId=1 (Mercury has no moons) — every planet row
    # must still be preserved, all with a NULL satellite name.
    morsels = list(session.execute_to_morsels("""
        SELECT p.name AS planet, s.name AS sat_name
        FROM $planets AS p
        LEFT OUTER JOIN testdata.satellites AS s
            ON p.id = s.planetId AND s.planetId = 1
    """))
    rows = sum(m.num_rows for m in morsels)
    assert rows == 9, f"Expected all 9 planets preserved, got {rows} rows"
    sat_names = [v for m in morsels for v in m.column("sat_name")]
    assert all(v is None for v in sat_names), sat_names

    # planetId=3 (Earth) has a real match (Moon) — that row should match while
    # every other planet remains preserved with NULL.
    morsels = list(session.execute_to_morsels("""
        SELECT p.name AS planet, s.name AS sat_name
        FROM $planets AS p
        LEFT OUTER JOIN testdata.satellites AS s
            ON p.id = s.planetId AND s.planetId = 3
        ORDER BY p.id
    """))
    rows = sum(m.num_rows for m in morsels)
    assert rows == 9, f"Expected all 9 planets preserved, got {rows} rows"
    planets = [v for m in morsels for v in m.column("planet")]
    sat_names = [v for m in morsels for v in m.column("sat_name")]
    expected_sat_names = [None] * 9
    expected_sat_names[planets.index("Earth")] = "Moon"
    assert sat_names == expected_sat_names, sat_names


def test_join_on_clause_bare_literal_conjunct_fails_loud():
    """Regression: a JOIN ON clause conjunct that references NO column at all
    (a bare literal like `AND FALSE`) has no join key to extract and no column
    for a Filter step to carry it on, so predicate_pushdown's ON-clause
    extraction (`_is_collectable`/`_inner`) had nowhere to route it and
    silently dropped it between planning and execution — e.g.
    `ON p.id = s.planetId AND FALSE` returned the unfiltered join result
    instead of zero rows. This must now fail loud at plan time instead,
    mirroring the existing WHERE-clause bare-literal rejection. Confirmed for
    both INNER (no NULL-extension) and LEFT OUTER (where the wrong fix would
    also have broken preserved-row semantics)."""
    import opteryx
    from opteryx.exceptions import UnsupportedSyntaxError

    session = opteryx.session()

    for join_kind in ("JOIN", "LEFT JOIN"):
        try:
            list(
                session.execute_to_morsels(
                    f"""
                    SELECT p.name, s.name
                    FROM $planets AS p
                    {join_kind} testdata.satellites AS s
                        ON p.id = s.planetId AND FALSE
                    """
                )
            )
            assert False, f"{join_kind} with a bare-literal ON conjunct should fail loud"
        except UnsupportedSyntaxError as err:
            assert "JOIN condition cannot be a bare literal" in str(err), err


def test_left_join_where_clause_still_filters_post_join():
    """Contrast to the ON-clause case above: a genuine WHERE predicate on the
    build side of a LEFT OUTER JOIN is standard SQL post-join filtering and
    correctly drops unmatched rows — this must NOT change with the ON-clause
    fix."""
    import opteryx

    session = opteryx.session()
    morsels = list(session.execute_to_morsels("""
        SELECT p.name AS planet, s.name AS sat_name
        FROM $planets AS p
        LEFT OUTER JOIN testdata.satellites AS s ON p.id = s.planetId
        WHERE s.planetId = 3
    """))
    rows = sum(m.num_rows for m in morsels)
    assert rows == 1, f"Expected only the matched Earth/Moon row, got {rows} rows"
    planets = [v for m in morsels for v in m.column("planet")]
    assert planets == ["Earth"], planets


if __name__ == "__main__":
    test_left_join_with_values()
    test_right_join_with_values()
    test_full_outer_join_with_values()
    test_right_outer_join_distinct_with_nulls()
    test_right_join_preserved_right_one_to_many("")
    test_right_join_preserved_right_one_to_many("LIMIT 1000")
    test_right_join_preserved_right_unmatched_names_present()
    test_left_join_on_clause_literal_conjunct_preserves_unmatched_rows()
    test_join_on_clause_bare_literal_conjunct_fails_loud()
    test_left_join_where_clause_still_filters_post_join()
    print("All tests passed.")
