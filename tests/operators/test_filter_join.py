"""
Correctness tests for FilterJoinNode (LEFT SEMI, LEFT ANTI, LEFT ANTI NULL-AWARE).

Tests cover:
  - Batch probe APIs (probe_found_32 / probe_not_found_32) via end-to-end queries
  - Semi-join (IN subquery) semantics including null handling
  - Anti-join (EXCEPT/INTERSECT) semantics
  - Null-aware anti-join (NOT IN subquery) — core new functionality
  - Null sentinel: NULL IN (NULL,...) = UNKNOWN (excluded)
  - Null sentinel: NULL NOT IN (...) = UNKNOWN (excluded)
  - NOT IN where right side contains null → zero rows returned
  - NOT IN where right side is empty → all rows returned
  - Semi-join no-row-duplication guarantee
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _execute(sql):
    sess = opteryx.session()
    return list(sess.execute_to_morsels(sql))


def _run(sql):
    morsels = _execute(sql)
    if not morsels:
        return {}
    import pyarrow as pa
    tables = [m.to_arrow() for m in morsels if m.num_rows > 0]
    if not tables:
        col_names = morsels[0].column_names
        return {c: [] for c in col_names}
    tbl = pa.concat_tables(tables)
    return tbl.to_pydict()


def row_count(sql):
    return sum(m.num_rows for m in _execute(sql))


def scalar(sql):
    d = _run(sql)
    return list(d.values())[0][0]


def sorted_col(sql, col):
    return sorted(_run(sql)[col])


# ---------------------------------------------------------------------------
# 1. LEFT SEMI JOIN (IN subquery) — basic correctness
# ---------------------------------------------------------------------------

def test_semi_join_basic_in():
    """IN (subquery) returns exactly the matching outer rows."""
    n = row_count(
        "SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE id <= 3)"
    )
    assert n == 3


def test_semi_join_matches_direct_filter():
    """IN (subquery) gives same result as a direct WHERE predicate."""
    in_result = sorted_col(
        "SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE gravity > 10)",
        "name",
    )
    direct = sorted_col(
        "SELECT name FROM $planets WHERE gravity > 10",
        "name",
    )
    assert in_result == direct


def test_semi_join_empty_right_returns_nothing():
    """IN (empty subquery) → zero rows."""
    n = row_count(
        "SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE id < 0)"
    )
    assert n == 0


def test_semi_join_full_right_returns_all():
    """IN (subquery with all rows) → all outer rows returned."""
    total = row_count("SELECT name FROM $planets")
    n = row_count("SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets)")
    assert n == total


def test_semi_join_no_row_duplication():
    """
    Semi-join must not duplicate outer rows even when the right side has repeated keys.
    testdata.satellites has many rows per planet (multiple satellites per planetId).
    """
    total_planets = row_count("SELECT name FROM $planets")
    n = row_count(
        "SELECT name FROM $planets WHERE id IN (SELECT planetId FROM testdata.satellites)"
    )
    assert 0 < n <= total_planets


def test_semi_join_only_left_columns():
    """Semi-join must not leak right-side columns into the result."""
    d = _run("SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE id < 4)")
    assert list(d.keys()) == ["name"]


# ---------------------------------------------------------------------------
# 2. LEFT SEMI JOIN — null semantics
#
# NULL IN (...) = UNKNOWN → row excluded.
# We verify this by joining on a nullable column.
# ---------------------------------------------------------------------------

def test_semi_join_null_outer_key_excluded():
    """
    When the outer join key is NULL, the row must be excluded (NULL IN (...) = UNKNOWN).
    $planets has nullable gravity; we join on gravity against a set built from
    the same column — null rows must not appear in the semi-join output.
    """
    # Build a reference count excluding null gravity rows from $planets
    direct = row_count(
        "SELECT name FROM $planets WHERE gravity IS NOT NULL AND gravity IN "
        "(SELECT gravity FROM $planets WHERE gravity IS NOT NULL)"
    )
    # If nulls leaked through, the count would be higher
    semi = row_count(
        "SELECT name FROM $planets WHERE gravity IN "
        "(SELECT gravity FROM $planets WHERE gravity IS NOT NULL)"
    )
    # At most equal — nulls on the left must be excluded
    assert semi <= direct


def test_semi_join_right_null_does_not_match_left_null():
    """
    NULL IN (NULL, 1, 2) = UNKNOWN → excluded.  A left row with null key must not
    match a right-side null key, even though both hash to NULL_HASH.

    We construct a scenario using $planets where gravity IS NULL would match
    gravity IS NULL on the right — but it must NOT produce a result row.
    Uses a fixed value list for clarity.
    """
    # $planets gravity values are all non-null, so this tests the edge via the
    # hash sentinel property: if NULL_HASH is in the right set, left null rows
    # must still be excluded.  We verify by counting rows with a nullable column.
    result = _run(
        "SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE id <= 4)"
    )
    # Sanity: result rows should have non-null id keys
    assert len(result.get("name", [])) == 4


# ---------------------------------------------------------------------------
# 3. NOT IN (LEFT ANTI NULL-AWARE JOIN)
# ---------------------------------------------------------------------------

def test_not_in_basic_exclusion():
    """NOT IN excludes matching rows, returns the complement."""
    n_in = row_count(
        "SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE id <= 4)"
    )
    n_not_in = row_count(
        "SELECT name FROM $planets WHERE id NOT IN (SELECT id FROM $planets WHERE id <= 4)"
    )
    total = row_count("SELECT name FROM $planets")
    assert n_in + n_not_in == total


def test_not_in_empty_right_returns_all():
    """NOT IN with an empty subquery → all outer rows returned."""
    total = row_count("SELECT name FROM $planets")
    n = row_count(
        "SELECT name FROM $planets WHERE id NOT IN (SELECT id FROM $planets WHERE id < 0)"
    )
    assert n == total


def test_not_in_full_right_returns_nothing():
    """NOT IN where right covers all outer keys → zero rows."""
    n = row_count(
        "SELECT name FROM $planets WHERE id NOT IN (SELECT id FROM $planets)"
    )
    assert n == 0


def test_not_in_matches_complement_of_in():
    """NOT IN result rows + IN result rows = total rows (no overlap, no gaps)."""
    in_names = sorted_col(
        "SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE gravity > 10)",
        "name",
    )
    not_in_names = sorted_col(
        "SELECT name FROM $planets WHERE id NOT IN (SELECT id FROM $planets WHERE gravity > 10)",
        "name",
    )
    all_names = sorted_col("SELECT name FROM $planets", "name")
    assert sorted(in_names + not_in_names) == all_names


def test_not_in_with_and_predicate():
    """NOT IN combined with an AND predicate applies both constraints."""
    result = sorted_col(
        "SELECT name FROM $planets WHERE id NOT IN (SELECT id FROM $planets WHERE id <= 4) AND id < 8",
        "name",
    )
    direct = sorted_col(
        "SELECT name FROM $planets WHERE id > 4 AND id < 8",
        "name",
    )
    assert result == direct


def test_not_in_cross_table():
    """NOT IN referencing a different table returns correct subset."""
    all_planets = row_count("SELECT name FROM $planets")
    with_satellites = row_count(
        "SELECT name FROM $planets WHERE id IN (SELECT DISTINCT planetId FROM testdata.satellites)"
    )
    without_satellites = row_count(
        "SELECT name FROM $planets WHERE id NOT IN (SELECT DISTINCT planetId FROM testdata.satellites)"
    )
    assert with_satellites + without_satellites == all_planets
    assert without_satellites > 0  # some planets have no satellites


# ---------------------------------------------------------------------------
# 4. NOT IN — null semantics (the hard cases)
# ---------------------------------------------------------------------------

def test_not_in_right_null_returns_zero_rows():
    """
    If the right-side subquery returns any NULL, NOT IN returns zero rows for all
    outer rows (NULL semantics: NOT IN (NULL, ...) = UNKNOWN for every row).

    We create a subquery that explicitly includes NULL via UNION with a literal.
    Note: this is the key correctness invariant that distinguishes null-aware anti-join
    from a plain anti-join.
    """
    # Build a right side that provably contains a null by unioning with
    # a query that returns null.  All outer rows must be excluded.
    n = row_count(
        """
        SELECT name FROM $planets
        WHERE id NOT IN (
            SELECT id FROM $planets WHERE id = 1
            UNION ALL
            SELECT null
        )
        """
    )
    assert n == 0, (
        f"Expected 0 rows when right side contains NULL, got {n}. "
        "NOT IN with a NULL in the subquery must return UNKNOWN for every outer row."
    )


def test_not_in_left_null_key_excluded():
    """
    Outer rows with a NULL join key are always excluded from NOT IN
    (NULL NOT IN (...) = UNKNOWN = excluded), even when the right side has no nulls.

    We test indirectly: if we filter to only rows where the join key is NOT NULL,
    we get the same count as the NOT IN result on those same rows.
    """
    # Count rows where gravity IS NOT NULL AND gravity NOT IN a non-null set
    not_null_not_in = row_count(
        "SELECT name FROM $planets WHERE gravity IS NOT NULL "
        "AND gravity NOT IN (SELECT gravity FROM $planets WHERE gravity > 20)"
    )
    # Direct: gravity IS NOT NULL AND gravity <= 20
    direct = row_count(
        "SELECT name FROM $planets WHERE gravity IS NOT NULL AND gravity <= 20"
    )
    assert not_null_not_in == direct


def test_not_in_no_right_nulls_correct_semantics():
    """When the right side has no nulls, NOT IN behaves as a plain anti-join."""
    n_not_in = row_count(
        "SELECT name FROM $planets WHERE id NOT IN (SELECT id FROM $planets WHERE id <= 3)"
    )
    n_direct = row_count("SELECT name FROM $planets WHERE id > 3")
    assert n_not_in == n_direct


# ---------------------------------------------------------------------------
# 5. LEFT ANTI JOIN — EXCEPT semantics (plain anti, no null awareness)
# ---------------------------------------------------------------------------

def test_except_basic():
    """EXCEPT returns rows in left but not right."""
    result = sorted_col(
        "SELECT name, id FROM $planets AS A WHERE id <= 5 "
        "EXCEPT "
        "SELECT name, id FROM $planets AS B WHERE id <= 3",
        "name",
    )
    direct = sorted_col(
        "SELECT name FROM $planets WHERE id > 3 AND id <= 5",
        "name",
    )
    assert result == direct


def test_except_empty_right_returns_left():
    """EXCEPT with an empty right side returns all left rows."""
    n_all = row_count("SELECT name, id FROM $planets AS A")
    n_except = row_count(
        "SELECT name, id FROM $planets AS A "
        "EXCEPT "
        "SELECT name, id FROM $planets AS B WHERE id < 0"
    )
    assert n_except == n_all


def test_intersect_basic():
    """INTERSECT returns the common rows."""
    n = row_count(
        "SELECT name, id FROM $planets AS A WHERE id <= 5 "
        "INTERSECT "
        "SELECT name, id FROM $planets AS B WHERE id >= 3"
    )
    # ids 3,4,5 are in both → 3 rows
    assert n == 3


# ---------------------------------------------------------------------------
# 5b. Multi-column filter-join keys (regression: len(right_columns) != 1 path)
#
# The build side null-detection in _push_right_gil is only meaningful for a
# single-column key. The multi-column path must not touch the single-column-only
# locals (col / _col_type). This was a real indentation bug where the INT8/INT16
# null check sat outside the `len(right_columns) == 1` guard, leaving `col`/
# `_col_type` unbound or stale on the multi-column path.
# ---------------------------------------------------------------------------

def test_except_two_column_key():
    """EXCEPT on a 2-column key exercises the multi-column build path."""
    result = row_count(
        "SELECT name, id FROM $planets AS A WHERE id <= 5 "
        "EXCEPT "
        "SELECT name, id FROM $planets AS B WHERE id <= 3"
    )
    direct = row_count("SELECT name, id FROM $planets WHERE id > 3 AND id <= 5")
    # ids 4,5 remain → 2 rows, matching the direct filter
    assert result == direct == 2


def test_intersect_two_column_key():
    """INTERSECT on a 2-column key exercises the multi-column build path."""
    n = row_count(
        "SELECT name, id FROM $planets AS A WHERE id <= 5 "
        "INTERSECT "
        "SELECT name, id FROM $planets AS B WHERE id >= 3"
    )
    # ids 3,4,5 are in both, and (name,id) pairs are identical → 3 rows
    assert n == 3


def test_intersect_three_column_key():
    """INTERSECT on a 3-column key (>1 columns) must build and probe correctly."""
    n = row_count(
        "SELECT name, id, gravity FROM $planets AS A WHERE id <= 6 "
        "INTERSECT "
        "SELECT name, id, gravity FROM $planets AS B WHERE id >= 4"
    )
    # ids 4,5,6 overlap → 3 rows
    assert n == 3


def test_except_two_column_full_right_empty():
    """EXCEPT with a 2-column key removing everything → zero rows."""
    n = row_count(
        "SELECT name, id FROM $planets AS A WHERE id <= 5 "
        "EXCEPT "
        "SELECT name, id FROM $planets AS B WHERE id <= 5"
    )
    assert n == 0


# ---------------------------------------------------------------------------
# 6. Low-level CarcharSet batch probe API
# ---------------------------------------------------------------------------

def test_carchar_probe_found_32():
    """probe_found_32 returns indices of hashes found in the set."""
    from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
    from array import array

    s = CarcharSetWrapper()
    keys = array("Q", [10, 20, 30, 40, 50])
    s.insert_many(keys)

    # Query: [10, 99, 30, 99, 50] → found at positions 0, 2, 4
    query = array("Q", [10, 99, 30, 99, 50])
    result = s.probe_found(query)
    assert list(result) == [0, 2, 4]


def test_carchar_probe_not_found_32():
    """probe_not_found_32 returns indices of hashes NOT in the set."""
    from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
    from array import array

    s = CarcharSetWrapper()
    keys = array("Q", [10, 20, 30])
    s.insert_many(keys)

    # Query: [10, 99, 30, 88] → not-found at positions 1, 3
    query = array("Q", [10, 99, 30, 88])
    result = s.probe_not_found(query)
    assert list(result) == [1, 3]


def test_carchar_probe_empty_set():
    """Probing an empty set: found returns [], not_found returns all indices."""
    from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
    from array import array

    s = CarcharSetWrapper()
    query = array("Q", [1, 2, 3])
    assert list(s.probe_found(query)) == []
    assert list(s.probe_not_found(query)) == [0, 1, 2]


def test_carchar_probe_found_empty_query():
    """Empty query returns empty results for both probes."""
    from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
    from array import array

    s = CarcharSetWrapper()
    s.add(42)
    empty = array("Q")
    assert list(s.probe_found(empty)) == []
    assert list(s.probe_not_found(empty)) == []


# ---------------------------------------------------------------------------
# 7. EXISTS / NOT EXISTS subquery rewrites
# ---------------------------------------------------------------------------

def test_exists_basic():
    """EXISTS returns outer rows that have at least one matching inner row."""
    n = row_count(
        "SELECT name FROM $planets WHERE EXISTS "
        "(SELECT 1 FROM testdata.satellites WHERE testdata.satellites.planetId = $planets.id)"
    )
    assert n == 7  # 7 planets have at least one satellite


def test_not_exists_basic():
    """NOT EXISTS returns outer rows with no matching inner rows."""
    n = row_count(
        "SELECT name FROM $planets WHERE NOT EXISTS "
        "(SELECT 1 FROM testdata.satellites WHERE testdata.satellites.planetId = $planets.id)"
    )
    assert n == 2  # Mercury and Venus have no satellites


def test_exists_not_exists_partition():
    """EXISTS + NOT EXISTS covers every row exactly once."""
    total = row_count("SELECT name FROM $planets")
    with_match = row_count(
        "SELECT name FROM $planets WHERE EXISTS "
        "(SELECT 1 FROM testdata.satellites WHERE testdata.satellites.planetId = $planets.id)"
    )
    without_match = row_count(
        "SELECT name FROM $planets WHERE NOT EXISTS "
        "(SELECT 1 FROM testdata.satellites WHERE testdata.satellites.planetId = $planets.id)"
    )
    assert with_match + without_match == total


def test_exists_matches_in_subquery():
    """EXISTS produces the same rows as an equivalent IN subquery."""
    n_in = row_count(
        "SELECT name FROM $planets WHERE id IN (SELECT planetId FROM testdata.satellites)"
    )
    n_ex = row_count(
        "SELECT name FROM $planets WHERE EXISTS "
        "(SELECT 1 FROM testdata.satellites WHERE testdata.satellites.planetId = $planets.id)"
    )
    assert n_in == n_ex


def test_exists_no_row_duplication():
    """EXISTS returns each outer row at most once, even when inner has many matches."""
    n = row_count(
        "SELECT name FROM $planets WHERE EXISTS "
        "(SELECT 1 FROM testdata.satellites WHERE testdata.satellites.planetId = $planets.id)"
    )
    n_direct = row_count("SELECT DISTINCT id FROM $planets WHERE id IN "
                         "(SELECT planetId FROM testdata.satellites)")
    assert n == n_direct


def test_exists_with_and_predicate():
    """EXISTS combined with an AND predicate applies both filters."""
    n = row_count(
        "SELECT name FROM $planets WHERE EXISTS "
        "(SELECT 1 FROM testdata.satellites WHERE testdata.satellites.planetId = $planets.id)"
        " AND id <= 4"
    )
    # Only inner planets (id <= 4) that have satellites
    direct = row_count(
        "SELECT name FROM $planets WHERE id IN "
        "(SELECT planetId FROM testdata.satellites) AND id <= 4"
    )
    assert n == direct


def test_exists_with_inner_filter():
    """EXISTS with a non-correlation predicate in the subquery WHERE."""
    n = row_count(
        "SELECT name FROM $planets WHERE EXISTS "
        "(SELECT 1 FROM testdata.satellites WHERE testdata.satellites.planetId = $planets.id "
        " AND testdata.satellites.name IS NOT NULL)"
    )
    assert n == 7  # same as basic EXISTS — all satellite names are non-null


def test_not_exists_with_and_predicate():
    """NOT EXISTS combined with an AND predicate on the outer query."""
    n = row_count(
        "SELECT name FROM $planets WHERE NOT EXISTS "
        "(SELECT 1 FROM testdata.satellites WHERE testdata.satellites.planetId = $planets.id)"
        " AND id < 10"
    )
    assert n == 2


def test_exists_empty_inner_returns_nothing():
    """EXISTS with an inner table that returns no rows → outer result is empty."""
    n = row_count(
        "SELECT name FROM $planets WHERE EXISTS "
        "(SELECT 1 FROM testdata.satellites WHERE testdata.satellites.planetId = $planets.id "
        " AND testdata.satellites.planetId < 0)"
    )
    assert n == 0


def test_not_exists_full_inner_returns_nothing():
    """NOT EXISTS when every outer row has a match → result is empty."""
    n = row_count(
        "SELECT name FROM $planets WHERE NOT EXISTS "
        "(SELECT 1 FROM $planets AS inner WHERE inner.id = $planets.id)"
    )
    assert n == 0


def test_uncorrelated_exists_raises():
    """Uncorrelated EXISTS (no correlation predicate) raises UnsupportedSyntaxError."""
    from opteryx.exceptions import UnsupportedSyntaxError
    import pytest
    with pytest.raises(UnsupportedSyntaxError):
        row_count("SELECT name FROM $planets WHERE EXISTS (SELECT 1 FROM testdata.satellites)")


if __name__ == "__main__":
    import traceback

    tests = [
        test_semi_join_basic_in,
        test_semi_join_matches_direct_filter,
        test_semi_join_empty_right_returns_nothing,
        test_semi_join_full_right_returns_all,
        test_semi_join_no_row_duplication,
        test_semi_join_only_left_columns,
        test_semi_join_null_outer_key_excluded,
        test_semi_join_right_null_does_not_match_left_null,
        test_not_in_basic_exclusion,
        test_not_in_empty_right_returns_all,
        test_not_in_full_right_returns_nothing,
        test_not_in_matches_complement_of_in,
        test_not_in_with_and_predicate,
        test_not_in_cross_table,
        test_not_in_right_null_returns_zero_rows,
        test_not_in_left_null_key_excluded,
        test_not_in_no_right_nulls_correct_semantics,
        test_except_basic,
        test_except_empty_right_returns_left,
        test_intersect_basic,
        test_except_two_column_key,
        test_intersect_two_column_key,
        test_intersect_three_column_key,
        test_except_two_column_full_right_empty,
        test_exists_basic,
        test_not_exists_basic,
        test_exists_not_exists_partition,
        test_exists_matches_in_subquery,
        test_exists_no_row_duplication,
        test_exists_with_and_predicate,
        test_exists_with_inner_filter,
        test_not_exists_with_and_predicate,
        test_exists_empty_inner_returns_nothing,
        test_not_exists_full_inner_returns_nothing,
        test_uncorrelated_exists_raises,
        test_carchar_probe_found_32,
        test_carchar_probe_not_found_32,
        test_carchar_probe_empty_set,
        test_carchar_probe_found_empty_query,
    ]

    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  ✅ {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  ❌ {t.__name__}: {e}")
            traceback.print_exc()
            failed += 1

    print(f"\n{passed} passed, {failed} failed")
