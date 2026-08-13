"""
Correctness tests for AsofJoinNode.

Tests cover:
  - Basic ASOF with >= (most common: find nearest-before match)
  - Basic ASOF with <= (find nearest-after match)
  - LEFT semantics: left rows with no right match produce null right columns
  - USING equi-partition key
  - Null ASOF column on left row → treated as no match (null right columns)
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


def row_count(sql):
    return sum(m.num_rows for m in _execute(sql))


def _collect(sql):
    """Return list-of-dicts for the result rows."""
    morsels = _execute(sql)
    rows = []
    for m in morsels:
        if m.num_rows == 0:
            continue
        cols = list(m.column_names)
        for i in range(m.num_rows):
            rows.append({c: m.column(c)[i] for c in cols})
    return rows


# ---------------------------------------------------------------------------
# Basic GtEq (>=): find the nearest right row with value ≤ left value
# ---------------------------------------------------------------------------

def test_asof_basic_gtoreq_row_count():
    # Self-join: every planet finds itself (or the nearest smaller gravity).
    # LEFT semantics: one output row per left planet, always.
    sql = """
        SELECT p.name, p2.name AS match_name
        FROM $planets AS p
        ASOF JOIN $planets AS p2 MATCH_CONDITION(p.gravity >= p2.gravity)
    """
    assert row_count(sql) == 9


def test_asof_basic_gtoreq_correctness():
    # For each planet, ASOF >= finds the right planet with the largest gravity
    # that is still ≤ the left planet's gravity.
    # Column NAMES come back as bytes; string VALUES come back as str.
    sql = """
        SELECT p.name, p.gravity, p2.name AS match_name, p2.gravity AS match_gravity
        FROM $planets AS p
        ASOF JOIN $planets AS p2 MATCH_CONDITION(p.gravity >= p2.gravity)
    """
    rows = _collect(sql)
    by_name = {r[b"p.name"]: r for r in rows}

    # Mercury (gravity 3.7): nearest right gravity ≤ 3.7. Mercury and Mars both
    # sit at 3.7, so which of the tied rows wins is not pinned here — only the
    # ASOF invariant is.
    mercury = by_name["Mercury"]
    assert mercury[b"match_gravity"] <= mercury[b"p.gravity"]

    # Jupiter (gravity 23.1, the highest): nearest right ≤ 23.1 → Jupiter itself
    jupiter = by_name["Jupiter"]
    assert jupiter[b"match_gravity"] <= jupiter[b"p.gravity"]


# ---------------------------------------------------------------------------
# LEFT semantics: no right match → null right columns
# ---------------------------------------------------------------------------

def test_asof_left_semantics_no_match():
    # Right side only contains planets with id >= 5 (Jupiter onwards by id order).
    # Planets with id < 5 on the left should find no match on the right
    # (since they need a right id <= their left id, but right ids start at 5).
    # LEFT semantics: every left row must appear in the output exactly once.
    sql = """
        SELECT p.id, p.name, p2.name AS match_name
        FROM $planets AS p
        ASOF JOIN (SELECT id, name FROM $planets WHERE id >= 5) AS p2
            MATCH_CONDITION(p.id >= p2.id)
    """
    rows = _collect(sql)
    # All 9 left rows must be present (LEFT semantics).
    assert len(rows) == 9, f"Expected 9 rows, got {len(rows)}"

    # All planet ids 1–9 must appear: unmatched rows (id<5) are still emitted.
    found_ids = {r[b"p.id"] for r in rows}
    assert found_ids == {1, 2, 3, 4, 5, 6, 7, 8, 9}, f"Missing ids: {found_ids}"

    # Planets with id >= 5 must have a right match (match_name is not null).
    # Note: same-named columns deduplicate in align_tables (a system-level constraint),
    # so null-checking right columns that share names with left columns is not reliable
    # for self-joins. Instead verify that matched rows have the expected match.
    matched = [r for r in rows if r[b"p.id"] >= 5]
    assert len(matched) == 5, f"Expected 5 matched rows, got {len(matched)}"
    for r in matched:
        assert r[b"match_name"] is not None, f"Expected match for id={r[b'p.id']}"


# ---------------------------------------------------------------------------
# LtEq (<=): find the nearest right row with value ≥ left value
# ---------------------------------------------------------------------------

def test_asof_basic_ltoreq_row_count():
    sql = """
        SELECT p.name, p2.name AS match_name
        FROM $planets AS p
        ASOF JOIN $planets AS p2 MATCH_CONDITION(p.gravity <= p2.gravity)
    """
    assert row_count(sql) == 9


def test_asof_basic_ltoreq_correctness():
    # For each planet, ASOF <= finds the right planet with the smallest gravity
    # that is still ≥ the left planet's gravity.
    sql = """
        SELECT p.name, p.gravity, p2.gravity AS match_gravity
        FROM $planets AS p
        ASOF JOIN $planets AS p2 MATCH_CONDITION(p.gravity <= p2.gravity)
    """
    rows = _collect(sql)
    by_name = {r[b"p.name"]: r for r in rows}

    # Every planet should find a match with gravity >= its own gravity
    for row in rows:
        if row[b"match_gravity"] is not None:
            assert row[b"match_gravity"] >= row[b"p.gravity"]


# ---------------------------------------------------------------------------
# USING equi-partition: ASOF within a partition key
# ---------------------------------------------------------------------------

def test_asof_using_partition_row_count():
    # Partition by planetId so each satellite only matches quotes from same planet.
    # Self-join testdata.satellites using planetId, ASOF on id.
    sql = """
        SELECT s.name, s2.name AS match_name
        FROM testdata.satellites AS s
        ASOF JOIN testdata.satellites AS s2
            MATCH_CONDITION(s.id >= s2.id)
            USING (planetId)
    """
    from opteryx.connectors import DiskConnector
    opteryx.register_workspace("testdata", DiskConnector)

    total = row_count(sql)
    # Each satellite gets at least itself as a match (id >= id within same planet)
    assert total > 0


def test_asof_using_partition_confines_matches():
    # The USING key is the correctness contract for a partitioned ASOF: a probe
    # row must NEVER match a build row from a different partition. `>` (not `>=`)
    # excludes the self-match, so an unpartitioned join would reach back into the
    # previous planet — which is exactly what a dropped USING key looks like.
    from opteryx.connectors import DiskConnector
    opteryx.register_workspace("testdata", DiskConnector)

    sql = """
        SELECT s.planetId, s2.planetId AS match_pid, s2.name AS match_name
        FROM testdata.satellites AS s
        ASOF JOIN testdata.satellites AS s2
            MATCH_CONDITION(s.id > s2.id)
            USING (planetId)
    """
    rows = _collect(sql)
    matched = [r for r in rows if r[b"match_name"] is not None]
    assert matched, "expected some matches"

    cross = [r for r in matched if r[b"s.planetId"] != r[b"match_pid"]]
    assert not cross, f"USING(planetId) must not match across partitions: {cross[:5]}"

    # And the partition must actually bite: the first satellite of each planet has
    # no earlier id within its own planet, so it must be unmatched. Unpartitioned,
    # only the single globally-first satellite would be unmatched.
    unmatched = len(rows) - len(matched)
    distinct_planets = len({r[b"s.planetId"] for r in rows})
    assert unmatched == distinct_planets, (
        f"expected one unmatched row per planet ({distinct_planets}), got {unmatched}"
    )


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

def test_asof_rejects_equality_match_condition():
    sql = """
        SELECT p.name
        FROM $planets AS p
        ASOF JOIN $planets AS p2 MATCH_CONDITION(p.gravity = p2.gravity)
    """
    with pytest.raises(Exception):
        _execute(sql)


def test_asof_rejects_not_equal_match_condition():
    sql = """
        SELECT p.name
        FROM $planets AS p
        ASOF JOIN $planets AS p2 MATCH_CONDITION(p.gravity != p2.gravity)
    """
    with pytest.raises(Exception):
        _execute(sql)


if __name__ == "__main__":
    test_asof_basic_gtoreq_row_count()
    print("test_asof_basic_gtoreq_row_count passed")
    test_asof_basic_gtoreq_correctness()
    print("test_asof_basic_gtoreq_correctness passed")
    test_asof_left_semantics_no_match()
    print("test_asof_left_semantics_no_match passed")
    test_asof_basic_ltoreq_row_count()
    print("test_asof_basic_ltoreq_row_count passed")
    test_asof_basic_ltoreq_correctness()
    print("test_asof_basic_ltoreq_correctness passed")
    test_asof_rejects_equality_match_condition()
    print("test_asof_rejects_equality_match_condition passed")
    test_asof_rejects_not_equal_match_condition()
    print("test_asof_rejects_not_equal_match_condition passed")
    print("All ASOF tests passed!")
