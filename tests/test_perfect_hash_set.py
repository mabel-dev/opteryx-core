"""
Regression tests for PerfectHashSet integration.

All SQL-level tests run both with and without OPTERYX_PERFECT_HASH=1.
Results must be identical — the env flag changes the execution path,
not the semantics.

Coverage:
  1. build_in_list_carchar — PerfectHashSet returned for small range
  2. build_in_list_carchar — CarcharSetWrapper returned for large range
  3. vector_in_list probe — membership (non-negated)
  4. vector_in_list probe — negation (NOT IN)
  5. SQL: IN-list with small integer range  (PerfectHashSet eligible)
  6. SQL: IN-list with large integer range  (CarcharSet fallback)
  7. SQL: NOT IN literal list
  8. SQL: DISTINCT Int64 (CarcharSet path — rugo upcasts narrow ints)
  9. SQL: DISTINCT on testdata/narrow_ints (Int8/Int16 stored, read as Int64)
 10. SQL: Semi-join (IN subquery) correctness
 11. SQL: Anti-join (NOT IN subquery) correctness
 12. SQL: Semi-join — empty right returns zero rows
 13. SQL: Anti-join — empty right returns all rows

Note on DISTINCT with narrow ints: rugo currently upcasts int8/int16 parquet
columns to Int64Vector on decode, so the DISTINCT PerfectHashSet path
(IntegerVector-only) is not exercised via SQL queries. The correctness gate
still holds because the result is identical with/without the env flag.
"""

import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import opteryx
from opteryx.compiled.vector_ops.vector_ops import build_in_list_carchar, vector_in_list


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rows(sql, flag_on):
    env_before = os.environ.get("OPTERYX_PERFECT_HASH")
    if flag_on:
        os.environ["OPTERYX_PERFECT_HASH"] = "1"
    elif "OPTERYX_PERFECT_HASH" in os.environ:
        del os.environ["OPTERYX_PERFECT_HASH"]
    try:
        sess = opteryx.session()
        return sum(m.num_rows for m in sess.execute_to_morsels(sql))
    finally:
        if env_before is None:
            os.environ.pop("OPTERYX_PERFECT_HASH", None)
        else:
            os.environ["OPTERYX_PERFECT_HASH"] = env_before


def _col(sql, col_bytes, flag_on):
    env_before = os.environ.get("OPTERYX_PERFECT_HASH")
    if flag_on:
        os.environ["OPTERYX_PERFECT_HASH"] = "1"
    elif "OPTERYX_PERFECT_HASH" in os.environ:
        del os.environ["OPTERYX_PERFECT_HASH"]
    try:
        sess = opteryx.session()
        morsels = list(sess.execute_to_morsels(sql))
        vals = []
        for m in morsels:
            if m.num_rows > 0:
                vals.extend(m.column(col_bytes).to_arrow().to_pylist())
        return sorted(v for v in vals if v is not None)
    finally:
        if env_before is None:
            os.environ.pop("OPTERYX_PERFECT_HASH", None)
        else:
            os.environ["OPTERYX_PERFECT_HASH"] = env_before


def _both_rows(sql):
    return _rows(sql, False), _rows(sql, True)


def _both_col(sql, col_bytes):
    return _col(sql, col_bytes, False), _col(sql, col_bytes, True)


# ---------------------------------------------------------------------------
# 1-4. Low-level: build_in_list_carchar + vector_in_list
# ---------------------------------------------------------------------------

def test_build_returns_perfect_hash_when_flag_set():
    os.environ["OPTERYX_PERFECT_HASH"] = "1"
    try:
        from opteryx.compiled.structures.perfect_hash_set import PerfectHashSet
        s = build_in_list_carchar([1, 2, 3, 4])
        assert isinstance(s, PerfectHashSet)
    finally:
        del os.environ["OPTERYX_PERFECT_HASH"]


def test_build_returns_carchar_without_flag():
    from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
    os.environ.pop("OPTERYX_PERFECT_HASH", None)
    s = build_in_list_carchar([1, 2, 3, 4])
    assert isinstance(s, CarcharSetWrapper)


def test_build_returns_carchar_for_large_range():
    """Range > 262144 must fall back to CarcharSetWrapper even with flag set."""
    from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
    os.environ["OPTERYX_PERFECT_HASH"] = "1"
    try:
        s = build_in_list_carchar([0, 300_000])
        assert isinstance(s, CarcharSetWrapper)
    finally:
        del os.environ["OPTERYX_PERFECT_HASH"]


def test_vector_in_list_perfect_hash_membership():
    """vector_in_list with PerfectHashSet returns same results as CarcharSet."""
    import draken

    tbl = pa.table({"id": pa.array([1, 2, 3, 4, 5], type=pa.int64())})
    m = draken.Morsel.from_arrow(tbl)
    vec = m.column(b"id")

    os.environ.pop("OPTERYX_PERFECT_HASH", None)
    s_carchar = build_in_list_carchar([1, 3, 5])
    result_carchar = vector_in_list(vec, s_carchar).to_arrow().to_pylist()

    os.environ["OPTERYX_PERFECT_HASH"] = "1"
    try:
        s_phash = build_in_list_carchar([1, 3, 5])
        result_phash = vector_in_list(vec, s_phash).to_arrow().to_pylist()
    finally:
        del os.environ["OPTERYX_PERFECT_HASH"]

    assert result_carchar == result_phash == [True, False, True, False, True]


def test_vector_in_list_perfect_hash_negation():
    """vector_in_list with negate=True returns complement."""
    import draken

    tbl = pa.table({"id": pa.array([1, 2, 3, 4, 5], type=pa.int64())})
    m = draken.Morsel.from_arrow(tbl)
    vec = m.column(b"id")

    os.environ["OPTERYX_PERFECT_HASH"] = "1"
    try:
        s = build_in_list_carchar([1, 3, 5])
        result = vector_in_list(vec, s, negate=True).to_arrow().to_pylist()
    finally:
        del os.environ["OPTERYX_PERFECT_HASH"]

    assert result == [False, True, False, True, False]


# ---------------------------------------------------------------------------
# 5-7. SQL: IN-list correctness
# ---------------------------------------------------------------------------

def test_sql_in_list_small_range_row_count():
    r_off, r_on = _both_rows("SELECT name FROM $planets WHERE id IN (1, 2, 3)")
    assert r_off == r_on == 3


def test_sql_in_list_small_range_values():
    off, on = _both_col("SELECT name FROM $planets WHERE id IN (1, 2, 3)", b"name")
    assert off == on


def test_sql_in_list_single_value():
    r_off, r_on = _both_rows("SELECT name FROM $planets WHERE id IN (1)")
    assert r_off == r_on == 1


def test_sql_in_list_no_matches():
    r_off, r_on = _both_rows("SELECT name FROM $planets WHERE id IN (100, 200, 300)")
    assert r_off == r_on == 0


def test_sql_in_list_large_range():
    """Large integer range (> 262144 cap) must fall back without errors."""
    r_off, r_on = _both_rows("SELECT name FROM $planets WHERE id IN (0, 300000)")
    assert r_off == r_on == 0


def test_sql_not_in_list_row_count():
    r_off, r_on = _both_rows("SELECT name FROM $planets WHERE id NOT IN (1, 2, 3)")
    assert r_off == r_on == 6


def test_sql_not_in_list_values():
    off, on = _both_col("SELECT name FROM $planets WHERE id NOT IN (1, 2, 3)", b"name")
    assert off == on


# ---------------------------------------------------------------------------
# 8-9. SQL: DISTINCT correctness
# ---------------------------------------------------------------------------

def test_sql_distinct_int64_row_count():
    r_off, r_on = _both_rows("SELECT DISTINCT id FROM $planets")
    assert r_off == r_on == 9


def test_sql_distinct_int64_values():
    off, on = _both_col("SELECT DISTINCT id FROM $planets", b"id")
    assert off == on


def test_sql_distinct_narrow_int_correctness():
    """
    testdata/narrow_ints has int8/int16 parquet columns; rugo upcasts them to
    Int64Vector on decode, so the DISTINCT PerfectHashSet (IntegerVector-only)
    path is not taken. Correctness must still hold with the flag on.
    """
    off8, on8 = _both_col("SELECT DISTINCT v8 FROM testdata.narrow_ints", b"v8")
    off16, on16 = _both_col("SELECT DISTINCT v16 FROM testdata.narrow_ints", b"v16")
    assert off8 == on8 == sorted([-5, 1, 2, 3, 4])
    assert off16 == on16 == sorted([-500, 100, 200, 300, 400])


# ---------------------------------------------------------------------------
# 10-13. SQL: Semi-join and anti-join correctness
# ---------------------------------------------------------------------------

def test_sql_semi_join_row_count():
    sql = "SELECT name FROM $planets p WHERE p.id IN (SELECT id FROM $planets WHERE id <= 5)"
    r_off, r_on = _both_rows(sql)
    assert r_off == r_on == 5


def test_sql_semi_join_values():
    sql = "SELECT name FROM $planets p WHERE p.id IN (SELECT id FROM $planets WHERE id <= 5)"
    off, on = _both_col(sql, b"name")
    assert off == on


def test_sql_semi_join_empty_right():
    sql = "SELECT name FROM $planets p WHERE p.id IN (SELECT id FROM $planets WHERE id < 0)"
    r_off, r_on = _both_rows(sql)
    assert r_off == r_on == 0


def test_sql_semi_join_full_right():
    sql = "SELECT name FROM $planets p WHERE p.id IN (SELECT id FROM $planets)"
    total = _rows("SELECT name FROM $planets", False)
    r_off, r_on = _both_rows(sql)
    assert r_off == r_on == total


def test_sql_anti_join_row_count():
    sql = "SELECT name FROM $planets p WHERE p.id NOT IN (SELECT id FROM $planets WHERE id > 5)"
    r_off, r_on = _both_rows(sql)
    assert r_off == r_on == 5


def test_sql_anti_join_values():
    sql = "SELECT name FROM $planets p WHERE p.id NOT IN (SELECT id FROM $planets WHERE id > 5)"
    off, on = _both_col(sql, b"name")
    assert off == on


def test_sql_anti_join_empty_right():
    sql = "SELECT name FROM $planets p WHERE p.id NOT IN (SELECT id FROM $planets WHERE id < 0)"
    total = _rows("SELECT name FROM $planets", False)
    r_off, r_on = _both_rows(sql)
    assert r_off == r_on == total


if __name__ == "__main__":
    from tests import run_tests

    run_tests()
