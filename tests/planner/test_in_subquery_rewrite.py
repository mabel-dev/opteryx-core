"""
Tests for IN (<subquery>) → LEFT SEMI JOIN plan rewrite.

Coverage:
  - Basic IN (SELECT ...) correctness
  - IN with additional WHERE predicates (remaining filter)
  - Multiple IN subqueries in one WHERE clause (fixed-point loop)
  - IN on a subquery that projects an aliased column
  - IN subquery with an inner WHERE clause
  - Qualified (table.col IN) and unqualified (col IN) outer column references
  - Empty subquery result (IN returns nothing)
  - IN where all outer rows match
  - Aggregate over IN-filtered result
  - IN inside a FROM-clause subquery
  - NOT IN is supported and null-aware (a NULL in the subquery yields no rows)
  - Multi-column subquery raises UnsupportedSyntaxError
  - Plan structure: result contains a Join node, not a Filter with embedded SUBQUERY
  - Semi-join semantics: no duplicate outer rows even if subquery has duplicates
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _execute(sql):
    """Execute SQL; return list of Morsels."""
    sess = opteryx.session()
    return list(sess.execute_to_morsels(sql))


def _to_dict(morsels):
    """Collect morsels into a column-keyed dict."""
    if not morsels:
        return {}
    import pyarrow as pa
    tables = [m.to_arrow() for m in morsels if m.num_rows > 0]
    if not tables:
        col_names = morsels[0].column_names
        return {c: [] for c in col_names}
    tbl = pa.concat_tables(tables)
    return tbl.to_pydict()


def run(sql):
    """Execute SQL and return column-keyed dict of results."""
    return _to_dict(_execute(sql))


def row_count(sql):
    """Return total row count for a query."""
    return sum(m.num_rows for m in _execute(sql))


def scalar(sql):
    """Return the single scalar value from a COUNT(*) style query."""
    d = run(sql)
    return list(d.values())[0][0]


def sorted_col(sql, col):
    """Return sorted list of values in `col` from the query result."""
    d = run(sql)
    return sorted(d[col])


# ---------------------------------------------------------------------------
# 1. Basic correctness
# ---------------------------------------------------------------------------

def test_in_subquery_basic():
    """IN (SELECT id ...) returns same rows as a direct filter on the same predicate."""
    in_result = sorted_col(
        "SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE id <= 4)",
        "name",
    )
    direct_result = sorted_col(
        "SELECT name FROM $planets WHERE id <= 4",
        "name",
    )
    assert in_result == direct_result


def test_in_subquery_row_count_matches_direct_filter():
    """Row count via IN equals a direct WHERE filter on the equivalent predicate."""
    n_in = row_count(
        "SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE gravity > 10)"
    )
    n_direct = row_count(
        "SELECT name FROM $planets WHERE gravity > 10"
    )
    assert n_in == n_direct


def test_in_subquery_inner_planets():
    """IN filtering to planets with id in {1, 2, 3} returns Mercury, Venus, Earth."""
    result = sorted_col(
        "SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE id < 4)",
        "name",
    )
    assert result == sorted(["Mercury", "Venus", "Earth"])


def test_in_subquery_outer_giants():
    """IN filtering to outer planets (id > 4) returns correct subset."""
    n_in = row_count(
        "SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE id > 4)"
    )
    n_direct = row_count("SELECT name FROM $planets WHERE id > 4")
    assert n_in == n_direct


# ---------------------------------------------------------------------------
# 2. Edge cases: empty and full subquery
# ---------------------------------------------------------------------------

def test_in_subquery_empty_subquery_returns_nothing():
    """IN on an empty subquery must return zero rows."""
    n = row_count(
        "SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE id < 0)"
    )
    assert n == 0


def test_in_subquery_full_match_returns_all():
    """IN where every outer row matches returns all outer rows."""
    total = row_count("SELECT name FROM $planets")
    n = row_count("SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets)")
    assert n == total


# ---------------------------------------------------------------------------
# 3. Qualified outer column reference
# ---------------------------------------------------------------------------

def test_in_subquery_qualified_outer_column():
    """Qualified outer column (alias.col IN ...) resolves correctly."""
    result = sorted_col(
        """
        SELECT name FROM $planets AS p
        WHERE p.id IN (SELECT id FROM $planets WHERE id <= 3)
        """,
        "name",
    )
    direct = sorted_col("SELECT name FROM $planets WHERE id <= 3", "name")
    assert result == direct


# ---------------------------------------------------------------------------
# 4. Remaining predicates (conjunctive AND)
# ---------------------------------------------------------------------------

def test_in_subquery_with_and_predicate():
    """IN + AND predicate: both constraints are applied correctly."""
    result = sorted_col(
        """
        SELECT name FROM $planets
        WHERE id IN (SELECT id FROM $planets WHERE gravity > 5)
          AND id < 6
        """,
        "name",
    )
    direct = sorted_col(
        "SELECT name FROM $planets WHERE gravity > 5 AND id < 6",
        "name",
    )
    assert result == direct


def test_in_subquery_combined_predicates_row_count():
    """Row count with combined IN + LIKE filter matches a direct equivalent query."""
    n_in = row_count(
        "SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE id > 3) AND name LIKE '%s%'"
    )
    n_direct = row_count(
        "SELECT name FROM $planets WHERE id > 3 AND name LIKE '%s%'"
    )
    assert n_in == n_direct


def test_in_subquery_predicate_on_left():
    """AND predicate to the left of IN is also applied (AND is commutative in plan)."""
    result = sorted_col(
        """
        SELECT name FROM $planets
        WHERE id < 6
          AND id IN (SELECT id FROM $planets WHERE gravity > 5)
        """,
        "name",
    )
    direct = sorted_col(
        "SELECT name FROM $planets WHERE id < 6 AND gravity > 5",
        "name",
    )
    assert result == direct


# ---------------------------------------------------------------------------
# 5. Multiple IN subqueries (fixed-point rewriter loop)
# ---------------------------------------------------------------------------

def test_in_subquery_two_in_clauses():
    """Two IN subqueries in one WHERE are each rewritten in successive rewriter passes."""
    result = sorted_col(
        """
        SELECT name FROM $planets
        WHERE id IN (SELECT id FROM $planets WHERE gravity > 5)
          AND id IN (SELECT id FROM $planets WHERE id < 8)
        """,
        "name",
    )
    direct = sorted_col(
        "SELECT name FROM $planets WHERE gravity > 5 AND id < 8",
        "name",
    )
    assert result == direct


def test_in_subquery_three_in_clauses():
    """Three IN subqueries in one WHERE are all rewritten correctly."""
    result = sorted_col(
        """
        SELECT name FROM $planets
        WHERE id IN (SELECT id FROM $planets WHERE gravity > 3)
          AND id IN (SELECT id FROM $planets WHERE id < 9)
          AND id IN (SELECT id FROM $planets WHERE id > 1)
        """,
        "name",
    )
    direct = sorted_col(
        "SELECT name FROM $planets WHERE gravity > 3 AND id < 9 AND id > 1",
        "name",
    )
    assert result == direct


# ---------------------------------------------------------------------------
# 6. Aliased subquery column
# ---------------------------------------------------------------------------

def test_in_subquery_aliased_column():
    """Subquery projects column with AS alias; outer reference resolves correctly."""
    result = sorted_col(
        "SELECT name FROM $planets WHERE id IN (SELECT id AS planet_id FROM $planets WHERE id <= 5)",
        "name",
    )
    direct = sorted_col("SELECT name FROM $planets WHERE id <= 5", "name")
    assert result == direct


# ---------------------------------------------------------------------------
# 7. Cross-table IN subquery
# ---------------------------------------------------------------------------

def test_in_subquery_cross_table():
    """IN subquery references a different table; result is subset of outer table."""
    total = row_count("SELECT name FROM $planets")
    n = row_count(
        "SELECT name FROM $planets WHERE id IN (SELECT DISTINCT planetId FROM testdata.satellites)"
    )
    assert 0 < n < total


# ---------------------------------------------------------------------------
# 8. Aggregate over IN-filtered result
# ---------------------------------------------------------------------------

def test_in_subquery_aggregate_count():
    """COUNT(*) over an IN-filtered relation gives the correct count."""
    n = scalar(
        "SELECT COUNT(*) FROM $planets WHERE id IN (SELECT id FROM $planets WHERE id IN (1, 2, 3))"
    )
    assert n == 3


def test_in_subquery_aggregate_sum():
    """SUM over an IN-filtered result is correct."""
    s = scalar(
        "SELECT SUM(id) FROM $planets WHERE id IN (SELECT id FROM $planets WHERE id <= 3)"
    )
    assert s == 6  # 1 + 2 + 3


# ---------------------------------------------------------------------------
# 9. IN inside a FROM-clause subquery
# ---------------------------------------------------------------------------

def test_in_subquery_wrapped_in_from_subquery():
    """IN rewrite works when the query is used as a FROM-clause subquery."""
    n = scalar(
        """
        SELECT COUNT(*) FROM (
            SELECT name FROM $planets
            WHERE id IN (SELECT id FROM $planets WHERE id <= 4)
        ) AS sub
        """
    )
    assert n == 4


# ---------------------------------------------------------------------------
# 10. Semi-join semantics: no row duplication
# ---------------------------------------------------------------------------

def test_in_subquery_no_row_duplication():
    """
    Even if the subquery returns many duplicate keys, the semi-join must not
    duplicate outer rows — it is an existence check, not a full join.
    """
    total_planets = row_count("SELECT name FROM $planets")
    n = row_count(
        "SELECT name FROM $planets WHERE id IN (SELECT planetId FROM testdata.satellites)"
    )
    assert n <= total_planets, (
        f"Semi-join duplicated rows: got {n} > total {total_planets}"
    )


# ---------------------------------------------------------------------------
# 11. Only outer columns in result (semi-join: no right-side column leakage)
# ---------------------------------------------------------------------------

def test_in_subquery_only_outer_columns():
    """Semi-join must not add subquery columns to the result set."""
    d = run("SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE id < 4)")
    assert list(d.keys()) == ["name"], f"Unexpected columns: {list(d.keys())}"


# ---------------------------------------------------------------------------
# 12. Plan structure: join appears, raw SUBQUERY expression does not
# ---------------------------------------------------------------------------

def test_in_subquery_plan_shows_join():
    """The optimized plan must contain a semi-join — the subquery is eliminated, not
    carried into execution as a SUBQUERY expression.

    The elimination is POST-BIND: it is `DecorrelateSubqueryStrategy` in the optimizer,
    not the pre-bind plan rewriter (the per-syntax-form `in_subquery_to_join` strategy
    was deleted when all subquery forms were unified onto one decorrelation pass). So
    this asserts on the optimized plan, which is where the join now appears.
    """
    d = run("EXPLAIN SELECT name FROM $planets WHERE id IN (SELECT id FROM $planets WHERE id < 5)")
    plan_text = "\n".join(
        v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else str(v)
        for col in d.values()
        for v in col
    )

    assert "LEFT SEMI JOIN" in plan_text, f"Expected a semi-join in the plan; got:\n{plan_text}"
    assert "decorrelate in subquery" in plan_text, (
        f"Expected the decorrelation optimization to fire; got:\n{plan_text}"
    )


# ---------------------------------------------------------------------------
# 13. Error cases
# ---------------------------------------------------------------------------

def test_not_in_subquery_is_supported():
    """NOT IN (<subquery>) is supported — it rewrites to a null-aware anti-join.

    It used to be rejected outright over NULL semantics; that restriction was lifted
    once the anti-join was made null-aware, so this pins the answer instead.
    """
    result = sorted_col(
        "SELECT name FROM $planets WHERE id NOT IN (SELECT id FROM $planets WHERE id > 5)",
        "name",
    )
    direct = sorted_col("SELECT name FROM $planets WHERE id <= 5", "name")
    assert result == direct


def test_not_in_subquery_is_null_aware():
    """A NULL anywhere in the subquery makes `x NOT IN (…)` UNKNOWN for EVERY row.

    This is the rule NOT IN has and NOT EXISTS does not — a plain anti-join would
    return the non-matching rows here instead of nothing.
    """
    n = row_count(
        """
        SELECT name FROM $planets
        WHERE id NOT IN (SELECT CASE WHEN id > 5 THEN id ELSE NULL END FROM $planets)
        """
    )
    assert n == 0

    # The positive form is unaffected: IN simply ignores the NULLs.
    assert sorted_col(
        """
        SELECT name FROM $planets
        WHERE id IN (SELECT CASE WHEN id > 5 THEN id ELSE NULL END FROM $planets)
        """,
        "name",
    ) == sorted_col("SELECT name FROM $planets WHERE id > 5", "name")


def test_in_subquery_multi_column_raises():
    """IN (<subquery>) projecting multiple columns must be rejected."""
    with pytest.raises((UnsupportedSyntaxError, Exception)):
        _execute("SELECT name FROM $planets WHERE id IN (SELECT id, name FROM $planets WHERE id < 5)")


# ---------------------------------------------------------------------------
# 14. Subquery with inner filter (subquery is not trivially passthrough)
# ---------------------------------------------------------------------------

def test_in_subquery_with_inner_where():
    """Subquery itself has a non-trivial WHERE clause that filters its build side."""
    result = sorted_col(
        """
        SELECT name FROM $planets
        WHERE id IN (
            SELECT id FROM $planets
            WHERE gravity BETWEEN 5 AND 15
        )
        """,
        "name",
    )
    direct = sorted_col(
        "SELECT name FROM $planets WHERE gravity BETWEEN 5 AND 15",
        "name",
    )
    assert result == direct


if __name__ == "__main__":
    import traceback

    tests = [
        test_in_subquery_basic,
        test_in_subquery_row_count_matches_direct_filter,
        test_in_subquery_inner_planets,
        test_in_subquery_outer_giants,
        test_in_subquery_empty_subquery_returns_nothing,
        test_in_subquery_full_match_returns_all,
        test_in_subquery_qualified_outer_column,
        test_in_subquery_with_and_predicate,
        test_in_subquery_combined_predicates_row_count,
        test_in_subquery_predicate_on_left,
        test_in_subquery_two_in_clauses,
        test_in_subquery_three_in_clauses,
        test_in_subquery_aliased_column,
        test_in_subquery_cross_table,
        test_in_subquery_aggregate_count,
        test_in_subquery_aggregate_sum,
        test_in_subquery_wrapped_in_from_subquery,
        test_in_subquery_no_row_duplication,
        test_in_subquery_only_outer_columns,
        test_in_subquery_plan_shows_join,
        test_not_in_subquery_is_supported,
        test_not_in_subquery_is_null_aware,
        test_in_subquery_multi_column_raises,
        test_in_subquery_with_inner_where,
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
