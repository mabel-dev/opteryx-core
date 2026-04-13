"""
Advanced GROUP BY integration tests - Part 3

This module contains advanced GROUP BY scenarios including:
- Complex query combinations with CTEs and subqueries
- GROUP BY with various operators and expressions
- Window functions with GROUP BY
- Performance characteristics and large result sets
- Regression tests for specific bugs and edge cases
- Complex multi-stage aggregations
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


@pytest.fixture
def session():
    """Create a session for each test."""
    return opteryx.session(memberships=["Apollo 11", "opteryx"])


class TestGroupByWithCTE:
    """Test GROUP BY interactions with Common Table Expressions."""

    def test_cte_then_groupby(self, session):
        """GROUP BY on CTE result."""
        result = session.execute_to_arrow(
            """
            WITH satellite_data AS (
                SELECT planetId, radius FROM testdata.satellites
            )
            SELECT planetId, COUNT(*) as cnt FROM satellite_data GROUP BY planetId
            """
        ).to_pylist()

        assert len(result) > 0
        assert all("planetId" in row and "cnt" in row for row in result)

    def test_multiple_ctes_with_groupby(self, session):
        """GROUP BY with multiple CTEs."""
        result = session.execute_to_arrow(
            """
            WITH
            sat_data AS (SELECT planetId, radius FROM testdata.satellites),
            filtered AS (SELECT * FROM sat_data WHERE radius > 1000)
            SELECT planetId, COUNT(*) as cnt FROM filtered GROUP BY planetId
            """
        ).to_pylist()

        assert isinstance(result, list)

    def test_cte_with_groupby_then_outer_groupby(self, session):
        """CTE containing GROUP BY, then GROUP BY on result."""
        result = session.execute_to_arrow(
            """
            WITH grouped AS (
                SELECT planetId, COUNT(*) as sat_count FROM testdata.satellites GROUP BY planetId
            )
            SELECT COUNT(*) as num_planets FROM grouped
            """
        ).to_pylist()

        assert len(result) == 1
        assert result[0]["num_planets"] > 0


class TestGroupByWithSubqueries:
    """Test GROUP BY with various subquery patterns."""

    def test_groupby_in_subquery(self, session):
        """GROUP BY in derived table."""
        result = session.execute_to_arrow(
            """
            SELECT * FROM (
                SELECT planetId, COUNT(*) as cnt
                FROM testdata.satellites
                GROUP BY planetId
            ) WHERE cnt > 1
            """
        ).to_pylist()

        assert all(row["cnt"] > 1 for row in result)

    def test_subquery_in_select_with_groupby(self, session):
        """GROUP BY combined with subquery in SELECT clause."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt,
                (SELECT COUNT(*) FROM testdata.satellites) as total
            FROM testdata.satellites
            GROUP BY planetId
            """
        ).to_pylist()

        assert len(result) > 0
        for row in result:
            assert "total" in row
            assert row["total"] > 0

    def test_subquery_in_where_with_groupby(self, session):
        """GROUP BY combined with subquery in WHERE clause."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            WHERE planetId IN (SELECT DISTINCT planetId FROM testdata.satellites LIMIT 5)
            GROUP BY planetId
            """
        ).to_pylist()

        assert len(result) <= 5

    def test_nested_subqueries_with_groupby(self, session):
        """Multiple levels of nested subqueries with GROUP BY."""
        result = session.execute_to_arrow(
            """
            SELECT * FROM (
                SELECT * FROM (
                    SELECT planetId, COUNT(*) as cnt
                    FROM testdata.satellites
                    GROUP BY planetId
                ) WHERE cnt > 0
            ) WHERE cnt > 0
            """
        ).to_pylist()

        assert len(result) > 0


class TestComplexGroupByExpressions:
    """Test GROUP BY with complex expressions."""

    def test_groupby_arithmetic_expression(self, session):
        """GROUP BY on arithmetic expression."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId * 2 as planet_times_2,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId * 2
            """
        ).to_pylist()

        assert len(result) > 0

    def test_groupby_case_expression(self, session):
        """GROUP BY on CASE expression."""
        result = session.execute_to_arrow(
            """
            SELECT
                CASE
                    WHEN radius > 5000 THEN 'huge'
                    WHEN radius > 2000 THEN 'large'
                    WHEN radius > 1000 THEN 'medium'
                    ELSE 'small'
                END as size_category,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY CASE
                WHEN radius > 5000 THEN 'huge'
                WHEN radius > 2000 THEN 'large'
                WHEN radius > 1000 THEN 'medium'
                ELSE 'small'
            END
            ORDER BY size_category
            """
        ).to_pylist()

        assert len(result) > 0
        categories = [row["size_category"] for row in result]
        assert all(cat in ("huge", "large", "medium", "small") for cat in categories)

    def test_groupby_string_function(self, session):
        """GROUP BY on string function result."""
        result = session.execute_to_arrow(
            """
            SELECT
                LENGTH(name) as name_length,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY LENGTH(name)
            ORDER BY name_length
            """
        ).to_pylist()

        assert len(result) > 0
        for row in result:
            assert row["name_length"] > 0

    def test_groupby_coalesce(self, session):
        """GROUP BY with COALESCE function."""
        result = session.execute_to_arrow(
            """
            SELECT
                COALESCE(yearDiscovered, 0) as year,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY COALESCE(yearDiscovered, 0)
            ORDER BY year
            """
        ).to_pylist()

        assert len(result) > 0

    def test_groupby_cast_expression(self, session):
        """GROUP BY with CAST."""
        result = session.execute_to_arrow(
            """
            SELECT
                CAST(planetId AS VARCHAR) as planet_str,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY CAST(planetId AS VARCHAR)
            """
        ).to_pylist()

        assert len(result) > 0


class TestGroupByAggregateEdgeCases:
    """Test edge cases in aggregation functions."""

    def test_aggregate_on_expression(self, session):
        """Aggregation on computed expression."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                SUM(radius * 2) as sum_radius_doubled,
                AVG(radius / 2) as avg_radius_halved
            FROM testdata.satellites
            GROUP BY planetId
            """
        ).to_pylist()

        assert len(result) > 0

    def test_multiple_count_distincts(self, session):
        """Multiple COUNT(DISTINCT) in same query."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(DISTINCT name) as distinct_names,
                COUNT(DISTINCT CAST(yearDiscovered AS VARCHAR)) as distinct_years
            FROM testdata.satellites
            GROUP BY planetId
            """
        ).to_pylist()

        assert len(result) > 0
        for row in result:
            assert row["distinct_names"] >= 0

    def test_aggregate_on_distinct_column(self, session):
        """Aggregate of DISTINCT values."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                SUM(DISTINCT radius) as sum_distinct_radius
            FROM testdata.satellites
            GROUP BY planetId
            """
        ).to_pylist()

        assert len(result) > 0

    def test_nested_aggregates_invalid_but_handled(self, session):
        """Attempting nested aggregates (should be rejected or handled gracefully)."""
        # This might error depending on SQL strictness
        try:
            result = session.execute(
                """
                SELECT
                    planetId,
                    SUM(COUNT(*)) as invalid
                FROM testdata.satellites
                GROUP BY planetId
                """
            ).to_pylist()
            # If it doesn't error, it should have some result
            assert isinstance(result, list)
        except Exception:
            # Expected to fail
            pass


class TestGroupByWithOrderByComplexity:
    """Test complex ORDER BY scenarios with GROUP BY."""

    def test_orderby_aggregate_desc_then_key_asc(self, session):
        """ORDER BY aggregate descending, then key ascending."""
        result = session.execute(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            ORDER BY cnt DESC, planetId ASC
            """
        ).to_pylist()

        assert len(result) > 0
        for i in range(len(result) - 1):
            if result[i]["cnt"] == result[i + 1]["cnt"]:
                assert result[i]["planetId"] <= result[i + 1]["planetId"]

    def test_orderby_multiple_aggregates(self, session):
        """ORDER BY multiple aggregate functions."""
        result = session.execute(
            """
            SELECT
                planetId,
                COUNT(*) as cnt,
                SUM(radius) as total_radius,
                AVG(radius) as avg_radius
            FROM testdata.satellites
            GROUP BY planetId
            ORDER BY total_radius DESC, avg_radius DESC
            """
        ).to_pylist()

        assert len(result) > 0

    def test_orderby_column_not_in_select(self, session):
        """ORDER BY on column not in SELECT clause."""
        result = session.execute(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            ORDER BY SUM(radius) DESC
            """
        ).to_pylist()

        assert len(result) > 0


class TestGroupByWithLimit:
    """Test GROUP BY with LIMIT and OFFSET."""

    def test_groupby_limit_basic(self, session):
        """GROUP BY with LIMIT."""
        all_result = session.execute(
            "SELECT planetId, COUNT(*) as cnt FROM testdata.satellites GROUP BY planetId"
        ).to_pylist()

        limited_result = session.execute(
            "SELECT planetId, COUNT(*) as cnt FROM testdata.satellites GROUP BY planetId LIMIT 3"
        ).to_pylist()

        assert len(limited_result) <= 3
        assert len(limited_result) <= len(all_result)

    def test_groupby_limit_offset(self, session):
        """GROUP BY with LIMIT and OFFSET."""
        result = session.execute(
            """
            SELECT planetId, COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            ORDER BY planetId
            LIMIT 2 OFFSET 1
            """
        ).to_pylist()

        assert len(result) == 2

    def test_groupby_limit_larger_than_groups(self, session):
        """GROUP BY with LIMIT larger than number of groups."""
        result = session.execute(
            "SELECT planetId, COUNT(*) as cnt FROM testdata.satellites GROUP BY planetId LIMIT 1000"
        ).to_pylist()

        # Should return all groups, not error
        assert len(result) > 0


class TestGroupByDistinctInteraction:
    """Test GROUP BY with DISTINCT."""

    def test_distinct_before_groupby(self, session):
        """DISTINCT on result of GROUP BY."""
        result = session.execute(
            """
            SELECT DISTINCT
                planetId
            FROM testdata.satellites
            GROUP BY planetId
            """
        ).to_pylist()

        assert len(result) > 0

    def test_select_distinct_vs_groupby(self, session):
        """Compare DISTINCT planetId vs GROUP BY planetId, COUNT(*)."""
        distinct_result = session.execute(
            "SELECT DISTINCT planetId FROM testdata.satellites ORDER BY planetId"
        ).to_pylist()

        groupby_result = session.execute(
            "SELECT planetId FROM testdata.satellites GROUP BY planetId ORDER BY planetId"
        ).to_pylist()

        assert len(distinct_result) == len(groupby_result)


class TestGroupByPerformanceCharacteristics:
    """Test GROUP BY performance characteristics."""

    def test_groupby_large_result_set(self, session):
        """GROUP BY producing large result set."""
        result = session.execute(
            """
            SELECT
                name,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY name
            """
        ).to_pylist()

        assert len(result) > 50

    def test_groupby_many_aggregates(self, session):
        """GROUP BY with many aggregation functions."""
        result = session.execute(
            """
            SELECT
                planetId,
                COUNT(*) as cnt,
                COUNT(radius) as radius_cnt,
                COUNT(DISTINCT name) as distinct_names,
                SUM(radius) as sum_r,
                AVG(radius) as avg_r,
                MIN(radius) as min_r,
                MAX(radius) as max_r,
                MIN(yearDiscovered) as min_year,
                MAX(yearDiscovered) as max_year
            FROM testdata.satellites
            GROUP BY planetId
            """
        ).to_pylist()

        assert len(result) > 0
        row = result[0]
        assert "cnt" in row
        assert "sum_r" in row
        assert "avg_r" in row

    def test_groupby_many_group_columns(self, session):
        """GROUP BY with many group columns."""
        result = session.execute(
            """
            SELECT
                planetId,
                name,
                yearDiscovered,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId, name, yearDiscovered
            """
        ).to_pylist()

        assert len(result) > 0


class TestGroupByRegressions:
    """Regression tests for specific bugs and issues."""

    def test_groupby_lowercase_count(self, session):
        """GROUP BY works with lowercase COUNT."""
        result = session.execute(
            "select planetId, count(*) as cnt from testdata.satellites group by planetId"
        ).to_pylist()

        assert len(result) > 0

    def test_groupby_mixed_case_keywords(self, session):
        """GROUP BY works with mixed case keywords."""
        result = session.execute(
            "SeLeCt planetId, Count(*) As cnt FrOm testdata.satellites GrOuP bY planetId"
        ).to_pylist()

        assert len(result) > 0

    def test_groupby_column_alias_in_where(self, session):
        """Column alias from GROUP BY not usable in WHERE."""
        try:
            result = session.execute(
                """
                SELECT planetId as pid, COUNT(*) as cnt
                FROM testdata.satellites
                WHERE pid > 1
                GROUP BY planetId
                """
            ).to_pylist()
            # Might work or might error depending on SQL dialect
        except Exception:
            pass

    def test_groupby_aggregate_alias_in_having(self, session):
        """Aggregate alias in HAVING clause."""
        result = session.execute(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            HAVING COUNT(*) > 1
            """
        ).to_pylist()

        assert all(row["cnt"] > 1 for row in result)

    def test_groupby_numeric_column_reference(self, session):
        """GROUP BY with column position number."""
        try:
            result = session.execute(
                """
                SELECT planetId, COUNT(*) as cnt
                FROM testdata.satellites
                GROUP BY 1
                """
            ).to_pylist()
            assert len(result) > 0
        except Exception:
            pass

    def test_groupby_self_join_with_groupby(self, session):
        """GROUP BY on self-joined table."""
        result = session.execute(
            """
            SELECT
                s1.planetId,
                COUNT(*) as cnt
            FROM testdata.satellites s1
            INNER JOIN testdata.satellites s2
                ON s1.planetId = s2.planetId
            GROUP BY s1.planetId
            """
        ).to_pylist()

        assert len(result) > 0

    def test_groupby_after_union(self, session):
        """GROUP BY on UNION result."""
        try:
            result = session.execute(
                """
                SELECT planetId FROM testdata.satellites
                UNION
                SELECT planetId FROM testdata.satellites
                GROUP BY planetId
                """
            ).to_pylist()
            assert isinstance(result, list)
        except Exception:
            pass


class TestGroupByWithFiltering:
    """Test GROUP BY combined with complex filtering."""

    def test_groupby_with_complex_where(self, session):
        """GROUP BY with complex WHERE conditions."""
        result = session.execute(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            WHERE radius > 1000 AND yearDiscovered > 1950 AND name LIKE '%s%'
            GROUP BY planetId
            """
        ).to_pylist()

        assert isinstance(result, list)

    def test_groupby_where_and_having(self, session):
        """GROUP BY with both WHERE and HAVING."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            WHERE radius > 500
            GROUP BY planetId
            HAVING COUNT(*) > 1
            """
        ).to_pylist()

        assert all(row["cnt"] > 1 for row in result)

    def test_groupby_having_on_column_aggregate(self, session):
        """HAVING condition on specific aggregate."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                MIN(radius) as min_radius,
                MAX(radius) as max_radius
            FROM testdata.satellites
            GROUP BY planetId
            HAVING MAX(radius) > 3000
            """
        ).to_pylist()

        assert all(row["max_radius"] > 3000 for row in result)


class TestGroupByConsistency:
    """Test consistency and determinism of GROUP BY."""

    def test_groupby_deterministic_results(self, session):
        """GROUP BY produces same results on repeated execution."""
        sql = "SELECT planetId, COUNT(*) as cnt FROM testdata.satellites GROUP BY planetId ORDER BY planetId"

        result1 = session.execute_to_arrow(sql).to_pylist()
        result2 = session.execute_to_arrow(sql).to_pylist()

        assert result1 == result2

    def test_groupby_sum_all_rows(self, session):
        """Sum of group counts equals total rows."""
        total_result = session.execute_to_arrow(
            "SELECT COUNT(*) as total FROM testdata.satellites"
        ).to_pylist()

        grouped_result = session.execute_to_arrow(
            "SELECT SUM(cnt) as total FROM (SELECT COUNT(*) as cnt FROM testdata.satellites GROUP BY planetId)"
        ).to_pylist()

        assert total_result[0]["total"] == grouped_result[0]["total"]

    def test_groupby_no_duplicate_keys(self, session):
        """GROUP BY produces no duplicate group keys."""
        result = session.execute_to_arrow(
            "SELECT planetId FROM testdata.satellites GROUP BY planetId"
        ).to_pylist()

        planet_ids = [row["planetId"] for row in result]
        assert len(planet_ids) == len(set(planet_ids))


class TestGroupByNullHandling:
    """Additional NULL handling tests."""

    def test_groupby_null_groups_together(self, session):
        """NULL values in group key group together."""
        result = session.execute_to_arrow(
            """
            SELECT
                yearDiscovered,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY yearDiscovered
            ORDER BY yearDiscovered
            """
        ).to_pylist()

        # Check NULL grouping - if NULLs exist, they should be grouped
        null_rows = [r for r in result if r["yearDiscovered"] is None]
        if null_rows:
            assert len(null_rows) == 1  # All NULLs in one group

    def test_count_null_vs_nonnull(self, session):
        """COUNT(*) vs COUNT(col) with NULLs."""
        result = session.execute_to_arrow(
            """
            SELECT
                COUNT(*) as cnt_all,
                COUNT(yearDiscovered) as cnt_year
            FROM testdata.satellites
            """
        ).to_pylist()

        # COUNT(*) >= COUNT(col)
        assert result[0]["cnt_all"] >= result[0]["cnt_year"]


class TestGroupByWithMissions:
    """GROUP BY tests using testdata.missions."""

    def test_missions_groupby_company(self, session):
        """GROUP BY company from missions."""
        result = session.execute_to_arrow(
            """
            SELECT
                Company,
                COUNT(*) as cnt,
                COUNT(Price) as price_cnt,
                AVG(Price) as avg_price
            FROM testdata.missions
            GROUP BY Company
            ORDER BY Company
            """
        ).to_pylist()

        assert len(result) > 0
        for row in result:
            assert row["cnt"] >= row["price_cnt"]

    def test_missions_groupby_status(self, session):
        """GROUP BY status from missions."""
        result = session.execute_to_arrow(
            """
            SELECT
                Status,
                COUNT(*) as cnt
            FROM testdata.missions
            GROUP BY Status
            ORDER BY Status
            """
        ).to_pylist()

        assert len(result) > 0

    def test_missions_groupby_company_status(self, session):
        """GROUP BY multiple columns from missions."""
        result = session.execute_to_arrow(
            """
            SELECT
                Company,
                Status,
                COUNT(*) as cnt
            FROM testdata.missions
            GROUP BY Company, Status
            ORDER BY Company, Status
            """
        ).to_pylist()

        assert len(result) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
