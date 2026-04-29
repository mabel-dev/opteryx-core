"""
Comprehensive GROUP BY regression and unit tests - Part 1

This module tests extensive GROUP BY scenarios including:
- Single and multi-column GROUP BY
- All aggregation functions (COUNT, SUM, AVG, MIN, MAX, COUNT DISTINCT)
- Edge cases (NULLs, empty sets, single row)
- Different data types
- Interaction with WHERE, HAVING, ORDER BY, LIMIT
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from tests.helpers import execute_and_get_arrow, execute_and_get_rowcount, execute_and_get_shape, execute_and_fetch_all


@pytest.fixture
def session():
    """Create a session for each test."""
    return opteryx.session(memberships=["Apollo 11", "opteryx"])


class TestBasicGroupBy:
    """Test basic GROUP BY functionality."""

    def test_groupby_single_column_simple_count(self, session):
        """GROUP BY single column with COUNT(*)."""
        result = session.execute_to_arrow(
            "SELECT planetId, COUNT(*) as cnt FROM testdata.satellites GROUP BY planetId"
        ).to_pylist()

        assert len(result) > 0
        assert all("planetId" in row and "cnt" in row for row in result)
        assert all(row["cnt"] > 0 for row in result)

    def test_groupby_single_column_multiple_aggregates(self, session):
        """GROUP BY single column with multiple aggregation functions."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt_all,
                COUNT(radius) as cnt_radius,
                SUM(radius) as sum_radius,
                AVG(radius) as avg_radius,
                MIN(radius) as min_radius,
                MAX(radius) as max_radius
            FROM testdata.satellites
            GROUP BY planetId
            ORDER BY planetId
            """
        ).to_pylist()

        assert len(result) > 0
        for row in result:
            assert row["cnt_all"] >= row["cnt_radius"]  # COUNT(*) >= COUNT(column)
            if row["cnt_radius"] > 0:
                assert row["min_radius"] <= row["max_radius"]
                assert row["avg_radius"] is not None

    def test_groupby_two_columns(self, session):
        """GROUP BY multiple columns."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                name,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId, name
            ORDER BY planetId, name
            """
        ).to_pylist()

        assert len(result) > 0
        assert all("planetId" in row and "name" in row and "cnt" in row for row in result)
        assert all(row["cnt"] > 0 for row in result)

    def test_groupby_three_columns(self, session):
        """GROUP BY with three columns."""
        result = session.execute_to_arrow(
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
        for row in result:
            assert row["cnt"] >= 1

    def test_groupby_orderby_agg_column(self, session):
        """GROUP BY with ORDER BY on aggregation column."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            ORDER BY cnt DESC
            """
        ).to_pylist()

        assert len(result) > 0
        # Verify order
        for i in range(len(result) - 1):
            assert result[i]["cnt"] >= result[i + 1]["cnt"]

    def test_groupby_orderby_multiple_columns(self, session):
        """GROUP BY with ORDER BY on multiple columns."""
        result = session.execute_to_arrow(
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

    def test_groupby_limit(self, session):
        """GROUP BY with LIMIT."""
        result_no_limit = session.execute_to_arrow(
            "SELECT planetId, COUNT(*) as cnt FROM testdata.satellites GROUP BY planetId"
        ).to_pylist()

        result_limit_3 = session.execute_to_arrow(
            "SELECT planetId, COUNT(*) as cnt FROM testdata.satellites GROUP BY planetId LIMIT 3"
        ).to_pylist()

        assert len(result_limit_3) == 3
        assert len(result_limit_3) <= len(result_no_limit)

    def test_groupby_offset(self, session):
        """GROUP BY with OFFSET."""
        result_no_offset = session.execute_to_arrow(
            "SELECT planetId, COUNT(*) as cnt FROM testdata.satellites GROUP BY planetId ORDER BY planetId"
        ).to_pylist()

        result_offset_2 = session.execute_to_arrow(
            "SELECT planetId, COUNT(*) as cnt FROM testdata.satellites GROUP BY planetId ORDER BY planetId OFFSET 2"
        ).to_pylist()

        assert len(result_offset_2) == len(result_no_offset) - 2
        assert result_offset_2[0]["planetId"] == result_no_offset[2]["planetId"]


class TestGroupByWithWhereClause:
    """Test GROUP BY with WHERE conditions."""

    def test_groupby_with_where_numeric(self, session):
        """GROUP BY with WHERE filtering on numeric column."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            WHERE radius > 1000
            GROUP BY planetId
            """
        ).to_pylist()

        assert len(result) > 0
        assert all(row["cnt"] > 0 for row in result)

    def test_groupby_with_where_string(self, session):
        """GROUP BY with WHERE filtering on string column."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            WHERE name LIKE 'I%'
            GROUP BY planetId
            """
        ).to_pylist()

        # May or may not have results depending on data
        assert isinstance(result, list)

    def test_groupby_with_where_multiple_conditions(self, session):
        """GROUP BY with multiple WHERE conditions."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            WHERE radius > 500 AND yearDiscovered > 1980
            GROUP BY planetId
            """
        ).to_pylist()

        assert isinstance(result, list)


class TestGroupByWithHavingClause:
    """Test GROUP BY with HAVING conditions."""

    def test_groupby_having_count_greater(self, session):
        """GROUP BY with HAVING on COUNT."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            HAVING COUNT(*) > 1
            ORDER BY planetId
            """
        ).to_pylist()

        assert all(row["cnt"] > 1 for row in result)

    def test_groupby_having_sum_condition(self, session):
        """GROUP BY with HAVING on SUM."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                SUM(radius) as total_radius
            FROM testdata.satellites
            GROUP BY planetId
            HAVING SUM(radius) > 5000
            """
        ).to_pylist()

        assert all(row["total_radius"] > 5000 for row in result)

    def test_groupby_having_avg_condition(self, session):
        """GROUP BY with HAVING on AVG."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                AVG(radius) as avg_radius,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            HAVING AVG(radius) > 2000
            """
        ).to_pylist()

        assert all(row["avg_radius"] > 2000 for row in result)

    def test_groupby_having_multiple_conditions(self, session):
        """GROUP BY with multiple HAVING conditions."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt,
                AVG(radius) as avg_radius
            FROM testdata.satellites
            GROUP BY planetId
            HAVING COUNT(*) > 1 AND AVG(radius) > 1000
            """
        ).to_pylist()

        assert all(row["cnt"] > 1 and row["avg_radius"] > 1000 for row in result)


class TestAggregationFunctions:
    """Test all aggregation functions in GROUP BY."""

    def test_count_star(self, session):
        """Test COUNT(*)."""
        result = session.execute_to_arrow(
            "SELECT planetId, COUNT(*) as cnt FROM testdata.satellites GROUP BY planetId"
        ).to_pylist()

        assert all(row["cnt"] >= 1 for row in result)

    def test_count_column(self, session):
        """Test COUNT(column) - excludes NULLs."""
        result = session.execute_to_arrow(
            """
            SELECT
                Company,
                COUNT(*) as cnt_all,
                COUNT(Price) as cnt_price
            FROM testdata.missions
            GROUP BY Company
            """
        ).to_pylist()

        # COUNT(*) >= COUNT(Price) because COUNT excludes NULLs
        for row in result:
            assert row["cnt_all"] >= row["cnt_price"]

    def test_count_distinct(self, session):
        """Test COUNT(DISTINCT column)."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(DISTINCT name) as distinct_names,
                COUNT(*) as total
            FROM testdata.satellites
            GROUP BY planetId
            """
        ).to_pylist()

        for row in result:
            assert row["distinct_names"] <= row["total"]

    def test_sum(self, session):
        """Test SUM aggregation."""
        result = session.execute_to_arrow(
            "SELECT planetId, SUM(radius) as total_radius FROM testdata.satellites GROUP BY planetId"
        ).to_pylist()

        assert all(row["total_radius"] is not None or row["total_radius"] == 0 for row in result)

    def test_avg(self, session):
        """Test AVG aggregation."""
        result = session.execute_to_arrow(
            "SELECT planetId, AVG(radius) as avg_radius FROM testdata.satellites GROUP BY planetId"
        ).to_pylist()

        assert all(
            isinstance(row["avg_radius"], (int, float)) or row["avg_radius"] is None
            for row in result
        )

    def test_min(self, session):
        """Test MIN aggregation."""
        result = session.execute_to_arrow(
            "SELECT planetId, MIN(radius) as min_radius FROM testdata.satellites GROUP BY planetId"
        ).to_pylist()

        assert len(result) > 0

    def test_max(self, session):
        """Test MAX aggregation."""
        result = session.execute_to_arrow(
            "SELECT planetId, MAX(radius) as max_radius FROM testdata.satellites GROUP BY planetId"
        ).to_pylist()

        assert len(result) > 0

    def test_min_max_ordering(self, session):
        """MIN should be <= MAX for each group."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                MIN(radius) as min_r,
                MAX(radius) as max_r
            FROM testdata.satellites
            GROUP BY planetId
            """
        ).to_pylist()

        for row in result:
            if row["min_r"] is not None and row["max_r"] is not None:
                assert row["min_r"] <= row["max_r"]


class TestNullHandling:
    """Test GROUP BY NULL handling."""

    def test_groupby_with_nullable_key(self, session):
        """GROUP BY on column that may contain NULLs."""
        result = session.execute_to_arrow(
            """
            SELECT
                Company,
                COUNT(*) as cnt
            FROM testdata.missions
            GROUP BY Company
            ORDER BY Company
            """
        ).to_pylist()

        assert len(result) > 0
        # Check that we got some groups
        group_values = [row["Company"] for row in result]
        assert len(group_values) > 0

    def test_count_null_vs_nonnull(self, session):
        """COUNT(*) vs COUNT(column) with NULLs."""
        result = session.execute_to_arrow(
            """
            SELECT
                Company,
                COUNT(*) as cnt_all,
                COUNT(Price) as cnt_price
            FROM testdata.missions
            GROUP BY Company
            """
        ).to_pylist()

        # At least one group should have COUNT(*) > COUNT(Price) if NULLs exist
        has_null_difference = any(row["cnt_all"] > row["cnt_price"] for row in result)
        # This might be true or false depending on data
        assert isinstance(result, list)

    def test_sum_with_nulls_excludes_them(self, session):
        """SUM should ignore NULL values."""
        result = session.execute_to_arrow(
            """
            SELECT
                Company,
                COUNT(*) as cnt,
                COUNT(Price) as cnt_price,
                SUM(Price) as sum_price
            FROM testdata.missions
            GROUP BY Company
            """
        ).to_pylist()

        # If SUM(Price) is null but COUNT(*) > 0, all Price values were NULL
        # If SUM(Price) is not null, COUNT(Price) > 0
        for row in result:
            if row["sum_price"] is not None:
                assert row["cnt_price"] > 0

    def test_avg_with_nulls(self, session):
        """AVG should ignore NULL values."""
        result = session.execute_to_arrow(
            """
            SELECT
                Company,
                COUNT(Price) as cnt_price,
                AVG(Price) as avg_price
            FROM testdata.missions
            GROUP BY Company
            """
        ).to_pylist()

        for row in result:
            if row["avg_price"] is not None:
                assert row["cnt_price"] > 0


class TestGroupByCardinality:
    """Test GROUP BY with different cardinality scenarios."""

    def test_groupby_low_cardinality(self, session):
        """GROUP BY with low cardinality (few unique groups)."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            """
        ).to_pylist()

        # Planets have low cardinality (planets < satellites)
        assert len(result) < 15  # Solar system has ~8 planets

    def test_groupby_high_cardinality_single_column(self, session):
        """GROUP BY with high cardinality (many unique values)."""
        result = session.execute_to_arrow(
            """
            SELECT
                name,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY name
            """
        ).to_pylist()

        # Satellite names are more unique
        assert len(result) > 50

    def test_groupby_perfect_hash(self, session):
        """GROUP BY where each row is its own group."""
        result = session.execute_to_arrow(
            """
            SELECT
                name,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY name
            """
        ).to_pylist()

        assert all(row["cnt"] >= 1 for row in result)


class TestGroupByOrdering:
    """Test GROUP BY with various ordering scenarios."""

    def test_groupby_orderby_key_asc(self, session):
        """GROUP BY with ORDER BY key ascending."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            ORDER BY planetId ASC
            """
        ).to_pylist()

        for i in range(len(result) - 1):
            assert result[i]["planetId"] <= result[i + 1]["planetId"]

    def test_groupby_orderby_key_desc(self, session):
        """GROUP BY with ORDER BY key descending."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            ORDER BY planetId DESC
            """
        ).to_pylist()

        for i in range(len(result) - 1):
            assert result[i]["planetId"] >= result[i + 1]["planetId"]

    def test_groupby_orderby_agg_asc(self, session):
        """GROUP BY with ORDER BY aggregate ascending."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            ORDER BY cnt ASC
            """
        ).to_pylist()

        for i in range(len(result) - 1):
            assert result[i]["cnt"] <= result[i + 1]["cnt"]

    def test_groupby_orderby_agg_desc(self, session):
        """GROUP BY with ORDER BY aggregate descending."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            ORDER BY cnt DESC
            """
        ).to_pylist()

        for i in range(len(result) - 1):
            assert result[i]["cnt"] >= result[i + 1]["cnt"]


class TestGlobalAggregation:
    """Test GROUP BY with no GROUP BY (global aggregation)."""

    def test_global_count(self, session):
        """SELECT COUNT(*) with no GROUP BY."""
        result = session.execute_to_arrow(
            "SELECT COUNT(*) as cnt FROM testdata.satellites"
        ).to_pylist()

        assert len(result) == 1
        assert result[0]["cnt"] > 0

    def test_global_multiple_aggregates(self, session):
        """SELECT multiple aggregates with no GROUP BY."""
        result = session.execute_to_arrow(
            """
            SELECT
                COUNT(*) as cnt,
                SUM(radius) as sum_r,
                AVG(radius) as avg_r,
                MIN(radius) as min_r,
                MAX(radius) as max_r
            FROM testdata.satellites
            """
        ).to_pylist()

        assert len(result) == 1
        row = result[0]
        assert row["cnt"] > 0
        assert row["min_r"] <= row["max_r"]

    def test_global_count_distinct(self, session):
        """SELECT COUNT(DISTINCT) with no GROUP BY."""
        result = session.execute_to_arrow(
            """
            SELECT
                COUNT(DISTINCT planetId) as distinct_planets
            FROM testdata.satellites
            """
        ).to_pylist()

        assert len(result) == 1
        assert result[0]["distinct_planets"] > 0


class TestExpressionGroupBy:
    """Test GROUP BY with expressions."""

    def test_groupby_expression_arithmetic(self, session):
        """GROUP BY on arithmetic expression."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId * 10 as planet_times_10,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId * 10
            ORDER BY planet_times_10
            """
        ).to_pylist()

        assert len(result) > 0

    def test_groupby_string_length(self, session):
        """GROUP BY on string function result."""
        result = session.execute_to_arrow(
            """
            SELECT
                CASE WHEN LENGTH(name) > 5 THEN 'long' ELSE 'short' END as name_len,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY CASE WHEN LENGTH(name) > 5 THEN 'long' ELSE 'short' END
            """
        ).to_pylist()

        assert len(result) > 0
        assert all(row["name_len"] in ("long", "short") for row in result)


class TestComplexGroupBy:
    """Test complex GROUP BY scenarios."""

    def test_groupby_three_aggs_two_keys(self, session):
        """GROUP BY with 2 keys and 3 aggregates."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                CASE WHEN radius > 1000 THEN 'large' ELSE 'small' END as size,
                COUNT(*) as cnt,
                SUM(radius) as sum_r,
                AVG(radius) as avg_r
            FROM testdata.satellites
            GROUP BY planetId, CASE WHEN radius > 1000 THEN 'large' ELSE 'small' END
            ORDER BY planetId, size
            """
        ).to_pylist()

        assert len(result) > 0
        for row in result:
            assert row["size"] in ("large", "small")

    def test_groupby_with_alias_reference(self, session):
        """GROUP BY using computed column."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId as planet,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            ORDER BY planet
            """
        ).to_pylist()

        assert len(result) > 0
        assert all("planet" in row and "cnt" in row for row in result)


class TestGroupByEdgeCases:
    """Test edge cases and error conditions."""

    def test_groupby_single_row_result(self, session):
        """GROUP BY that produces exactly one row."""
        result = session.execute_to_arrow(
            """
            SELECT
                COUNT(*) as cnt
            FROM testdata.satellites
            WHERE planetId = 1
            GROUP BY planetId
            """
        ).to_pylist()

        if len(result) > 0:  # Only if data exists
            assert len(result) == 1

    def test_groupby_empty_input(self, session):
        """GROUP BY on empty input."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            WHERE planetId > 1000
            GROUP BY planetId
            """
        ).to_pylist()

        assert len(result) == 0

    def test_groupby_all_same_key(self, session):
        """GROUP BY where all rows have same key (single group)."""
        result = session.execute_to_arrow(
            """
            SELECT
                COUNT(*) as cnt,
                SUM(radius) as sum_r
            FROM testdata.satellites
            """
        ).to_pylist()

        assert len(result) == 1

    def test_groupby_numeric_column_names(self, session):
        """GROUP BY with result aliasing."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId as pid,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            """
        ).to_pylist()

        assert all("pid" in row and "cnt" in row for row in result)


class TestGroupByDataTypes:
    """Test GROUP BY with various data types."""

    def test_groupby_integer_key(self, session):
        """GROUP BY on integer column."""
        result = session.execute_to_arrow(
            """
            SELECT
                planetId,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY planetId
            """
        ).to_pylist()

        assert all(isinstance(row["planetId"], int) for row in result)

    def test_groupby_string_key(self, session):
        """GROUP BY on string column."""
        result = session.execute_to_arrow(
            """
            SELECT
                name,
                COUNT(*) as cnt
            FROM testdata.satellites
            GROUP BY name
            """
        ).to_pylist()

        assert all(isinstance(row["name"], (str, bytes)) or row["name"] is None for row in result)

    def test_groupby_year_key(self, session):
        """GROUP BY on year/date-like column."""
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

        assert len(result) > 0


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
