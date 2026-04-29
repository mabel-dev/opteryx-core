import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from tests.helpers import execute_and_get_rowcount


def test_inline_alias_within_subquery():
    sql = (
        "SELECT DISTINCT mission\n"
        "FROM (SELECT missions, year % 2 == 0 AS even_launch_year FROM testdata.astronauts) AS astro\n"
        "CROSS JOIN UNNEST(missions) AS mission\n"
        "WHERE even_launch_year = TRUE"
    )
    count = execute_and_get_rowcount(sql)
    assert count > 0  # Should have results


def test_inline_alias_from_cte():
    sql = """
WITH astro AS (
    SELECT missions, year % 2 == 0 AS even_launch_year
    FROM testdata.astronauts
)
SELECT DISTINCT mission
FROM astro CROSS JOIN UNNEST(missions) AS mission
WHERE even_launch_year = TRUE
"""
    count = execute_and_get_rowcount(sql)
    assert count > 0  # Should have results


def test_inline_alias_with_comparison():
    sql = """
SELECT DISTINCT planet_name
FROM (SELECT name AS planet_name, id FROM testdata.planets) AS planets
WHERE id > 4
"""
    count = execute_and_get_rowcount(sql)
    assert count == 5
