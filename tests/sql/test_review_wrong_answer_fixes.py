"""Wrong-answer fixes from the 2026-10-30 code review: each query returned a
plausible but wrong result before its fix."""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx


def _scalar(sql):
    values = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        values.extend(morsel.column(morsel.column_names[0]).to_pylist())
    assert len(values) == 1, values
    return values[0]


def test_join_elimination_needs_distinct_on_the_key():
    # DISTINCT (planetId, name) is not unique on planetId: the INNER join keeps
    # one row per satellite; a semi-join rewrite would count planets instead.
    sql = (
        "SELECT COUNT(*) FROM testdata.planets AS p "
        "INNER JOIN (SELECT DISTINCT planetId, name FROM testdata.satellites) AS s "
        "ON p.id = s.planetId"
    )
    assert _scalar(sql) == _scalar("SELECT COUNT(*) FROM testdata.satellites")


def test_not_equal_any_is_not_rewritten_to_not_in():
    # != ANY means "differs from at least one element", NOT IN means "from all".
    # The rewrite that conflated them is gone; with no native kernel lowering for
    # != ANY the query now refuses instead of answering 7.
    from opteryx.exceptions import NotSupportedError

    with pytest.raises(NotSupportedError):
        _scalar("SELECT COUNT(*) FROM $planets WHERE id != ANY([1, 2])")


def test_substring_negative_start_with_length():
    assert _scalar("SELECT SUBSTRING('hello', -2, 2)") == "lo"


def test_datediff_counts_calendar_months():
    assert _scalar("SELECT DATEDIFF('months', CAST('2023-01-01' AS DATE), CAST('2023-03-01' AS DATE))") == 2
    assert _scalar("SELECT DATEDIFF('years', CAST('2020-01-01' AS DATE), CAST('2024-12-31' AS DATE))") == 4


def test_float_to_int64_out_of_range_fails():
    with pytest.raises(Exception, match="out of range for INT64"):
        _scalar("SELECT CAST(1e19 AS INT64)")
    assert _scalar("SELECT SAFE_CAST(1e19 AS INT64)") is None


def test_rounding_to_zero_groups_with_zero():
    # CEIL(-0.5) / TRUNC(-0.5) / ROUND(-0.4) are -0.0, which must hash as 0.0.
    assert _scalar("SELECT COUNT(DISTINCT c) FROM (SELECT CEIL(x) AS c FROM UNNEST([-0.5, 0.0]) AS x)") == 1
    assert _scalar("SELECT COUNT(DISTINCT c) FROM (SELECT TRUNC(x) AS c FROM UNNEST([-0.5, 0.2]) AS x)") == 1
    assert _scalar("SELECT COUNT(DISTINCT c) FROM (SELECT ROUND(x) AS c FROM UNNEST([-0.4, 0.0]) AS x)") == 1


def test_statistics_only_answer_needs_the_scan_column_itself():
    # The file statistics describe testdata.planets.id; a subquery that redefines
    # `id` shares the name but not the values.
    assert _scalar("SELECT MAX(id) FROM testdata.planets") == 9
    assert _scalar("SELECT MAX(id) FROM (SELECT -id AS id FROM testdata.planets) AS s") == -1
    assert _scalar("SELECT MIN(id) FROM (SELECT id + 100 AS id FROM testdata.planets) AS s") == 101


def test_position_counts_characters_for_nvarchar():
    # NVARCHAR counts characters (as SUBSTRING does); VARCHAR stays byte-based.
    assert _scalar("SELECT POSITION('b' IN CAST('éb' AS NVARCHAR))") == 2
    assert _scalar("SELECT POSITION('b' IN 'xb')") == 2


@pytest.mark.parametrize(
    "subquery_condition",
    [
        "EXISTS (SELECT 1 FROM testdata.satellites AS s WHERE s.planetId = p.id LIMIT 1)",
        "EXISTS (SELECT 1 FROM testdata.satellites AS s WHERE s.planetId = p.id LIMIT 1 OFFSET 1)",
        "NOT EXISTS (SELECT 1 FROM testdata.satellites AS s WHERE s.planetId = p.id LIMIT 1)",
        "p.id IN (SELECT s.planetId FROM testdata.satellites AS s WHERE s.planetId = p.id LIMIT 1)",
        "(SELECT COUNT(*) FROM testdata.satellites AS s WHERE s.planetId = p.id LIMIT 1) > 1",
    ],
)
def test_correlated_limit_is_refused_not_applied_globally(subquery_condition):
    # Lifting the correlation into a join key put the LIMIT above every binding:
    # EXISTS ... LIMIT 1 kept only Earth (DuckDB: 7 planets).
    from opteryx.exceptions import UnsupportedSyntaxError

    with pytest.raises(UnsupportedSyntaxError):
        _scalar(f"SELECT COUNT(*) FROM testdata.planets AS p WHERE {subquery_condition}")


def test_limit_below_correlation_and_scalar_top1_still_decorrelate():
    # Expected values checked against DuckDB 2.0.0.
    assert _scalar(
        "SELECT COUNT(*) FROM testdata.planets AS p WHERE EXISTS (SELECT 1 FROM "
        "(SELECT planetId FROM testdata.satellites ORDER BY id LIMIT 5) AS x WHERE x.planetId = p.id)"
    ) == 3
    assert _scalar(
        "SELECT COUNT(*) FROM testdata.planets AS p WHERE (SELECT s.radius FROM testdata.satellites AS s "
        "WHERE s.planetId = p.id ORDER BY s.radius DESC LIMIT 1) > 1000"
    ) == 4


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
