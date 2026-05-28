"""
The COUNT(*) optimization is brittle, it was being missed if 'COUNT' was in
lowercase, which is why this additional testcase was written.

This optimization relies quite heavily on the AST being exactly the same as it is
when the optimization was written.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from tests.helpers import execute_and_fetch_all

def test_count_star_parquet():
    """ if is just SELECT COUNT(*) for parquet files, we don't read the rows"""
    result = execute_and_fetch_all("SELECT count(*) FROM testdata.flat.formats.parquet")
    first = result[0]["count(*)"]
    assert first == 100000, first

def test_count_star_non_parquet():
    """ if is just SELECT COUNT(*) for non-parquet files, we read the rows"""
    result = execute_and_fetch_all("SELECT COUNT(*) FROM testdata.flat.ten_files;")
    first = result[0]["COUNT(*)"]
    assert first == 250, first

def test_count_identifier_parquest_read_the_rows():
    """ we're counting an identifier, so we need to read the rows """
    result = execute_and_fetch_all("SELECT COUNT(user_name) FROM testdata.flat.formats.parquet;")
    first = result[0]["COUNT(user_name)"]
    assert first == 100000, first

def test_count_star_group_by():
    """ we're reading data from the file, even though it starts SELECT COUNT(*) FROM """
    result = execute_and_fetch_all(
        "SELECT COUNT(*) FROM testdata.flat.formats.parquet GROUP BY tweet_id;"
    )
    assert len(result) > 0

def test_incorrect_pushdown():
    """
    This is a regression test for a pushdown bug relating to COUNT(*)
    subqueries and DISTINCT - its not how I would have written this
    query (count_distinct) so went undetected as a bug
    """
    result = execute_and_fetch_all(
        "SELECT COUNT(*) FROM (SELECT DISTINCT name FROM $planets) AS S"
    )
    first = result[0]["COUNT(*)"]
    assert first == 9, first

def test_count_star_with_where_greater_than():
    """COUNT(*) with WHERE clause should return correct count."""
    result = execute_and_fetch_all("SELECT COUNT(*) FROM $planets WHERE id > 5")
    first = result[0]["COUNT(*)"]
    assert first == 4, f"Expected 4, got {first}"

def test_count_star_with_where_equals():
    """COUNT(*) with WHERE equals should return correct count."""
    result = execute_and_fetch_all("SELECT COUNT(*) FROM $planets WHERE id = 3")
    first = result[0]["COUNT(*)"]
    assert first == 1, f"Expected 1, got {first}"

def test_count_star_with_where_no_match():
    """COUNT(*) with WHERE that matches no rows should return 0."""
    result = execute_and_fetch_all("SELECT COUNT(*) FROM $planets WHERE id < 0")
    first = result[0]["COUNT(*)"]
    assert first == 0, f"Expected 0, got {first}"

def test_count_star_with_is_null():
    """COUNT(*) with IS NULL WHERE clause should return correct count."""
    result = execute_and_fetch_all("SELECT COUNT(*) FROM testdata.astronauts WHERE death_date IS NULL")
    first = result[0]["COUNT(*)"]
    assert first == 305, f"Expected 305, got {first}"

def test_count_star_with_is_not_null():
    """COUNT(*) with IS NOT NULL WHERE clause should return correct count."""
    result = execute_and_fetch_all("SELECT COUNT(*) FROM testdata.astronauts WHERE death_date IS NOT NULL")
    first = result[0]["COUNT(*)"]
    assert first == 52, f"Expected 52, got {first}"

def test_count_star_is_null_plus_is_not_null():
    """Sum of NULL and NOT NULL should equal total count."""
    null_result = execute_and_fetch_all("SELECT COUNT(*) FROM testdata.astronauts WHERE death_date IS NULL")
    not_null_result = execute_and_fetch_all("SELECT COUNT(*) FROM testdata.astronauts WHERE death_date IS NOT NULL")
    total_result = execute_and_fetch_all("SELECT COUNT(*) FROM testdata.astronauts")

    null_count = null_result[0]["COUNT(*)"]
    not_null_count = not_null_result[0]["COUNT(*)"]
    total_count = total_result[0]["COUNT(*)"]

    assert null_count + not_null_count == total_count, \
        f"NULL count ({null_count}) + NOT NULL count ({not_null_count}) != total ({total_count})"

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
