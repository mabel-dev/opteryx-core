import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests.helpers import execute_and_fetch_all

def test_sum_parquet():
    result = execute_and_fetch_all("SELECT SUM(followers) FROM testdata.flat.formats.parquet")
    total = result[0]["SUM(followers)"]
    assert total == 308125800, total

def test_sum_non_parquet():
    result = execute_and_fetch_all("SELECT SUM(followers) FROM testdata.flat.ten_files;")
    total = result[0]["SUM(followers)"]
    assert total == 1875090667, total

def test_sum_group_by():
    """ we're reading data from the file, even though it starts SELECT COUNT(*) FROM """
    result = execute_and_fetch_all(
        "SELECT SUM(followers) FROM testdata.flat.formats.parquet GROUP BY tweet_id ORDER BY tweet_id;"
    )
    total = result[0]["SUM(followers)"]
    assert total == 6.0, total

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
