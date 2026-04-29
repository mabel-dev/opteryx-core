import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests.helpers import execute_and_fetch_all

def test_min_parquet():
    result = execute_and_fetch_all("SELECT MIN(followers) FROM testdata.flat.formats.parquet")
    first = result[0]["MIN(followers)"]
    assert first == 0, first

def test_min_non_parquet():
    result = execute_and_fetch_all("SELECT MIN(followers) FROM testdata.flat.ten_files;")
    first = result[0]["MIN(followers)"]
    assert first == 100, first

def test_min_group_by():
    """ we're reading data from the file, even though it starts SELECT COUNT(*) FROM """
    result = execute_and_fetch_all(
        "SELECT MIN(followers) FROM testdata.flat.formats.parquet GROUP BY tweet_id ORDER BY tweet_id;"
    )
    first = result[0]["MIN(followers)"]
    assert first == 6.0, first

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests
    
    run_tests()
