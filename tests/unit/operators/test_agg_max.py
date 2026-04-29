import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests.helpers import execute_and_fetch_all

def test_max_parquet():
    result = execute_and_fetch_all("SELECT MAX(followers) FROM testdata.flat.formats.parquet")
    first = result[0]["MAX(followers)"]
    assert first == 8266250, first

def test_max_non_parquet():
    result = execute_and_fetch_all("SELECT MAX(followers) FROM testdata.flat.ten_files;")
    first = result[0]["MAX(followers)"]
    assert first == 10415949, first

def test_max_group_by():
    """ we're reading data from the file, even though it starts SELECT COUNT(*) FROM """
    result = execute_and_fetch_all(
        "SELECT MAX(followers) FROM testdata.flat.formats.parquet GROUP BY tweet_id ORDER BY tweet_id;"
    )
    first = result[0]["MAX(followers)"]
    assert first == 6.0, first

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
