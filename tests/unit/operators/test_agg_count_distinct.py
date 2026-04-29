"""
COUNT(DISTINCT) tests
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow as pa
from opteryx.compiled.nanobind.carchar_native import CarcharSet

import opteryx
import opteryx.compiled.aggregations.scalar_kernels as count_distinct_module  # type: ignore[attr-defined]
from tests.helpers import execute_and_fetch_all

python_count_distinct = count_distinct_module.count_distinct
count_distinct_draken = count_distinct_module.count_distinct_draken


def _distinct_size(func, column):
    return func(column, None).size()


def test_count_distinct_parquet():
    result = execute_and_fetch_all("SELECT COUNT(DISTINCT user_name) FROM testdata.flat.formats.parquet;")
    first = result[0]["COUNT(DISTINCT user_name)"]
    assert first == 83606, first


def test_count_distinct_identifier_group_by():
    """we're reading data from the file, even though it starts SELECT COUNT(*) FROM"""
    result = execute_and_fetch_all(
        "SELECT COUNT(DISTINCT user_name) AS un FROM testdata.flat.formats.parquet GROUP BY following ORDER BY un DESC;"
    )
    first = result[0]["un"]
    assert first == 481, first


def test_draken_hash_matches_python_for_int64():
    column = pa.array([1, 2, 2, None, -5, None, 42], type=pa.int64())
    assert _distinct_size(python_count_distinct, column) == _distinct_size(
        count_distinct_draken, column
    )


def test_draken_hash_matches_python_for_chunked_arrays():
    column = pa.chunked_array(
        [
            pa.array(list(range(1000)) + [None], type=pa.int64()),
            pa.array(list(range(500)) + [None], type=pa.int64()),
        ]
    )
    assert _distinct_size(python_count_distinct, column) == _distinct_size(
        count_distinct_draken, column
    )


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
