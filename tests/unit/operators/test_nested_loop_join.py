
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import time

import opteryx
from tests.helpers import execute_and_get_arrow, execute_and_get_rowcount, execute_and_get_shape, execute_and_fetch_all
from opteryx.compiled.joins import nested_loop_join


def test_nested_loop_join_simple():
    left = execute_and_get_arrow("SELECT * FROM $planets")
    right = execute_and_get_arrow("SELECT * FROM testdata.satellites")
    result = nested_loop_join(left, right, ["id"], ["id"])
    assert len(result) == 2
    assert len(result[0]) == 9


def test_nested_loop_join_timed():
    left = execute_and_get_arrow("SELECT * FROM testdata.missions")
    right = execute_and_get_arrow("SELECT * FROM testdata.missions")
    start = time.monotonic_ns()
    result = nested_loop_join(left, right, ["Mission"], ["Mission"])
    end = time.monotonic_ns()
    duration = (end - start) / 1_000_000  # Convert to milliseconds
    print(f"Nested Loop Join took {duration:.2f} ms")

    assert len(result) == 2, result
    assert len(result[0]) == 4956, result
    

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
