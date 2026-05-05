"""
TPC-H regression tests

We're using the tiny TPC-H dataset from https://github.com/ElanHR/Databases because we're more
interested in functional regression than performance in this context.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import glob

import pytest

import opteryx


def get_tests():
    tests = sorted(glob.glob(f"**/tpch/**.sql", recursive=True))
    for test in tests:
        with open(test, mode="r") as test_file:
            yield (test.split("/")[-1], test_file.read())


TPCH_TESTS = list(get_tests())
TPCH_SCALE = "001"  # 001 = 0.01

# To create a new TPCH dataset - this example uses scale factor 10
# - this creates the files in the current directory
# $ cargo install tpchgen-cli
# $ tpchgen-cli -s 10 --format=parquet


@pytest.mark.parametrize("test_id, statement", TPCH_TESTS)
def test_tpch(test_id, statement):
    try:
        statement = statement.replace("testdata.tpch_tiny.", f"testdata.tpch_{TPCH_SCALE}.")
        statement = statement.replace("testdata.tpch.", f"testdata.tpch_{TPCH_SCALE}.")
        session = opteryx.session()
        morsels = session.execute_to_morsels(statement)
        for _ in morsels:
            pass
        outcome = True
    except Exception as err:
        outcome = False

    assert outcome


if __name__ == "__main__":  # pragma: no cover
    """
    Running in the IDE we do some formatting - it's not functional but helps
    when reading the outputs.
    """

    import shutil
    import time

    from opteryx.connectors import DiskConnector
    from opteryx.utils.formatter import format_sql
    from tests import trunc_printable

    opteryx.register_workspace("testdata", DiskConnector)

    width = shutil.get_terminal_size((80, 20))[0]

    print("WARMING UP")
    test_tpch(0, f"SELECT COUNT(*) FROM testdata.tpch_{TPCH_SCALE}.lineitem")

    print(f"RUNNING BATTERY OF {len(TPCH_TESTS)} TPC-H TESTS\n")
    print(f"SCALE FACTOR: {TPCH_SCALE}")
    pass_count = 0
    total_count = 0
    start_time = time.monotonic_ns()
    for index, (test, statement) in enumerate(TPCH_TESTS):
        total_count += 1
        detail = f"\033[0;35m{test}\033[0m {format_sql(statement)}"
        detail = trunc_printable(detail, width - 20)
        print(
            f"\033[0;36m{(index + 1):04}\033[0m {detail.ljust(width)}",
            end="",
        )
        try:
            start = time.monotonic_ns()
            test_tpch(test, statement)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start) / 1e6)).rjust(4)}ms\033[0m ✅",
            )
            pass_count += 1
        except AssertionError as err:
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start) / 1e6)).rjust(4)}ms ❌\033[0m")

    print(f"\n--- ✅ \033[0;32m{pass_count}/{total_count} passed\033[0m")
    print(f"--- ⏱ \033[0;36m{str(int((time.monotonic_ns() - start_time) / 1e6)).rjust(4)}ms\033[0m")
