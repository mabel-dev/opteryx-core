"""
TPC-H regression tests (execution-only: every query must run to completion).

Runs all 22 Opteryx-dialect TPC-H queries from tests/performance/tpch/opteryx/queries/
against the SF=0.01 dataset (testdata/tpch_001). This battery asserts the queries
EXECUTE without error; value correctness for the verified subset is asserted by
test_battery_tpch_results.py against DuckDB goldens.
"""

import glob
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx
from opteryx.connectors import DiskConnector

opteryx.register_workspace("testdata", DiskConnector)

_QUERY_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "performance", "tpch", "opteryx", "queries",
)


def get_tests():
    tests = sorted(glob.glob(os.path.join(_QUERY_DIR, "*.sql")))
    for test in tests:
        with open(test, mode="r") as test_file:
            yield (os.path.basename(test), test_file.read())


TPCH_TESTS = list(get_tests())
assert len(TPCH_TESTS) == 22, (
    f"expected 22 TPC-H queries in {_QUERY_DIR}, found {len(TPCH_TESTS)}"
)
TPCH_SCALE = "001"  # 001 = 0.01

# To create a new TPCH dataset - this example uses scale factor 10
# - this creates the files in the current directory
# $ cargo install tpchgen-cli
# $ tpchgen-cli -s 10 --format=parquet


@pytest.mark.parametrize(
    "test_id, statement", TPCH_TESTS, ids=[test_id for test_id, _ in TPCH_TESTS]
)
def test_tpch(test_id, statement):
    statement = statement.replace("testdata.tpch_tiny.", f"testdata.tpch_{TPCH_SCALE}.")
    statement = statement.replace("testdata.tpch.", f"testdata.tpch_{TPCH_SCALE}.")
    session = opteryx.session()
    morsels = session.execute_to_morsels(statement)
    for _ in morsels:
        pass


if __name__ == "__main__":  # pragma: no cover
    """
    Running in the IDE we do some formatting - it's not functional but helps
    when reading the outputs.
    """

    import shutil
    import time

    from opteryx.utils.formatter import format_sql
    from tests import trunc_printable

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
        except Exception as err:
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start) / 1e6)).rjust(4)}ms ❌\033[0m")
            print(f"     \033[0;31m{type(err).__name__}: {err}\033[0m")

    print(f"\n--- ✅ \033[0;32m{pass_count}/{total_count} passed\033[0m")
    print(f"--- ⏱ \033[0;36m{str(int((time.monotonic_ns() - start_time) / 1e6)).rjust(4)}ms\033[0m")
