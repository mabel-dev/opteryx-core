"""
Basic shape tests for `testdata.split` dataset

This mirrors the style and setup in `test_shapes_basic.py` with a small set of
queries to validate the shapes (row/column counts) returned by the queries.
"""
import pytest
import os
import sys

from typing import Optional

sys.path.insert(1, os.path.join(sys.path[0], "../../../orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from tests.helpers import execute_and_get_arrow, execute_and_get_rowcount, execute_and_get_shape, execute_and_fetch_all, execute_with_memberships

from opteryx.exceptions import (
    DatasetNotFoundError,
    SqlError,
    UnsupportedSyntaxError,
)
from opteryx.utils.formatter import format_sql

# fmt:off
STATEMENTS = [
    ("CREATE VIEW test.views.test_01 AS SELECT * FROM $planets", None),
    ("CREATE OR REPLACE VIEW test.views.test_02 (col1, col2, col3) AS SELECT name, mass, radius FROM $planets", None),
    ("ALTER VIEW test.views.test_01 AS SELECT name, mass FROM $planets WHERE mass > 5e24", None),
    ("DROP VIEW test.views.test_02", None),
    # Error cases
    ("CREATE VIEW test.views.test_01 AS SELECT * FROM nonexistent_table", None),
    ("ALTER VIEW test.views.nonexistent_view AS SELECT * FROM $planets", None),
    ("DROP VIEW test.views.nonexistent_view", None),
]
# fmt:on


@pytest.mark.parametrize("statement, exception", STATEMENTS)
def test_sql_battery(statement:str, exception: Optional[Exception]):
    """
    Test a battery of statements
    """
    try:
        execute_with_memberships(statement, memberships=["Apollo 11", "opteryx"])
        assert (
            exception is None
        ), f"Exception {exception} not raised but expected\n{format_sql(statement)}"
    except AssertionError as error:
        raise error
    except Exception as error:
        if not type(error) == exception:
            raise ValueError(
                f"{format_sql(statement)}\nQuery failed with error {type(error)} but error {exception} was expected"
            ) from error


if __name__ == "__main__":  # pragma: no cover
    import shutil
    import time
    from tests import trunc_printable

    start_suite = time.monotonic_ns()
    width = shutil.get_terminal_size((80, 20))[0] - 15
    passed:int = 0
    failed:int = 0
    nl:str = "\n"
    failures = []

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} VIEW TESTS")
    for index, (statement, exception) in enumerate(STATEMENTS):
        printable = statement
        if hasattr(printable, "decode"):
            printable = printable.decode()
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(printable), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_sql_battery(statement, exception)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(f" \033[0;31m{failed}\033[0m")
            else:
                print()
        except Exception as err:
            failed += 1
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms ❌ {failed}\033[0m")
            print(">", err)
            failures.append((statement, err))

    print("--- ✅ \033[0;32mdone\033[0m")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for statement, err in failures:
            print(err)

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )

    # Exit with appropriate code to signal success/failure to parent process
    if failed > 0:
        sys.exit(1)
    else:
        sys.exit(0)

