# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Predicate pushdown into Parquet scans.

Two numbers per query: how many rows come back, and how many rows the SCAN had
to emit to produce them. The second is the point - a pushed-down predicate is one
the scan applied itself, so the rows never crossed into the rest of the plan.

This ran on `opteryx.connect()` / `cursor()` / `cur.telemetry["rows_read"]`, none
of which exist now. Rows out of the scan are read from
`telemetry["operations"][<scan node>]["records_out"]`, which is where that
measurement lives today.
"""

import os
import sys
import time

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.utils.formatter import format_sql

# Environment setup
os.environ["GCP_PROJECT_ID"] = "mabeldev"

# query, rows returned, rows the scan emitted
test_cases = [
    (
        "SELECT user_name FROM testdata.flat.formats.parquet WHERE user_verified = TRUE;",
        711,
        711,
    ),
    (
        "SELECT user_name FROM testdata.flat.formats.parquet WHERE user_verified = TRUE and following < 1000;",
        266,
        266,
    ),
    (
        # This case expected 711 rows out of the scan when it was written: the LIKE
        # was not pushed down, so the scan emitted every `user_verified` row and the
        # LIKE was applied above it. The scan now emits 86 - it applies both - which
        # is the same answer for less work.
        "SELECT user_name FROM testdata.flat.formats.parquet WITH(NO_PARTITION) WHERE user_verified = TRUE and user_name LIKE '%b%';",
        86,
        86,
    ),
    (
        # `Lauched_at < '2000-01-01'` as written no longer binds - a VARCHAR literal
        # is not implicitly a TIMESTAMP - so the comparison says what it means.
        "SELECT * FROM testdata.flat.space_missions WHERE Lauched_at < CAST('2000-01-01' AS TIMESTAMP);",
        3014,
        3014,
    ),
]


def scan_rows_emitted(session) -> int:
    """Rows handed up by the scan operators - what pushdown moves."""
    return sum(
        operation["records_out"]
        for operation in session.telemetry["operations"].values()
        if "dataset" in operation  # only scans read a dataset
    )


@pytest.mark.parametrize("query, expected_rowcount, expected_rows_read", test_cases)
def test_predicate_pushdowns_blobs_parquet(query, expected_rowcount, expected_rows_read):
    session = opteryx.session()
    for _ in session.execute_to_morsels(query):
        pass

    assert (
        session.rowcount == expected_rowcount
    ), f"Expected rowcount: {expected_rowcount}, got: {session.rowcount}"
    rows_read = scan_rows_emitted(session)
    assert (
        rows_read == expected_rows_read
    ), f"Expected rows out of the scan: {expected_rows_read}, got: {rows_read}"

    session.close()


if __name__ == "__main__":  # pragma: no cover
    import shutil

    from tests import trunc_printable

    start_suite = time.monotonic_ns()
    passed = 0
    failed = 0

    width = shutil.get_terminal_size((80, 20))[0] - 15

    print(f"RUNNING BATTERY OF {len(test_cases)} TESTS")
    for index, (statement, returned_rows, read_rows) in enumerate(test_cases):
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(statement), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_predicate_pushdowns_blobs_parquet(statement, returned_rows, read_rows)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(" \033[0;31m*\033[0m")
            else:
                print()
        except Exception as err:
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms ❌ *\033[0m")
            print(">", err)
            failed += 1

    print("--- ✅ \033[0;32mdone\033[0m")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
