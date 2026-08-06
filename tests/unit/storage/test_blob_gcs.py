# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Reading datasets out of Google Cloud Storage.

This ran on `opteryx.connect()` / `cursor()`, which no longer exist; it now uses
`opteryx.session()` / `execute_to_morsels()`, and the per-scan telemetry that the
old `cur.telemetry["rows_read"]` / `["columns_read"]` / `["blobs_read"]` moved
into.

KNOWN FAILING, and not because of the API: as of this rewrite every case fails
with

    DatasetReadError: Cannot read information for dataset 'opteryx.space_missions':
    head_many: HTTP 404:
    https://storage.googleapis.com/opteryx/space_missions%2F_opteryx_manifest.parquet

The bucket no longer holds the manifest these queries read. The expected row,
column and telemetry numbers below are therefore UNVERIFIED - they are the
numbers this test has always carried, kept as they were rather than rewritten to
match a run that cannot happen. Whoever restores the dataset should re-confirm
them, starting with `columns_read`: it counts projected AND filtered columns, so
the 1 expected for a query which filters on a column it does not select looks low.
"""

import os
import sys
from collections import namedtuple

import pytest

os.environ.pop("OPTERYX_DEBUG", None)

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.connectors import GcpCloudStorageConnector
from opteryx.utils.formatter import format_sql

# Define the test case structure
TestCase = namedtuple("TestCase", "query expected_rowcount expected_columncount stats")

# Example parameterization data using named tuples for clarity
test_cases = [
    TestCase(
        query="SELECT * FROM opteryx.space_missions;",
        expected_rowcount=4630,
        expected_columncount=8,
        stats={},
    ),
    TestCase(
        query="SELECT * FROM opteryx.space_missions LIMIT 10;",
        expected_rowcount=10,
        expected_columncount=8,
        stats={},
    ),
    TestCase(
        query="SELECT COUNT(*) AS Missions, Company FROM opteryx.space_missions GROUP BY Company;",
        expected_rowcount=62,
        expected_columncount=2,
        stats={},
    ),
    TestCase(
        query="SELECT Company FROM opteryx.space_missions WHERE Rocket_Status = 'Active';",
        expected_rowcount=1010,
        expected_columncount=1,
        stats={"columns_read": 1, "rows_read": 1010},
    ),
    TestCase(
        query="SELECT COUNT(*) FROM opteryx.many",
        expected_rowcount=1,
        expected_columncount=1,
        stats={"blobs_read": 1018, "rows_read": 9162},
    ),
]


def scan_statistics(session) -> dict:
    """Totals across the scan operators - what the query actually read.

    `rows_read` was the name for the rows a scan handed up; that measurement is
    `records_out` on the scan's telemetry today.
    """
    scans = [
        operation
        for operation in session.telemetry["operations"].values()
        if "dataset" in operation  # only scans read a dataset
    ]
    return {
        "rows_read": sum(scan["records_out"] for scan in scans),
        "columns_read": sum(scan["columns_read"] for scan in scans),
        "blobs_read": sum(scan["blobs_read"] for scan in scans),
    }


@pytest.mark.parametrize("test_case", test_cases)
def test_gcs_storage(test_case):
    opteryx.register_workspace("opteryx", GcpCloudStorageConnector)
    opteryx.register_workspace("mabel_data", GcpCloudStorageConnector)

    session = opteryx.session()
    for _ in session.execute_to_morsels(test_case.query):
        pass

    # Assertions for rowcount and columncount
    assert (
        session.rowcount == test_case.expected_rowcount
    ), f"Expected rowcount {test_case.expected_rowcount}, got {session.rowcount}"
    assert (
        len(session.column_names) == test_case.expected_columncount
    ), f"Expected columncount {test_case.expected_columncount}, got {len(session.column_names)}"

    # Assertions for telemetry
    observed = scan_statistics(session)
    for key, expected_value in test_case.stats.items():
        actual_value = observed.get(key)
        assert (
            actual_value == expected_value
        ), f"Stats check failed for {key}: expected {expected_value}, got {actual_value}"

    session.close()


def main():
    """
    Running in the IDE we do some formatting - it's not functional but helps
    when reading the outputs.
    """

    import shutil
    import time

    from tests import trunc_printable

    start_suite = time.monotonic_ns()

    width = shutil.get_terminal_size((80, 20))[0] - 15

    passed = 0
    failed = 0

    failures = []

    print(f"RUNNING BATTERY OF {len(test_cases)} Google Cloud Storage TESTS")
    for index, test_case in enumerate(test_cases):
        (statement, rows, cols, stats) = test_case

        printable = statement
        if isinstance(printable, bytes):
            printable = printable.decode()
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(printable), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_gcs_storage(test_case)
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


if __name__ == "__main__":  # pragma: no cover
    main()
