"""Schema evolution on the native parquet scan, through a real catalog dataset.

A column ADDED to a table's schema after its data files were written is absent from
those files; the native scan reads it as NULL from them (see
tests/unit/operators/test_native_scan_absent_columns.py for the file-level cases).
These tests compose that with what only a catalog dataset carries: merge-on-read
deletes (a row admission) and the row address `$ordinal` a DELETE/UPDATE/MERGE scan
asks for. The environment is the MERGE suite's, imported rather than copied.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx  # noqa: E402
from tests.integration.test_merge_into_local import TARGET  # noqa: E402
from tests.integration.test_merge_into_local import _rows  # noqa: E402
from tests.integration.test_merge_into_local import merge_env  # noqa: E402,F401
from tests.integration.test_merge_into_local import pytestmark  # noqa: E402,F401


@pytest.fixture
def evolved_target(merge_env):  # noqa: F811
    """The merge target (cve, details, revision; rows 1..3) with a column `extra`
    declared in its schema that its only data file does not hold."""
    meta = merge_env["col.tgt"].metadata
    columns = next(s for s in meta.schemas if s["schema_id"] == meta.current_schema_id)["columns"]
    columns.append({"id": 4, "name": "extra", "type": "INTEGER"})
    return merge_env


def _clear():
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache

    clear_parsed_manifest_cache()


def _native_rows(sql):
    _clear()
    session = opteryx.session(user="tester")
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(tuple(morsel[i]))
    sources = session.telemetry.get("scan_sources") or {}
    assert sources and set(sources.values()) == {"NativeParquetScanSource"}, sources
    return sorted(out, key=lambda r: tuple(-1 if v is None else v for v in r))


def test_an_added_column_is_null_for_every_row(evolved_target):
    assert _native_rows(f"SELECT cve, extra FROM {TARGET}") == [(1, None), (2, None), (3, None)]


def test_a_query_reading_only_the_added_column_counts_every_row(evolved_target):
    """The file holds none of the read columns: nothing is read, the rows come from
    the footer."""
    assert _native_rows(f"SELECT extra FROM {TARGET}") == [(None,)] * 3


def test_merge_on_read_deletes_exclude_rows_of_an_unread_file(evolved_target):
    """The row admission composes with a file the scan never reads: the deleted row
    is not counted."""
    _clear()
    list(opteryx.session(user="tester").execute_to_morsels(f"DELETE FROM {TARGET} WHERE cve = 2"))
    target = evolved_target["col.tgt"]
    assert target.snapshot(None).summary["total-deleted-records"] == 1
    assert _native_rows(f"SELECT extra FROM {TARGET}") == [(None,)] * 2
    assert _native_rows(f"SELECT cve, extra FROM {TARGET}") == [(1, None), (3, None)]


def test_a_row_addressing_write_reads_its_target_through_an_absent_column(evolved_target):
    """UPDATE reads the target with `$file`/`$ordinal` appended; an absent column in
    the read set must not move the addresses."""
    _clear()
    list(opteryx.session(user="tester").execute_to_morsels(
        f"UPDATE {TARGET} SET details = details + 1 WHERE cve = 3"))
    assert _native_rows(f"SELECT cve, details, extra FROM {TARGET}") == [
        (1, 10, None), (2, 20, None), (3, 31, None)]
