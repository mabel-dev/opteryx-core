"""`SET write_coalesce_rows` governs the row-group size of every write that
streams data files, end to end through the engine — local disk, no GCS.

The variable is the row count DataFileStream coalesces morsels up to before
writing each parquet row group, so it is observable as the row-group count of
the files a statement adds. INSERT is the control: it has always honoured it.

The environment (a real catalog-backed dataset of 360,000 rows on local disk)
is the OPTIMIZE suite's, imported rather than copied so the two cannot drift.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx  # noqa: E402

# The package spelling, for the reason given in test_update_delete_local.py.
from tests.integration.test_optimize_local import ROWS_PER_SEED_FILE  # noqa: E402
from tests.integration.test_optimize_local import SEED_FILES  # noqa: E402
from tests.integration.test_optimize_local import TARGET  # noqa: E402
from tests.integration.test_optimize_local import _current_entries  # noqa: E402
from tests.integration.test_optimize_local import _row_group_count  # noqa: E402
from tests.integration.test_optimize_local import optimize_env  # noqa: E402,F401
from tests.integration.test_optimize_local import pytestmark  # noqa: E402,F401

TOTAL = SEED_FILES * ROWS_PER_SEED_FILE  # 360,000
COALESCE = 10_000


def _run_with_coalesce(statement):
    session = opteryx.session(user="tester")
    list(session.execute_to_morsels(f"SET write_coalesce_rows = {COALESCE}"))
    list(session.execute_to_morsels(statement))


def _added_row_groups(target, before_paths):
    """Row groups per file this statement added, and the rows they hold."""
    added = [e for e in _current_entries(target) if e["file_path"] not in before_paths]
    assert added, "the statement added no data file"
    return sum(_row_group_count(e["file_path"]) for e in added), sum(e["record_count"] for e in added)


def _paths(target):
    return {e["file_path"] for e in _current_entries(target)}


def test_insert_honours_write_coalesce_rows(optimize_env):
    target, _ = optimize_env
    before = _paths(target)

    _run_with_coalesce(f"INSERT INTO {TARGET} SELECT k FROM {TARGET}")

    row_groups, rows = _added_row_groups(target, before)
    assert rows == TOTAL
    assert row_groups == TOTAL // COALESCE  # 36, not the 6 of 65,536-row groups


def test_update_honours_write_coalesce_rows(optimize_env):
    target, _ = optimize_env
    before = _paths(target)

    _run_with_coalesce(f"UPDATE {TARGET} SET k = k + 1")

    row_groups, rows = _added_row_groups(target, before)
    assert rows == TOTAL
    assert row_groups == TOTAL // COALESCE


def test_optimize_honours_write_coalesce_rows(optimize_env):
    target, _ = optimize_env
    before = _paths(target)

    _run_with_coalesce(f"OPTIMIZE TABLE {TARGET}")

    row_groups, rows = _added_row_groups(target, before)
    assert rows == TOTAL
    assert row_groups == TOTAL // COALESCE
