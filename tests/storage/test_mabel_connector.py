# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for MabelConnector - partition pruning + as_at/frame.complete resolution
over the legacy Mabel dataset layout, on top of the modern parquet read path.
"""

import datetime
import os
import shutil
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import draken.draken_native as dn
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector
from rugo.parquet import write_parquet

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.io_systems import OpteryxLocalFileSystem
from opteryx.connectors.mabel_connector import MabelConnector
from opteryx.connectors.mabel_connector import MabelTable
from opteryx.connectors.mabel_connector import UnsupportedSegmentationError
from opteryx.exceptions import DatasetReadError


def _write_part(folder: str, values: list) -> None:
    os.makedirs(folder, exist_ok=True)
    morsel = Morsel.from_vectors(["id"], [Vector(dn.vector_from_sequence(values))])
    with open(os.path.join(folder, "part.parquet"), "wb") as fh:
        fh.write(write_parquet(morsel, bloom_filters=False, dictionary=False))


def _touch(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "wb").close()


def _day_dir(root: str, date: datetime.date) -> str:
    return os.path.join(
        root, f"year_{date.year:04d}", f"month_{date.month:02d}", f"day_{date.day:02d}"
    )


_WORKSPACE_ROOT = "mabelconnector_tmp"


@pytest.fixture
def workspace():
    """Register a MabelConnector-backed workspace addressed as a relative-to-CWD
    path (dotted dataset name == path, matching how FileSystemConnector-backed
    tests address local data), cleaned up after the test."""
    prefix = _WORKSPACE_ROOT
    os.makedirs(prefix, exist_ok=True)
    register_workspace(
        prefix, MabelConnector, filesystem=OpteryxLocalFileSystem(), storage_type="LOCAL"
    )
    try:
        yield prefix
    finally:
        shutil.rmtree(prefix, ignore_errors=True)


def _row_count(dataset: str, at: str = None) -> int:
    sql = f"SELECT * FROM {dataset}"
    if at is not None:
        sql += f" TIMESTAMP AS OF '{at}'"
    session = opteryx.session()
    return sum(m.num_rows for m in session.execute_to_morsels(sql))


def test_single_snapshot_no_by_hour(workspace):
    prefix = workspace
    dataset = f"{prefix}.single"
    date = datetime.date(2026, 6, 15)
    day_dir = _day_dir(os.path.join(prefix, "single"), date)

    _write_part(os.path.join(day_dir, "as_at_0001"), [1, 2, 3])
    _touch(os.path.join(day_dir, "as_at_0001", "frame.complete"))

    assert _row_count(dataset, at="2026-06-15 00:00:00") == 3


def test_newest_complete_as_at_wins(workspace):
    prefix = workspace
    dataset = f"{prefix}.newest"
    date = datetime.date(2026, 6, 16)
    day_dir = _day_dir(os.path.join(prefix, "newest"), date)

    _write_part(os.path.join(day_dir, "as_at_0001"), [1])
    _touch(os.path.join(day_dir, "as_at_0001", "frame.complete"))
    _write_part(os.path.join(day_dir, "as_at_0002"), [2, 2])
    _touch(os.path.join(day_dir, "as_at_0002", "frame.complete"))

    # only the newest complete as_at's rows should be read - not a union of both
    # (as_at_0001 has 1 row, as_at_0002 has 2 - the counts disambiguate the pick)
    assert _row_count(dataset, at="2026-06-16 00:00:00") == 2


def test_incomplete_as_at_is_skipped_for_older_complete_one(workspace):
    prefix = workspace
    dataset = f"{prefix}.incomplete"
    date = datetime.date(2026, 6, 17)
    day_dir = _day_dir(os.path.join(prefix, "incomplete"), date)

    _write_part(os.path.join(day_dir, "as_at_0001"), [1])
    _touch(os.path.join(day_dir, "as_at_0001", "frame.complete"))
    # newest as_at has data but no frame.complete - must not be selected
    _write_part(os.path.join(day_dir, "as_at_0002"), [2, 2])

    assert _row_count(dataset, at="2026-06-17 00:00:00") == 1


def test_ignored_as_at_is_excluded_even_with_frame_complete(workspace):
    prefix = workspace
    dataset = f"{prefix}.ignored"
    date = datetime.date(2026, 6, 18)
    day_dir = _day_dir(os.path.join(prefix, "ignored"), date)

    _write_part(os.path.join(day_dir, "as_at_0001"), [1])
    _touch(os.path.join(day_dir, "as_at_0001", "frame.complete"))
    _write_part(os.path.join(day_dir, "as_at_0002"), [2, 2])
    _touch(os.path.join(day_dir, "as_at_0002", "frame.complete"))
    _touch(os.path.join(day_dir, "as_at_0002", "frame.ignore"))

    # as_at_0002 is marked complete AND ignored - falls back to as_at_0001
    assert _row_count(dataset, at="2026-06-18 00:00:00") == 1


def test_by_hour_partitions_are_unioned(workspace):
    prefix = workspace
    dataset = f"{prefix}.hourly"
    date = datetime.date(2026, 6, 19)
    day_dir = _day_dir(os.path.join(prefix, "hourly"), date)

    for hour, values in ((9, [1]), (14, [2, 2])):
        hour_dir = os.path.join(day_dir, "by_hour", f"hour={hour:02d}")
        _write_part(os.path.join(hour_dir, "as_at_0001"), values)
        _touch(os.path.join(hour_dir, "as_at_0001", "frame.complete"))

    # a point-in-time read with no pinned hour unions every hour bucket present
    assert _row_count(dataset, at="2026-06-19 00:00:00") == 3


def test_no_at_clause_defaults_to_today(workspace):
    prefix = workspace
    dataset = f"{prefix}.today"
    today = datetime.datetime.now(datetime.UTC).date()
    day_dir = _day_dir(os.path.join(prefix, "today"), today)

    _write_part(os.path.join(day_dir, "as_at_0001"), [7])
    _touch(os.path.join(day_dir, "as_at_0001", "frame.complete"))

    assert _row_count(dataset) == 1


def test_no_data_for_date_raises(workspace):
    prefix = workspace
    dataset = f"{prefix}.empty"

    with pytest.raises(DatasetReadError):
        _row_count(dataset, at="2026-01-01 00:00:00")


def test_unsupported_segmentation_raises(workspace):
    prefix = workspace
    dataset = f"{prefix}.badseg"
    date = datetime.date(2026, 6, 20)
    day_dir = _day_dir(os.path.join(prefix, "badseg"), date)

    _write_part(os.path.join(day_dir, "by_region", "region=uk", "as_at_0001"), [1])
    _touch(os.path.join(day_dir, "by_region", "region=uk", "as_at_0001", "frame.complete"))

    with pytest.raises(UnsupportedSegmentationError):
        _row_count(dataset, at="2026-06-20 00:00:00")


def test_preserve_sql_case_uses_original_relation_verbatim():
    # Pure resolution logic - no filesystem I/O, so no need for a real backend.
    table = MabelTable(
        dataset="mabel_ws.raw.nvd",
        filesystem=None,
        storage_type="LOCAL",
        preserve_sql_case=True,
        original_relation="mabel_ws.RAW.NVD",
        telemetry=None,
    )
    assert table.dataset == "mabel_ws/RAW/NVD"


def test_preserve_sql_case_without_original_relation_raises():
    # requires_original_case wasn't honoured (e.g. the binder wiring is missing) -
    # fail loud rather than silently falling back to the lowercased guess.
    with pytest.raises(DatasetReadError):
        MabelTable(
            dataset="mabel_ws.raw.nvd",
            filesystem=None,
            storage_type="LOCAL",
            preserve_sql_case=True,
            telemetry=None,
        )


def test_connector_rejects_case_map_and_preserve_sql_case_together():
    with pytest.raises(ValueError):
        MabelConnector(filesystem=None, case_map={}, preserve_sql_case=True)


def test_connector_sets_requires_original_case_flag():
    assert MabelConnector(filesystem=None, preserve_sql_case=True).requires_original_case is True
    assert MabelConnector(filesystem=None).requires_original_case is False


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
