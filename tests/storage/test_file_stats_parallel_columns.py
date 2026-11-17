# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
FileStats.add_row_group spreads a row group's columns over threads
(file_stats.hpp). Each column's statistics must be what that column alone
produces (one column = one thread), and repeated runs must be byte-identical.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx
from opteryx.compiled.planner.native_manifest import FileStats

_SQL = (
    "SELECT CAST(g AS INT32) AS i32, "
    "CASE WHEN g % 7 = 0 THEN NULL ELSE CAST(g * 31 AS INT64) END AS i64, "
    "CAST(g AS VARCHAR) AS s, "
    "CASE WHEN g % 3 = 0 THEN NULL ELSE CAST(g % 50 AS VARCHAR) END AS s_low, "
    "CAST(g AS FLOAT64) / 3 AS f, "
    "CAST(g AS DATE) AS d, "
    "g % 2 = 0 AS b "
    "FROM generate_series(1, 200000) AS g"
)


def _morsels():
    return [m for m in opteryx.session().execute_to_morsels(_SQL) if len(m)]


def _row(morsels):
    stats = FileStats()
    for morsel in morsels:
        stats.add_row_group(morsel)
    rows = sum(len(m) for m in morsels)
    return stats.file_row("f.parquet", "PARQUET", rows, 1, len(morsels), stats.uncompressed_size)


def test_each_column_matches_that_column_alone():
    morsels = _morsels()
    wide = _row(morsels)
    for k, name in enumerate(morsels[0].column_names):
        alone = _row([m.select([name]) for m in morsels])
        assert wide.cell(0, k) == alone.cell(0, 0), name


def test_repeated_runs_are_identical():
    morsels = _morsels()
    images = {_row(morsels).to_parquet() for _ in range(5)}
    assert len(images) == 1


if __name__ == "__main__":  # pragma: no cover
    test_each_column_matches_that_column_alone()
    test_repeated_runs_are_identical()
    print("okay")
