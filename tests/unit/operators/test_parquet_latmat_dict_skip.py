# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Late materialization over a row group whose dictionary lacks the needle.

When a pushed equality conjunct's dictionary lacks every needle in a row group,
rugo's pass-1 decoder flags the whole row group ``empty_filtered`` and
``LatmatScanSource`` (src/cpp/engine/native_latmat_scan_source.hpp, pass 1) drops
it without evaluating it. That Source records no per-row-group counters, so these
tests cannot observe the skip itself; they pin the ANSWER over a fixture built to
take it, against a plain-Python oracle.

The fixture: ``key`` is a dictionary-encoded int column written across 10 row
groups whose [min,max] range brackets the needle in EVERY row group (so min/max
statistics never prune a row group before pass 1), but whose dictionary contains
the needle in only two of them. ``payload`` (the global row index) is projected
and is not a predicate column, so pass 2 has a column to fetch and the
``WHERE key = 333 ORDER BY key LIMIT n`` shape is late-materialized.
"""

import os
import sys
import tempfile

import pyarrow as pa  # test-only dep, used to WRITE parquet only
import pyarrow.parquet as pq

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx import config
from opteryx.connectors import DiskConnector

import pytest


NEEDLE = 333
_NEEDLE_ROW_GROUPS = (3, 7)
_NEEDLE_ROWS = (10, 990)


def _build_table():
    """Two int64 columns over 10 row groups of 1000 rows.

    ``key`` is dictionary-encoded. Every row group pins min=0 and max=1000 so the
    needle (333) is inside [min,max] for ALL row groups — min/max row-group
    pruning therefore cannot eliminate any row group, forcing each one into the
    pass-1 decoder. The needle is only present in the dictionaries of row groups
    3 and 7; the other 8 dictionaries lack it and travel the decode-skip branch.

    ``payload`` carries the global row index so result correctness is exact.
    """
    keys, payloads = [], []
    for rg in range(10):
        for i in range(1000):
            if i == 0:
                k = 0          # fixes per-row-group min
            elif i == 1:
                k = 1000       # fixes per-row-group max → brackets the needle
            else:
                k = (i % 40) * 5  # multiples of 5 in [0,195]; 333 never appears
            if rg in _NEEDLE_ROW_GROUPS and i in _NEEDLE_ROWS:
                k = NEEDLE
            keys.append(k)
            payloads.append(rg * 1000 + i)
    return pa.table(
        {
            "key": pa.array(keys, type=pa.int64()),
            "payload": pa.array(payloads, type=pa.int64()),
        }
    )


def _expected_payloads():
    return sorted(rg * 1000 + i for rg in _NEEDLE_ROW_GROUPS for i in _NEEDLE_ROWS)


_WS_COUNTER = [0]


def _unique_ws():
    _WS_COUNTER[0] += 1
    return f"ws_latmat_skip_{_WS_COUNTER[0]}"


def _run(sql, *, latmat):
    """Write the fixture into a fresh workspace, run ``sql`` with the LATMAT flag
    set as requested, and return (sorted payload rows, scan_sources)."""
    table = _build_table()
    ws = _unique_ws()
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, ws, "t")
        os.makedirs(data_dir)
        # use_dictionary=True + per-row-group writes → per-RG dictionaries; no
        # bloom filters are written by default, so only min/max stats can prune
        # (and they cannot, by construction).
        pq.write_table(
            table,
            os.path.join(data_dir, "data.parquet"),
            use_dictionary=True,
            row_group_size=1000,
        )
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            config.features.parquet_late_materialization = latmat
            opteryx.register_workspace(ws, DiskConnector)
            session = opteryx.session()
            rows = []
            for m in session.execute_to_morsels(sql.format(ws=ws)):
                rows.extend(m.column(b"payload").to_pylist())
            telemetry = session.telemetry
            return sorted(rows), list(telemetry["scan_sources"].values())
        finally:
            os.chdir(cwd)


@pytest.fixture(autouse=True)
def _restore_latmat_config():
    orig = config.features.parquet_late_materialization
    yield
    config.features.parquet_late_materialization = orig


_SQL = "SELECT key, payload FROM {ws}.t WHERE key = " + str(NEEDLE) + " ORDER BY key LIMIT 10"


def test_latmat_dict_skip_result_matches_oracle():
    """On LatmatScanSource, the row groups whose dictionary lacks the needle must
    not drop or corrupt any surviving row: exactly the needle-bearing rows come
    back (LIMIT 10 exceeds the 4 survivors, so all of them)."""
    rows, src = _run(_SQL, latmat=True)
    assert src == ["LatmatScanSource"], src
    assert rows == _expected_payloads(), rows


def test_single_pass_result_matches_oracle():
    """With the feature off the same query is single-pass and answers the same."""
    rows, src = _run(_SQL, latmat=False)
    assert src == ["NativeParquetScanSource"], src
    assert rows == _expected_payloads(), rows
