# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression test for the LATMAT pass-1 dictionary-membership skip branch in
``ParquetReadNode._run_pass1`` (opteryx/operators/parquet_read/parquet_read.pyx).

When the two-pass late-materialization path is active and a pushed equality/IN
conjunct's dictionary lacks every needle in a row group, the C++ pass-1 decoder
flags the whole row group ``empty_filtered`` and the source hands the consumer a
``vectors is None`` sentinel. ``_run_pass1`` must account the pre-filter rows and
record the skip via ``self.scan_readings.record_pass1_skipped()`` — NOT
``self.record_pass1_skipped()`` (no such method on the node). That latent
AttributeError crashed any query reaching this branch and had ZERO coverage,
because the branch only fires when LATMAT is active AND dictionary pruning
eliminates a whole row group during pass 1.

The fixture below makes the branch fire deterministically: ``key`` is a
dictionary-encoded int column written across 10 row groups whose [min,max] range
brackets the needle in EVERY row group (so min/max statistics never prune a row
group before pass 1), but whose dictionary contains the needle in only two of
them. The remaining 8 row groups reach the pass-1 decoder, fail dict-membership,
and travel the ``vectors is None`` branch. A second, non-filter column
(``payload``) is projected so two-pass late-materialization is eligible.
"""

import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx import config
from opteryx.connectors import DiskConnector

import pytest


NEEDLE = 333
_NEEDLE_ROW_GROUPS = (3, 7)
_NEEDLE_ROWS = (10, 990)


def _get_read_operation(telemetry: dict) -> dict:
    """Return the first ReadRel operation dict from session telemetry."""
    for operation in telemetry.get("operations", {}).values():
        if operation.get("type") == "ReadRel":
            return operation
    raise AssertionError("No ReadRel operation found in telemetry")


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
    """Write the fixture, run ``sql`` (with the LATMAT flag set as requested),
    and return (sorted payload rows, ReadRel telemetry operation).

    The native footer gate is DECLINED for the duration so the scan takes the
    Python trampoline. The two-pass late-materialization path under test lives in
    ``ParquetReadNode._run_pass1`` — i.e. on the trampoline — and this query now
    selects ``NativeParquetScanSource`` on its own (the predicate relocates
    natively, WP-02), which is single-pass and leaves every ``parquet_latmat_*``
    sensor at zero. Without forcing, this test asserted 2 == 0 and the branch it
    exists to guard was never entered at all.
    """
    from opteryx.connectors.parquet_io import pool_reader

    table = _build_table()
    ws = _unique_ws()
    saved_gate = pool_reader.native_scan_supported
    pool_reader.native_scan_supported = lambda *a, **k: False
    try:
        return _run_inner(sql, table, ws, latmat)
    finally:
        pool_reader.native_scan_supported = saved_gate


def _run_inner(sql, table, ws, latmat):
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
            read_op = _get_read_operation(session.telemetry)
            return sorted(rows), read_op
        finally:
            os.chdir(cwd)


@pytest.fixture(autouse=True)
def _restore_latmat_config():
    orig = config.features.parquet_late_materialization
    yield
    config.features.parquet_late_materialization = orig


def test_latmat_pass1_dict_membership_skip_branch():
    """LATMAT pass-1 must survive a whole-row-group dictionary-membership skip
    (the ``vectors is None`` branch) and return the correct rows.

    Telemetry proves the branch was taken: 8 row groups are skipped, yet only 2
    were ever *evaluated* in pass 1 (the two that carry the needle). A skip that
    increments ``skipped_row_groups`` WITHOUT incrementing ``pass1_row_groups``
    can only be the source-level dict-membership skip — the mask-all-false skip
    path evaluates the row group first (and so would push pass1 to 10)."""
    rows, read_op = _run(
        "SELECT payload FROM {ws}.t WHERE key = " + str(NEEDLE),
        latmat=True,
    )

    # Correctness: exactly the needle-bearing rows, value-checked.
    assert rows == _expected_payloads(), rows

    # Two-pass late-materialization actually engaged.
    assert read_op.get("parquet_latmat_pass1_row_groups", 0) == 2, (
        "only the 2 needle-bearing row groups should be decoded+evaluated in pass 1"
    )
    assert read_op.get("parquet_latmat_pass2_row_groups", 0) == 2, (
        "pass 2 must run for the 2 surviving row groups"
    )

    # The decode-skip branch fired for the other 8 row groups.
    assert read_op.get("parquet_latmat_skipped_row_groups", 0) == 8, (
        "the 8 bracketed row groups must hit the vectors-is-None decode-skip branch"
    )
    # Pin it to the source-level branch: 8 skipped, 0 of them evaluated.
    skipped = read_op.get("parquet_latmat_skipped_row_groups", 0)
    evaluated = read_op.get("parquet_latmat_pass1_row_groups", 0)
    survived = read_op.get("parquet_latmat_pass2_row_groups", 0)
    assert evaluated - survived == 0, (
        "no row group should be skipped via the mask-all-false (evaluated) path; "
        f"evaluated={evaluated} survived={survived} skipped={skipped}"
    )


def test_latmat_dict_skip_result_matches_single_pass():
    """The two-pass result must be byte-identical to the single-pass (feature
    OFF) result — the decode-skip branch must not drop or corrupt any row."""
    rows_on, _ = _run(
        "SELECT payload FROM {ws}.t WHERE key = " + str(NEEDLE), latmat=True
    )
    rows_off, read_op_off = _run(
        "SELECT payload FROM {ws}.t WHERE key = " + str(NEEDLE), latmat=False
    )
    assert rows_on == rows_off == _expected_payloads()
    # Sanity: with the feature off, the two-pass sensors stay at zero.
    assert read_op_off.get("parquet_latmat_pass1_row_groups", 0) == 0


if __name__ == "__main__":
    test_latmat_pass1_dict_membership_skip_branch()
    test_latmat_dict_skip_result_matches_single_pass()
    print("✅ LATMAT pass-1 dict-membership skip regression tests passed")
