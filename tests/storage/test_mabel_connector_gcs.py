# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
MabelConnector against a real GCS bucket (gs://mabel_data/RAW/NVD/CVE_LIST).

Requires live GCP credentials (Application Default Credentials) with read
access to gs://mabel_data - no skip guard, matching test_blob_gcs.py's
convention of failing outright rather than silently skipping without creds.

The queried partition (2025-08-03) is a completed, immutable historical
snapshot (frame.complete written, per Mabel's write-once convention), so the
exact row/column counts below are stable, not a guess.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.mabel_connector import create_gcs_mabel_connector
from opteryx.exceptions import DatasetReadError

_CASE_MAP = {
    "mabel_data.raw.nvd.cve_list": "mabel_data/RAW/NVD/CVE_LIST",
}


@pytest.fixture
def workspace():
    # register_workspace bakes its kwargs into a cache-key tuple, which requires
    # every value to be hashable - a raw dict isn't, so pass its bound .get
    # (hashable, same lookup semantics) rather than the dict itself.
    register_workspace("mabel_data", create_gcs_mabel_connector, case_map=_CASE_MAP.get)
    return "mabel_data"


@pytest.fixture
def workspace_preserve_case():
    # Same "mabel_data" prefix, different strategy - re-registering overwrites
    # the prior fixture's entry, which is safe: tests in one worker never run
    # concurrently, and pytest-xdist workers don't share this process-global
    # registry across processes.
    register_workspace("mabel_data", create_gcs_mabel_connector, preserve_sql_case=True)
    return "mabel_data"


def test_real_partition_resolves_and_reads(workspace):
    session = opteryx.session()
    sql = "SELECT * FROM mabel_data.RAW.NVD.CVE_LIST TIMESTAMP AS OF '2025-08-03'"
    morsels = list(session.execute_to_morsels(sql))

    assert sum(m.num_rows for m in morsels) == 209_317
    assert morsels[0].num_columns == 5

    scan = [v for v in session.telemetry["operations"].values() if v.get("type") == "ReadRel"][0]
    # 15 data blobs under as_at_20250803-030013/ (one frame.complete control blob
    # alongside them, correctly excluded from the data read).
    assert scan["blobs_read"] == 15


def test_unmapped_dataset_raises(workspace):
    session = opteryx.session()
    with pytest.raises(DatasetReadError):
        list(
            session.execute_to_morsels(
                "SELECT * FROM mabel_data.RAW.NVD.NOT_A_REAL_DATASET TIMESTAMP AS OF '2025-08-03'"
            )
        )


def test_preserve_sql_case_resolves_and_reads(workspace_preserve_case):
    # No case_map at all - the real-cased path is recovered from the relation
    # exactly as typed here (RAW/NVD/CVE_LIST), threaded through by the binder's
    # requires_original_case wiring.
    session = opteryx.session()
    sql = "SELECT * FROM mabel_data.RAW.NVD.CVE_LIST TIMESTAMP AS OF '2025-08-03'"
    morsels = list(session.execute_to_morsels(sql))

    assert sum(m.num_rows for m in morsels) == 209_317


def test_preserve_sql_case_wrong_case_raises(workspace_preserve_case):
    # preserve_sql_case is faithful, not case-insensitive - mistyped casing is a
    # real miss (no such GCS path), not silently corrected.
    session = opteryx.session()
    sql = "SELECT * FROM mabel_data.raw.nvd.cve_list TIMESTAMP AS OF '2025-08-03'"
    with pytest.raises(DatasetReadError):
        list(session.execute_to_morsels(sql))


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
