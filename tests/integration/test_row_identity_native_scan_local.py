"""Row-addressing writes read their target on the NATIVE parquet scan — local disk.

MERGE, UPDATE, DELETE and OPTIMIZE ask the target Scan for row identity (`$file`,
`$ordinal`, constants/row_identity.py). Those columns are synthesized by the scan,
never read from a data file, so a native scan that hands them to the footer gate
asks for a column no footer holds. Once the Python trampoline was deleted (no
fallback, ruled 2026-10-03) that refusal was the whole statement: release 0.9.155
refused EVERY MERGE in production with

    Reading <target> (footer_gate) is not supported.

while a SELECT of every column of the same table passed. These tests run the
production statement shapes end to end and assert each scan went native with no
residual reason recorded — a `footer_gate` here fails the test by name rather than
as an incidental exception.

The environments are the merge and optimize suites', imported rather than copied
so the suites cannot drift.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx  # noqa: E402

# The package spelling (see test_update_delete_local.py): a bare module import
# would load the fixture modules twice, with two sets of globals.
from tests.integration.test_merge_into_local import SOURCE  # noqa: E402
from tests.integration.test_merge_into_local import TARGET  # noqa: E402
from tests.integration.test_merge_into_local import _target_rows  # noqa: E402
from tests.integration.test_merge_into_local import merge_env  # noqa: E402,F401
from tests.integration.test_merge_into_local import pytestmark  # noqa: E402,F401
from tests.integration.test_optimize_local import ROWS_PER_SEED_FILE  # noqa: E402
from tests.integration.test_optimize_local import SEED_FILES  # noqa: E402
from tests.integration.test_optimize_local import TARGET as OPTIMIZE_TARGET  # noqa: E402
from tests.integration.test_optimize_local import _current_entries  # noqa: E402
from tests.integration.test_optimize_local import _scalar  # noqa: E402
from tests.integration.test_optimize_local import optimize_env  # noqa: E402,F401

_NATIVE_SOURCES = {"NativeParquetScanSource", "LatmatScanSource"}

# The hourly NVD sync, as production runs it: a guarded MATCHED arm that leaves an
# unchanged row alone, an INSERT for new keys, and a full-sync DELETE for keys the
# source no longer carries.
_SYNC = f"""
MERGE INTO {TARGET} AS n
USING {SOURCE} AS u
   ON n.cve = u.cve
 WHEN MATCHED AND (n.details IS DISTINCT FROM u.details)
      THEN UPDATE SET details = u.details, revision = n.revision + 1
 WHEN NOT MATCHED
      THEN INSERT (cve, details, revision) VALUES (u.cve, u.details, 1)
 WHEN NOT MATCHED BY SOURCE
      THEN DELETE
"""


def _run_native(sql):
    """Run `sql` to completion and assert every scan it planned went native (a scan
    neither native Source admits raises NativeScanRefusedError). Returns the scan
    count."""
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache

    clear_parsed_manifest_cache()
    session = opteryx.session(user="tester")
    for _ in session.execute_to_morsels(sql):
        pass
    telemetry = session.telemetry
    sources = telemetry.get("scan_sources") or {}
    assert sources, "no parquet scan observed"
    assert set(sources.values()) <= _NATIVE_SOURCES, f"non-native scan Source: {sources}"
    return len(sources)


def test_guarded_sync_merge_reads_its_target_natively(merge_env):
    """The production statement: IS DISTINCT FROM guard, INSERT, BY SOURCE DELETE."""
    assert _run_native(_SYNC) == 2, "the target and the source are both scanned"

    assert _target_rows() == [
        # cve 1 is absent from the source — deleted by the BY SOURCE arm
        (2, 20, 1),  # matched, IS NOT DISTINCT — left alone
        (3, 99, 2),  # matched and changed — replaced, revision read from the old row
        (4, 40, 1),  # not matched — inserted
    ]
    summary = merge_env["col.tgt"].snapshot(None).summary
    assert summary["deleted-records"] == 2  # cve 1 (BY SOURCE) and the old cve 3
    assert summary["added-records"] == 2  # the new cve 3 and cve 4


def test_merge_from_a_source_pinned_by_version_reads_natively(merge_env):
    """`VERSION AS OF` on the source relation itself (not inside a sub-query)."""
    _run_native(_SYNC.replace(f"USING {SOURCE} AS u", f"USING {SOURCE} VERSION AS OF 1000 AS u"))
    assert _target_rows() == [(2, 20, 1), (3, 99, 2), (4, 40, 1)]


def test_merge_into_a_target_carrying_delete_vectors_reads_natively(merge_env):
    """The second hourly fire: the target now carries the first MERGE's merge-on-read
    delete vector, so its row-identity scan composes deletes with `$ordinal` — and
    the ordinals it emits must still address the right rows."""
    _run_native(_SYNC)
    target = merge_env["col.tgt"]
    assert target.snapshot(None).summary["total-deleted-records"] == 2

    # Re-publish the same source: nothing differs, so nothing may be written.
    before = target.metadata.current_snapshot_id
    _run_native(_SYNC)
    assert target.metadata.current_snapshot_id == before
    assert _target_rows() == [(2, 20, 1), (3, 99, 2), (4, 40, 1)]

    # Change a survivor of the first merge: it lives in the APPENDED file, and the
    # first merge's deletes are against the seed file — the address must land on
    # the live copy, not a deleted one.
    _run_native(f"""
        MERGE INTO {TARGET} AS n
        USING (SELECT cve, details + 1 AS details FROM {SOURCE} WHERE cve = 3) AS u
           ON n.cve = u.cve
         WHEN MATCHED AND (n.details IS DISTINCT FROM u.details)
              THEN UPDATE SET details = u.details, revision = n.revision + 1
    """)
    assert _target_rows() == [(2, 20, 1), (3, 100, 3), (4, 40, 1)]


def test_update_and_delete_read_their_target_natively(merge_env):
    """UPDATE and DELETE desugar through the same row-identity scan."""
    _run_native(f"UPDATE {TARGET} SET details = details + 1 WHERE cve = 2")
    _run_native(f"DELETE FROM {TARGET} WHERE cve = 1")
    assert _target_rows() == [(2, 21, 1), (3, 30, 1)]


def _second_file():
    """Append cve 4 as a second data file: the target is then seed.parquet (cve 1-3)
    plus one file holding only cve 4, so `WHERE cve = 4` prunes the seed file."""
    _run_native(f"INSERT INTO {TARGET} (cve, details, revision) "
                f"SELECT cve, details, 1 FROM {SOURCE} WHERE cve = 4")
    assert _target_rows() == [(1, 10, 1), (2, 20, 1), (3, 30, 1), (4, 40, 1)]


def _files_read(sql):
    """Run `sql`; return how many files its (single) parquet scan read."""
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache

    clear_parsed_manifest_cache()
    session = opteryx.session(user="tester")
    for _ in session.execute_to_morsels(sql):
        pass
    (facts,) = session._telemetry._reading["native_scan_facts"].values()
    return facts["files_read"]


def test_update_whose_where_prunes_a_file_replaces_the_right_row(merge_env):
    """THE 0.9.153-0.9.155 CORRUPTION. `$file` was the file's position in the scan's
    PRUNED manifest while the sink mapped it through the UNPRUNED list: with the seed
    file pruned, cve 4's file was index 0 to the scan and seed.parquet to the sink, so
    the UPDATE deleted cve 1 (seed ordinal 0) and left the old cve 4 alive."""
    _second_file()
    assert _files_read(f"UPDATE {TARGET} SET details = details + 1 WHERE cve = 4") == 1, (
        "the WHERE no longer prunes the seed file - this test no longer exercises pruning")
    assert _target_rows() == [(1, 10, 1), (2, 20, 1), (3, 30, 1), (4, 41, 1)]


def test_delete_whose_where_prunes_a_file_deletes_the_right_row(merge_env):
    _second_file()
    assert _files_read(f"DELETE FROM {TARGET} WHERE cve = 4") == 1
    assert _target_rows() == [(1, 10, 1), (2, 20, 1), (3, 30, 1)]


def test_writes_after_a_pruned_update_keep_addressing_the_right_rows(merge_env):
    """The production sequence that surfaced it: a pruned UPDATE, then a DELETE of a
    row in a later file, then a full sync MERGE over three files with deletes."""
    _second_file()
    _run_native(f"UPDATE {TARGET} SET details = details + 1 WHERE cve = 4")
    _run_native(f"DELETE FROM {TARGET} WHERE cve = 2")
    assert _target_rows() == [(1, 10, 1), (3, 30, 1), (4, 41, 1)]
    _run_native(_SYNC)
    # source is cve 2 -> 20, 3 -> 99, 4 -> 40: cve 1 deleted BY SOURCE, 2 inserted,
    # 3 and 4 replaced (revision read from the live row each time).
    assert _target_rows() == [(2, 20, 1), (3, 99, 2), (4, 40, 2)]


def test_optimize_of_a_multi_file_table_with_deletes_reads_natively(optimize_env):
    """OPTIMIZE over several files carrying a delete vector: the compaction scan
    goes native, drops the deleted rows and leaves one file with no delete debt."""
    target, _disk_io = optimize_env
    total = SEED_FILES * ROWS_PER_SEED_FILE
    _run_native(f"DELETE FROM {OPTIMIZE_TARGET} WHERE k % 10 = 0")
    deleted = total // 10
    assert target.snapshot(None).summary["total-deleted-records"] == deleted
    assert len(_current_entries(target)) == SEED_FILES

    _run_native(f"OPTIMIZE TABLE {OPTIMIZE_TARGET}")

    snap = target.snapshot(None)
    assert snap.operation_type == "compact"
    assert snap.summary["deleted-data-files"] == SEED_FILES
    assert snap.summary["added-records"] == total - deleted
    assert snap.summary.get("total-deleted-records", 0) == 0
    assert len(_current_entries(target)) == 1
    assert _scalar(f"SELECT COUNT(*) FROM {OPTIMIZE_TARGET}") == total - deleted
    assert _scalar(f"SELECT COUNT(*) FROM {OPTIMIZE_TARGET} WHERE k % 10 = 0") == 0


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
