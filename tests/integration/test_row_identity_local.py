"""Row identity ($file, $ordinal) from the native parquet scan, end to end — local disk.

The native scan numbers each row it emits by its physical position in its file
(opteryx/constants/row_identity.py), from the footer's row-group row counts and the
rows the decode kept. Every path that drops rows before the scan emits them must
keep that number exact, or DELETE / UPDATE / MERGE address the wrong rows. What each
test protects, on a target of many row groups with many small pages:
  * the worker prefilter (survivors gathered on the decode workers) together with
    PageIndex page pruning: a DELETE whose predicate prunes pages AND filters rows;
  * merge-on-read deletes (rows masked before decode): an UPDATE after that DELETE;
  * a scan reading no column at all (DELETE with no predicate) numbers every row;
  * MERGE over the same target.
Each is checked against the rows the statement must leave, computed here.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx  # noqa: E402
from opteryx.connectors import OpteryxConnector  # noqa: E402

from tests.integration.test_merge_into_local import _LocalDiskIO  # noqa: E402
from tests.integration.test_merge_into_local import _build_dataset  # noqa: E402
from tests.integration.test_merge_into_local import _rows  # noqa: E402
from tests.integration.test_merge_into_local import pytestmark  # noqa: E402,F401

WORKSPACE = "ridws"
TARGET = f"{WORKSPACE}.col.big"
SOURCE = f"{WORKSPACE}.col.upd"
ROWS = 12_000


def _seed():
    # High-entropy payloads keep the compressed chunks large against their page index,
    # which is what the reader's page-pruning cost gate weighs.
    return [(i, (i * 2654435761) % 2147483647, (i * 40503 + 7) % 1000003) for i in range(ROWS)]


@pytest.fixture
def big_env(tmp_path):
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache

    import opteryx.connectors as connectors

    clear_parsed_manifest_cache()
    disk_io = _LocalDiskIO()
    # 3 row groups of 4,000 rows, each column in several pages with a page index: `cve`
    # ascends, so an IN list on it prunes pages inside a row group.
    target = _build_dataset(
        str(tmp_path / "big"), "col.big", ["cve", "details", "revision"], _seed(), disk_io,
        max_rows_per_row_group=4000, max_page_bytes=4096, row_groups_per_block=1,
        dictionary=False,
    )
    source = _build_dataset(
        str(tmp_path / "upd"), "col.upd", ["cve", "details"],
        [(i, -i) for i in range(3_500, 3_520)] + [(ROWS + 1, 5)], disk_io,
    )
    datasets = {"col.big": target, "col.upd": source}

    class _FakeCatalog:
        def __init__(self, workspace=None, **kwargs):
            self.workspace = workspace
            self.io = disk_io

        def dataset_exists(self, identifier):
            return identifier in datasets

        def load_dataset(self, identifier):
            if identifier not in datasets:
                raise KeyError(identifier)
            return datasets[identifier]

        def list_vector_indexes(self, identifier):
            return []

        def get_relation(self, identifier):
            if identifier in datasets:
                return "dataset", datasets[identifier]
            return None, None

    saved_default = connectors._default_connector
    saved_prefixes = dict(connectors._storage_prefixes)
    saved_cache = dict(connectors._connector_cache)
    connectors._storage_prefixes.pop(WORKSPACE, None)
    connectors._connector_cache.clear()
    opteryx.set_default_connector(OpteryxConnector, catalog=_FakeCatalog)
    try:
        yield datasets
    finally:
        connectors._default_connector = saved_default
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved_prefixes)
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved_cache)


def _run(sql):
    session = opteryx.session(user="tester")
    list(session.execute_to_morsels(sql))
    return session


def _target():
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache

    clear_parsed_manifest_cache()
    return sorted(_rows(f"SELECT cve, details, revision FROM {TARGET}"))


def test_delete_under_page_pruning_and_the_prefilter_addresses_the_right_rows(big_env):
    # `cve` prunes pages (and row groups); `details` filters rows within what is left.
    wanted = list(range(4321, 4900, 3)) + list(range(9001, 9400, 2))
    in_list = ", ".join(str(c) for c in wanted)
    session = _run(f"DELETE FROM {TARGET} WHERE cve IN ({in_list}) AND details % 7 = 3")
    # Both row-dropping paths actually ran: pages were pruned inside row groups, and the
    # decode workers filtered rows (survivors fewer than the rows they saw).
    (diag,) = session.telemetry["io_scan_diagnostics"]
    assert diag["page_index_pages_pruned"] > 0
    assert 0 < diag["prefilter_rows_out"] < diag["prefilter_rows_in"]
    wanted = set(wanted)
    expected = [r for r in _seed() if not (r[0] in wanted and r[1] % 7 == 3)]
    assert _target() == expected


def test_update_after_a_delete_addresses_the_right_rows(big_env):
    _run(f"DELETE FROM {TARGET} WHERE details % 5 = 0")
    _run(f"UPDATE {TARGET} SET revision = 2 WHERE cve >= 1500 AND cve < 2500 AND details % 3 = 1")
    expected = [
        (c, d, 2 if (1500 <= c < 2500 and d % 3 == 1) else r)
        for c, d, r in _seed() if d % 5 != 0
    ]
    assert _target() == expected


def test_delete_without_a_predicate_numbers_every_row(big_env):
    _run(f"DELETE FROM {TARGET} WHERE details % 2 = 0")
    _run(f"DELETE FROM {TARGET}")
    assert _target() == []


def test_merge_addresses_the_right_rows(big_env):
    _run(f"DELETE FROM {TARGET} WHERE cve % 4 = 0")
    _run(f"""
        MERGE INTO {TARGET} AS n USING {SOURCE} AS t ON n.cve = t.cve
         WHEN MATCHED THEN UPDATE SET details = t.details, revision = n.revision + 1
         WHEN NOT MATCHED THEN INSERT (cve, details, revision) VALUES (t.cve, t.details, 1)
    """)
    updates = {c: d for c, d in [(i, -i) for i in range(3_500, 3_520)]}
    expected = [
        (c, updates[c], r + 1) if c in updates else (c, d, r)
        for c, d, r in _seed() if c % 4 != 0
    ]
    expected += [(c, updates[c], 1) for c in updates if c % 4 == 0] + [(ROWS + 1, 5, 1)]
    assert _target() == sorted(expected)
