"""MERGE INTO, end to end through the engine — local disk, no GCS.

The catalog's `merge_commit` (appends and row-deletes in one snapshot) is tested
in opteryx-catalog/tests/test_merge_commit.py. This module proves the ENGINE
half: that the desugared join classifies each row correctly, that the target
scan emits a usable row address, and that the sink turns those into the two
halves of a single commit.

The arms are exercised together because their interaction is the whole point —
in particular the guarded MATCHED arm, which is what makes a republished but
unchanged row cost nothing at all.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

_CATALOG_REPO = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "opteryx-catalog")
)
if os.path.isdir(_CATALOG_REPO) and _CATALOG_REPO not in sys.path:
    sys.path.insert(1, _CATALOG_REPO)

import opteryx  # noqa: E402
from opteryx.connectors import OpteryxConnector  # noqa: E402

try:
    import opteryx_catalog  # noqa: F401

    _HAVE_CATALOG = True
except ImportError:
    _HAVE_CATALOG = False

pytestmark = pytest.mark.skipif(
    not _HAVE_CATALOG,
    reason=f"opteryx_catalog not importable (expected sibling repo at {_CATALOG_REPO})",
)

WORKSPACE = "mrgws"
TARGET = f"{WORKSPACE}.col.tgt"
SOURCE = f"{WORKSPACE}.col.src"


class _LocalDiskIO:
    """Catalog-side FileIO over absolute local paths."""

    class _In:
        def __init__(self, path):
            self._path = path

        def open(self):
            return open(self._path, "rb")

    class _Out:
        def __init__(self, path):
            self._path = path
            self._chunks = []

        def create(self):
            return self

        def write(self, data):
            self._chunks.append(data)

        def close(self):
            os.makedirs(os.path.dirname(self._path), exist_ok=True)
            with open(self._path, "wb") as f:
                for chunk in self._chunks:
                    f.write(chunk)

    def new_input(self, path):
        return self._In(path)

    def new_output(self, path):
        return self._Out(path)


def _build_dataset(location, identifier, columns, rows, disk_io):
    """A real SimpleDataset on local disk holding one data file."""
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    from opteryx_catalog.catalog.dataset import SimpleDataset
    from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry_from_bytes
    from opteryx_catalog.catalog.metadata import DatasetMetadata
    from opteryx_catalog.catalog.metadata import Snapshot
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    os.makedirs(f"{location}/data", exist_ok=True)
    os.makedirs(f"{location}/metadata", exist_ok=True)

    morsel = Morsel()
    for index, name in enumerate(columns):
        morsel.append_vector(name, vector_from_sequence([r[index] for r in rows], dtype="INTEGER"))
    data = write_parquet(morsel, compression="zstd")
    path = f"{location}/data/seed.parquet"
    with open(path, "wb") as f:
        f.write(data)

    field_ids = {name: i + 1 for i, name in enumerate(columns)}
    entry = build_parquet_manifest_entry_from_bytes(
        data, path, len(data), field_id_by_name=field_ids
    ).to_dict()

    class _ManifestWriterCatalog:
        io = disk_io
        write_parquet_manifest = OpteryxCatalog.write_parquet_manifest

        def save_snapshot(self, identifier, snapshot):
            pass

        def save_dataset_metadata(self, identifier, metadata, **kwargs):
            pass

    writer_catalog = _ManifestWriterCatalog()
    snapshot_id = 1000
    manifest_path = writer_catalog.write_parquet_manifest(snapshot_id, [entry], location)

    meta = DatasetMetadata(
        dataset_identifier=identifier,
        location=location,
        schema=None,
        properties={},
        schemas=[
            {
                "schema_id": "s1",
                "columns": [
                    {"id": i + 1, "name": name, "type": "INTEGER"}
                    for i, name in enumerate(columns)
                ],
            }
        ],
        current_schema_id="s1",
    )
    meta.snapshots.append(
        Snapshot(
            snapshot_id=snapshot_id,
            timestamp_ms=snapshot_id,
            author="seed",
            sequence_number=1,
            user_created=True,
            operation_type="append",
            manifest_list=manifest_path,
            schema_id="s1",
        )
    )
    meta.current_snapshot_id = snapshot_id

    ds = SimpleDataset(identifier=identifier, _metadata=meta)
    ds.io = disk_io
    ds.catalog = writer_catalog
    return ds


@pytest.fixture
def merge_env(tmp_path):
    """Target and source datasets, wired behind a catalog connector.

    Function-scoped: a MERGE mutates the target, so each test needs its own.
    """
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache

    import opteryx.connectors as connectors

    clear_parsed_manifest_cache()
    disk_io = _LocalDiskIO()

    target = _build_dataset(
        str(tmp_path / "tgt"),
        "col.tgt",
        ["cve", "details", "revision"],
        [(1, 10, 1), (2, 20, 1), (3, 30, 1)],
        disk_io,
    )
    source = _build_dataset(
        str(tmp_path / "src"),
        "col.src",
        ["cve", "details"],
        [(2, 20), (3, 99), (4, 40)],
        disk_io,
    )
    # A source that names cve 2 twice: the cardinality violation. Kept as a
    # separate relation so the well-formed tests are not shaped around it.
    dup_source = _build_dataset(
        str(tmp_path / "dup"),
        "col.dup",
        ["cve", "details"],
        [(2, 77), (2, 88)],
        disk_io,
    )
    datasets = {"col.tgt": target, "col.src": source, "col.dup": dup_source}

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


def _rows(sql):
    session = opteryx.session(user="tester")
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(tuple(morsel[i]))
    return out


def _target_rows():
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache

    # The merge committed a new snapshot; the parsed-manifest cache is keyed on
    # path and would otherwise serve the pre-merge manifest back.
    clear_parsed_manifest_cache()
    return sorted(_rows(f"SELECT cve, details, revision FROM {TARGET}"))


_UPSERT = f"""
MERGE INTO {TARGET} AS n
USING {SOURCE} AS t
   ON n.cve = t.cve
 WHEN MATCHED AND n.details <> t.details
      THEN UPDATE SET details = t.details, revision = n.revision + 1
 WHEN NOT MATCHED
      THEN INSERT (cve, details, revision) VALUES (t.cve, t.details, 1)
"""


def test_upsert_applies_every_arm(merge_env):
    assert _target_rows() == [(1, 10, 1), (2, 20, 1), (3, 30, 1)]

    list(opteryx.session(user="tester").execute_to_morsels(_UPSERT))

    assert _target_rows() == [
        (1, 10, 1),  # never mentioned by the source — untouched
        (2, 20, 1),  # matched but unchanged — the guard sent it to NOOP
        (3, 99, 2),  # matched and changed — replaced, revision read from the old row
        (4, 40, 1),  # not matched — inserted
    ]


def test_unchanged_rows_are_not_rewritten(merge_env):
    """The guarded arm's whole purpose: a republished-but-identical row must
    cost nothing — no delete position, no appended copy."""
    target = merge_env["col.tgt"]
    before = target.metadata.current_snapshot_id

    list(opteryx.session(user="tester").execute_to_morsels(_UPSERT))

    snap = target.snapshot(None)
    assert target.metadata.current_snapshot_id != before
    assert snap.operation_type == "merge"
    # cve 3 replaced (1 delete + 1 append) and cve 4 inserted; cve 1 and cve 2
    # contributed nothing at all.
    assert snap.summary["deleted-records"] == 1
    assert snap.summary["added-records"] == 2


def test_merge_is_one_snapshot(merge_env):
    target = merge_env["col.tgt"]
    before = len(target.metadata.snapshots)
    list(opteryx.session(user="tester").execute_to_morsels(_UPSERT))
    assert len(target.metadata.snapshots) == before + 1


def test_delete_arm_removes_rows(merge_env):
    sql = f"""
    MERGE INTO {TARGET} AS n
    USING {SOURCE} AS t
       ON n.cve = t.cve
     WHEN MATCHED THEN DELETE
    """
    list(opteryx.session(user="tester").execute_to_morsels(sql))
    # cve 2 and 3 matched the source and were deleted; cve 1 never matched.
    assert _target_rows() == [(1, 10, 1)]


def test_insert_only_merge(merge_env):
    sql = f"""
    MERGE INTO {TARGET} AS n
    USING {SOURCE} AS t
       ON n.cve = t.cve
     WHEN NOT MATCHED THEN INSERT (cve, details, revision) VALUES (t.cve, t.details, 1)
    """
    list(opteryx.session(user="tester").execute_to_morsels(sql))
    assert _target_rows() == [(1, 10, 1), (2, 20, 1), (3, 30, 1), (4, 40, 1)]


def test_merge_that_changes_nothing_commits_nothing(merge_env):
    """Every row NOOP is a successful merge that did no work — not a failure,
    and not a snapshot describing nothing."""
    target = merge_env["col.tgt"]
    before = target.metadata.current_snapshot_id
    sql = f"""
    MERGE INTO {TARGET} AS n
    USING {SOURCE} AS t
       ON n.cve = t.cve
     WHEN MATCHED AND n.details = -1 THEN DELETE
    """
    list(opteryx.session(user="tester").execute_to_morsels(sql))
    assert target.metadata.current_snapshot_id == before
    assert _target_rows() == [(1, 10, 1), (2, 20, 1), (3, 30, 1)]


def test_repeated_merge_is_idempotent(merge_env):
    """A feed re-run over the same delta must not keep replacing rows: after the
    first pass every source row is either identical or absent, so the second
    pass is entirely NOOP."""
    list(opteryx.session(user="tester").execute_to_morsels(_UPSERT))
    first = _target_rows()
    target = merge_env["col.tgt"]
    after_first = target.metadata.current_snapshot_id

    list(opteryx.session(user="tester").execute_to_morsels(_UPSERT))
    assert _target_rows() == first
    assert target.metadata.current_snapshot_id == after_first


DUP_SOURCE = f"{WORKSPACE}.col.dup"


def test_cardinality_violation_is_refused(merge_env):
    """A target row matched by two source rows must raise, not be acted on twice.

    Acting twice would mark one ordinal deleted and append two replacements —
    one row silently becoming two, in storage. This is the check the whole
    row-address machinery exists to make possible.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    target = merge_env["col.tgt"]
    before = target.metadata.current_snapshot_id

    sql = f"""
    MERGE INTO {TARGET} AS n
    USING {DUP_SOURCE} AS t
       ON n.cve = t.cve
     WHEN MATCHED THEN UPDATE SET details = t.details
    """
    with pytest.raises(UnsupportedSyntaxError, match="cardinality violation"):
        list(opteryx.session(user="tester").execute_to_morsels(sql))

    # Refused before anything committed: no snapshot, target untouched.
    assert target.metadata.current_snapshot_id == before
    assert _target_rows() == [(1, 10, 1), (2, 20, 1), (3, 30, 1)]


def test_merge_scales_past_the_removed_row_cap(tmp_path):
    """A merge an order of magnitude past MERGE's original 2^20 row cap.

    The cap existed because every acted-on row's address was held in a hash set
    until the commit — ~48 bytes each, so rows were the thing that bound the
    statement. The addresses are roaring bitmaps now, bounded by construction
    and dense for exactly this shape, so the row count no longer bounds anything.
    This is the test that would fail if that regressed.
    """
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache

    import opteryx.connectors as connectors

    n_target = 1_500_000   # over the old 1,048,576 cap
    n_delta = 300_000      # every one of them a real update

    clear_parsed_manifest_cache()
    disk_io = _LocalDiskIO()
    target = _build_dataset(
        str(tmp_path / "big_tgt"),
        "col.tgt",
        ["cve", "details", "revision"],
        [(i, i, 1) for i in range(n_target)],
        disk_io,
    )
    source = _build_dataset(
        str(tmp_path / "big_src"),
        "col.src",
        ["cve", "details"],
        [(i, i + 1) for i in range(n_delta)],
        disk_io,
    )
    datasets = {"col.tgt": target, "col.src": source}

    class _FakeCatalog:
        def __init__(self, workspace=None, **kwargs):
            self.workspace = workspace
            self.io = disk_io

        def dataset_exists(self, identifier):
            return identifier in datasets

        def load_dataset(self, identifier):
            return datasets[identifier]

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
        list(opteryx.session(user="tester").execute_to_morsels(_UPSERT))
        snap = target.snapshot(None)
        assert snap.operation_type == "merge"
        assert snap.summary["deleted-records"] == n_delta
        assert snap.summary["added-records"] == n_delta
    finally:
        connectors._default_connector = saved_default
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved_prefixes)
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved_cache)


def test_explain_shows_the_plan_without_running_it(merge_env):
    """EXPLAIN must never mutate. `plan_explain` dispatches through the builder
    table now, so a statement with no query `body` gets a plan instead of the
    raw KeyError it used to die on."""
    target = merge_env["col.tgt"]
    before = target.metadata.current_snapshot_id

    rows = _rows("EXPLAIN " + _UPSERT)
    tree = " ".join(str(r[0]) for r in rows)
    assert "Merge" in tree
    assert "left_outer" in tree.lower() or "LEFT OUTER" in tree

    assert target.metadata.current_snapshot_id == before
    assert _target_rows() == [(1, 10, 1), (2, 20, 1), (3, 30, 1)]


def test_explain_analyze_refuses_to_run_a_write(merge_env):
    """ANALYZE measures by running. For a statement that writes, that would make
    plan inspection mutate the table."""
    from opteryx.exceptions import UnsupportedSyntaxError

    with pytest.raises(UnsupportedSyntaxError, match="EXPLAIN ANALYZE"):
        list(opteryx.session(user="tester").execute_to_morsels("EXPLAIN ANALYZE " + _UPSERT))


def test_a_lost_race_fails_the_statement_and_writes_nothing(merge_env, monkeypatch):
    """A merge that loses the commit race FAILS. It is not retried.

    Whether the work survives depends on what won the race — an append leaves
    row addresses valid, a compaction moves rows between files and invalidates
    them. Rather than encode that judgement in the engine, the caller re-runs.
    """
    from opteryx.exceptions import ConcurrentModificationError
    from opteryx_catalog.exceptions import SnapshotRaceError

    target = merge_env["col.tgt"]
    before = target.metadata.current_snapshot_id

    def _lost(*args, **kwargs):
        raise SnapshotRaceError("col.tgt moved while this commit was being built")

    monkeypatch.setattr(type(target), "merge_commit", _lost)

    with pytest.raises(ConcurrentModificationError) as err:
        list(opteryx.session(user="tester").execute_to_morsels(_UPSERT))

    message = str(err.value)
    assert "Re-run the statement" in message
    # The store's own exception type does not travel into the engine, but it is
    # kept as the cause so the real reason is not lost.
    assert isinstance(err.value.__cause__, SnapshotRaceError)

    # Nothing published: no snapshot, target byte-identical.
    assert target.metadata.current_snapshot_id == before
    assert _target_rows() == [(1, 10, 1), (2, 20, 1), (3, 30, 1)]
