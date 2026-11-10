"""A fork made by CREATE TABLE ... CLONE is isolated from its upstream - local disk, no GCS.

Every SQL mutation of the clone (INSERT, DELETE, UPDATE, MERGE, TRUNCATE, OPTIMIZE,
OPTIMIZE FINAL), the maintenance sweeps that run on it (snapshot expiration, deep
clean) and DROP of it must leave the upstream exactly as it was: same current
snapshot, same manifest entries, every file under its location byte-identical, same
query answers. And every file the clone writes lands under the clone's own location.

The reverse holds too (FORKS_DESIGN.md S5.1): mutating and expiring the upstream
after the clone leaves the clone reading its original content, because the clone
pins its base snapshot on the upstream.

The clone is made by the real `OpteryxCatalog.clone_dataset`, borrowed by the
local-disk catalog (tests/tools/local_catalog.py), which stands in only for its
metastore documents.
"""

import hashlib
import os
import sys
import time

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

from tests.tools.local_catalog import LocalDiskIO  # noqa: E402
from tests.tools.local_catalog import local_catalog_class  # noqa: E402

WORKSPACE = "optws"
UPSTREAM = f"{WORKSPACE}.col.up"
CLONE = f"{WORKSPACE}.col.cl"
ROWS_PER_FILE = 20_000
FILES = 3
TOTAL = ROWS_PER_FILE * FILES


def _seed_upstream(catalog):
    """The upstream: FILES data files of (k, g = k % 7), committed by the real add_files."""
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    schema = {"columns": [{"name": "k", "type": "INTEGER"}, {"name": "g", "type": "INTEGER"}]}
    ds = catalog.create_dataset("col.up", schema, author="seed")
    paths = []
    for n in range(FILES):
        ks = list(range(n * ROWS_PER_FILE, (n + 1) * ROWS_PER_FILE))
        morsel = Morsel()
        morsel.append_vector("k", vector_from_sequence(ks, dtype="INTEGER"))
        morsel.append_vector("g", vector_from_sequence([k % 7 for k in ks], dtype="INTEGER"))
        path = os.path.join(ds.metadata.location, "data", f"seed_{n}.parquet")
        with open(path, "wb") as f:
            f.write(write_parquet(morsel, compression="zstd"))
        paths.append(path)
    ds.add_files(paths, author="seed", read_sources=[])
    return ds


class Env:
    def __init__(self, catalog_cls):
        self.catalog_cls = catalog_cls
        self.catalog = catalog_cls(workspace=WORKSPACE)

    @property
    def upstream(self):
        return self.catalog_cls.store[UPSTREAM]

    @property
    def clone(self):
        return self.catalog_cls.store[CLONE]


@pytest.fixture
def fork_env(tmp_path):
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache

    import opteryx.connectors as connectors

    clear_parsed_manifest_cache()
    catalog_cls = local_catalog_class(str(tmp_path), LocalDiskIO())
    env = Env(catalog_cls)
    _seed_upstream(env.catalog)

    saved_default = connectors._default_connector
    saved_prefixes = dict(connectors._storage_prefixes)
    saved_cache = dict(connectors._connector_cache)
    connectors._storage_prefixes.pop(WORKSPACE, None)
    connectors._connector_cache.clear()

    opteryx.set_default_connector(OpteryxConnector, catalog=catalog_cls)
    try:
        _run(f"CREATE TABLE {CLONE} CLONE {UPSTREAM}")
        yield env
    finally:
        connectors._default_connector = saved_default
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved_prefixes)
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved_cache)


def _run(sql):
    from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache

    clear_parsed_manifest_cache()
    return list(opteryx.session(user="tester").execute_to_morsels(sql))


def _aggregates(relation):
    (morsel,) = _run(
        f"SELECT COUNT(*), SUM(k), MIN(k), MAX(k), SUM(g), COUNT(DISTINCT g) FROM {relation}"
    )
    return tuple(morsel[0][i] for i in range(6))


ORIGINAL = (TOTAL, TOTAL * (TOTAL - 1) // 2, 0, TOTAL - 1, sum(k % 7 for k in range(TOTAL)), 7)


def _entries(ds):
    from opteryx_catalog.catalog.manifest import read_manifest_rows

    if not ds.snapshot(None).manifest_list:
        return []  # an emptied dataset (TRUNCATE) commits a snapshot with no manifest
    with open(ds.snapshot(None).manifest_list, "rb") as f:
        return read_manifest_rows(f.read())


def _checksums(location):
    out = {}
    for directory, _, names in os.walk(location):
        for name in names:
            path = os.path.join(directory, name)
            with open(path, "rb") as f:
                out[path] = hashlib.sha256(f.read()).hexdigest()
    return out


def _upstream_state(env):
    up = env.upstream
    return {
        "snapshot": up.metadata.current_snapshot_id,
        "entries": sorted(
            (e["file_path"], e["record_count"], e.get("delete_file_path")) for e in _entries(up)
        ),
        "files": _checksums(up.metadata.location),
        "aggregates": _aggregates(UPSTREAM),
    }


def _assert_upstream_unchanged(env, before):
    after = _upstream_state(env)
    assert after["snapshot"] == before["snapshot"], "upstream's current snapshot moved"
    assert after["entries"] == before["entries"], "upstream's manifest entries changed"
    assert set(after["files"]) == set(before["files"]), (
        f"files under the upstream's location changed: "
        f"gone={sorted(set(before['files']) - set(after['files']))} "
        f"new={sorted(set(after['files']) - set(before['files']))}"
    )
    assert after["files"] == before["files"], "an upstream file's bytes changed"
    assert after["aggregates"] == before["aggregates"] == ORIGINAL


def _assert_clone_writes_are_its_own(env, before):
    """Every file the clone names is either borrowed from the upstream's base manifest
    or under the clone's own location - never a new file anywhere else."""
    borrowed = {path for path, _, _ in before["entries"]}
    own = env.clone.metadata.location.rstrip("/") + "/"
    assert not env.clone.metadata.location.startswith(env.upstream.metadata.location + "/")
    for entry in _entries(env.clone):
        for key in ("file_path", "delete_file_path"):
            path = entry.get(key)
            if path and path not in borrowed:
                assert path.startswith(own), f"clone {key} {path} is outside {own}"


def test_the_clone_starts_as_the_upstream(fork_env):
    env = fork_env
    assert env.clone.metadata.fork.source.dataset == UPSTREAM
    assert env.catalog.list_forks(UPSTREAM)[0]["fork"] == CLONE
    assert _aggregates(CLONE) == ORIGINAL
    assert sorted(e["file_path"] for e in _entries(env.clone)) == sorted(
        e["file_path"] for e in _entries(env.upstream)
    ), "a clone borrows the upstream's files"


MUTATIONS = {
    "insert": (
        f"INSERT INTO {CLONE} SELECT CAST({TOTAL} AS INTEGER) AS k, CAST(1 AS INTEGER) AS g "
        "FROM $planets WHERE id = 1",
        lambda a: a[0] == TOTAL + 1 and a[3] == TOTAL,
    ),
    "delete": (
        f"DELETE FROM {CLONE} WHERE k < 1000",
        lambda a: a[0] == TOTAL - 1000 and a[2] == 1000,
    ),
    "update": (
        f"UPDATE {CLONE} SET g = g + 100 WHERE k < 1000",
        lambda a: a[0] == TOTAL and a[4] == ORIGINAL[4] + 100 * 1000,
    ),
    "merge": (
        f"MERGE INTO {CLONE} AS t USING {UPSTREAM} AS s ON t.k = s.k "
        "WHEN MATCHED AND (s.k < 500) THEN UPDATE SET g = s.g + 100",
        lambda a: a[0] == TOTAL and a[4] == ORIGINAL[4] + 100 * 500,
    ),
    "truncate": (f"TRUNCATE TABLE {CLONE}", lambda a: a[0] == 0),
}


@pytest.mark.parametrize("name", list(MUTATIONS))
def test_mutating_the_clone_leaves_the_upstream_untouched(fork_env, name):
    env = fork_env
    sql, expected = MUTATIONS[name]
    before = _upstream_state(env)
    clone_snapshot = env.clone.metadata.current_snapshot_id

    _run(sql)

    assert env.clone.metadata.current_snapshot_id != clone_snapshot, f"{name} committed nothing"
    assert expected(_aggregates(CLONE)), f"{name} on the clone: {_aggregates(CLONE)}"
    _assert_clone_writes_are_its_own(env, before)
    _assert_upstream_unchanged(env, before)


def test_truncate_persists_the_moved_snapshot_pointer(fork_env, monkeypatch):
    """The local catalog shares in-memory metadata, so the pointer moving there proves
    nothing: TRUNCATE must hand the new pointer to save_dataset_metadata, or a real
    catalog keeps serving the pre-truncate rows."""
    env = fork_env
    saved = []
    monkeypatch.setattr(
        env.catalog_cls,
        "save_dataset_metadata",
        lambda self, identifier, metadata, **kwargs: saved.append(
            (self._key(identifier), metadata.current_snapshot_id)
        ),
    )

    _run(f"TRUNCATE TABLE {CLONE}")

    assert saved == [(CLONE, env.clone.metadata.current_snapshot_id)]


def test_optimize_on_the_clone_leaves_the_upstream_untouched(fork_env):
    env = fork_env
    before = _upstream_state(env)
    _run(f"DELETE FROM {CLONE} WHERE k < 1000")

    _run(f"OPTIMIZE TABLE {CLONE}")

    assert env.clone.snapshot(None).operation_type == "compact"
    assert _aggregates(CLONE)[0] == TOTAL - 1000
    _assert_clone_writes_are_its_own(env, before)
    _assert_upstream_unchanged(env, before)


def test_optimize_final_on_the_clone_leaves_the_upstream_untouched(fork_env):
    env = fork_env
    before = _upstream_state(env)
    env.clone.metadata.sort_orders = ["k"]
    _run(f"DELETE FROM {CLONE} WHERE k < 1000")

    _run(f"OPTIMIZE TABLE {CLONE} FINAL")

    snap = env.clone.snapshot(None)
    assert snap.operation_type == "compact"
    assert snap.summary["deleted-data-files"] == FILES, "FINAL took the whole key window"
    (out,) = _entries(env.clone)
    assert out["file_path"].startswith(env.clone.metadata.location + "/")
    assert _aggregates(CLONE)[:4] == (TOTAL - 1000, ORIGINAL[1] - 999 * 1000 // 2, 1000, TOTAL - 1)
    _assert_upstream_unchanged(env, before)


def _churn(relation):
    """Enough history that the clone's base would be condemned: retention keeps the
    head, the last user commit and the one before it, so after two DELETEs, a
    compaction and two INSERTs, the base and both delete vectors fall outside."""
    _run(f"DELETE FROM {relation} WHERE k < 1000")
    _run(f"DELETE FROM {relation} WHERE k < 2000")
    _run(f"OPTIMIZE TABLE {relation}")
    for k in (-1, -2):
        _run(
            f"INSERT INTO {relation} SELECT CAST({k} AS INTEGER) AS k, CAST(0 AS INTEGER) AS g "
            "FROM $planets WHERE id = 1"
        )
    assert _aggregates(relation)[0] == TOTAL - 2000 + 2


def _expire(env, identifier, monkeypatch):
    """Two expiration runs with the age gates at 1 ms, so the two-strike quarantine
    actually deletes on the second run (a file must still be OLDER than 1 ms by the
    listing of the dataset's own location to be a candidate)."""
    from opteryx_catalog.catalog import expiration
    from opteryx_catalog.catalog.orphan_quarantine import OrphanQuarantine

    monkeypatch.setattr(expiration, "DATA_FILE_ORPHAN_MIN_AGE_MS", 1)
    monkeypatch.setattr(expiration, "MANIFEST_ORPHAN_MIN_AGE_MS", 1)
    deleted = []
    for _ in range(2):
        time.sleep(0.01)
        runner = expiration.SnapshotExpiration(
            env.catalog, author="gc", quarantine=OrphanQuarantine(env.catalog, min_age_ms=1)
        )
        summary = runner.expire_dataset(identifier, dry_run=False) or {}
        deleted += summary.get("deleted_files", []) + summary.get("deleted_manifests", [])
    return deleted


def test_expiring_and_deep_cleaning_the_clone_leaves_the_upstream_untouched(
    fork_env, monkeypatch
):
    from opteryx_catalog.catalog.deep_clean import DatasetDeepClean
    from opteryx_catalog.catalog.deep_clean import find_orphaned_files

    env = fork_env
    before = _upstream_state(env)
    _churn(CLONE)

    deleted = _expire(env, "col.cl", monkeypatch)

    # The sweep must have reclaimed something of the clone's own, or it proves nothing.
    own = env.clone.metadata.location + "/"
    assert deleted, "expiration of the clone reclaimed nothing"
    assert all(path.startswith(own) for path in deleted), deleted
    _assert_upstream_unchanged(env, before)

    orphans = find_orphaned_files(env.catalog, "col.cl") or set()
    assert all(path.startswith(own) for path in orphans), orphans
    DatasetDeepClean(env.catalog).clean_dataset("col.cl", dry_run=False)

    assert _aggregates(CLONE)[0] == TOTAL - 2000 + 2
    _assert_upstream_unchanged(env, before)


def test_dropping_the_clone_leaves_the_upstream_untouched(fork_env):
    env = fork_env
    before = _upstream_state(env)
    _run(f"INSERT INTO {CLONE} SELECT CAST(-1 AS INTEGER) AS k, CAST(0 AS INTEGER) AS g FROM $planets WHERE id = 1")
    clone_location = env.clone.metadata.location

    _run(f"DROP TABLE {CLONE}")

    assert CLONE not in env.catalog_cls.store
    assert not os.path.exists(clone_location)
    assert env.catalog.list_forks(UPSTREAM) == [], "dropping the fork released its pin"
    _assert_upstream_unchanged(env, before)


def test_mutating_and_expiring_the_upstream_leaves_the_clone_on_its_base(fork_env, monkeypatch):
    env = fork_env
    clone_snapshot = env.clone.metadata.current_snapshot_id
    clone_entries = sorted(e["file_path"] for e in _entries(env.clone))
    base = env.clone.metadata.fork.source.snapshot_id

    _churn(UPSTREAM)
    assert env.upstream.metadata.current_snapshot_id != base

    deleted = _expire(env, "col.up", monkeypatch)

    # The upstream's own delete vectors went, so the sweep ran with teeth.
    assert deleted, "expiration of the upstream reclaimed nothing"

    assert base in {s.snapshot_id for s in env.upstream.metadata.snapshots}, (
        "the fork's base snapshot was expired off the upstream"
    )
    assert all(os.path.exists(p) for p in clone_entries), "a file the clone reads was deleted"
    assert env.clone.metadata.current_snapshot_id == clone_snapshot
    assert sorted(e["file_path"] for e in _entries(env.clone)) == clone_entries
    assert _aggregates(CLONE) == ORIGINAL
