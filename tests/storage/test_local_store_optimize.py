# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
OPTIMIZE on the local store: compaction, and the vacuum that is part of it.

OPTIMIZE here compacts AND removes every file the current snapshot does not
reference - there is no separate VACUUM and no grace period. What keeps that safe
for a write still in flight is the commit lock plus the commit's check that every
file it publishes exists: the write is refused, loudly, never published broken.
"""

import os
import threading

import pytest

from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
import opteryx
from opteryx.connectors import OpteryxConnector
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import COMMIT_LOCK
from opteryx.connectors.local_store_connector import COMPACTION_LOCK
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.connectors.local_store_connector import _commit_lock
from opteryx.connectors.local_store_connector import _read_current_rows
from opteryx.exceptions import ConcurrentModificationError
from opteryx.exceptions import ExecutionError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.types.logical_type import INT64, VARCHAR
from opteryx.types.schema import ColumnDescriptor, RelationDescriptor

from tests.storage.test_local_store_connector import _on_disk
from tests.storage.test_local_store_connector import _rows


def _schema():
    return RelationDescriptor(
        name="t",
        columns=[
            ColumnDescriptor(name="id", column_type=INT64, nullable=False),
            ColumnDescriptor(name="name", column_type=VARCHAR, nullable=True),
        ],
    )


@pytest.fixture
def connector(tmp_path):
    return LocalStoreConnector(store_root=str(tmp_path))


def _by_prefix(directory):
    names = os.listdir(directory)
    return {
        "data": sorted(n for n in names if n.startswith("data-") and n.endswith(".parquet")),
        "manifests": sorted(n for n in names if n.startswith("manifest-")),
        "snapshots": sorted(n for n in names if n.startswith("snapshot-")),
    }


def _run(session, sql):
    return list(session.execute_to_morsels(sql))


def _session(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))
    return opteryx.session()


def _live_paths(connector, relation):
    directory = connector._relation_dir(relation)
    current = _read_current_rows(directory, connector._read_dataset_json(directory))
    return [] if current is None else current.file_paths()


# --- through SQL ------------------------------------------------------------------


def test_optimize_compacts_small_files_and_removes_what_it_superseded(tmp_path):
    session = _session(tmp_path)
    directory = tmp_path / "ws" / "t"
    _run(session, "CREATE TABLE ws.t (a BIGINT, b VARCHAR)")
    for i in range(5):
        _run(session, f"INSERT INTO ws.t VALUES ({i}, 'x')")
    before = _by_prefix(directory)
    assert len(before["data"]) == 5
    assert len(before["manifests"]) == 5 and len(before["snapshots"]) == 5

    _run(session, "OPTIMIZE TABLE ws.t")

    after = _by_prefix(directory)
    assert len(after["data"]) == 1
    assert len(after["manifests"]) == 1 and len(after["snapshots"]) == 1
    assert not set(before["data"]) & set(after["data"])
    rows = _run(session, "SELECT COUNT(*), SUM(a) FROM ws.t")[0].to_arrow().to_pylist()
    assert rows == [{"COUNT(*)": 5, "SUM(a)": 10}]


def test_optimize_with_nothing_to_compact_still_vacuums(tmp_path):
    session = _session(tmp_path)
    directory = tmp_path / "ws" / "t"
    _run(session, "CREATE TABLE ws.t (a BIGINT, b VARCHAR)")
    for i in range(3):
        _run(session, f"INSERT INTO ws.t VALUES ({i}, 'x')")
    _run(session, "TRUNCATE TABLE ws.t")
    assert len(_by_prefix(directory)["data"]) == 3  # orphaned by the TRUNCATE

    _run(session, "OPTIMIZE TABLE ws.t")

    after = _by_prefix(directory)
    assert after["data"] == []
    assert len(after["manifests"]) == 1 and len(after["snapshots"]) == 1
    # and the relation is still a working table
    _run(session, "INSERT INTO ws.t VALUES (9, 'z')")
    rows = _run(session, "SELECT a FROM ws.t")[0].to_arrow().to_pylist()
    assert rows == [{"a": 9}]


# --- vacuum_relation --------------------------------------------------------------


def test_vacuum_removes_unreferenced_files_of_every_kind(connector):
    connector.create_relation("t", _schema())
    directory = connector._relation_dir("t")
    _on_disk(connector, "t", "data-live.parquet")
    connector.insert("t", _rows(("data-live.parquet", 1, 1)))
    _on_disk(
        connector,
        "t",
        "data-orphan.parquet",
        "data-writing.parquet.tmp",
        "snapshot-stale.json",
        "manifest-stale.parquet",
        "snapshot-half.json.tmp",
        "dataset.json.tmp",
    )

    removed = connector.vacuum_relation("t")

    assert removed == {"data": 1, "manifests": 1, "snapshots": 1, "tmp": 3}
    assert _live_paths(connector, "t") == ["data-live.parquet"]
    assert os.path.isfile(os.path.join(directory, "data-live.parquet"))
    assert len(_by_prefix(directory)["manifests"]) == 1
    assert len(_by_prefix(directory)["snapshots"]) == 1


def test_vacuum_never_touches_files_it_does_not_own(connector):
    connector.create_relation("t", _schema())
    directory = connector._relation_dir("t")
    _on_disk(connector, "t", "notes.txt", "relationships.json", "view.json")
    connector.vacuum_relation("t")
    for name in ("notes.txt", "relationships.json", "view.json", "dataset.json", COMMIT_LOCK):
        assert os.path.exists(os.path.join(directory, name)), name


def test_vacuum_on_a_relation_that_does_not_exist_raises(connector):
    from opteryx.exceptions import DatasetNotFoundError

    with pytest.raises(DatasetNotFoundError):
        connector.vacuum_relation("nope")


# --- a write in flight ------------------------------------------------------------


def test_a_write_whose_files_were_vacuumed_is_refused_not_published(connector):
    """The ruled trade-off: no grace period, so an in-flight write loses its closed
    files to the vacuum. Its commit must be refused - never published naming
    files that are gone."""
    connector.create_relation("t", _schema())
    _on_disk(connector, "t", "data-inflight.parquet")  # closed, committed by nobody yet

    assert connector.vacuum_relation("t")["data"] == 1

    with pytest.raises(ConcurrentModificationError, match="missing"):
        connector.insert("t", _rows(("data-inflight.parquet", 1, 1)))
    directory = connector._relation_dir("t")
    assert connector._read_dataset_json(directory).current_snapshot is None
    assert _by_prefix(directory)["snapshots"] == []


def test_a_writer_whose_tmp_was_vacuumed_fails_at_close_naming_the_cause(connector):
    connector.create_relation("t", _schema())
    writer = connector.open_data_file_writer("t")
    vector = vector_from_sequence([1, 2], dtype=INT64.physical)
    writer.write_row_group(Morsel.from_vectors(["id"], [vector]))

    assert connector.vacuum_relation("t")["tmp"] == 1

    with pytest.raises(FileNotFoundError, match="was removed before it could be finished"):
        writer.close()


def test_a_commit_waits_for_the_commit_lock(connector):
    connector.create_relation("t", _schema())
    _on_disk(connector, "t", "data-a.parquet")
    directory = connector._relation_dir("t")
    finished = threading.Event()

    def insert():
        connector.insert("t", _rows(("data-a.parquet", 1, 1)))
        finished.set()

    with _commit_lock(directory):
        worker = threading.Thread(target=insert)
        worker.start()
        assert not finished.wait(0.3), "a commit went through while the lock was held"
    worker.join(timeout=10)
    assert finished.is_set()
    assert _live_paths(connector, "t") == ["data-a.parquet"]


# --- compaction_commit ------------------------------------------------------------


def test_compaction_commit_replaces_retired_files_and_keeps_the_rest(connector):
    connector.create_relation("t", _schema())
    directory = connector._relation_dir("t")
    _on_disk(connector, "t", "data-a.parquet", "data-b.parquet", "data-c.parquet", "data-merged.parquet")
    for name in ("data-a.parquet", "data-b.parquet", "data-c.parquet"):
        connector.insert("t", _rows((name, 1, 1)))

    connector.compaction_commit(
        "t",
        _rows(("data-merged.parquet", 2, 2)),
        [os.path.join(directory, "data-a.parquet"), os.path.join(directory, "data-b.parquet")],
        baseline_snapshot_id=None,
    )

    # c was not retired - as a file a concurrent INSERT added would not be
    assert _live_paths(connector, "t") == ["data-c.parquet", "data-merged.parquet"]


def test_compaction_commit_refuses_when_a_retired_file_is_no_longer_current(connector):
    connector.create_relation("t", _schema())
    directory = connector._relation_dir("t")
    _on_disk(connector, "t", "data-a.parquet", "data-merged.parquet")
    connector.insert("t", _rows(("data-a.parquet", 1, 1)))
    connector.truncate_relation("t")  # a concurrent TRUNCATE

    with pytest.raises(ConcurrentModificationError, match="no longer current"):
        connector.compaction_commit(
            "t",
            _rows(("data-merged.parquet", 1, 1)),
            [os.path.join(directory, "data-a.parquet")],
        )
    assert _live_paths(connector, "t") == []


def test_compaction_commit_refuses_vector_index_files(connector):
    connector.create_relation("t", _schema())
    with pytest.raises(InvalidInternalStateError):
        connector.compaction_commit("t", _rows(), [], index_files={"some-index": object()})


def test_a_second_compaction_is_refused_while_one_holds_the_lease(connector):
    connector.create_relation("t", _schema())
    lease = connector.claim_compaction_lease("t", "first")
    with pytest.raises(ExecutionError, match="already being compacted"):
        connector.claim_compaction_lease("t", "second")
    assert lease.release() is True
    again = connector.claim_compaction_lease("t", "third")
    assert again.release() is True
    assert os.path.exists(os.path.join(connector._relation_dir("t"), COMPACTION_LOCK))


# --- the contract -----------------------------------------------------------------


def test_only_the_local_store_vacuums_on_optimize():
    assert LocalStoreConnector.vacuums_on_optimize is True
    assert OpteryxConnector.vacuums_on_optimize is False
    assert LocalStoreConnector(store_root="unused").table_engine("a.b").snapshot_id is None
