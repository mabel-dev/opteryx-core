# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import json
import pytest
import tempfile
from pathlib import Path

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import (
    DatasetNotFoundError,
    ReadOnlyConnectorError,
    UnsupportedSyntaxError,
)


def _setup_workspace(tmp_path):
    """Set up a temporary workspace for testing."""
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))


def test_insert_single_row(tmp_path):
    """INSERT single row into a table."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    # Create table
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))

    # Insert single row
    result = list(session.execute_to_morsels("INSERT INTO ws.t VALUES (1, 'hello')"))
    assert result is not None

    # Verify snapshot created
    dataset_path = tmp_path / "ws" / "t"
    with open(dataset_path / "dataset.json") as f:
        dataset_info = json.load(f)

    assert dataset_info.get("current_snapshot") is not None

    # Verify snapshot has one parquet file
    snapshot_name = dataset_info["current_snapshot"]
    with open(dataset_path / snapshot_name) as f:
        snapshot = json.load(f)

    assert len(snapshot.get("files", [])) == 1


def test_insert_multiple_rows_one_statement(tmp_path):
    """INSERT multiple rows in one statement."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    # Create table
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))

    # Insert multiple rows
    result = list(
        session.execute_to_morsels(
            "INSERT INTO ws.t VALUES (1, 'a'), (2, 'b'), (3, 'c')"
        )
    )
    assert result is not None

    # Verify snapshot has one file (all rows in one VALUES statement = one morsel)
    dataset_path = tmp_path / "ws" / "t"
    with open(dataset_path / "dataset.json") as f:
        dataset_info = json.load(f)

    snapshot_name = dataset_info["current_snapshot"]
    with open(dataset_path / snapshot_name) as f:
        snapshot = json.load(f)

    assert len(snapshot.get("files", [])) == 1


def test_insert_round_trip_via_rugo(tmp_path):
    """INSERT rows and verify parquet file is readable via rugo."""
    import rugo

    _setup_workspace(tmp_path)
    session = opteryx.session()

    # Create table
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))

    # Insert rows
    list(session.execute_to_morsels("INSERT INTO ws.t VALUES (10, 'x'), (20, 'y')"))

    # Read the parquet file directly
    dataset_path = tmp_path / "ws" / "t"
    with open(dataset_path / "dataset.json") as f:
        dataset_info = json.load(f)

    snapshot_name = dataset_info["current_snapshot"]
    with open(dataset_path / snapshot_name) as f:
        snapshot = json.load(f)

    # Verify parquet file can be read via rugo without error
    parquet_file = dataset_path / snapshot["files"][0]["file_path"]
    assert parquet_file.exists(), f"Parquet file not found: {parquet_file}"

    with open(parquet_file, "rb") as f:
        data = f.read()
        # Just verify we can read the parquet data without error
        result = rugo.parquet_reader.read_parquet(data)
        assert result is not None
        # Result is a list of morsels; get the first one
        if isinstance(result, list):
            morsel = result[0]
        else:
            morsel = result
        # Verify it has 2 rows
        assert len(morsel) == 2


def test_insert_two_statements_chain_snapshots(tmp_path):
    """Two INSERT statements create a snapshot chain."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    # Create table
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT)"))

    # First insert
    list(session.execute_to_morsels("INSERT INTO ws.t VALUES (1)"))

    # Read first snapshot
    dataset_path = tmp_path / "ws" / "t"
    with open(dataset_path / "dataset.json") as f:
        dataset_info = json.load(f)
    first_snapshot_name = dataset_info["current_snapshot"]

    # Second insert
    list(session.execute_to_morsels("INSERT INTO ws.t VALUES (2)"))

    # Verify snapshots chain
    with open(dataset_path / "dataset.json") as f:
        dataset_info = json.load(f)
    second_snapshot_name = dataset_info["current_snapshot"]

    assert first_snapshot_name != second_snapshot_name

    # Read second snapshot and verify parent
    with open(dataset_path / second_snapshot_name) as f:
        second_snapshot = json.load(f)

    assert second_snapshot.get("parent_snapshot") == first_snapshot_name
    # Second snapshot should have 2 files total (1 from first insert + 1 new)
    # The snapshot includes all files from the parent
    assert len(second_snapshot.get("files", [])) == 2


def test_insert_into_missing_relation(tmp_path):
    """INSERT into non-existent table raises DatasetNotFoundError."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    with pytest.raises(DatasetNotFoundError):
        list(session.execute_to_morsels("INSERT INTO ws.nonexistent VALUES (1, 'x')"))


def test_insert_readonly_connector_rejected(tmp_path):
    """INSERT on read-only connector raises ReadOnlyConnectorError."""
    # Do not register ws; use filesystem path which is read-only
    session = opteryx.session()

    with pytest.raises(ReadOnlyConnectorError, match="does not support INSERT"):
        list(
            session.execute_to_morsels("INSERT INTO somefile.foo VALUES (1, 'x')")
        )


def test_insert_column_count_mismatch(tmp_path):
    """INSERT with wrong number of values raises error."""
    _setup_workspace(tmp_path)
    session = opteryx.session()

    # Create table with 2 columns
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))

    # Try to insert only 1 value
    with pytest.raises(UnsupportedSyntaxError, match="INSERT row has"):
        list(session.execute_to_morsels("INSERT INTO ws.t VALUES (1)"))


def test_insert_concurrent_modification_propagates(tmp_path):
    """INSERT propagates concurrent modification error if dataset.json mutates mid-commit."""
    from opteryx.connectors import _connector_cache
    from opteryx.connectors.local_store_connector import LocalStoreConnector
    from opteryx.exceptions import ConcurrentModificationError

    # Clear connector cache so we get a fresh connector instance for this test
    _connector_cache.clear()

    _setup_workspace(tmp_path)
    session = opteryx.session()

    # Create table — this also creates and caches a connector instance
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT)"))

    # Find the cached LocalStoreConnector instance for our tmp_path
    connector = None
    for cached in _connector_cache.values():
        if isinstance(cached, LocalStoreConnector) and getattr(cached, "store_root", None) == str(tmp_path):
            connector = cached
            break

    assert connector is not None, "Could not find cached LocalStoreConnector for this test"

    dataset_json_path = tmp_path / "ws" / "t" / "dataset.json"

    def mutate_hook():
        # Simulate a concurrent writer by changing current_snapshot
        with open(dataset_json_path, "r") as f:
            data = json.load(f)
        data["current_snapshot"] = "snapshot-concurrent-fake.json"
        with open(dataset_json_path, "w") as f:
            json.dump(data, f)

    connector._pre_commit_recheck_hook = mutate_hook

    try:
        with pytest.raises(ConcurrentModificationError):
            list(session.execute_to_morsels("INSERT INTO ws.t VALUES (1)"))
    finally:
        connector._pre_commit_recheck_hook = None
