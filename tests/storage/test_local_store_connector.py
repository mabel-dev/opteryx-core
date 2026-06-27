# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for LocalStoreConnector.
"""

import json
import os
import pytest

from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import ConcurrentModificationError
from opteryx.models.file_entry import FileEntry
from opteryx.types.logical_type import INT64, TIMESTAMP, VARCHAR
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity


@pytest.fixture
def connector(tmp_path):
    """Create a LocalStoreConnector with temporary storage root."""
    return LocalStoreConnector(store_root=str(tmp_path))


@pytest.fixture
def simple_schema():
    """Create a simple test schema."""
    return RelationSchema(
        name="events",
        columns=[
            SchemaColumn(
                name="id",
                column_type=INT64,
                identity=mint_column_identity("events", "id"),
                nullable=False,
            ),
            SchemaColumn(
                name="name",
                column_type=VARCHAR,
                identity=mint_column_identity("events", "name"),
                nullable=True,
            ),
            SchemaColumn(
                name="timestamp",
                column_type=TIMESTAMP(),
                identity=mint_column_identity("events", "timestamp"),
                nullable=False,
            ),
        ],
    )


def test_create_relation_writes_dataset_json(connector, simple_schema):
    """Test creating a relation writes dataset.json with expected fields."""
    connector.create_relation("test_table", simple_schema)

    # Check directory exists
    table_dir = connector._relation_dir("test_table")
    assert os.path.isdir(table_dir)

    # Check dataset.json exists and has correct structure
    dataset_path = os.path.join(table_dir, "dataset.json")
    assert os.path.isfile(dataset_path)

    with open(dataset_path, "r") as f:
        data = json.load(f)

    assert data["format_version"] == 1
    assert data["relation_name"] == "test_table"
    assert data["current_snapshot"] is None
    assert "schema" in data
    assert "created_at" in data


def test_create_relation_nested_schema(connector, simple_schema):
    """Test creating a relation with nested namespace."""
    connector.create_relation("a.b.c.events", simple_schema)

    table_dir = connector._relation_dir("a.b.c.events")
    assert os.path.isdir(table_dir)

    dataset_path = os.path.join(table_dir, "dataset.json")
    assert os.path.isfile(dataset_path)


def test_create_relation_rejects_invalid_name(connector, simple_schema):
    """Test that invalid relation names are rejected."""
    invalid_names = [
        "",  # empty
        "a..b",  # double dot
        "a/b",  # slash
        "a\\b",  # backslash
        "a.1bad",  # starts with digit
        "a.-b",  # invalid char
    ]

    for name in invalid_names:
        with pytest.raises(ValueError, match="invalid relation name"):
            connector.create_relation(name, simple_schema)


def test_create_relation_rejects_existing(connector, simple_schema):
    """Test that creating a relation twice raises error."""
    connector.create_relation("test_table", simple_schema)

    with pytest.raises(ValueError, match="relation already exists"):
        connector.create_relation("test_table", simple_schema)


def test_relation_exists(connector, simple_schema):
    """Test relation_exists method."""
    assert not connector.relation_exists("test_table")

    connector.create_relation("test_table", simple_schema)
    assert connector.relation_exists("test_table")

    connector.drop_relation("test_table")
    assert not connector.relation_exists("test_table")


def test_drop_relation_removes_folder(connector, simple_schema):
    """Test that drop_relation removes the entire directory."""
    connector.create_relation("test_table", simple_schema)
    table_dir = connector._relation_dir("test_table")
    assert os.path.isdir(table_dir)

    connector.drop_relation("test_table")
    assert not os.path.isdir(table_dir)


def test_drop_relation_missing_raises(connector):
    """Test drop_relation with missing table."""
    with pytest.raises(ValueError, match="relation does not exist"):
        connector.drop_relation("nonexistent")

    # With if_exists=True, should not raise
    connector.drop_relation("nonexistent", if_exists=True)


def test_truncate_creates_empty_snapshot(connector, simple_schema):
    """Test truncate creates a snapshot with empty files list."""
    connector.create_relation("test_table", simple_schema)
    connector.truncate_relation("test_table")

    # Read dataset.json
    table_dir = connector._relation_dir("test_table")
    dataset_path = os.path.join(table_dir, "dataset.json")
    with open(dataset_path, "r") as f:
        descriptor = json.load(f)

    # Should have current_snapshot pointing to a snapshot file
    assert descriptor["current_snapshot"] is not None
    snapshot_name = descriptor["current_snapshot"]

    # Read snapshot
    snapshot_path = os.path.join(table_dir, snapshot_name)
    with open(snapshot_path, "r") as f:
        snapshot = json.load(f)

    assert snapshot["format_version"] == 1
    assert snapshot["files"] == []
    assert snapshot["parent_snapshot"] is None


def test_insert_appends_to_snapshot_chain(connector, simple_schema):
    """Test insert creates snapshot chain with parent references."""
    connector.create_relation("test_table", simple_schema)

    # Create first file entry
    entry1 = FileEntry(
        file_path="data-001.parquet",
        file_format="PARQUET",
        record_count=100,
        file_size_in_bytes=5000,
    )

    # Insert first entry
    connector.insert("test_table", [entry1])

    table_dir = connector._relation_dir("test_table")
    with open(os.path.join(table_dir, "dataset.json"), "r") as f:
        descriptor1 = json.load(f)
    snapshot_name1 = descriptor1["current_snapshot"]

    # Insert second entry
    entry2 = FileEntry(
        file_path="data-002.parquet",
        file_format="PARQUET",
        record_count=150,
        file_size_in_bytes=7000,
    )
    connector.insert("test_table", [entry2])

    with open(os.path.join(table_dir, "dataset.json"), "r") as f:
        descriptor2 = json.load(f)
    snapshot_name2 = descriptor2["current_snapshot"]

    # Read second snapshot
    with open(os.path.join(table_dir, snapshot_name2), "r") as f:
        snapshot2 = json.load(f)

    # Should have parent_snapshot pointing to first snapshot
    assert snapshot2["parent_snapshot"] == snapshot_name1
    assert len(snapshot2["files"]) == 2

    # First file should match entry1
    assert snapshot2["files"][0]["file_path"] == "data-001.parquet"
    assert snapshot2["files"][1]["file_path"] == "data-002.parquet"


def test_concurrent_commit_aborts(connector, simple_schema):
    """Test that concurrent modifications are detected and aborted."""
    connector.create_relation("test_table", simple_schema)

    # Setup hook to simulate concurrent modification
    def simulate_concurrent_mod():
        table_dir = connector._relation_dir("test_table")
        dataset_path = os.path.join(table_dir, "dataset.json")
        with open(dataset_path, "r") as f:
            descriptor = json.load(f)
        # Modify current_snapshot to simulate concurrent change
        descriptor["current_snapshot"] = "snapshot-fake.json"
        with open(dataset_path, "w") as f:
            json.dump(descriptor, f)

    connector._pre_commit_recheck_hook = simulate_concurrent_mod

    # Try to insert - should detect concurrent modification
    entry = FileEntry(
        file_path="data-001.parquet",
        file_format="PARQUET",
        record_count=100,
        file_size_in_bytes=5000,
    )

    with pytest.raises(ConcurrentModificationError, match="modified concurrently"):
        connector.insert("test_table", [entry])

    connector._pre_commit_recheck_hook = None


def test_file_entry_round_trip(connector):
    """Test FileEntry serialization and deserialization."""
    # Create entry with non-trivial bounds
    entry = FileEntry(
        file_path="data.parquet",
        file_format="PARQUET",
        record_count=1000,
        file_size_in_bytes=50000,
        uncompressed_size_in_bytes=60000,
        lower_bounds={0: b"abc", 1: b"\x00\x01\x02"},
        upper_bounds={0: b"xyz", 1: b"\xff\xfe\xfd"},
        null_value_counts={0: 5, 1: 0},
    )

    # Round-trip through JSON
    json_dict = entry.to_json_dict()
    restored = FileEntry.from_json_dict(json_dict)

    # Verify all fields match
    assert restored.file_path == entry.file_path
    assert restored.file_format == entry.file_format
    assert restored.record_count == entry.record_count
    assert restored.file_size_in_bytes == entry.file_size_in_bytes
    assert restored.uncompressed_size_in_bytes == entry.uncompressed_size_in_bytes
    assert restored.lower_bounds == entry.lower_bounds
    assert restored.upper_bounds == entry.upper_bounds
    assert restored.null_value_counts == entry.null_value_counts
