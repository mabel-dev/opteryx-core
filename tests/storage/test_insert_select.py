# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Stage 5 tests: INSERT ... SELECT and explicit column lists."""

import json
import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import ColumnNotFoundError, UnsupportedSyntaxError
from opteryx.models.manifest_io import read_manifest_file_entries


def _setup_workspace(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))


def _read_snapshot(tmp_path, relation):
    """Return (dataset_info, snapshot_pointer, file_entries, dataset_path).

    snapshot_pointer is the small commit-log JSON dict (format_version,
    created_at, parent_snapshot, manifest_file). file_entries is the decoded
    FileEntry list from the sibling Parquet manifest it points to.
    """
    dataset_path = tmp_path / "ws" / relation
    with open(dataset_path / "dataset.json") as f:
        dataset_info = json.load(f)
    snapshot_name = dataset_info["current_snapshot"]
    with open(dataset_path / snapshot_name) as f:
        snapshot = json.load(f)
    manifest_file = snapshot.get("manifest_file")
    entries = []
    if manifest_file:
        with open(dataset_path / manifest_file, "rb") as f:
            entries, _native = read_manifest_file_entries(f.read())
    return dataset_info, snapshot, entries, dataset_path


def _read_parquet(dataset_path, file_entry):
    from rugo import parquet

    parquet_file = dataset_path / file_entry.file_path
    with open(parquet_file, "rb") as f:
        with parquet.read_parquet(f.read()) as reader:
            morsels = list(reader)
    return morsels[0]


def test_insert_select_literal(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))
    list(session.execute_to_morsels("INSERT INTO ws.t SELECT 1, 'hello'"))

    _, _snapshot, entries, _ = _read_snapshot(tmp_path, "t")
    assert len(entries) == 1


def test_insert_select_from_values_subquery(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))
    list(
        session.execute_to_morsels(
            "INSERT INTO ws.t SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(x, y)"
        )
    )

    _, _snapshot, entries, dataset_path = _read_snapshot(tmp_path, "t")
    assert len(entries) == 1
    morsel = _read_parquet(dataset_path, entries[0])
    assert len(morsel) == 2


def test_insert_select_with_filter(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.src (a BIGINT)"))
    list(session.execute_to_morsels("CREATE TABLE ws.tgt (a BIGINT)"))
    list(
        session.execute_to_morsels(
            "INSERT INTO ws.src VALUES (1), (2), (3), (4), (5)"
        )
    )
    list(session.execute_to_morsels("INSERT INTO ws.tgt SELECT * FROM ws.src WHERE a > 2"))

    _, _snapshot, entries, _ = _read_snapshot(tmp_path, "tgt")
    total_rows = sum(fe.record_count for fe in entries)
    assert total_rows == 3


def test_insert_select_column_count_mismatch(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))
    with pytest.raises(UnsupportedSyntaxError, match=r"\*\*INSERT\*\* row has"):
        list(session.execute_to_morsels("INSERT INTO ws.t SELECT 1, 'x', 99"))


def test_insert_select_type_mismatch(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT)"))
    with pytest.raises(UnsupportedSyntaxError, match="type mismatch"):
        list(session.execute_to_morsels("INSERT INTO ws.t SELECT 'hello'"))


def test_insert_select_integer_to_double_widening(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a DOUBLE)"))
    list(session.execute_to_morsels("INSERT INTO ws.t SELECT 42"))

    _, _snapshot, entries, _ = _read_snapshot(tmp_path, "t")
    assert len(entries) == 1


def test_insert_explicit_columns_reorder(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))
    list(
        session.execute_to_morsels(
            "INSERT INTO ws.t (b, a) VALUES ('hello', 1)"
        )
    )

    _, _snapshot, entries, dataset_path = _read_snapshot(tmp_path, "t")
    assert len(entries) == 1
    morsel = _read_parquet(dataset_path, entries[0])
    pydict = morsel.to_arrow().to_pydict()
    # Column 'a' should hold the integer, 'b' the string — i.e. INSERT
    # respected the user-supplied (b, a) ordering.
    assert pydict["a"] == [1]
    b_value = pydict["b"][0]
    if isinstance(b_value, bytes):
        b_value = b_value.decode()
    assert b_value == "hello"


def test_insert_explicit_columns_unknown_name(tmp_path):
    """A name in the column list that the target does not have is a
    ColumnNotFoundError, naming the column and suggesting the near miss - not
    the blanket UnsupportedSyntaxError this asserted before the binder started
    raising the specific error. Nothing about the statement is unsupported; one
    named column simply is not there."""
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))
    with pytest.raises(ColumnNotFoundError, match="z") as excinfo:
        list(
            session.execute_to_morsels(
                "INSERT INTO ws.t (z, a) VALUES (1, 2)"
            )
        )
    # The suggestion is the reason this error is worth preferring; assert it
    # survives so a future refactor cannot quietly drop back to a bare message.
    assert "a" in str(excinfo.value)


def test_insert_explicit_columns_partial_rejected(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))
    with pytest.raises(UnsupportedSyntaxError, match="Partial column inserts"):
        list(session.execute_to_morsels("INSERT INTO ws.t (a) VALUES (1)"))


def test_insert_select_single_snapshot_per_statement(tmp_path):
    """One INSERT ... SELECT statement commits exactly one new snapshot."""
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT)"))

    list(session.execute_to_morsels("INSERT INTO ws.t SELECT 1"))
    info_before, _, _, _ = _read_snapshot(tmp_path, "t")
    pre_snapshot = info_before["current_snapshot"]

    list(
        session.execute_to_morsels(
            "INSERT INTO ws.t SELECT * FROM (VALUES (2), (3), (4)) AS v(x)"
        )
    )
    info_after, snapshot_after, _entries, _ = _read_snapshot(tmp_path, "t")

    assert info_after["current_snapshot"] != pre_snapshot
    assert snapshot_after.get("parent_snapshot") == pre_snapshot
