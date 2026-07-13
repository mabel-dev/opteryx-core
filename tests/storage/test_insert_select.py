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
from opteryx.exceptions import UnsupportedSyntaxError


def _setup_workspace(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))


def _read_snapshot(tmp_path, relation):
    dataset_path = tmp_path / "ws" / relation
    with open(dataset_path / "dataset.json") as f:
        dataset_info = json.load(f)
    snapshot_name = dataset_info["current_snapshot"]
    with open(dataset_path / snapshot_name) as f:
        snapshot = json.load(f)
    return dataset_info, snapshot, dataset_path


def _read_parquet(dataset_path, file_entry):
    from rugo import parquet

    parquet_file = dataset_path / file_entry["file_path"]
    with parquet.read_parquet(str(parquet_file)) as reader:
        morsels = list(reader)
    return morsels[0]


def test_insert_select_literal(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))
    list(session.execute_to_morsels("INSERT INTO ws.t SELECT 1, 'hello'"))

    _, snapshot, _ = _read_snapshot(tmp_path, "t")
    assert len(snapshot["files"]) == 1


def test_insert_select_from_values_subquery(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))
    list(
        session.execute_to_morsels(
            "INSERT INTO ws.t SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(x, y)"
        )
    )

    _, snapshot, dataset_path = _read_snapshot(tmp_path, "t")
    assert len(snapshot["files"]) == 1
    morsel = _read_parquet(dataset_path, snapshot["files"][0])
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

    _, snapshot, _ = _read_snapshot(tmp_path, "tgt")
    total_rows = sum(f.get("record_count", 0) for f in snapshot["files"])
    assert total_rows == 3


def test_insert_select_column_count_mismatch(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))
    with pytest.raises(UnsupportedSyntaxError, match="INSERT row has"):
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

    _, snapshot, _ = _read_snapshot(tmp_path, "t")
    assert len(snapshot["files"]) == 1


def test_insert_explicit_columns_reorder(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))
    list(
        session.execute_to_morsels(
            "INSERT INTO ws.t (b, a) VALUES ('hello', 1)"
        )
    )

    _, snapshot, dataset_path = _read_snapshot(tmp_path, "t")
    assert len(snapshot["files"]) == 1
    morsel = _read_parquet(dataset_path, snapshot["files"][0])
    pydict = morsel.to_arrow().to_pydict()
    # Column 'a' should hold the integer, 'b' the string — i.e. INSERT
    # respected the user-supplied (b, a) ordering.
    assert pydict["a"] == [1]
    b_value = pydict["b"][0]
    if isinstance(b_value, bytes):
        b_value = b_value.decode()
    assert b_value == "hello"


def test_insert_explicit_columns_unknown_name(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR)"))
    with pytest.raises(UnsupportedSyntaxError, match="does not exist"):
        list(
            session.execute_to_morsels(
                "INSERT INTO ws.t (z, a) VALUES (1, 2)"
            )
        )


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
    info_before, _, _ = _read_snapshot(tmp_path, "t")
    pre_snapshot = info_before["current_snapshot"]

    list(
        session.execute_to_morsels(
            "INSERT INTO ws.t SELECT * FROM (VALUES (2), (3), (4)) AS v(x)"
        )
    )
    info_after, snapshot_after, _ = _read_snapshot(tmp_path, "t")

    assert info_after["current_snapshot"] != pre_snapshot
    assert snapshot_after.get("parent_snapshot") == pre_snapshot
