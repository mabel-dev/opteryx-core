# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Stage 6 tests: LocalStoreConnector SELECT read path."""

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import DatasetNotFoundError, DatasetReadError


def _setup_workspace(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))


def _morsels_to_rows(morsels):
    rows = []
    for morsel in morsels:
        if morsel is None:
            continue
        pydict = morsel.to_arrow().to_pydict()
        n = len(next(iter(pydict.values()))) if pydict else 0
        for i in range(n):
            row = {}
            for k, vs in pydict.items():
                v = vs[i]
                if isinstance(v, bytes):
                    v = v.decode()
                row[k] = v
            rows.append(row)
    return rows


def test_select_from_empty_relation(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT)"))
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT * FROM ws.t"))
    assert rows == []


def test_select_from_inserted_data(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ws.t VALUES (1), (2), (3)"))
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT * FROM ws.t"))
    assert sorted(r["a"] for r in rows) == [1, 2, 3]


def test_select_with_where(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ws.t VALUES (1), (2), (3), (4), (5)"))
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT * FROM ws.t WHERE a > 2"))
    assert sorted(r["a"] for r in rows) == [3, 4, 5]


def test_select_specific_columns(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT, b VARCHAR, c BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ws.t VALUES (1, 'x', 10), (2, 'y', 20)"))
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT a, c FROM ws.t"))
    assert sorted(rows, key=lambda r: r["a"]) == [
        {"a": 1, "c": 10},
        {"a": 2, "c": 20},
    ]


def test_select_count(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ws.t VALUES (1), (2), (3), (4)"))
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT COUNT(*) AS c FROM ws.t"))
    assert len(rows) == 1
    assert rows[0]["c"] == 4


def test_select_after_truncate(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ws.t VALUES (1), (2)"))
    list(session.execute_to_morsels("TRUNCATE TABLE ws.t"))
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT * FROM ws.t"))
    assert rows == []


def test_select_after_drop_raises(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT)"))
    list(session.execute_to_morsels("DROP TABLE ws.t"))
    with pytest.raises((DatasetNotFoundError, DatasetReadError)):
        list(session.execute_to_morsels("SELECT * FROM ws.t"))


def test_select_multi_snapshot_chain(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.t (a BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ws.t VALUES (1), (2)"))
    list(session.execute_to_morsels("INSERT INTO ws.t VALUES (3), (4), (5)"))
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT * FROM ws.t"))
    assert sorted(r["a"] for r in rows) == [1, 2, 3, 4, 5]


def test_select_nested_schema_name(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.a.b.c (x BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ws.a.b.c VALUES (42)"))
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT * FROM ws.a.b.c"))
    assert [r["x"] for r in rows] == [42]
