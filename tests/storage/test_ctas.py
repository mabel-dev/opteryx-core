# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Stage 6 tests: CREATE TABLE ... AS SELECT (CTAS)."""

import json
import os

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import ReadOnlyConnectorError, UnsupportedSyntaxError


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


def _read_dataset_json(tmp_path, relation):
    parts = relation.split(".")
    p = tmp_path
    for part in parts:
        p = p / part
    with open(p / "dataset.json") as f:
        return json.load(f)


def test_ctas_basic(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(
        session.execute_to_morsels(
            "CREATE TABLE ws.dst AS SELECT 1 AS a, 'hello' AS b"
        )
    )
    info = _read_dataset_json(tmp_path / "ws", "dst")
    schema_cols = {c["name"]: c for c in info["schema"]["columns"]}
    assert "a" in schema_cols and "b" in schema_cols
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT * FROM ws.dst"))
    assert len(rows) == 1
    assert rows[0]["a"] == 1
    assert rows[0]["b"] == "hello"


def test_ctas_from_values(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(
        session.execute_to_morsels(
            "CREATE TABLE ws.dst AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(x, y)"
        )
    )
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT * FROM ws.dst"))
    assert len(rows) == 2
    assert sorted((r["x"], r["y"]) for r in rows) == [(1, "a"), (2, "b")]


def test_ctas_from_existing_relation(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.src (a BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ws.src VALUES (-1), (1), (2), (3)"))
    list(
        session.execute_to_morsels(
            "CREATE TABLE ws.dst AS SELECT * FROM ws.src WHERE a > 0"
        )
    )
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT * FROM ws.dst"))
    assert sorted(r["a"] for r in rows) == [1, 2, 3]


def test_ctas_existing_relation_rejected(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.dst (a BIGINT)"))
    with pytest.raises(ValueError, match="already exists"):
        list(session.execute_to_morsels("CREATE TABLE ws.dst AS SELECT 1 AS a"))


def test_ctas_if_not_exists_existing_skip(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.dst (a BIGINT)"))
    list(session.execute_to_morsels("INSERT INTO ws.dst VALUES (99)"))
    # No-op: existing relation; CTAS should not raise and content unchanged.
    list(
        session.execute_to_morsels(
            "CREATE TABLE IF NOT EXISTS ws.dst AS SELECT 1 AS a"
        )
    )
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT * FROM ws.dst"))
    assert [r["a"] for r in rows] == [99]


def test_ctas_with_column_definitions_rejected(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    with pytest.raises(UnsupportedSyntaxError, match="cannot specify column"):
        list(
            session.execute_to_morsels(
                "CREATE TABLE ws.dst (a BIGINT) AS SELECT 1"
            )
        )


def test_ctas_readonly_connector_rejected(tmp_path):
    # Do not register ws; default connector is filesystem (read-only)
    session = opteryx.session()
    with pytest.raises(ReadOnlyConnectorError):
        list(session.execute_to_morsels("CREATE TABLE not_writable AS SELECT 1 AS a"))


def test_ctas_unresolved_type_rejected(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    with pytest.raises(UnsupportedSyntaxError, match="unresolved type"):
        list(session.execute_to_morsels("CREATE TABLE ws.dst AS SELECT NULL AS x"))


def test_ctas_or_replace_creates_when_missing(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(
        session.execute_to_morsels(
            "CREATE OR REPLACE TABLE ws.dst AS SELECT 1 AS a, 'hello' AS b"
        )
    )
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT * FROM ws.dst"))
    assert len(rows) == 1
    assert rows[0]["a"] == 1


def test_ctas_or_replace_succeeds_when_exists(tmp_path):
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.dst AS SELECT 1 AS a, 'old' AS b"))
    list(
        session.execute_to_morsels(
            "CREATE OR REPLACE TABLE ws.dst AS SELECT 2 AS a, 'new' AS b"
        )
    )
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT * FROM ws.dst"))
    assert len(rows) == 1
    assert rows[0]["a"] == 2
    assert rows[0]["b"] == "new"


def test_ctas_or_replace_schema_change_allowed_local_store(tmp_path):
    """LocalStoreConnector has no field-id lineage to preserve, so REPLACE may
    change the column set - unlike the catalog connector (see relation.py)."""
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.dst AS SELECT 1 AS a"))
    list(
        session.execute_to_morsels(
            "CREATE OR REPLACE TABLE ws.dst AS SELECT 'x' AS c, 'y' AS d"
        )
    )
    rows = _morsels_to_rows(session.execute_to_morsels("SELECT * FROM ws.dst"))
    assert rows == [{"c": "x", "d": "y"}]


def test_ctas_or_replace_atomic_on_failure(tmp_path):
    """A REPLACE that fails partway must leave the existing relation untouched."""
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.dst AS SELECT 1 AS a, 'old' AS b"))

    from opteryx.connectors import connector_factory

    conn = connector_factory("ws.dst", telemetry=None)

    def _boom():
        raise RuntimeError("forced replace failure")

    conn._pre_commit_recheck_hook = _boom
    try:
        with pytest.raises(RuntimeError, match="forced replace failure"):
            list(
                session.execute_to_morsels(
                    "CREATE OR REPLACE TABLE ws.dst AS SELECT 2 AS a, 'new' AS b"
                )
            )
    finally:
        conn._pre_commit_recheck_hook = None

    rows = _morsels_to_rows(session.execute_to_morsels("SELECT * FROM ws.dst"))
    assert rows == [{"a": 1, "b": "old"}]


def test_ctas_without_or_replace_existing_still_rejected(tmp_path):
    """Plain CTAS (no OR REPLACE) into an existing relation is unchanged."""
    _setup_workspace(tmp_path)
    session = opteryx.session()
    list(session.execute_to_morsels("CREATE TABLE ws.dst AS SELECT 1 AS a"))
    with pytest.raises(ValueError, match="already exists"):
        list(session.execute_to_morsels("CREATE TABLE ws.dst AS SELECT 2 AS a"))


def test_ctas_partial_failure_leaves_empty_relation(tmp_path):
    _setup_workspace(tmp_path)

    # Force the snapshot commit to fail by raising in the pre-commit hook.
    # Because create_relation runs first (lazy on first morsel/EOS) and only
    # the insert commit fails, the empty dataset.json remains on disk.
    from opteryx.connectors import _connector_cache

    # Wipe cache so we get a fresh connector instance to attach the hook to.
    _connector_cache.clear()
    session = opteryx.session()

    # Get the connector used by 'ws' by invoking factory once via a lookup.
    from opteryx.connectors import connector_factory

    conn = connector_factory("ws.x", telemetry=None)

    def _boom():
        raise RuntimeError("forced commit failure")

    conn._pre_commit_recheck_hook = _boom

    try:
        with pytest.raises(RuntimeError, match="forced commit failure"):
            list(session.execute_to_morsels("CREATE TABLE ws.dst AS SELECT 1 AS a"))
    finally:
        conn._pre_commit_recheck_hook = None

    # dataset.json exists (create succeeded)
    info = _read_dataset_json(tmp_path / "ws", "dst")
    assert info["current_snapshot"] is None

    # Re-running CTAS without DROP should fail (relation exists).
    with pytest.raises(ValueError, match="already exists"):
        list(session.execute_to_morsels("CREATE TABLE ws.dst AS SELECT 1 AS a"))
