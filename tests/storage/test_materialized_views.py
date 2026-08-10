# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""CREATE MATERIALIZED VIEW / DROP MATERIALIZED VIEW.

An MV is CTAS plus registration: the SELECT executes now and its result is
written as an ordinary backing table; the defining SQL and the extracted
source tables are then registered through the connector. On the local store
the registration record is `materialized_view.json` next to `dataset.json`.
"""

import json

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import UnsupportedSyntaxError


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


def _mv_record(tmp_path, relation):
    p = tmp_path
    for part in relation.split("."):
        p = p / part
    with open(p / "materialized_view.json") as f:
        return json.load(f)


_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]
_WRITER_POLICY = [{"pattern": "*", "role": "writer"}]


def _seed_source(session, name="ws.src"):
    list(session.execute_to_morsels(f"CREATE TABLE {name} (a BIGINT)"))
    list(session.execute_to_morsels(f"INSERT INTO {name} VALUES (-1), (1), (2), (3)"))


# --- creation


def test_create_mv_executes_and_registers(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)

    list(
        owner.execute_to_morsels(
            "CREATE MATERIALIZED VIEW ws.mv AS SELECT * FROM ws.src WHERE a > 0"
        )
    )

    # The backing table is real and queryable.
    rows = _morsels_to_rows(owner.execute_to_morsels("SELECT * FROM ws.mv"))
    assert sorted(r["a"] for r in rows) == [1, 2, 3]

    # The registration carries the defining SQL and the extracted sources.
    record = _mv_record(tmp_path / "ws", "mv")
    assert record["source_tables"] == ["ws.src"]
    assert "SELECT" in record["sql"].upper()
    assert "ws.src" in record["sql"]
    assert record["author"] == "olive"


def test_create_mv_multiple_sources_deduplicated(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner, "ws.left_t")
    _seed_source(owner, "ws.right_t")

    list(
        owner.execute_to_morsels(
            "CREATE MATERIALIZED VIEW ws.mv AS "
            "SELECT l.a AS la, r.a AS ra FROM ws.left_t AS l "
            "JOIN ws.right_t AS r ON l.a = r.a "
            "WHERE l.a IN (SELECT a FROM ws.left_t)"
        )
    )

    record = _mv_record(tmp_path / "ws", "mv")
    assert sorted(record["source_tables"]) == ["ws.left_t", "ws.right_t"]


def test_create_or_replace_mv_re_registers(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)

    list(
        owner.execute_to_morsels(
            "CREATE MATERIALIZED VIEW ws.mv AS SELECT a FROM ws.src WHERE a > 0"
        )
    )
    list(
        owner.execute_to_morsels(
            "CREATE OR REPLACE MATERIALIZED VIEW ws.mv AS SELECT a FROM ws.src WHERE a > 1"
        )
    )

    rows = _morsels_to_rows(owner.execute_to_morsels("SELECT * FROM ws.mv"))
    assert sorted(r["a"] for r in rows) == [2, 3]
    record = _mv_record(tmp_path / "ws", "mv")
    assert "> 1" in record["sql"]


def test_create_mv_existing_relation_rejected(tmp_path):
    """Without OR REPLACE, an existing relation is an error (CTAS semantics)."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    list(owner.execute_to_morsels("CREATE TABLE ws.mv (a BIGINT)"))

    with pytest.raises(ValueError, match="already exists"):
        list(
            owner.execute_to_morsels(
                "CREATE MATERIALIZED VIEW ws.mv AS SELECT * FROM ws.src"
            )
        )


def test_create_mv_if_not_exists_rejected(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    # The message carries markdown emphasis, so match around the styling.
    with pytest.raises(UnsupportedSyntaxError, match="does not support IF NOT"):
        list(
            owner.execute_to_morsels(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS ws.mv AS SELECT * FROM ws.src"
            )
        )


# --- source validation


def test_create_mv_virtual_source_rejected(tmp_path):
    """$planets can never fire a refresh - not a catalog table."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="not a catalog table"):
        list(
            owner.execute_to_morsels(
                "CREATE MATERIALIZED VIEW ws.mv AS SELECT * FROM $planets"
            )
        )


def test_create_mv_no_catalog_source_rejected(tmp_path):
    """A VALUES-only SELECT has zero catalog sources - nothing fires a refresh."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    with pytest.raises(UnsupportedSyntaxError, match="at least one catalog table"):
        list(
            owner.execute_to_morsels(
                "CREATE MATERIALIZED VIEW ws.mv AS "
                "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(x, y)"
            )
        )


def test_create_mv_mixed_virtual_source_rejected(tmp_path):
    """A single non-catalog scan poisons the MV even next to a catalog table."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)

    with pytest.raises(UnsupportedSyntaxError, match="not a catalog table"):
        list(
            owner.execute_to_morsels(
                "CREATE MATERIALIZED VIEW ws.mv AS "
                "SELECT s.a FROM ws.src AS s CROSS JOIN $planets AS p"
            )
        )


# --- permissions


def test_create_mv_needs_only_write_on_target(tmp_path):
    """Writer tier is enough for an MV target, exactly as it is for CTAS.

    An MV does nothing its creator could not do by hand with a CTAS into the
    same place, so it needs no more authority than one.
    """
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)

    writer = opteryx.session(user="wendy", access_policies=_WRITER_POLICY)
    list(writer.execute_to_morsels("CREATE MATERIALIZED VIEW ws.mv AS SELECT * FROM ws.src"))
    assert _mv_record(tmp_path, "ws.mv")["source_tables"] == ["ws.src"]


def test_create_mv_refused_without_write_on_target(tmp_path):
    """Reader on the target is not enough - the view still writes a table."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)

    reader = opteryx.session(user="rhea", access_policies=[{"pattern": "*", "role": "reader"}])
    with pytest.raises(PermissionError, match="write required"):
        list(reader.execute_to_morsels("CREATE MATERIALIZED VIEW ws.mv AS SELECT * FROM ws.src"))
    assert not (tmp_path / "ws" / "mv").exists()


def test_create_mv_needs_only_read_on_sources(tmp_path):
    """If you can read a table you may derive from it.

    Requiring write on sources would mean no view could ever be built over
    data you are only permitted to read, which is most of what views are for.
    """
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)

    # Writer where the view lands, reader-only on the source it reads.
    mixed = opteryx.session(
        user="mara",
        access_policies=[
            {"pattern": "ws.mv", "role": "writer"},
            {"pattern": "ws.src", "role": "reader"},
        ],
    )
    list(mixed.execute_to_morsels("CREATE MATERIALIZED VIEW ws.mv AS SELECT * FROM ws.src"))
    assert _mv_record(tmp_path, "ws.mv")["source_tables"] == ["ws.src"]


def test_create_mv_refused_without_read_on_sources(tmp_path):
    """No grant at all on the source is still refused.

    This is the check that keeps a pinned `runs-as` owner from turning edits
    into a confused deputy: it runs on every registration, against whoever is
    executing, so an editor can never repoint a view at data they cannot read.
    """
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)

    blind = opteryx.session(user="bram", access_policies=[{"pattern": "ws.mv", "role": "writer"}])
    with pytest.raises(PermissionError):
        list(blind.execute_to_morsels("CREATE MATERIALIZED VIEW ws.mv AS SELECT * FROM ws.src"))
    assert not (tmp_path / "ws" / "mv").exists()


# --- drops


def test_drop_materialized_view(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    list(
        owner.execute_to_morsels(
            "CREATE MATERIALIZED VIEW ws.mv AS SELECT * FROM ws.src"
        )
    )

    list(owner.execute_to_morsels("DROP MATERIALIZED VIEW ws.mv"))
    assert not (tmp_path / "ws" / "mv").exists()


def test_drop_table_on_mv_rejected(tmp_path):
    """DROP TABLE against an MV must point at DROP MATERIALIZED VIEW."""
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    list(
        owner.execute_to_morsels(
            "CREATE MATERIALIZED VIEW ws.mv AS SELECT * FROM ws.src"
        )
    )

    with pytest.raises(ValueError, match="DROP MATERIALIZED VIEW"):
        list(owner.execute_to_morsels("DROP TABLE ws.mv"))
    assert (tmp_path / "ws" / "mv").exists()


def test_drop_materialized_view_on_plain_table_rejected(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)

    with pytest.raises(ValueError, match="not a materialized view"):
        list(owner.execute_to_morsels("DROP MATERIALIZED VIEW ws.src"))
    assert (tmp_path / "ws" / "src").exists()


def test_drop_materialized_view_requires_owner(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    _seed_source(owner)
    list(
        owner.execute_to_morsels(
            "CREATE MATERIALIZED VIEW ws.mv AS SELECT * FROM ws.src"
        )
    )

    writer = opteryx.session(user="wendy", access_policies=_WRITER_POLICY)
    with pytest.raises(PermissionError, match="permission to drop"):
        list(writer.execute_to_morsels("DROP MATERIALIZED VIEW ws.mv"))
    assert (tmp_path / "ws" / "mv").exists()


def test_drop_materialized_view_if_exists_missing_ok(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)

    list(owner.execute_to_morsels("DROP MATERIALIZED VIEW IF EXISTS ws.mv"))
