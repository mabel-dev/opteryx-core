"""PostgresConnector end to end: bind a workspace to a live PostgreSQL server
and run real queries through the engine — the native wire-protocol client,
the plan-time describe, predicate/LIMIT pushdown into the statement, the
native Source decoding binary rows into morsels, and every refusal path.

Needs a reachable server. The connection URL comes from, in order,
POSTGRES_TEST_CONNECTION, DATA_CATALOG_CONNECTION (the environment, then the
repo-root `.env`). CI passes DATA_CATALOG_CONNECTION as a secret. With no URL
the module FAILS rather than skipping, matching test_mabel_connector_gcs.py's
convention: a silently skipped suite is a false green.

Only relations every PostgreSQL server has are touched (information_schema,
pg_catalog), and only read. The one thing this cannot cover on system
relations is the numeric/date/uuid/json decoders; those are exercised
against a user table (public.planets on the shared dev server) by the
proof-of-concept driver and are not gated here.
"""

import os
import sys
import urllib.parse

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.connectors import PostgresConnector
from opteryx.connectors import postgres_connector
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import ReadOnlyConnectorError
from opteryx.exceptions import UnsupportedSyntaxError

WORKSPACE = "pgtest"


def _connection_url() -> str:
    for name in ("POSTGRES_TEST_CONNECTION", "DATA_CATALOG_CONNECTION"):
        value = os.environ.get(name)
        if value:
            return value
    env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    if os.path.exists(env_path):
        with open(env_path) as env_file:
            for line in env_file:
                if line.startswith("DATA_CATALOG_CONNECTION="):
                    value = line.split("=", 1)[1].strip()
                    if len(value) > 1 and value[0] == value[-1] and value[0] in "\"'":
                        value = value[1:-1]
                    return value
    raise RuntimeError(
        "No PostgreSQL server configured: set POSTGRES_TEST_CONNECTION (or "
        "DATA_CATALOG_CONNECTION) to a postgresql:// URL"
    )


def _register():
    url = urllib.parse.urlsplit(_connection_url())
    options = dict(urllib.parse.parse_qsl(url.query))
    opteryx.register_workspace(
        WORKSPACE,
        PostgresConnector,
        host=url.hostname,
        port=url.port or 5432,
        dbname=url.path.lstrip("/"),
        user=url.username,
        password=urllib.parse.unquote(url.password or ""),
        sslmode=options.get("sslmode", "require"),
    )


_register()


def _morsels(sql):
    return list(opteryx.session().execute_to_morsels(sql))


def _column(sql, name):
    values = []
    for morsel in _morsels(sql):
        values.extend(morsel.column(name).to_pylist())
    return values


def _explain_text(sql) -> str:
    cells = []
    for morsel in _morsels("EXPLAIN " + sql):
        for column_name in morsel.column_names:
            for value in morsel.column(column_name).to_pylist():
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
                cells.append(str(value))
    return "\n".join(cells)


def test_scan_a_system_relation():
    names = _column(f"SELECT schema_name FROM {WORKSPACE}.information_schema.schemata", "schema_name")
    assert "pg_catalog" in names
    assert "information_schema" in names


def test_explain_shows_the_postgres_reader():
    text = _explain_text(f"SELECT schema_name FROM {WORKSPACE}.information_schema.schemata")
    assert "Postgres Reader" in text


def test_equality_predicate_is_pushed_and_answered_correctly():
    sql = (
        f"SELECT table_schema, table_name FROM {WORKSPACE}.information_schema.tables "
        "WHERE table_schema = 'pg_catalog'"
    )
    schemas = _column(sql, "table_schema")
    assert schemas and set(schemas) == {"pg_catalog"}
    text = _explain_text(sql)
    assert "predicate pushdown into sc" in text
    assert "Filter" not in text


def test_like_with_a_wildcard_underscore_is_pushed():
    # 'pg_%' keeps the LIKE as a comparison (the `_` is a wildcard, so the
    # rewriter cannot lower it to a prefix test), and the connector pushes it.
    names = _column(f"SELECT table_name FROM {WORKSPACE}.information_schema.tables", "table_name")
    expected = sum(1 for name in names if name.startswith("pg") and len(name) >= 3)
    sql = f"SELECT COUNT(*) FROM {WORKSPACE}.information_schema.tables WHERE table_name LIKE 'pg_%'"
    assert _column(sql, "COUNT(*)")[0] == expected > 0
    assert "predicate pushdown into sc" in _explain_text(sql)


def test_like_lowered_to_a_function_is_declined_not_broken():
    # 'pg%' lowers to _STARTS_WITH, a FUNCTION the translator has no SQL for:
    # it stays a Filter above the scan and the answer is still right.
    names = _column(f"SELECT table_name FROM {WORKSPACE}.information_schema.tables", "table_name")
    expected = sum(1 for name in names if name.startswith("pg"))
    sql = f"SELECT COUNT(*) FROM {WORKSPACE}.information_schema.tables WHERE table_name LIKE 'pg%'"
    assert _column(sql, "COUNT(*)")[0] == expected > 0
    text = _explain_text(f"SELECT table_name FROM {WORKSPACE}.information_schema.tables WHERE table_name LIKE 'pg%'")
    assert "predicate pushdown declined" in text
    assert "Filter" in text


def test_limit_is_pushed():
    sql = f"SELECT table_name FROM {WORKSPACE}.information_schema.tables LIMIT 3"
    assert len(_column(sql, "table_name")) == 3
    assert "limit pushdown" in _explain_text(sql)


def test_count_star_matches_a_full_scan():
    total = _column(f"SELECT COUNT(*) FROM {WORKSPACE}.information_schema.tables", "COUNT(*)")[0]
    assert total > 0
    assert total == len(_column(f"SELECT table_name FROM {WORKSPACE}.information_schema.tables", "table_name"))


def test_two_postgres_scans_in_one_query():
    sql = (
        f"SELECT COUNT(*) FROM {WORKSPACE}.information_schema.tables AS t "
        f"INNER JOIN {WORKSPACE}.information_schema.columns AS c ON t.table_name = c.table_name "
        "WHERE t.table_schema = 'pg_catalog' AND c.table_schema = 'pg_catalog'"
    )
    assert _column(sql, "COUNT(*)")[0] > 0


def test_join_with_a_native_relation():
    sql = (
        f"SELECT COUNT(*) FROM {WORKSPACE}.information_schema.schemata AS s "
        "CROSS JOIN $planets AS p"
    )
    schemata = len(_column(f"SELECT schema_name FROM {WORKSPACE}.information_schema.schemata", "schema_name"))
    assert _column(sql, "COUNT(*)")[0] == schemata * 9


def test_mixed_types_from_pg_stat_database():
    sql = (
        "SELECT datname, numbackends, xact_commit, blk_read_time, stats_reset "
        f"FROM {WORKSPACE}.pg_catalog.pg_stat_database LIMIT 5"
    )
    morsels = _morsels(sql)
    assert morsels
    morsel = morsels[0]
    types = {name: str(morsel.column(name).type) for name in ("datname", "numbackends", "xact_commit", "blk_read_time", "stats_reset")}
    assert types["datname"].endswith("VARCHAR")
    assert types["numbackends"].endswith("INT32")
    assert types["xact_commit"].endswith("INT64")
    assert types["blk_read_time"].endswith("FLOAT64")
    assert types["stats_reset"].endswith("TIMESTAMP64")


def test_small_source_batches_lose_and_duplicate_nothing(monkeypatch):
    # The Source cuts the server stream every POSTGRES_SCAN_BATCH_ROWS rows; the
    # engine's output edge re-batches, so the cut is not observable as a morsel
    # count here. What IS observable is that a many-batch stream carries exactly
    # the rows a one-batch stream does.
    sql = f"SELECT column_name FROM {WORKSPACE}.information_schema.columns"
    reference = sorted(_column(sql, "column_name"))
    monkeypatch.setattr(postgres_connector, "POSTGRES_SCAN_BATCH_ROWS", 50)
    batched = sorted(_column(sql, "column_name"))
    assert len(reference) > 50
    assert batched == reference
    total = _column(f"SELECT COUNT(*) FROM {WORKSPACE}.information_schema.columns", "COUNT(*)")[0]
    assert total == len(batched)


def test_missing_relation_is_not_found():
    with pytest.raises(DatasetNotFoundError):
        _morsels(f"SELECT * FROM {WORKSPACE}.public.no_such_relation_xyz")


def test_unsupported_column_type_is_refused_by_column_name():
    # pg_namespace.nspacl is aclitem[] — no Draken mapping, refused at bind.
    with pytest.raises(NotSupportedError, match="nspacl"):
        _morsels(f"SELECT nspname FROM {WORKSPACE}.pg_catalog.pg_namespace")


def test_writes_are_refused():
    with pytest.raises(ReadOnlyConnectorError):
        _morsels(f"INSERT INTO {WORKSPACE}.information_schema.tables (table_name) VALUES ('x')")


def test_version_travel_is_refused():
    with pytest.raises(UnsupportedSyntaxError):
        _morsels(f"SELECT schema_name FROM {WORKSPACE}.information_schema.schemata VERSION AS OF 1")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
