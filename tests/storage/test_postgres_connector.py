"""PostgresConnector end to end: bind a workspace to a live PostgreSQL server
and run real queries through the engine — the native wire-protocol client,
the plan-time describe, predicate/LIMIT/top-N/aggregate/DISTINCT pushdown into
the statement, the
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
import re
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
        user=urllib.parse.unquote(url.username or ""),
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


def test_every_lowered_like_shape_is_pushed_and_answered_correctly():
    # The optimizer lowers an anchored LIKE to a _STARTS_WITH/_ENDS_WITH FUNCTION
    # and an unanchored one to an InStr comparison before any connector sees it.
    # All three are spelled back as `LIKE $1` here; the server's answer has to
    # equal what the engine computes locally, and no Filter may remain.
    names = _column(f"SELECT table_name FROM {WORKSPACE}.information_schema.tables", "table_name")
    cases = [
        ("LIKE 'pg%'", "pg%", sum(1 for n in names if n.startswith("pg"))),
        ("LIKE '%tables'", "%tables", sum(1 for n in names if n.endswith("tables"))),
        ("LIKE '%constraint%'", "%constraint%", sum(1 for n in names if "constraint" in n)),
        ("NOT LIKE 'pg%'", "pg%", sum(1 for n in names if not n.startswith("pg"))),
        ("NOT LIKE '%tables'", "%tables", sum(1 for n in names if not n.endswith("tables"))),
        ("NOT LIKE '%constraint%'", "%constraint%", sum(1 for n in names if "constraint" not in n)),
    ]
    for predicate, expected_parameter, expected_count in cases:
        sql = (
            f"SELECT COUNT(*) FROM {WORKSPACE}.information_schema.tables "
            f"WHERE table_name {predicate}"
        )
        count, captured = _pushed_params(sql)
        assert count == expected_count > 0, predicate
        # The pattern is rebuilt by the renderer, so the bind value is the proof
        # that the right one was rebuilt.
        assert captured == [[expected_parameter]], predicate
        text = _explain_text(
            f"SELECT table_name FROM {WORKSPACE}.information_schema.tables "
            f"WHERE table_name {predicate}"
        )
        assert "predicate pushdown into sc" in text, predicate
        assert "Filter" not in text, predicate


def test_a_like_pattern_body_escapes_its_metacharacters():
    # The only metacharacter that can reach the renderer inside a lowered
    # pattern is a backslash (the rewriter lowers a LIKE only when the body has
    # no `%` or `_`), and backslash is LIKE's own escape character: unescaped,
    # `a\b%` would ask the server to match a literal 'b' where the engine asks
    # for a backslash followed by 'b'.
    sql = (
        f"SELECT COUNT(*) FROM {WORKSPACE}.information_schema.tables "
        "WHERE table_name LIKE 'a\\b%'"
    )
    count, captured = _pushed_params(sql)
    assert captured == [["a\\\\b%"]]
    assert count == 0


def test_case_insensitive_like_is_not_pushed():
    # ILIKE folds case by the server's locale and by the engine's own rules, so
    # it stays a local Filter and the answer is still right.
    names = _column(f"SELECT table_name FROM {WORKSPACE}.information_schema.tables", "table_name")
    expected = sum(1 for name in names if name.lower().startswith("pg"))
    sql = f"SELECT COUNT(*) FROM {WORKSPACE}.information_schema.tables WHERE table_name ILIKE 'PG%'"
    assert _column(sql, "COUNT(*)")[0] == expected > 0
    text = _explain_text(
        f"SELECT table_name FROM {WORKSPACE}.information_schema.tables WHERE table_name ILIKE 'PG%'"
    )
    assert "predicate pushdown declined" in text
    assert "Filter" in text


def test_in_list_is_pushed_and_matches_the_unpushed_plan():
    sql = (
        f"SELECT table_schema, table_name FROM {WORKSPACE}.information_schema.tables "
        "WHERE table_schema IN ('pg_catalog', 'information_schema')"
    )
    pushed = sorted(_rows(sql))
    assert pushed == sorted(_rows_without("disable_predicate_pushdown", sql))
    assert pushed and {row[0] for row in pushed} == {"pg_catalog", "information_schema"}
    text = _explain_text(sql)
    assert "predicate pushdown into sc" in text
    assert "Filter" not in text


def test_not_in_list_is_pushed_and_drops_null_rows_like_the_server():
    # `NOT IN` is NULL for a NULL subject on both sides, so the row is dropped by
    # the server exactly as the engine drops it; the unpushed plan is the oracle.
    sql = (
        f"SELECT table_schema, table_name FROM {WORKSPACE}.information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog')"
    )
    pushed = sorted(_rows(sql))
    assert pushed == sorted(_rows_without("disable_predicate_pushdown", sql))
    assert pushed and all(row[0] != "pg_catalog" for row in pushed)
    assert "predicate pushdown into sc" in _explain_text(sql)


def test_an_or_of_equalities_folds_into_a_pushed_in_list():
    # DisjunctiveDomainPushdownStrategy rewrites this to an IN-list before the
    # scan gate sees it, so it pushes now that IN-lists are spellable.
    sql = (
        f"SELECT table_schema, table_name FROM {WORKSPACE}.information_schema.tables "
        "WHERE table_schema = 'pg_catalog' OR table_schema = 'information_schema'"
    )
    pushed = sorted(_rows(sql))
    assert pushed == sorted(_rows_without("disable_predicate_pushdown", sql))
    assert pushed
    assert "predicate pushdown into sc" in _explain_text(sql)


# The fixture the temporal tests read. Provisioned out of band on the shared dev
# server (the suite itself is read-only); recreate it with:
#
#   CREATE TABLE test.temporal (id integer PRIMARY KEY, d date, ts timestamp,
#                               tstz timestamptz);
#   INSERT INTO test.temporal VALUES
#     (1, DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.000001', TIMESTAMPTZ '1969-12-31 23:59:59.000001+00'),
#     (2, DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00',        TIMESTAMPTZ '1970-01-01 00:00:00+00'),
#     (3, DATE '1998-09-01', TIMESTAMP '1998-09-01 10:11:12.000001', TIMESTAMPTZ '1998-09-01 10:11:12.000001+00'),
#     (4, DATE '2024-02-29', TIMESTAMP '2024-02-29 12:00:00',        TIMESTAMPTZ '2024-02-29 12:00:00+00'),
#     (5, NULL, NULL, NULL);
#
# Row 1 is the point of the pre-epoch row: its DATE is day -1, so it proves the
# sign path, which an all-positive fixture would leave untested.
TEMPORAL = f"{WORKSPACE}.test.temporal"


def _pushed_params(sql):
    """Run `sql` and return the bind parameters each pushed scan was given."""
    captured = []
    original = postgres_connector.build_scan_statement

    def _capture(table, columns, predicates, limit, **pushed):
        statement = original(table, columns, predicates, limit, **pushed)
        captured.append(statement.params)
        return statement

    postgres_connector.build_scan_statement = _capture
    try:
        rows = _column(sql, "COUNT(*)")
    finally:
        postgres_connector.build_scan_statement = original
    return rows[0], captured


def test_a_temporal_predicate_is_pushed_as_a_temporal_parameter():
    """The regression the unit fixtures could not see.

    A temporal literal reaches the connector as its PHYSICAL storage integer -
    a DATE is days since the epoch, a TIMESTAMP microseconds - so rendering the
    bind parameter from the Python value alone sent `'10470'` where the server
    wanted `'1998-09-01'`, and EVERY date or timestamp predicate on a
    Postgres-bound relation failed with 22007/22008. The old unit test
    hand-built a `datetime.date` the planner never produces, so it stayed green
    throughout.

    Both halves are asserted: the PARAMETER is what the bug corrupted, and the
    COUNT is what proves the parameter meant what it said. A parameter that
    merely parses as a date would pass the first assertion alone.
    """
    cases = [
        ("d >= CAST('1970-01-01' AS DATE)", 3, "1970-01-01"),
        ("d < CAST('1970-01-01' AS DATE)", 1, "1970-01-01"),
        ("d = CAST('1969-12-31' AS DATE)", 1, "1969-12-31"),  # day -1
        ("d = CAST('2024-02-29' AS DATE)", 1, "2024-02-29"),
        ("ts > CAST('1998-09-01 10:11:12' AS TIMESTAMP)", 2, "1998-09-01T10:11:12.000000"),
        ("ts < CAST('1970-01-01 00:00:00' AS TIMESTAMP)", 1, "1970-01-01T00:00:00.000000"),
        ("tstz > CAST('1998-09-01 10:11:12' AS TIMESTAMP)", 2, "1998-09-01T10:11:12.000000"),
    ]
    for predicate, expected_rows, expected_param in cases:
        sql = f"SELECT COUNT(*) FROM {TEMPORAL} WHERE {predicate}"
        rows, params = _pushed_params(sql)
        assert params == [[expected_param]], (predicate, params)
        assert rows == expected_rows, (predicate, rows)
        assert "predicate pushdown into sc" in _explain_text(sql), predicate


def test_a_temporal_between_is_pushed_as_two_temporal_parameters():
    sql = (
        f"SELECT COUNT(*) FROM {TEMPORAL} "
        "WHERE d BETWEEN CAST('1969-12-31' AS DATE) AND CAST('1970-01-01' AS DATE)"
    )
    rows, params = _pushed_params(sql)
    assert params == [["1969-12-31", "1970-01-01"]], params
    assert rows == 2


def test_a_null_temporal_column_is_not_matched_by_a_pushed_bound():
    # Row 5 is all NULL; a pushed comparison must not claim it.
    assert _column(f"SELECT COUNT(*) FROM {TEMPORAL}", "COUNT(*)")[0] == 5
    rows, _ = _pushed_params(
        f"SELECT COUNT(*) FROM {TEMPORAL} WHERE d >= CAST('1900-01-01' AS DATE)"
    )
    assert rows == 4


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


# ---- remote pushdown: top-N, aggregate, DISTINCT, filtered LIMIT --------------------
#
# Every case below runs the query twice — with the strategy on and with its
# FEATURE_DISABLE_* kill-switch — and the two must agree exactly (sorted where the
# query has no ORDER BY). The kill-switch is the correctness oracle; EXPLAIN's
# optimizer telemetry lines prove the pushdown actually happened.

from opteryx import config as _config


def _rows(sql):
    rows = []
    for morsel in _morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    return rows


def _rows_without(flag, sql):
    setattr(_config.features, flag, True)
    try:
        return _rows(sql)
    finally:
        setattr(_config.features, flag, False)


def test_topn_is_pushed_and_matches_the_unpushed_plan():
    # A text key: the statement carries COLLATE "C" so the server's order is
    # bytewise like draken's; NULLS LAST is spelled because DESC on the engine
    # puts NULL last and the server's default DESC puts it first.
    sql = (
        f"SELECT column_name, ordinal_position FROM {WORKSPACE}.information_schema.columns "
        "ORDER BY column_name DESC LIMIT 7"
    )
    pushed = _rows(sql)
    assert len(pushed) == 7
    assert pushed == _rows_without("disable_topn_scan_pushdown", sql)
    text = _explain_text(sql)
    assert "topn scan pushdown" in text
    assert "Heap Sort" in text  # the local cut is retained


def test_topn_multi_key_and_predicate_in_one_statement():
    sql = (
        f"SELECT column_name, table_schema, ordinal_position FROM {WORKSPACE}.information_schema.columns "
        "WHERE table_schema = 'pg_catalog' ORDER BY ordinal_position DESC, column_name ASC LIMIT 5"
    )
    pushed = _rows(sql)
    assert len(pushed) == 5 and all(row[1] == "pg_catalog" for row in pushed)
    assert pushed == _rows_without("disable_topn_scan_pushdown", sql)
    assert "topn scan pushdown" in _explain_text(sql)


def test_topn_ascending_keeps_the_null_rows_first():
    # pg_stat_database has one row with a NULL datname (the shared objects
    # row). The engine sorts NULL below every value, so ASC LIMIT 1 IS that
    # row — the server's default (NULLS LAST under ASC) would discard it.
    sql = f"SELECT datname FROM {WORKSPACE}.pg_catalog.pg_stat_database ORDER BY datname ASC LIMIT 1"
    assert _rows(sql) == [(None,)]
    assert _rows(sql) == _rows_without("disable_topn_scan_pushdown", sql)
    desc = f"SELECT datname FROM {WORKSPACE}.pg_catalog.pg_stat_database ORDER BY datname DESC LIMIT 1"
    assert _rows(desc)[0][0] is not None
    assert _rows(desc) == _rows_without("disable_topn_scan_pushdown", desc)


def _operator_in_plan(text, name):
    """An operator line of EXPLAIN's plan tree — not a strategy name in its
    REWRITE TRACE (`DistinctScanPushdownStrategy` also starts with 'Distinct')."""
    return re.search(rf"(?:^|─ ){name}\b", text, flags=re.MULTILINE) is not None


def test_grouped_aggregate_is_answered_by_the_server():
    sql = (
        "SELECT table_schema, COUNT(*), MIN(table_name), MAX(table_name), "
        f"COUNT(DISTINCT table_type), COUNT(table_name) FROM {WORKSPACE}.information_schema.tables "
        "GROUP BY table_schema"
    )
    pushed = sorted(_rows(sql))
    assert pushed and pushed == sorted(_rows_without("disable_aggregate_scan_pushdown", sql))
    text = _explain_text(sql)
    assert "aggregate scan pushdown" in text
    assert not _operator_in_plan(text, r"(?:Grouped |Ungrouped )?Aggregate")  # removed, not retained


def test_ungrouped_aggregates_including_sum_and_avg():
    # information_schema.columns, not pg_stat_*: the statistics views are live
    # counters and the two runs of the oracle would see different numbers.
    # character_maximum_length is NULL for most rows — COUNT/MAX skip them.
    sql = (
        "SELECT COUNT(*), SUM(ordinal_position), COUNT(character_maximum_length), "
        f"MAX(character_maximum_length), AVG(ordinal_position) FROM {WORKSPACE}.information_schema.columns"
    )
    pushed = _rows(sql)
    local = _rows_without("disable_aggregate_scan_pushdown", sql)
    assert len(pushed) == len(local) == 1
    assert pushed[0][:4] == local[0][:4]
    # AVG: the server averages exactly in numeric then rounds once to float8;
    # the engine accumulates in double. Equal to well inside double precision.
    assert abs(pushed[0][4] - local[0][4]) <= 1e-9 * max(1.0, abs(local[0][4]))
    assert "aggregate scan pushdown" in _explain_text(sql)


def test_aggregate_over_a_pushed_predicate_and_over_no_rows():
    sql = (
        f"SELECT COUNT(*), COUNT(table_name) FROM {WORKSPACE}.information_schema.tables "
        "WHERE table_schema = 'pg_catalog'"
    )
    assert _rows(sql) == _rows_without("disable_aggregate_scan_pushdown", sql)
    assert _rows(sql)[0][0] > 0
    # Zero matching rows: one row out, COUNT 0, MIN/MAX NULL — both sides agree.
    empty = (
        f"SELECT COUNT(*), MIN(table_name), MAX(table_name) FROM {WORKSPACE}.information_schema.tables "
        "WHERE table_schema = 'no_such_schema_xyz'"
    )
    assert _rows(empty) == [(0, None, None)]
    assert _rows(empty) == _rows_without("disable_aggregate_scan_pushdown", empty)
    grouped_empty = (
        f"SELECT table_schema, COUNT(*) FROM {WORKSPACE}.information_schema.tables "
        "WHERE table_schema = 'no_such_schema_xyz' GROUP BY table_schema"
    )
    assert _rows(grouped_empty) == []


def test_no_aggregate_group_by_is_pushed():
    sql = f"SELECT table_schema FROM {WORKSPACE}.information_schema.tables GROUP BY table_schema"
    assert sorted(_rows(sql)) == sorted(_rows_without("disable_aggregate_scan_pushdown", sql))
    assert "aggregate scan pushdown" in _explain_text(sql)


def test_having_keeps_the_aggregate_local():
    sql = (
        f"SELECT table_schema, COUNT(*) FROM {WORKSPACE}.information_schema.tables "
        "GROUP BY table_schema HAVING COUNT(*) > 1"
    )
    assert sorted(_rows(sql)) == sorted(_rows_without("disable_aggregate_scan_pushdown", sql))
    assert "aggregate scan pushdown" not in _explain_text(sql)


def test_unspellable_aggregate_keeps_the_aggregate_local():
    # STDDEV accumulates differently on the two sides; declined, still answered.
    sql = f"SELECT STDDEV(ordinal_position) FROM {WORKSPACE}.information_schema.columns"
    assert _rows(sql) == _rows_without("disable_aggregate_scan_pushdown", sql)
    assert "aggregate scan pushdown" not in _explain_text(sql)


def test_distinct_is_pushed_and_matches():
    sql = f"SELECT DISTINCT table_schema, table_type FROM {WORKSPACE}.information_schema.tables"
    pushed = sorted(_rows(sql))
    assert pushed == sorted(_rows_without("disable_distinct_scan_pushdown", sql))
    assert len(pushed) == len(set(pushed))
    text = _explain_text(sql)
    assert "distinct scan pushdown" in text
    assert not _operator_in_plan(text, "Distinction")


def test_limit_over_a_pushed_predicate_is_pushed():
    sql = (
        f"SELECT table_schema, table_name FROM {WORKSPACE}.information_schema.tables "
        "WHERE table_schema = 'pg_catalog' LIMIT 3"
    )
    rows = _rows(sql)
    assert len(rows) == 3 and all(row[0] == "pg_catalog" for row in rows)
    text = _explain_text(sql)
    assert "limit pushdown" in text
    assert "predicate pushdown into sc" in text


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
