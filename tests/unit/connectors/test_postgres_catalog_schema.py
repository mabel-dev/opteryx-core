"""The catalog is authoritative: a PostgreSQL-bound relation's schema is built
from the catalog's record of it, and the server is not asked.

These cover the plan-time half - what the connector does with the record the
catalog resolution step hands it. The execution-time half (the RowDescription
the first stream carries must agree with the OIDs planned from that record) is
enforced in native_postgres_scan_source.hpp and covered by the storage tests.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.connectors.postgres_connector import PostgresConnector
from opteryx.exceptions import DatasetReadError
from opteryx.types.logical_type import DrakenType


class _Metadata:
    def __init__(self, schema, statistics=None):
        self.schema = schema
        self.statistics = statistics


class _Record:
    """Shaped like the catalog's SimpleDataset, for the connector only."""

    def __init__(self, schema, statistics=None):
        self.metadata = _Metadata(schema, statistics)


def _columns():
    return [
        {"id": 1, "name": "id", "type": "INTEGER", "nullable": False,
         "remote-type": 23},
        {"id": 2, "name": "name", "type": "VARCHAR", "nullable": True,
         "remote-type": 1043},
        # NUMERIC: the typmod is rebuilt from these, never stored alongside them.
        {"id": 3, "name": "total", "type": "DECIMAL(15, 2)", "nullable": True,
         "precision": 15, "scale": 2, "remote-type": 1700},
    ]


def _table(record, prefix="pg"):
    gateway = PostgresConnector(host="h", dbname="d", user="u", password="p")
    gateway._matched_prefix = prefix
    return gateway.table_engine("pg.public.orders", telemetry=None, prefetched_table=record)


def _no_wire():
    """`_pg_helpers` with the two server-facing helpers replaced by trip-wires.

    The other two it returns - the OID -> type map and the OID -> name map - are
    pure lookups that touch no socket, and the catalog path uses them to turn a
    stored OID into a ColumnType. So the assertion here is "nothing went to the
    server", not "this function was not called".
    """

    def describe(*_args, **_kwargs):
        raise AssertionError("the server was described; the catalog is authoritative")

    def query_text(*_args, **_kwargs):
        raise AssertionError("the server was queried; the catalog is authoritative")

    from opteryx.operators._operators import pg_draken_type_for_oid
    from opteryx.operators._operators import pg_type_name_for_oid

    return describe, query_text, pg_draken_type_for_oid, pg_type_name_for_oid


def test_schema_is_built_from_the_catalog_without_asking_the_server(monkeypatch):
    monkeypatch.setattr(
        "opteryx.connectors.postgres_connector._pg_helpers", _no_wire
    )
    table = _table(_Record(_columns(), {"row-count": 15000}))

    schema = table.get_dataset_schema()

    assert [column.name for column in schema.columns] == ["id", "name", "total"]
    assert schema.columns[0].column_type.physical == DrakenType.INT32
    assert schema.columns[1].column_type.physical == DrakenType.VARCHAR
    assert schema.columns[2].column_type.logical.precision == 15
    assert schema.columns[2].column_type.logical.scale == 2
    # The refresh's row count, which knows more than `reltuples` does.
    assert schema.row_count_estimate == 15000
    assert table.schema_from_catalog is True


def test_the_oid_is_carried_through_for_the_wire_decoder(monkeypatch):
    """`remote-type` exists because the engine type cannot say which PostgreSQL
    type the wire will carry - the decoder is chosen by OID.

    `column_oid` and `pg_name` are the SAME accessors the compiler reads when it
    builds the pushed statement, so this also pins that a catalog-planned relation
    is indistinguishable from a server-described one downstream."""
    monkeypatch.setattr(
        "opteryx.connectors.postgres_connector._pg_helpers", _no_wire
    )
    table = _table(_Record(_columns()))
    schema = table.get_dataset_schema()

    by_name = {column.name: column for column in schema.columns}
    assert table.column_oid(by_name["name"]) == 1043
    assert table.column_oid(by_name["total"]) == 1700
    # The name as the SERVER spells it, which is what the statement quotes.
    assert table.pg_name(by_name["id"]) == "id"


def test_a_numeric_with_no_stored_precision_names_the_refresh(monkeypatch):
    """The typmod is rebuilt from `precision`/`scale`, so a NUMERIC missing them
    cannot be typed. Refused naming the refresh, rather than reported as the
    connector's usual "declare it as numeric(p, s)" - the server may well declare
    it perfectly well and the catalog simply not have recorded it."""
    monkeypatch.setattr(
        "opteryx.connectors.postgres_connector._pg_helpers", _no_wire
    )
    columns = [{"id": 1, "name": "total", "type": "DECIMAL(15, 2)",
                "nullable": True, "remote-type": 1700}]
    table = _table(_Record(columns))

    with pytest.raises(DatasetReadError) as error:
        table.get_dataset_schema()

    assert "out of date" in str(error.value)
    assert "precision and scale" in str(error.value)


def test_a_record_without_remote_type_names_the_refresh(monkeypatch):
    """An entry written before `remote-type` existed. Refused, not guessed at:
    several PostgreSQL types bind to one engine type, so inferring the OID back
    would decode some relations as the wrong thing rather than fail."""
    monkeypatch.setattr(
        "opteryx.connectors.postgres_connector._pg_helpers", _no_wire
    )
    stale = [{"id": 1, "name": "id", "type": "INTEGER", "nullable": False}]
    table = _table(_Record(stale))

    with pytest.raises(DatasetReadError) as error:
        table.get_dataset_schema()

    message = str(error.value)
    assert "out of date" in message
    assert "Refresh" in message


def test_no_catalog_record_still_asks_the_server():
    """A connector built without a record - the catalog refresh itself is one -
    describes from the server. That is not a fallback for a relation the catalog
    knows about; it is the only path for one it does not."""
    asked = []

    def _helpers():
        def describe(config, sql):
            asked.append(sql)
            return [("id", 23, -1)]

        def query_text(config, sql, params=None):
            return [("42",)]

        from opteryx.operators._operators import pg_draken_type_for_oid
        from opteryx.operators._operators import pg_type_name_for_oid

        return describe, query_text, pg_draken_type_for_oid, pg_type_name_for_oid

    import opteryx.connectors.postgres_connector as pgc

    original = pgc._pg_helpers
    pgc._pg_helpers = _helpers
    try:
        table = _table(None)
        schema = table.get_dataset_schema()
    finally:
        pgc._pg_helpers = original

    assert len(asked) == 1
    assert [column.name for column in schema.columns] == ["id"]
    assert table.schema_from_catalog is False


def test_an_undescribed_catalog_entry_asks_the_server():
    """A catalog entry that exists but carries no columns - never refreshed.
    Honoured as "the catalog does not describe this yet", not as a relation with
    no columns, which is what used to answer COUNT(*) as zero."""
    asked = []

    def _helpers():
        def describe(config, sql):
            asked.append(sql)
            return [("id", 23, -1)]

        def query_text(config, sql, params=None):
            return []

        from opteryx.operators._operators import pg_draken_type_for_oid
        from opteryx.operators._operators import pg_type_name_for_oid

        return describe, query_text, pg_draken_type_for_oid, pg_type_name_for_oid

    import opteryx.connectors.postgres_connector as pgc

    original = pgc._pg_helpers
    pgc._pg_helpers = _helpers
    try:
        table = _table(_Record(None))
        table.get_dataset_schema()
    finally:
        pgc._pg_helpers = original

    assert len(asked) == 1


if __name__ == "__main__":  # pragma: no cover
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
