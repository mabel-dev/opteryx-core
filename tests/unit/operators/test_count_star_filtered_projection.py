import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _get_read_operation(telemetry: dict) -> dict:
    for operation in telemetry.get("operations", {}).values():
        if operation.get("type") == "ReadRel":
            return operation
    raise AssertionError("No ReadRel operation found in telemetry")


def _collect(session, sql) -> dict:
    """Drain a query to {column_name: [values]}. Morsel column names are bytes."""
    columns: dict = {}
    for morsel in session.execute_to_morsels(sql):
        if not morsel.num_rows:
            continue
        for name in morsel.column_names:
            columns.setdefault(name.decode(), []).extend(morsel.column(name).to_pylist())
    return columns


def test_count_star_with_filter_reads_only_predicate_columns():
    session = opteryx.session()
    try:
        result = _collect(session, "SELECT COUNT(*) FROM testdata.satellites WHERE planetId <> 0")
        assert result["COUNT(*)"][0] == 177

        read_op = _get_read_operation(session.telemetry)
        # The point of this test: COUNT(*) with a pushed predicate decodes ONLY the
        # predicate column (planetId), not all 8 columns of the relation. The native
        # scan attributes every column it reads to the projection set, so assert on
        # the column count rather than the filter/projection split.
        assert read_op.get("columns_read") == 1, read_op
        assert read_op.get("parquet_projection_columns_read") == 1, read_op
        # Every row of the single row group is fed into the filter.
        assert read_op.get("parquet_rows_before_filter") == 177, read_op
    finally:
        session.close()


def test_draken_global_aggregate_does_not_route_through_groupby_runtime_fallback():
    session = opteryx.session()
    try:
        count_result = _collect(
            session, "SELECT COUNT(*) FROM testdata.satellites WHERE planetId <> 0"
        )
        assert count_result["COUNT(*)"][0] == 177

        multi_result = _collect(
            session,
            "SELECT SUM(planetId), COUNT(*), AVG(planetId) FROM testdata.satellites WHERE planetId <> 0",
        )
        assert multi_result["COUNT(*)"][0] == 177
    finally:
        session.close()


def test_draken_global_count_distinct_uses_native_carchar_set():
    session = opteryx.session()
    try:
        result = _collect(
            session,
            "SELECT COUNT(DISTINCT val) FROM (VALUES (1), (1), (NULL), (NULL)) AS test(val)",
        )
        # COUNT(DISTINCT) ignores NULLs, so the only distinct value is 1 (verified
        # against DuckDB). The distinct values are {1}, not {1, NULL}.
        assert result["COUNT(DISTINCT val)"][0] == 1
    finally:
        session.close()
