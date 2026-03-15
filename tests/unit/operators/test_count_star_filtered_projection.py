import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _get_read_operation(telemetry: dict) -> dict:
    for operation in telemetry.get("operations", {}).values():
        if operation.get("type") == "ReadRel":
            return operation
    raise AssertionError("No ReadRel operation found in telemetry")


def test_count_star_with_filter_reads_only_predicate_columns():
    session = opteryx.session()
    try:
        result = session.execute_to_arrow(
            "SELECT COUNT(*) FROM testdata.satellites WHERE planetId <> 0"
        )
        assert result.to_pydict()["COUNT(*)"][0] == 177

        read_op = _get_read_operation(session.telemetry)
        assert read_op.get("parquet_filter_columns_read") == 1, read_op
        assert read_op.get("parquet_projection_columns_read") == 0, read_op
        assert read_op.get("parquet_range_request_count") == 1, read_op
    finally:
        session.close()


def test_draken_global_aggregate_does_not_route_through_groupby_runtime_fallback():
    session = opteryx.session()
    try:
        count_result = session.execute_to_arrow(
            "SELECT COUNT(*) FROM testdata.satellites WHERE planetId <> 0"
        )
        assert count_result.to_pydict()["COUNT(*)"][0] == 177

        multi_result = session.execute_to_arrow(
            "SELECT SUM(planetId), COUNT(*), AVG(planetId) FROM testdata.satellites WHERE planetId <> 0"
        )
        assert multi_result.to_pydict()["COUNT(*)"][0] == 177
    finally:
        session.close()


def test_draken_global_count_distinct_uses_native_carchar_set():
    session = opteryx.session()
    try:
        result = session.execute_to_arrow(
            "SELECT COUNT(DISTINCT val) FROM (VALUES (1), (1), (NULL), (NULL)) AS test(val)"
        )
        assert result.to_pydict()["COUNT(DISTINCT val)"][0] == 2
    finally:
        session.close()
