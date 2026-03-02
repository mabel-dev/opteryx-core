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

