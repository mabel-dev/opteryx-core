from __future__ import annotations

import datetime
import io
import os
import sys
from typing import List
from typing import Tuple

sys.path.insert(1, os.path.join(sys.path[0], "../../../orso"))
sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
sys.path.insert(1, os.path.join(sys.path[0], "../../../../pyiceberg-firestore-gcs"))
sys.path.insert(1, os.path.join(sys.path[0], "../../../../opteryx-catalog"))

import opteryx
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from opteryx.connectors import OpteryxConnector
from opteryx_catalog import OpteryxCatalog
from orso.logging import get_logger


logger = get_logger()

SIZE_THRESHOLD_BYTES = 256 * 1024 * 1024  # 256 MB
FIRESTORE_DATABASE = os.environ.get("FIRESTORE_DATABASE")
BUCKET_NAME = os.environ.get("GCS_BUCKET")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

opteryx.set_default_connector(
    OpteryxConnector,
    catalog=OpteryxCatalog,
    firestore_project=GCP_PROJECT_ID,
    firestore_database=FIRESTORE_DATABASE,
    gcs_bucket=BUCKET_NAME,
)


def _estimate_table_bytes(table: pa.Table) -> int:
    """Estimate the memory size of a pyarrow Table by summing buffer sizes.

    This avoids making expensive copies (e.g. to_pandas()) and should be
    accurate enough for deciding when to flush to disk.
    """
    total = 0
    for col in table.itercolumns():
        # ChunkedArray
        for chunk in col.chunks:
            for buf in chunk.buffers():
                if buf is not None:
                    total += buf.size
    return total


def _write_parquet_table(table: pa.Table, gcs_path: str = "") -> int:
    """Write a pyarrow Table to the given gs:// path using zstd and disabled statistics.

    For testing, this writes to /dev/null to simulate the write without persisting.
    Returns the number of bytes written if available (otherwise -1).
    """
    # Disable writing statistics, avoid dictionary encoding (can slow writes),
    # and prefer Parquet v2 data page format for better I/O behavior.
    pq_write_kwargs = dict(
        compression="zstd",
        compression_level=2,
        write_statistics=False,
        use_dictionary=False,
        data_page_version="2.0",
    )
    # For testing, write to an in-memory buffer and discard
    buffer = io.BytesIO()
    pq.write_table(table, buffer, **pq_write_kwargs)
    return buffer.tell()


def _write_manifest(manifest: dict, manifest_path: str = "") -> None:
    """Write a JSON manifest to the given gs:// path.

    For testing, this writes to an in-memory buffer and discards it.
    """
    # In-memory write, just serialize and discard
    _ = orjson.dumps(manifest)


def create_statement(sql_text: str, identity: str) -> dict:
    """Create a fake Firestore document representing a statement to execute.
    
    Args:
        sql_text: The SQL query to execute
        identity: A unique identifier for this statement (used as document ID)
        
    Returns:
        A dictionary representing a Firestore document with the statement data
    """
    return {
        "handle": identity,
        "sql_text": sql_text,
        "status": "PENDING",
        "created_at": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        "identity": identity,
    }


def worker_executor(
    statement: dict,
    batch_size: int = 50_000,
) -> dict:
    """Execute a query statement and return the modified statement with results metadata.
    
    This function executes the SQL query from the statement dict using the opteryx engine,
    chunks the results the same way as the deployed worker, collects telemetry, but does
    not persist results to cloud storage.
    
    Args:
        statement: A dictionary representing a statement (from create_statement or similar)
        batch_size: Number of rows per batch for result processing
        
    Returns:
        The modified statement dictionary with execution metadata, telemetry, and result info
    """
    statement_handle = statement.get("handle")
    sql = statement.get("sql_text")
    
    if not sql:
        statement["status"] = "FAILED"
        statement["error"] = "missing sql_text"
        statement["updated_at"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        statement["finished_at"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        return statement

    # Mark as executing
    statement["status"] = "EXECUTING"
    statement["updated_at"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    statement["started_at"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

    total_size_estimate = 0

    try:
        with opteryx.connect() as conn:
            cursor = conn.cursor()
            batches = cursor.execute_to_arrow_batches(sql, batch_size=batch_size)

            # Iterate batches and calculate sizes (same as deployed version)
            part_index = 0
            buffered_batches: List[pa.RecordBatch] = []
            buffered_rows = 0
            parts: List[Tuple[str, int, int]] = []  # (filename, rows, approx_size)

            for batch in batches:
                buffered_batches.append(batch)
                buffered_rows += batch.num_rows

                # Estimate bytes for the accumulated buffered batches
                buffered_table = pa.Table.from_batches(buffered_batches)
                buffered_bytes = _estimate_table_bytes(buffered_table)

                # When we exceed threshold, flush to a parquet part
                if buffered_bytes >= SIZE_THRESHOLD_BYTES:
                    part_name = f"part_{part_index:04d}.parquet"
                    # Write to in-memory buffer (discarded)
                    _write_parquet_table(buffered_table, "")
                    parts.append((part_name, buffered_rows, buffered_bytes))
                    # Reset buffers
                    buffered_batches = []
                    buffered_rows = 0
                    part_index += 1
                    total_size_estimate += buffered_bytes

            # At the end write any remaining buffered batches as the final part
            if buffered_batches:
                last_table = pa.Table.from_batches(buffered_batches)
                part_name = f"part_{part_index:04d}.parquet"
                _write_parquet_table(last_table, "")
                last_table_size = _estimate_table_bytes(last_table)
                parts.append((part_name, buffered_rows, last_table_size))
                total_size_estimate += last_table_size

            telemetry = cursor.telemetry

        total_rows = sum(rows for _, rows, _ in parts)
        columns = [{"name": f.name, "type": str(f.type)} for f in cursor.schema.columns]

        # Build manifest (same format as deployed version)
        manifest = {
            "parts": [
                {
                    "path": f"gs://opteryx_results/{statement_handle}/{pname}",
                    "rows": rows,
                    "approx_size": approx_size,
                }
                for pname, rows, approx_size in parts
            ],
            "total_parts": len(parts),
            "total_rows": total_rows,
            "total_size_estimate": total_size_estimate,
            "compression": "zstd",
            "compression_level": 2,
            "write_statistics": False,
            "columns": columns,
            "created_at": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        }
        # Write manifest to in-memory buffer (discarded)
        _write_manifest(manifest, "")

        # Update statement with completion info
        statement["status"] = "COMPLETED"
        statement["updated_at"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        statement["finished_at"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        statement["telemetry"] = telemetry
        statement["result_manifest"] = manifest
        statement["total_rows"] = total_rows
        statement["columns"] = columns
        statement["total_size_estimate"] = total_size_estimate

        execution_log = telemetry.copy() if isinstance(telemetry, dict) else {}
        execution_log["statement_handle"] = statement_handle
        execution_log["statement"] = sql
        execution_log["result_manifest"] = manifest
        logger.info("Executed statement %s: %s rows", statement_handle, total_rows)

        return statement

    except Exception as exc:
        logger.error("Error executing statement %s: %s", statement_handle, str(exc))
        statement["status"] = "FAILED"
        statement["error"] = str(exc)
        statement["updated_at"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        statement["finished_at"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        raise


if __name__ == "__main__":

    statement = create_statement(
        sql_text="SELECT 1",
        identity="test_statement_001",
    )
    result = worker_executor(statement, batch_size=100_000)
    print(orjson.dumps(result, option=orjson.OPT_INDENT_2).decode("utf-8"))