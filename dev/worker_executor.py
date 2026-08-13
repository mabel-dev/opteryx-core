"""A local stand-in for the deployed worker: run one statement the way it does.

Developer tooling, not a test — `dev/` is not packaged and is never imported by
production code. It lived at `tests/integration/worker/test_worker.py`, where its
`test_`-prefixed FILENAME got it collected by pytest even though it defines no tests.
Collection IMPORTS every file it collects before running anything, so the
`set_default_connector` call this once made at module scope pointed the whole session
at the catalog and failed nine unrelated tests in other files. Both halves of that are
fixed: the global now happens in `__main__` only, and the file no longer looks like a
test.

Run it directly:

    python dev/worker_executor.py

It needs the catalog credentials in `.env` (loaded when opteryx is imported) and the
`opteryx-catalog` sibling repo checked out next to this one.
"""

from __future__ import annotations

import datetime
import io
import logging
import os
import sys
from typing import List, Tuple

# The repo root, and the catalog sibling repo beside it. Anchored on __file__ rather
# than sys.path[0], which is the CALLER's directory and not this file's.
_DEV_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, os.path.join(_DEV_DIR, ".."))
sys.path.insert(1, os.path.join(_DEV_DIR, "..", "..", "opteryx-catalog"))

from opteryx.third_party import yyjson as orjson
import pyarrow as pa
import pyarrow.parquet as pq
from draken.morsels.morsel import Morsel

import opteryx

logger = logging.getLogger(__name__)

SIZE_THRESHOLD_BYTES = 256 * 1024 * 1024  # 256 MB
FIRESTORE_DATABASE = os.environ.get("FIRESTORE_DATABASE")
BUCKET_NAME = os.environ.get("GCS_BUCKET")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")


def _point_default_connector_at_the_catalog() -> None:
    """Route every unprefixed dataset name through the catalog, as the deployed worker does.

    Called from __main__ ONLY, and that placement is the point — see the module docstring
    for what it cost when it was not. `set_default_connector` is PROCESS-GLOBAL: it
    catches every dataset whose name matches no registered prefix, so anything that
    imports this module would inherit it. Code that needs the same global inside a test
    must take it the way `tests/integration/test_catalog_gcs_scan.py` does — a fixture
    that saves and restores the connector state around the tests that want it.

    The `opteryx_catalog` import is deferred with it: the catalog is a sibling repo
    rather than a dependency, and at module scope a machine without it checked out could
    not import this file at all.
    """
    from opteryx_catalog import OpteryxCatalog

    from opteryx.connectors import OpteryxConnector

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
        session = opteryx.session()
        try:
            morsels = session.execute_to_morsels(sql, max_size=batch_size)

            # Iterate morsels and calculate sizes (same as deployed version)
            part_index = 0
            buffered_morsels: List[Morsel] = []
            buffered_bytes = 0
            buffered_rows = 0
            parts: List[Tuple[str, int, int]] = []  # (filename, rows, approx_size)
            result_schema: pa.Schema | None = None

            for morsel in morsels:
                buffered_morsels.append(morsel)
                buffered_rows += morsel.num_rows
                buffered_bytes += morsel.nbytes

                # When we exceed threshold, flush to a parquet part
                if buffered_bytes >= SIZE_THRESHOLD_BYTES:
                    part_name = f"part_{part_index:04d}.parquet"
                    combined = (
                        buffered_morsels[0]
                        if len(buffered_morsels) == 1
                        else Morsel.combine(buffered_morsels)
                    )
                    buffered_table = combined.to_arrow()
                    if result_schema is None:
                        result_schema = buffered_table.schema
                    # Write to in-memory buffer (discarded)
                    _write_parquet_table(buffered_table, "")
                    parts.append((part_name, buffered_rows, buffered_bytes))
                    # Reset buffers
                    buffered_morsels = []
                    buffered_rows = 0
                    total_size_estimate += buffered_bytes
                    buffered_bytes = 0
                    part_index += 1

            # At the end write any remaining buffered batches as the final part
            if buffered_morsels:
                combined = (
                    buffered_morsels[0]
                    if len(buffered_morsels) == 1
                    else Morsel.combine(buffered_morsels)
                )
                last_table = combined.to_arrow()
                if result_schema is None:
                    result_schema = last_table.schema
                part_name = f"part_{part_index:04d}.parquet"
                _write_parquet_table(last_table, "")
                parts.append((part_name, buffered_rows, buffered_bytes))
                total_size_estimate += buffered_bytes

            telemetry = session.telemetry
        finally:
            session.close()

        total_rows = sum(rows for _, rows, _ in parts)
        columns = (
            [{"name": f.name, "type": str(f.type)} for f in result_schema]
            if result_schema is not None
            else []
        )

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
            "columns": columns,
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
    _point_default_connector_at_the_catalog()
    statement = create_statement(
        sql_text="SELECT 1",
        identity="test_statement_001",
    )
    result = worker_executor(statement, batch_size=100_000)
    print(orjson.dumps(result, option=orjson.OPT_INDENT_2).decode("utf-8"))
