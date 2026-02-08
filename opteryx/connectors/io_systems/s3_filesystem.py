"""
S3 filesystem implementation using Opteryx's optimized I/O.

This implements pyarrow.fs.FileSystem interface but uses Opteryx's
stream wrappers for high-performance S3 access.
"""

import io
import os
from dataclasses import dataclass
from typing import List
from typing import Optional
from typing import Union

from minio.select import OutputSerialization
from minio.xml import SubElement

from opteryx.connectors.capabilities import PredicatePushable
from opteryx.exceptions import MissingDependencyError
from opteryx.exceptions import UnmetRequirementError
from opteryx.third_party.alantsd.base64 import encode


@dataclass(frozen=True)
class ParquetOutputSerialization(OutputSerialization):
    """Parquet output serialization for SelectRequest.

    payload: optional bytes that will be base64-encoded and placed inside a
    `<Payload>` child element. Many S3 Select servers do not expect a payload
    for Parquet output; this option is provided for clients/tests that need to
    embed a small binary value in the XML.
    """

    compression_algorithm: str = "zstd"
    write_statistics: bool = False
    compression_level: Optional[int] = None
    payload: Optional[bytes] = None

    def toxml(self, element):
        """Convert to XML by appending a `<Parquet>` node and optional payload."""
        node = SubElement(element, "Parquet")
        if self.payload is not None:
            encoded = encode(self.payload).decode("ascii")
            SubElement(node, "Payload", encoded)
        if self.compression_algorithm is not None:
            SubElement(node, "CompressionAlgorithm", self.compression_algorithm)
        SubElement(node, "WriteStatistics", str(self.write_statistics).lower())
        if self.compression_level is not None:
            SubElement(node, "CompressionLevel", str(self.compression_level))
        return node


def _format_value_for_sql(value):
    """Format a Python value for embedding in an S3 Select SQL expression.

    - None -> NULL
    - strings and datetime-like -> quoted ISO strings
    - numpy.datetime64 -> quoted datetime string
    - others -> str(value)
    """
    import datetime as _dt

    if value is None:
        return "NULL"

    if isinstance(value, (_dt.date, _dt.datetime)):
        return f"'{value.isoformat()}'"

    if isinstance(value, str):
        return f"'{value}'"

    return str(value)


def _build_select_query(columns, filters):
    """Build S3 Select SQL query from columns and DNF filters.

    Args:
        columns: List of column names, or None for all
        filters: DNF filter structure [[(col, op, val), ...], ...]

    Returns:
        SQL query string
    """
    # Build SELECT clause
    select_clause = "SELECT " + ", ".join(columns) if columns else "SELECT *"

    # Build WHERE clause from DNF
    where_clause = ""
    if filters:
        dnf_filter, processed_selection = PredicatePushable.to_dnf(filters)
        dnf_filter.extend(processed_selection)

        conditions = []
        for and_group in dnf_filter:
            column, op, value = and_group
            formatted_value = _format_value_for_sql(value)
            conditions.append(f"{column} {op} {formatted_value}")

        where_clause = " WHERE " + " AND ".join(conditions)

    return select_clause + " FROM s3object" + where_clause


def _read_from_s3(client, bucket, object_name, columns, filters):
    from minio.select import ParquetInputSerialization
    from minio.select import SelectRequest

    if columns or filters:
        expression = _build_select_query(columns, filters)
        # DEBUG: print("[S3] SELECT ", expression)

        select_request = SelectRequest(
            # expression="SELECT URL FROM S3Object WHERE RegionID < 6",
            expression=expression,
            input_serialization=ParquetInputSerialization(),
            output_serialization=ParquetOutputSerialization(
                compression_algorithm="zstd",
                write_statistics=False,
                compression_level=4,
            ),
        )

        reader = client.select_object_content(bucket, object_name, select_request)
    else:
        reader = client.get_object(bucket_name=bucket, object_name=object_name)

    payload = b""
    try:
        for chunk in reader.stream():
            payload += chunk
    finally:
        reader.close()

    return payload


class S3File(io.BytesIO):
    """
    File-like wrapper for S3 objects.

    Reads the entire object into memory on open for maximum performance.
    Optionally uses S3 Select to filter/project data.
    """

    def __init__(self, path: str, minio_client, columns=None, filters=None):
        """Initialize S3 file, using S3 Select if columns/filters provided.

        Args:
            path: S3 object path
            minio_client: MinIO client instance
            columns: Optional list of column names to select
            filters: Optional DNF filter structure: [[(col, op, val), ...], ...]
        """
        from opteryx.utils import paths

        bucket, object_path, name, extension = paths.get_parts(path)
        full_object_name = object_path + "/" + name + extension
        stream = None
        try:
            # Implement S3 Select for column/filter pushdown
            if columns or filters:
                # Build and execute S3 Select query
                from adapters.minio.parquet_output_serialization import ParquetOutputSerialization
                from minio.select import ParquetInputSerialization
                from minio.select import SelectRequest

                sql = _build_select_query(columns, filters)
                request = SelectRequest(
                    expression=sql,
                    input_serialization=ParquetInputSerialization(),
                    output_serialization=ParquetOutputSerialization(),
                )
                stream = minio_client.select_object_content(
                    bucket_name=bucket, object_name=full_object_name, request=request
                )
            else:
                stream = minio_client.get_object(bucket_name=bucket, object_name=full_object_name)
            content = stream.read()
            # Initialize BytesIO with the content
            super().__init__(content)
            # Expose memoryview and flag whether filters were applied by the filesystem
            try:
                self.memoryview = memoryview(content)
            except Exception:
                self.memoryview = None
            self.filters_applied = bool(filters)
        finally:
            if stream:
                stream.close()


class OpteryxS3FileSystem:
    """
    Custom S3 filesystem using MinIO client for optimal performance.

    Supports both AWS S3 and MinIO-compatible storage. Provides Arrow-compatible
    filesystem interface via duck typing.
    """

    def __init__(self, bucket=None, region=None, **kwargs):
        self.bucket = bucket
        self.region = region

        try:
            from minio import Minio  # type:ignore
        except ImportError as err:  # pragma: no cover
            raise MissingDependencyError(err.name) from err

        # fmt:off
        end_point = kwargs.get("S3_END_POINT", os.environ.get("MINIO_END_POINT"))
        access_key = kwargs.get("S3_ACCESS_KEY", os.environ.get("MINIO_ACCESS_KEY"))
        secret_key = kwargs.get("S3_SECRET_KEY", os.environ.get("MINIO_SECRET_KEY"))
        secure = kwargs.get("S3_SECURE", str(os.environ.get("MINIO_SECURE", "TRUE")).lower() == "true")
        # fmt:on

        if end_point is None:  # pragma: no cover
            raise UnmetRequirementError(
                "MinIo (S3) adapter requires MINIO_END_POINT, MINIO_ACCESS_KEY and MINIO_SECRET_KEY set in environment variables."
            )

        # Minio v7 uses keyword-only args for construction (endpoint=...).
        try:
            self.minio = Minio(
                end_point, access_key=access_key, secret_key=secret_key, secure=secure
            )
        except TypeError:
            # Fall back to positional args for older Minio versions.
            self.minio = Minio(end_point, access_key, secret_key, secure=secure)

    def get_file_info(self, paths: Union[str, List[str]]):
        """Get info about S3 objects."""
        from pyarrow.fs import FileInfo
        from pyarrow.fs import FileType

        # Handle both single path and list of paths
        single_path = isinstance(paths, str)
        if single_path:
            paths = [paths]

        infos = []
        for path in paths:
            from opteryx.utils import paths as path_utils

            bucket, object_path, name, extension = path_utils.get_parts(path)
            full_object_name = object_path + "/" + name + extension

            try:
                stat = self.minio.stat_object(bucket_name=bucket, object_name=full_object_name)
                info = FileInfo(path=path, type=FileType.File, size=stat.size)
            except:
                info = FileInfo(path=path, type=FileType.NotFound)
            infos.append(info)

        return infos[0] if single_path else infos

    def open_input_stream(self, path: str, columns=None, filters=None):
        """Open an S3 object for reading as a stream.

        Args:
            path: S3 object path
            columns: Optional list of columns to select
            filters: Optional DNF filter structure
        """
        return S3File(path, self.minio, columns=columns, filters=filters)

    def open_input_file(self, path: str, columns=None, filters=None):
        """Open an S3 object for random access reading.

        Args:
            path: S3 object path
            columns: Optional list of columns to select
            filters: Optional DNF filter structure
        """
        return S3File(path, self.minio, columns=columns, filters=filters)

    async def async_read_blob(
        self, *, blob_name, pool, session, telemetry, columns=None, filters=None, **kwargs
    ):
        """
        Asynchronously read an S3 object blob and commit to memory pool.

        Uses a thread pool executor to avoid blocking the event loop on MinIO I/O.
        Supports S3 Select for column projection and filtering.

        Args:
            blob_name: S3 object path (e.g., s3://bucket/key)
            pool: AsyncMemoryPool instance
            session: aiohttp session (unused for S3, but required for interface compatibility)
            telemetry: Telemetry object for tracking metrics
            columns: Optional list of column names to project
            filters: Optional DNF filter structure for S3 Select
            **kwargs: Additional parameters

        Returns:
            Reference to committed memory in pool

        Raises:
            DatasetReadError: If object cannot be read
        """
        import asyncio

        from opteryx.exceptions import DatasetReadError
        from opteryx.utils import paths

        # Strip s3:// prefix
        if blob_name.startswith("s3://"):
            blob_name = blob_name[5:]

        # DEBUG: print("[S3] READ   ", f"s3://{blob_name}")
        bucket, object_path, name, extension = paths.get_parts(blob_name)
        full_object_name = object_path + "/" + name + extension

        # Run blocking MinIO I/O in thread pool to avoid blocking event loop
        loop = asyncio.get_event_loop()
        column_names = [col.source_column for col in columns] if columns else None
        data = await loop.run_in_executor(
            None, _read_from_s3, self.minio, bucket, full_object_name, column_names, filters
        )
        if data is None:
            return

        try:
            import opteryx.rugo.parquet as parquet_meta

            metadata = parquet_meta.read_metadata_from_memoryview(
                memoryview(data), schema_only=True, max_row_groups=1, include_statistics=False
            )
            if metadata["num_rows"] == 0:
                return
        except Exception as e:
            print(f"Error reading Parquet metadata with rugo: {e}")
            import traceback

            traceback.print_exc()

        # Commit to pool with retries
        ref = await pool.commit(data)
        max_retries = 10
        attempts = 0
        while (ref is None or ref == -1) and attempts < max_retries:
            attempts += 1
            telemetry.stalls_io_waiting_on_engine += 1
            telemetry.cpu_wait_seconds += 0.1
            await asyncio.sleep(0.1)
            try:
                ref = await pool.commit(data)
            except Exception as e:
                ref = None

        if ref is None or ref == -1:
            raise DatasetReadError(f"Unable to commit data to MemoryPool after {attempts} attempts")

        telemetry.bytes_read += len(data)
        # Indicate whether the connector applied the selection filters remotely.
        filters_applied = bool(filters)
        return (ref, filters_applied)

    def _build_select_query(self, columns, filters):
        """Build S3 Select SQL query from columns and DNF filters.

        Args:
            columns: List of column names or LogicalColumn objects, or None for all
            filters: DNF filter structure [[(col, op, val), ...], ...]

        Returns:
            SQL query string
        """
        # Build SELECT clause
        if columns:
            # Extract column names from LogicalColumn objects if needed
            col_names = []
            for col in columns:
                # Handle LogicalColumn objects
                if hasattr(col, "schema_column") and hasattr(col.schema_column, "name"):
                    col_names.append(col.schema_column.name)
                elif hasattr(col, "name"):
                    col_names.append(col.name)
                else:
                    col_names.append(str(col))
            select_clause = "SELECT " + ", ".join(f'"{c}"' for c in col_names)
        else:
            select_clause = "SELECT *"

        # Build WHERE clause from DNF
        where_clause = ""
        if filters:
            or_conditions = []
            for and_group in filters:
                and_conditions = []
                for column, op, value in and_group:
                        # Format the value appropriately (dates/strings quoted, None -> NULL, etc.)
                    formatted_value = _format_value_for_sql(value)

                    and_conditions.append(f'"{column}" {op} {formatted_value}')

                or_conditions.append("(" + " AND ".join(and_conditions) + ")")

            where_clause = " WHERE " + " OR ".join(or_conditions)

        return select_clause + " FROM s3object" + where_clause
