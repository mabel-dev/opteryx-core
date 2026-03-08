"""
Generic filesystem connector using Arrow FileSystem interface.

This provides a gateway connector (FileSystemConnector) and transient table reader
(FileSystemTable) following the same pattern as OpteryxConnector/OpteryxTable.
"""

import os
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from threading import Lock
from typing import Dict
from typing import Optional
from typing import Tuple

import pyarrow
from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx.connectors import TableType
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.base.base_connector import BaseTable
from opteryx.connectors.capabilities import LimitPushable
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.draken.morsels.morsel import Morsel
from opteryx.exceptions import DataError
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import EmptyDatasetError
from opteryx.tracing import record_event
from opteryx.utils.parquet_decoder import parquet_decoder

OS_SEP = os.sep
PARQUET_SUFFIX = ".parquet"


class FileSystemTable(BaseTable, PredicatePushable, LimitPushable):
    """
    Transient table reader for filesystem-based datasets.

    Created per query to read a specific dataset. Holds reference to parent
    connector's filesystem for optimized I/O.
    """

    __mode__ = "Blob"
    __synchronousity__ = "synchronous"

    # Capability declarations
    supports_predicate_pushdown = True
    supports_limit_pushdown = True
    supports_async = True

    PUSHABLE_OPS: Dict[str, bool] = {
        "Eq": True,
        "NotEq": True,
        "Gt": True,
        "GtEq": True,
        "Lt": True,
        "LtEq": True,
    }

    PUSHABLE_TYPES = {
        OrsoTypes.BLOB,
        OrsoTypes.BOOLEAN,
        OrsoTypes.DOUBLE,
        OrsoTypes.INTEGER,
        OrsoTypes.VARCHAR,
        OrsoTypes.TIMESTAMP,
        OrsoTypes.DATE,
    }

    _executor = None  # Lazy initialization
    _max_workers = 8

    def __init__(self, dataset: str, filesystem, storage_type: str, **kwargs):
        """
        Initialize the table reader for a specific dataset.

        Args:
            dataset: The dataset name/path
            filesystem: Reference to the filesystem from parent connector
            storage_type: Type identifier for telemetry (LOCAL, GCS, S3, etc.)
            **kwargs: Additional parameters passed to BaseTable
        """
        BaseTable.__init__(self, dataset=dataset, **kwargs)
        PredicatePushable.__init__(self, **kwargs)
        LimitPushable.__init__(self, **kwargs)

        self.filesystem = filesystem
        self.__type__ = storage_type

        # Initialize counters for telemetry
        self.rows_seen = 0
        self.blobs_seen = 0

        # Normalize dataset path
        if self.dataset and OS_SEP not in self.dataset and "/" not in self.dataset:
            self.dataset = self.dataset.replace(".", OS_SEP)

        self._stats_lock = Lock()

    def get_executor(self):
        """Get or create the thread pool executor."""
        if FileSystemTable._executor is None:
            FileSystemTable._executor = ThreadPoolExecutor(
                max_workers=self._max_workers, thread_name_prefix="opteryx-io-"
            )
        return FileSystemTable._executor

    def get_list_of_blob_names(self, prefix: str, predicates=None):
        """
        Get list of blob names (file paths) matching the prefix.

        Args:
            prefix: Directory/path prefix to list files from
            predicates: Optional predicates (not used for file listing)

        Returns:
            List of file paths
        """
        from pyarrow.fs import FileSelector

        # Create file selector to list files recursively
        selector = FileSelector(prefix, recursive=True)
        file_infos = self.filesystem.get_file_info(selector)

        # Extract paths from FileInfo objects
        return [info.path for info in file_infos]

    def read_blob(
        self, *, blob_name: str, decoder, just_schema=False, projection=None, selection=None
    ):
        """
        Read a single blob using the filesystem.

        Args:
            blob_name: Path to the blob
            decoder: Decoder function for the file format
            just_schema: If True, only return schema
            projection: Columns to project
            selection: Predicates to push down

        Returns:
            Decoded data or schema
        """
        from opteryx import config as _config

        _tracing = _config.OPTERYX_TRACE
        if _tracing:
            record_event("download_start", file_id=blob_name, connector=self.__type__)
        # Open file through the filesystem, passing through projection/selection for remote filtering.
        # Ensure the stream is deterministically closed so memory-mapped handles don't accumulate.
        data = self.filesystem.open_input_file(blob_name, columns=projection, filters=selection)
        try:
            if _tracing:
                record_event(
                    "download_complete",
                    file_id=blob_name,
                    bytes_received=data.memoryview.nbytes,
                    connector=self.__type__,
                )
            self.telemetry.bytes_read += data.memoryview.nbytes

            # If the underlying filesystem already applied the selection (e.g., S3 Select),
            # don't pass the selection to the decoder again to avoid duplicate filtering.
            selection_to_pass = None if getattr(data, "filters_applied", False) else selection

            if _tracing:
                record_event("decode_start", file_id=blob_name, connector=self.__type__)
            result = decoder(
                data.memoryview,
                projection=projection,
                selection=selection_to_pass,
                just_schema=just_schema,
            )
            if _tracing:
                record_event("decode_complete", file_id=blob_name, connector=self.__type__)

            return result
        finally:
            close = getattr(data, "close", None)
            if callable(close):
                close()

    def read_dataset(
        self,
        columns: list = None,
        predicates: list = None,
        just_schema: bool = False,
        **kwargs,
    ) -> Morsel:
        """
        Read the entire dataset from the filesystem.

        Args:
            columns: Columns to project
            predicates: Predicates to push down
            just_schema: If True, only return schema

        Yields:
            Morsel or schemas
        """
        blob_names = self.get_list_of_blob_names(prefix=self.dataset, predicates=predicates or [])
        blob_names = [name for name in blob_names if name.lower().endswith(PARQUET_SUFFIX)]

        from opteryx import config as _config

        if _config.OPTERYX_TRACE:
            # include storage type/connector so traces can be filtered by backend
            record_event(
                "dataset_discovered",
                dataset=self.dataset,
                file_count=len(blob_names),
                connector=self.__type__,
            )
            for blob_name in blob_names:
                record_event(
                    "file_discovered",
                    file_id=blob_name,
                    blob_name=blob_name,
                    connector=self.__type__,
                )

        if just_schema:
            for blob_name in blob_names:
                try:
                    schema = self.read_blob(
                        blob_name=blob_name,
                        decoder=parquet_decoder,
                        just_schema=True,
                    )
                    blob_count = len(blob_names)
                    if schema.row_count_metric and blob_count > 1:
                        schema.row_count_estimate = schema.row_count_metric * blob_count
                        schema.row_count_metric = None
                        self.telemetry.estimated_row_count += schema.row_count_estimate
                    yield schema
                except pyarrow.ArrowInvalid:
                    with self._stats_lock:
                        self.telemetry.unreadable_data_blobs += 1
                except Exception as err:
                    raise DataError(
                        f"Unable to read file {blob_name}: {type(err).__name__}"
                    ) from err
            return

        def process_result(num_rows, raw_size, decoded):
            self.telemetry.rows_seen += num_rows
            self.rows_seen += num_rows
            self.blobs_seen += 1
            self.telemetry.bytes_raw += raw_size
            return decoded

        max_workers = min(self._max_workers, len(blob_names)) or 1

        if max_workers <= 1:
            # Single-threaded path
            for blob_name in blob_names:
                try:
                    num_rows, _, raw_size, decoded = self._read_blob_task(
                        blob_name,
                        columns,
                        predicates,
                    )
                except pyarrow.ArrowInvalid:
                    with self._stats_lock:
                        self.telemetry.unreadable_data_blobs += 1
                    continue
                except Exception as err:
                    raise DataError(
                        f"Unable to read file {blob_name}: {type(err).__name__}"
                    ) from err

                decoded = process_result(num_rows, raw_size, decoded)
                yield Morsel.from_arrow(decoded)
        else:
            # Multi-threaded path
            blob_iter = iter(blob_names)
            pending = {}

            executor = self.get_executor()
            for _ in range(max_workers):
                try:
                    blob_name = next(blob_iter)
                except StopIteration:
                    break
                future = executor.submit(
                    self._read_blob_task,
                    blob_name,
                    columns,
                    predicates,
                )
                pending[future] = blob_name

            while pending:
                done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
                for future in done:
                    blob_name = pending.pop(future)
                    try:
                        num_rows, _, raw_size, decoded = future.result()
                    except pyarrow.ArrowInvalid:
                        with self._stats_lock:
                            self.telemetry.unreadable_data_blobs += 1
                    except Exception as err:
                        for remaining_future in list(pending):
                            remaining_future.cancel()
                        raise DataError(
                            f"Unable to read file {blob_name}: {type(err).__name__}"
                        ) from err
                    else:
                        decoded = process_result(num_rows, raw_size, decoded)
                        yield Morsel.from_arrow(decoded)

                    try:
                        next_blob = next(blob_iter)
                    except StopIteration:
                        continue
                    future = executor.submit(
                        self._read_blob_task,
                        next_blob,
                        columns,
                        predicates,
                    )
                    pending[future] = next_blob

    def _read_blob_task(self, blob_name: str, columns, predicates):
        """Helper for reading a blob in a thread pool."""
        return self.read_blob(
            blob_name=blob_name,
            decoder=parquet_decoder,
            just_schema=False,
            projection=columns,
            selection=predicates,
        )

    def get_dataset_schema(self) -> RelationSchema:
        """
        Retrieve the schema of the dataset.

        Returns:
            The schema of the dataset.
        """
        if self.schema:
            return self.schema

        for schema in self.read_dataset(just_schema=True):
            self.schema = schema
            break

        if self.schema is None:
            if os.path.isdir(self.dataset):
                raise EmptyDatasetError(dataset=self.dataset.replace(OS_SEP, "."))
            raise DatasetNotFoundError(dataset=self.dataset, connector=self.__type__)

        return self.schema

    def get_dataset_metadata(self) -> Tuple[RelationSchema, "Manifest"]:
        """
        Get dataset schema and build manifest from file metadata.

        Returns both schema and manifest to enable statistics-based optimizations.
        Manifest contains file-level statistics (record counts, bounds, etc.)
        extracted from file metadata without reading data.

        Returns:
            Tuple of (RelationSchema, Manifest)
        """
        from opteryx.models.file_entry import FileEntry
        from opteryx.models.manifest import Manifest

        # Get the schema first
        schema = self.get_dataset_schema()

        # Get list of files in the dataset
        blob_names = self.get_list_of_blob_names(self.dataset)

        # Build FileEntry objects from file metadata
        file_entries = []
        for blob_name in blob_names:
            # Only Parquet files are supported in external scans.
            if not blob_name.lower().endswith(PARQUET_SUFFIX):
                continue

            try:
                # Open the file and read just the metadata. Ensure stream closes promptly.
                file_stream = self.filesystem.open_input_file(blob_name)
                try:
                    # Determine file format
                    file_format = "PARQUET"

                    # Extract record count from Parquet metadata without reading row data.
                    record_count = 0
                    file_size = file_stream.size() if hasattr(file_stream, "size") else 0

                    try:
                        import pyarrow.parquet as pq

                        parquet_file = pq.ParquetFile(file_stream)
                        record_count = parquet_file.metadata.num_rows
                    except (OSError, ValueError, RuntimeError) as ex:
                        # Fallback: set to 0 if we can't read metadata
                        _ = ex
                        record_count = 0

                    # Create FileEntry
                    entry = FileEntry(
                        file_path=blob_name,
                        file_format=file_format,
                        record_count=record_count,
                        file_size_in_bytes=file_size,
                    )
                    file_entries.append(entry)
                finally:
                    close = getattr(file_stream, "close", None)
                    if callable(close):
                        close()

            except (OSError, ValueError, RuntimeError):
                # Skip files we can't read metadata from
                continue

        # Create and return manifest
        manifest = Manifest(file_entries, schema)
        return schema, manifest


class FileSystemConnector(BaseConnector):
    """
    Gateway connector for filesystem-based datasets.

    Long-lived connector cached by storage configuration. Creates transient
    FileSystemTable instances for each dataset query.

    Works with:
    - OpteryxLocalFileSystem (local storage)
    - OpteryxGcsFileSystem (Google Cloud Storage)
    - OpteryxS3FileSystem (S3/MinIO)
    - Any other pyarrow-compatible filesystem

    Note: Filesystems only support tables, not views.
    """

    __mode__ = "Blob"

    # Declare capabilities of FileSystemTable readers
    supports_predicate_pushdown = True
    supports_limit_pushdown = True

    def __init__(self, filesystem, storage_type="FILESYSTEM", **kwargs):
        """
        Initialize the filesystem gateway connector.

        Args:
            filesystem: A filesystem instance (e.g., OpteryxLocalFileSystem)
            storage_type: Type identifier for telemetry (LOCAL, GCS, S3, etc.)
            **kwargs: Additional configuration parameters (ignored for gateway)
        """
        self.filesystem = filesystem
        self.storage_type = storage_type
        self.__type__ = storage_type

    def locate_object(self, name: str) -> Tuple[Optional[TableType], any]:
        """
        Determine if a name refers to a table (always tables for filesystems).

        Args:
            name: Dataset name

        Returns:
            (TableType.Table, None) - filesystems only support tables
        """
        return (TableType.Table, None)

    def table_engine(self, name: str, **kwargs):
        """
        Create a transient table reader for the specified dataset.

        Args:
            name: Dataset name/path
            **kwargs: Additional parameters (telemetry, etc.)

        Returns:
            FileSystemTable instance configured to read the dataset
        """
        # Extract telemetry from kwargs, default to None if not provided
        telemetry = kwargs.pop("telemetry", None)

        return FileSystemTable(
            dataset=name,
            filesystem=self.filesystem,
            storage_type=self.storage_type,
            telemetry=telemetry,
            **kwargs,
        )
