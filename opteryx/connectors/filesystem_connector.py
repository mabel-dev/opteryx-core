"""
Generic filesystem connector using Arrow FileSystem interface.

This provides a gateway connector (FileSystemConnector) and transient table reader
(FileSystemTable) following the same pattern as OpteryxConnector/OpteryxTable.
"""

import os
from threading import Lock
from typing import Dict, Generator, Optional, Tuple

from opteryx.connectors import TableType
from opteryx.connectors.base.base_connector import BaseConnector, BaseTable
from opteryx.connectors.capabilities import LimitPushable, PredicatePushable
from opteryx.exceptions import (
    DataError,
    DatasetNotFoundError,
    EmptyDatasetError,
    UnsupportedSyntaxError,
)
from opteryx.tracing import record_event
from opteryx.types import OrsoTypes
from opteryx.types.schema import RelationSchema

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
        "Like": True,
        "NotLike": True,
        "ILike": True,
        "NotILike": True,
        "InStr": True,
        "NotInStr": True,
        "IInStr": True,
        "NotIInStr": True,
        "InList": True,
        "NotInList": True,
        "RLike": True,
        "NotRLike": True,
        "Between": True,
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

    def get_list_of_blob_names(self, prefix: str, predicates=None):
        """
        Get list of blob names (file paths) matching the prefix.

        Args:
            prefix: Directory/path prefix to list files from
            predicates: Optional predicates (not used for file listing)

        Returns:
            List of file paths
        """
        return self.filesystem.list_files(prefix, recursive=True)

    def read_blob(self, *, blob_name: str, just_schema=False, projection=None, selection=None):
        """
        Read a single blob using the filesystem.

        FileSystemConnector is a legacy path that is no longer used by the query executor.
        All parquet scans are routed to ParquetReadNode via the physical planner.

        For schema-only reads, this uses rugo to extract Parquet metadata without
        loading data.

        Args:
            blob_name: Path to the blob
            just_schema: If True, only return schema
            projection: Columns to project
            selection: Predicates to push down

        Returns:
            RelationSchema if just_schema=True

        Raises:
            UnsupportedSyntaxError: For data reads
        """
        if not just_schema:
            raise UnsupportedSyntaxError(
                "All Parquet scans use ParquetReadNode. FileSystemConnector data reads are not supported."
            )

        # Schema-only read using rugo metadata extraction
        try:
            from rugo.converters.orso import (
                rugo_to_orso_schema,  # type: ignore[import]
            )
            from rugo.parquet_reader import (
                read_metadata_from_memoryview,  # type: ignore[import]
            )
        except ImportError as e:
            raise RuntimeError(
                "rugo is required for schema-only reads but not available. "
                "Ensure rugo is compiled and in the Python path."
            ) from e

        try:
            # Open the file and extract metadata from memoryview
            stream = self.filesystem.open_input_stream(blob_name)
            try:
                mv = stream.memoryview
                rugo_metadata = read_metadata_from_memoryview(mv, schema_only=True)
                schema = rugo_to_orso_schema(rugo_metadata, schema_name=blob_name)
                return schema
            finally:
                stream.close()
        except Exception as e:
            if isinstance(e, UnsupportedSyntaxError):
                raise
            raise DataError(
                f"Unable to read Parquet metadata from {blob_name}: {type(e).__name__}: {e}"
            ) from e

    def read_dataset(
        self,
        columns: list = None,
        predicates: list = None,
        just_schema: bool = False,
        **kwargs,
    ) -> Generator[RelationSchema, None, None]:
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
                        just_schema=True,
                    )
                    blob_count = len(blob_names)
                    if schema.row_count_metric and blob_count > 1:
                        schema.row_count_estimate = schema.row_count_metric * blob_count
                        schema.row_count_metric = None
                        self.telemetry.estimated_row_count += schema.row_count_estimate
                    yield schema
                except Exception as err:
                    if "Invalid" in type(err).__name__ or "Arrow" in type(err).__name__:
                        with self._stats_lock:
                            self.telemetry.unreadable_data_blobs += 1
                        continue
                    raise DataError(
                        f"Unable to read file {blob_name}: {type(err).__name__}"
                    ) from err
            return

        raise UnsupportedSyntaxError(
            "All Parquet scans use ParquetReadNode. FileSystemConnector data reads are not supported."
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
                file_format = "PARQUET"
                record_count = 0
                file_size = 0

                try:
                    file_info = self.filesystem.get_file_info(blob_name)
                    file_size = getattr(file_info, "size", 0) or 0
                except Exception:
                    pass

                min_values = None
                max_values = None

                try:
                    from opteryx.connectors.parquet_io.reader import fetch_footer

                    meta = fetch_footer(self.filesystem, blob_name, file_size=file_size or None)
                    record_count = sum(rg.get("num_rows", 0) for rg in meta.get("row_groups", []))
                    if file_size == 0:
                        file_size = meta.get("__footer_bytes__", 0)

                    # Extract min/max statistics from row groups
                    min_values, max_values = self._extract_column_stats(meta, schema)
                except Exception:
                    record_count = 0

                entry = FileEntry(
                    file_path=blob_name,
                    file_format=file_format,
                    record_count=record_count,
                    file_size_in_bytes=file_size,
                    min_values=min_values,
                    max_values=max_values,
                )
                file_entries.append(entry)

            except (OSError, ValueError, RuntimeError):
                # Skip files we can't read metadata from
                continue

        # Create and return manifest
        manifest = Manifest(file_entries, schema)
        return schema, manifest

    def _extract_column_stats(self, footer_meta: dict, schema: RelationSchema) -> tuple:
        """
        Extract min/max column statistics from parquet footer metadata.

        Returns tuple of (min_values, max_values) lists indexed by field position,
        with values aggregated across all row groups.
        """
        if not footer_meta or not footer_meta.get("row_groups"):
            return None, None

        # Initialize min/max lists by column count
        num_columns = len(schema.columns)
        min_values = [None] * num_columns
        max_values = [None] * num_columns

        col_name_to_field_id = {col.name: i for i, col in enumerate(schema.columns)}

        # Aggregate statistics across all row groups
        for rg in footer_meta.get("row_groups", []):
            for col_meta in rg.get("columns", []):
                field_id = col_name_to_field_id.get(col_meta.get("name", ""))
                if field_id is None:
                    continue

                # Extract min/max from column metadata
                min_val = col_meta.get("min")
                max_val = col_meta.get("max")

                if min_val is not None:
                    if min_values[field_id] is None:
                        min_values[field_id] = min_val
                    else:
                        try:
                            if min_val < min_values[field_id]:
                                min_values[field_id] = min_val
                        except TypeError:
                            # Skip comparison if types don't match
                            pass

                if max_val is not None:
                    if max_values[field_id] is None:
                        max_values[field_id] = max_val
                    else:
                        try:
                            if max_val > max_values[field_id]:
                                max_values[field_id] = max_val
                        except TypeError:
                            # Skip comparison if types don't match
                            pass

        # Return None if no stats were found
        return (min_values if any(v is not None for v in min_values) else None,
                max_values if any(v is not None for v in max_values) else None)


class FileSystemConnector(BaseConnector):
    """
    Gateway connector for filesystem-based datasets.

    Long-lived connector cached by storage configuration. Creates transient
    FileSystemTable instances for each dataset query.

    Works with:
    - OpteryxLocalFileSystem (local storage)
    - OpteryxGcsFileSystem (Google Cloud Storage)
    - Any other Opteryx filesystem-compatible backend

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
