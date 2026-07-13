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
from opteryx.types.logical_type import LogicalCategory
from opteryx.types.schema import RelationSchema

OS_SEP = os.sep
PARQUET_SUFFIX = ".parquet"
STATS_SIDECAR_SUFFIX = ".stats.json"
STATS_SCHEMA_VERSION = 1

# Process-global manifest cache: dataset -> (signature, schema, manifest).
# The gateway connector is recreated per query, so the built manifest (list +
# stat + per-file footer-stats parse, ~5ms on a 99-file dataset) would otherwise
# be rebuilt every time. Keyed on a (name, size, mtime) file-set signature, so
# any add/remove/resize/rewrite changes the signature and the entry is rebuilt —
# a hit provably describes the current dataset, never a stale read. Bounded by
# entry count (manifests can be large); FIFO eviction — the working set of
# distinct datasets in a session is small.
_MANIFEST_CACHE: dict = {}
_MANIFEST_CACHE_MAX = 128


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
        "IsNull": True,
        "IsNotNull": True,
        "IsEmpty": True,
        "IsNotEmpty": True,
    }

    PUSHABLE_TYPES = {
        LogicalCategory.VARBINARY,
        LogicalCategory.BOOLEAN,
        LogicalCategory.FLOAT,
        LogicalCategory.INTEGER,
        LogicalCategory.VARCHAR,
        LogicalCategory.TIMESTAMP,
        LogicalCategory.DATE,
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
        from opteryx.connectors._rugo_schema import rugo_to_relation_schema

        try:
            from rugo.parquet import (
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
                rugo_metadata = read_metadata_from_memoryview(mv)
                schema = rugo_to_relation_schema(rugo_metadata, schema_name=blob_name)
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

        # Parquet files in the dataset, with size + mtime from a single stat pass.
        blob_names = self.get_list_of_blob_names(self.dataset)
        parquet_names = [b for b in blob_names if b.lower().endswith(PARQUET_SUFFIX)]
        infos = self.filesystem.get_file_info(parquet_names)
        sizes = {i.path: (getattr(i, "size", 0) or 0) for i in infos}

        # File-set signature: any add/remove/resize/rewrite changes it, so a
        # cache hit provably describes the current dataset (no stale reads).
        signature = tuple(
            (i.path, getattr(i, "size", 0) or 0, getattr(i, "mtime", 0.0) or 0.0)
            for i in sorted(infos, key=lambda x: x.path)
        )
        # Schema is recomputed fresh every query (~0.5ms): downstream projection
        # pushdown prunes columns on the returned schema, so a cached/shared
        # schema would surface a ColumnNotFoundError on the next query. Only the
        # expensive-to-build file entries (per-file footer-stats parse) are cached.
        schema = self.get_dataset_schema()

        cached = _MANIFEST_CACHE.get(self.dataset)
        if cached is not None and cached[0] == signature:
            # Fresh Manifest over a COPY of the cached file list — optimizer
            # strategies reassign manifest.files (prune, limit, statistics-only
            # COUNT(*) sets it to []), so the cached list is never handed out raw.
            return schema, Manifest(list(cached[1]), schema)

        # Miss (or first build): build the manifest from file metadata.
        # Build FileEntry objects from file metadata
        file_entries = []
        for blob_name in parquet_names:
            try:
                file_format = "PARQUET"
                record_count = 0
                file_size = sizes.get(blob_name, 0)

                column_stats = None

                try:
                    from opteryx.connectors.parquet_io.pool_reader import fetch_column_stats

                    record_count, footer_size, column_stats = fetch_column_stats(
                        blob_name, file_size=file_size or -1
                    )
                    if file_size == 0:
                        file_size = footer_size

                    column_stats.bind_schema([col.name for col in schema.columns])
                except Exception:
                    record_count = 0
                    column_stats = None

                min_k_hashes = self._load_sidecar_min_k_hashes(blob_name, schema)

                entry = FileEntry(
                    file_path=blob_name,
                    file_format=file_format,
                    record_count=record_count,
                    file_size_in_bytes=file_size,
                    column_stats=column_stats,
                    min_k_hashes=min_k_hashes,
                )
                file_entries.append(entry)

            except (OSError, ValueError, RuntimeError):
                # Skip files we can't read metadata from
                continue

        # Cache an INDEPENDENT copy of the file list (the returned manifest below
        # may be mutated by the optimizer); hand the caller its own Manifest.
        if self.dataset not in _MANIFEST_CACHE and len(_MANIFEST_CACHE) >= _MANIFEST_CACHE_MAX:
            # FIFO evict the oldest entry to bound memory.
            _MANIFEST_CACHE.pop(next(iter(_MANIFEST_CACHE)), None)
        _MANIFEST_CACHE[self.dataset] = (signature, list(file_entries))
        return schema, Manifest(file_entries, schema)


    def _load_sidecar_min_k_hashes(self, blob_name: str, schema: RelationSchema):
        """Load the optional ``<blob_name>.stats.json`` sidecar.

        Returns a positional list aligned with ``schema.columns`` of K-min hash
        lists, or None if the sidecar is missing, malformed, or its embedded
        field-id mapping disagrees with the schema (stale stats).
        """
        sidecar_path = blob_name + STATS_SIDECAR_SUFFIX

        # Slurp the sidecar via the same filesystem the connector uses.
        try:
            stream = self.filesystem.open_input_stream(sidecar_path)
        except Exception:
            return None
        try:
            try:
                payload = bytes(stream.memoryview)
            except AttributeError:
                payload = stream.read()
        except Exception:
            return None
        finally:
            try:
                stream.close()
            except Exception:
                pass

        import json

        try:
            data = json.loads(payload)
        except (ValueError, TypeError):
            return None

        if not isinstance(data, dict):
            return None
        if data.get("schema_version") != STATS_SCHEMA_VERSION:
            return None

        sidecar_field_ids = data.get("field_ids")
        sidecar_hashes = data.get("min_k_hashes")
        if not isinstance(sidecar_field_ids, dict) or not isinstance(sidecar_hashes, dict):
            return None

        expected_field_ids = {col.name: i for i, col in enumerate(schema.columns)}
        # Sidecar must agree with the schema's positional field ids exactly.
        if sidecar_field_ids != expected_field_ids:
            import sys

            print(
                f"[opteryx] discarding stale stats sidecar: {sidecar_path} "
                f"(field_id mapping disagrees with current schema)",
                file=sys.stderr,
            )
            return None

        num_columns = len(schema.columns)
        positional: list = [None] * num_columns
        for fid_str, hashes in sidecar_hashes.items():
            try:
                fid = int(fid_str)
            except (TypeError, ValueError):
                return None
            if fid < 0 or fid >= num_columns:
                return None
            if not isinstance(hashes, list):
                return None
            positional[fid] = [int(h) for h in hashes]

        if not any(h is not None for h in positional):
            return None

        return positional


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
