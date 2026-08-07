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
from opteryx.models.dataset_format import JSONL
from opteryx.models.dataset_format import PARQUET
from opteryx.models.dataset_format import SKENE
from opteryx.models.dataset_format import dataset_format
from opteryx.models.dataset_format import format_for_path
from opteryx.types.logical_type import LogicalCategory
from opteryx.types.schema import RelationSchema

OS_SEP = os.sep

# Process-global manifest cache: dataset -> (signature, file_entries, min_k,
# histogram, bounds_are_ordinal, char_class).
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

    # Capability declarations. limit-pushdown and can_push are re-gated per
    # instance once the dataset's format is discovered (get_dataset_metadata):
    # only formats whose reader honors a pushed limit/predicate may accept one.
    supports_predicate_pushdown = True
    supports_limit_pushdown = True
    supports_async = True

    # Until discovery runs, assume parquet (the discovery default for an empty
    # dataset). get_dataset_metadata overwrites this per instance.
    dataset_file_format = PARQUET

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

    def can_push(self, operator, types: set = None) -> bool:
        """Format-aware predicate gate.

        Parquet and skene take PredicatePushable's generic gate against this
        class's PUSHABLE_OPS/TYPES — both readers apply accepted predicates
        exactly, with the same engine filter kernel (skene:
        SkeneReadNode.read_morsels; parquet: the pass1/pass2 gate), and both
        feed manifest file pruning. JSONL delegates to JsonlPredicatePushable —
        the same (deliberately narrower) gate READ_JSONL uses, so a dataset
        scan and READ_JSONL over the same files push identically. Anything
        else declines: a declined predicate stays behind as a Filter node — a
        missed optimization, never a dropped predicate.
        """
        if self.dataset_file_format in (PARQUET, SKENE):
            return PredicatePushable.can_push(self, operator, types)
        if self.dataset_file_format == JSONL:
            from opteryx.connectors.jsonl_io import JsonlPredicatePushable

            return JsonlPredicatePushable.can_push(JsonlPredicatePushable(), operator, types)
        return False

    def get_list_of_blob_names(self, prefix: str, predicates=None):
        """
        Get list of blob names (file paths) matching the prefix.

        Excludes the dataset manifest. It is itself a `.parquet` sitting in the
        tree it describes, and every discovery path here filters on that suffix —
        so without this it would be read back as a DATA file: its columns would
        become the dataset's schema, and its rows would be returned as results.
        This is the single funnel all discovery goes through (schema reads, the
        executor's file list, ANALYZE), so the exclusion lives here rather than
        being re-derived — and missed — at each call site.

        Args:
            prefix: Directory/path prefix to list files from
            predicates: Optional predicates (not used for file listing)

        Returns:
            List of file paths, manifest excluded
        """
        from opteryx.models.manifest_io import is_dataset_manifest

        return [
            name
            for name in self.filesystem.list_files(prefix, recursive=True)
            if not is_dataset_manifest(name)
        ]

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

    def _read_skene_schema(self, blob_name: str) -> RelationSchema:
        """Schema for a skene dataset, read exactly from the first file's footer."""
        from opteryx.connectors.skene_io import skene_metadata_to_schema
        from skene import SkeneError
        from skene import read_metadata as _skene_read_metadata

        file_obj = self.filesystem.open_input_file(blob_name)
        try:
            metadata = _skene_read_metadata(file_obj.memoryview)
        except SkeneError as err:
            raise DataError(f"Cannot read skene file '{blob_name}': {err}") from err
        finally:
            file_obj.close()
        return skene_metadata_to_schema(metadata, self.dataset)

    def _infer_jsonl_schema(self, blob_name: str) -> RelationSchema:
        """Schema for a JSONL dataset, inferred by decoding its first file.

        JSONL has no cheap metadata path (rugo.jsonl.read_metadata fully
        decodes too), so this pays one file's decode at bind time — the same
        cost READ_JSONL's binder branch pays for the same reason.
        """
        from opteryx.connectors.jsonl_io import JSONL_SUPPORTED_TYPES
        from opteryx.types.logical_type import column_type_from_vector
        from opteryx.types.schema import SchemaColumn
        from opteryx.types.schema import mint_column_identity
        from rugo.jsonl import read_jsonl as _rugo_read_jsonl

        file_obj = self.filesystem.open_input_file(blob_name)
        try:
            with _rugo_read_jsonl(file_obj.memoryview) as reader:
                sample_morsel = next(iter(reader))
        except RuntimeError as err:
            raise DataError(f"Cannot read JSONL file '{blob_name}': {err}") from err
        finally:
            file_obj.close()

        schema_columns = []
        for raw_name in sample_morsel.column_names:
            name = raw_name.decode("utf-8") if isinstance(raw_name, bytes) else raw_name
            vector = sample_morsel.column(name.encode("utf-8"))
            if vector.type not in JSONL_SUPPORTED_TYPES:
                raise DataError(
                    f"JSONL dataset {self.dataset}: column '{name}' has inferred type "
                    f"{vector.type!r}, which the JSONL reader does not support."
                )
            schema_columns.append(
                SchemaColumn(
                    name=name,
                    column_type=column_type_from_vector(vector),
                    identity=mint_column_identity(self.dataset, name),
                )
            )
        return RelationSchema(name=self.dataset, columns=schema_columns)

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
        # Single-format discovery: raises on a mixed listing, never drops files.
        dataset_fmt = dataset_format(blob_names, self.dataset) or PARQUET
        blob_names = [name for name in blob_names if format_for_path(name) == dataset_fmt]

        if just_schema and dataset_fmt == SKENE and blob_names:
            # Skene's footer IS the schema — exact DrakenType + LogicalType per
            # column, no inference and no translation loss. Read from the first
            # file; every file is validated against it at read time
            # (SkeneReadNode's per-file name/type checks).
            yield self._read_skene_schema(blob_names[0])
            return

        if just_schema and dataset_fmt == JSONL and blob_names:
            # JSONL carries no footer: the schema is inferred from the FIRST
            # file (architect decision 2026-08-07 — catalog-declared schema when
            # one exists, first-file inference as the fallback; filesystem
            # datasets have no declared schema, so this is the fallback path).
            # Every other file is validated against it at read time by
            # JsonlReadNode's per-file/per-chunk fail-loud checks.
            yield self._infer_jsonl_schema(blob_names[0])
            return

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
        from opteryx.models.manifest_io import DATASET_MANIFEST_NAME

        # Data files. get_list_of_blob_names already excludes the dataset
        # manifest, so this is data only; the manifest is addressed by its known
        # path instead of being fished back out of the listing.
        # Format is discovered from the listing (datasets are single-format —
        # dataset_format raises on a mixed listing rather than dropping files);
        # an empty listing is an empty relation and defaults to PARQUET.
        blob_names = self.get_list_of_blob_names(self.dataset)
        dataset_fmt = dataset_format(blob_names, self.dataset) or PARQUET
        data_names = [b for b in blob_names if format_for_path(b) == dataset_fmt]
        # Bind-time capability gating (the optimizer runs after this): pushdown
        # a reader cannot honor must be DECLINED here, because a pushed limit or
        # predicate is REMOVED from the plan — accepting one the reader ignores
        # silently returns wrong answers. JsonlReadNode honors predicates
        # (rugo tuples, gated by can_push below) but not limits.
        self.dataset_file_format = dataset_fmt
        self.supports_limit_pushdown = dataset_fmt == PARQUET
        manifest_path = os.path.join(self.dataset, DATASET_MANIFEST_NAME)
        # Stat the manifest alongside the data: ANALYZE rewrites only the manifest,
        # so a data-only signature would serve stale sketches from cache forever.
        infos = self.filesystem.get_file_info(data_names + [manifest_path])
        infos = [i for i in infos if (getattr(i, "size", None) is not None)]
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
            # The sketch vectors are immutable and shared (kernels only read them).
            return schema, Manifest(
                list(cached[1]),
                schema,
                min_k_vector=cached[2],
                histogram_vector=cached[3],
                bounds_are_ordinal=cached[4],
                char_class_vector=cached[5],
            )

        # ANALYZE's per-dataset manifest, when it describes exactly this file set.
        # Order matters: the sketch vectors' rows are positional to the manifest's
        # rows, so file_entries must be built in that same order to stay aligned.
        ordered_names, min_k_vector, histogram_vector, char_class_vector, manifest_bounds = (
            self._read_dataset_manifest(manifest_path, data_names)
        )
        # manifest_bounds' lower/upper bounds (when present) are ANALYZE's
        # Vector.ordinalize() ordinal keys, not real values — this Manifest's
        # bounds_are_ordinal flag must travel with them so prune_files knows to
        # ordinalize predicate literals before comparing (see Manifest.__init__).
        bounds_are_ordinal = bool(manifest_bounds)

        # Miss (or first build): build the manifest from file metadata.
        #
        # ONE batched acquisition for the whole file set, not a fetch per file. A
        # gs:// dataset (this connector backs `gcs_connector()`) REQUIRES it: the
        # C++ footer fetches carry no Authorization header, so each path has to be
        # rewritten to a signed URL before it reaches C++ while the caches stay
        # keyed by the original path. fetch_column_stats_many owns that split, and
        # fetches the whole set concurrently with the GIL released instead of one
        # serial, GIL-held round trip (plus a signing round trip) per file.
        # Footer statistics per format. Formats without a footer (JSONL) take
        # the stats-absent path below (record_count=None — UNKNOWN, never 0).
        stats_by_name: Dict[str, tuple] = {}
        if dataset_fmt == SKENE:
            # Skene's footer carries an exact row_count and per-column min/max
            # ORDINALS (draken ordinalize dialect — format.h ColumnStatistics:
            # "the same dialect the catalog manifest speaks").
            #
            # row_count: without it the join ordering optimizer is blind —
            # measured on TPC-H Q9, the unordered plan put a 6M-row table on a
            # join BUILD side and ran ~10x slower than the ordered plan.
            #
            # bounds: feed the SAME manifest slots ANALYZE's ordinal bounds use
            # (FileEntry.lower/upper_bounds + bounds_are_ordinal=True), so the
            # optimizer's manifest pruning drops provably-excluded files with
            # no skene-specific pruning code. Bounds are keyed by the column's
            # SCHEMA position — resolved by NAME from each file's own footer,
            # never by footer position, so a file whose column order diverges
            # from the schema cannot land bounds on the wrong column. Only
            # columns with BOTH kStatMin and kStatMax are emitted (an all-null
            # column carries neither; emitting a half-bound would prune wrong).
            #
            # read_metadata parses only the footer; the mmap'd open touches
            # footer pages, not the data region.
            from skene import SkeneError
            from skene import read_metadata as _skene_read_metadata

            _KSTAT_MIN_MAX = 0x3  # kStatMin | kStatMax (skene format.h StatFlag)
            position_by_name = {col.name: idx for idx, col in enumerate(schema.columns)}
            skene_bounds: Dict[str, tuple] = {}
            for blob_name in ordered_names:
                file_obj = self.filesystem.open_input_file(blob_name)
                try:
                    footer = _skene_read_metadata(file_obj.memoryview)
                except SkeneError as err:
                    raise DataError(f"Cannot read skene file '{blob_name}': {err}") from err
                finally:
                    file_obj.close()
                stats_by_name[blob_name] = (footer["row_count"], None)
                lower: Dict[int, int] = {}
                upper: Dict[int, int] = {}
                for column in footer["columns"]:
                    statistics = column["statistics"]
                    if statistics is None:
                        continue
                    if (statistics["flags"] & _KSTAT_MIN_MAX) != _KSTAT_MIN_MAX:
                        continue
                    position = position_by_name.get(column["name"])
                    if position is None:
                        continue
                    lower[position] = statistics["min_ordinal"]
                    upper[position] = statistics["max_ordinal"]
                if lower:
                    skene_bounds[blob_name] = (lower, upper)
            if skene_bounds:
                manifest_bounds = skene_bounds
                bounds_are_ordinal = True
        elif dataset_fmt == PARQUET:
            try:
                from opteryx.connectors.parquet_io.pool_reader import fetch_column_stats_many

                schema_column_names = [col.name for col in schema.columns]
                # strict: the returned list is parallel to ordered_names by contract,
                # and a silent zip truncation here would hand a file another file's
                # statistics from that point on.
                for blob_name, (record_count, column_stats) in zip(
                    ordered_names,
                    fetch_column_stats_many(self.filesystem, ordered_names, sizes),
                    strict=True,
                ):
                    column_stats.bind_schema(schema_column_names)
                    stats_by_name[blob_name] = (record_count, column_stats)
            except (OSError, ValueError, RuntimeError):
                # No statistics for this dataset. The C++ footer batch is
                # all-or-nothing, so one unreadable file costs the whole set, and
                # every entry below falls back to record_count=None — UNKNOWN, never
                # a fabricated 0, which would let the optimizer answer COUNT(*) as 0
                # and delete LIMIT nodes. Files are still listed and still read.
                stats_by_name = {}

        # Build FileEntry objects from file metadata. Every name in ordered_names
        # yields exactly one entry, in order, whether or not it has statistics —
        # the sketch vectors are positional to this list (row i describes
        # ordered_names[i]), so skipping an entry here would read one file's
        # sketch against another's.
        file_entries = []
        for blob_name in ordered_names:
            record_count, column_stats = stats_by_name.get(blob_name, (None, None))
            manifest_lower, manifest_upper = manifest_bounds.get(blob_name, (None, None))
            file_entries.append(
                FileEntry(
                    file_path=blob_name,
                    file_format=dataset_fmt,
                    record_count=record_count,
                    file_size_in_bytes=sizes.get(blob_name, 0),
                    column_stats=column_stats,
                    lower_bounds=manifest_lower,
                    upper_bounds=manifest_upper,
                )
            )

        # Cache an INDEPENDENT copy of the file list (the returned manifest below
        # may be mutated by the optimizer); hand the caller its own Manifest.
        if self.dataset not in _MANIFEST_CACHE and len(_MANIFEST_CACHE) >= _MANIFEST_CACHE_MAX:
            # FIFO evict the oldest entry to bound memory.
            _MANIFEST_CACHE.pop(next(iter(_MANIFEST_CACHE)), None)
        _MANIFEST_CACHE[self.dataset] = (
            signature,
            list(file_entries),
            min_k_vector,
            histogram_vector,
            bounds_are_ordinal,
            char_class_vector,
        )
        return schema, Manifest(
            file_entries,
            schema,
            min_k_vector=min_k_vector,
            histogram_vector=histogram_vector,
            bounds_are_ordinal=bounds_are_ordinal,
            char_class_vector=char_class_vector,
        )

    def _read_dataset_manifest(self, manifest_path, parquet_names):
        """ANALYZE's per-dataset manifest, as
        ``(ordered_names, min_k, histogram, char_class, bounds_by_path)``.

        Returns the data files in the manifest's own row order — the sketch vectors
        are positional to those rows, so the caller must build its FileEntry list in
        this order to keep row i describing ordered_names[i].

        `bounds_by_path` maps each file's path to its
        ``(lower_bounds, upper_bounds)`` dicts as read straight off the manifest —
        ANALYZE's `Vector.ordinalize()` ordinal int64 keys, NOT real decoded
        values (see manifest_io.write_manifest_parquet's docstring). The caller
        must pair this with `Manifest(bounds_are_ordinal=True)` so `prune_files`
        ordinalizes predicate literals before comparing; it must never be merged
        with a real-value bounds source (e.g. LocalStoreConnector's footer
        bounds) within one Manifest.

        The sketches (and bounds) are used ONLY when the manifest describes
        exactly the current file set. A dataset directory is ad-hoc: files can be
        added or removed under it at any time, and a manifest that has drifted
        holds an INCOMPLETE picture — `estimate_cardinality` returns an EXACT
        count when the merged sketch is under K, so serving it from a partial
        file set would be a wrong answer, not a worse estimate. On any drift (or
        no manifest) the sketches and bounds are dropped and the globbed order is
        returned; ANALYZE re-run restores them.
        """
        from opteryx.models.manifest_io import read_manifest_file_entries

        # No manifest is the norm (a dataset nobody has ANALYZE'd) — an explicit
        # check, not an exception, so a genuine read failure below stays visible.
        if not os.path.isfile(manifest_path):
            return parquet_names, None, None, None, {}

        try:
            stream = self.filesystem.open_input_stream(manifest_path)
            try:
                payload = bytes(stream.memoryview)
            except AttributeError:
                payload = stream.read()
            stream.close()
            entries, native = read_manifest_file_entries(payload)
        except (OSError, ValueError, RuntimeError):
            return parquet_names, None, None, None, {}

        ordered = [entry.file_path for entry in entries]
        if set(ordered) != set(parquet_names):
            return parquet_names, None, None, None, {}

        bounds_by_path = {
            entry.file_path: (entry.lower_bounds, entry.upper_bounds) for entry in entries
        }
        return (
            ordered,
            native.get("min_k_hashes"),
            native.get("histogram_counts"),
            native.get("char_class_counts"),
            bounds_by_path,
        )


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
