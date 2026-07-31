# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Mabel Connector

Reads Mabel-formatted partitioned datasets:

    {dataset}/year_YYYY/month_MM/day_DD/[by_hour/hour=HH/]as_at_<ts>/*.parquet

Each ``as_at_<ts>`` snapshot folder is marked valid by a sibling ``frame.complete``
control blob; a sibling ``frame.ignore`` marks it invalid regardless. This is the
legacy Mabel write-completion convention, ported near-verbatim from the original
partition-scheme implementation (``extract_prefix`` / ``is_complete_and_not_ignored``
/ ``resolve_as_at`` below) since that resolution logic has run in production for
years and is trusted as-is.

Point-in-time only, via ``TIMESTAMP AS OF <expr>`` (the ``Diachronic`` capability):
a query without an AT clause reads "today" (UTC); a query with AT resolves to that
expression's date. Date ranges are not supported - this mirrors the legacy scheme's
actual usage, not its unused range parameters. The semantics of "point in time"
also differ deliberately from the Opteryx catalog's diachronic reads: the catalog
walks backward through snapshot history to the nearest committed write before the
requested time; Mabel looks only at the requested day and resolves "the last event
in" - the newest complete ``as_at_`` for that day. There is no fallback to an
earlier day. If ``by_hour`` segmentation is present and the AT expression doesn't
pin an hour (a bare DATE, or no AT clause), every hour bucket present that day is
resolved independently and unioned, mirroring the legacy scheme's default
full-day range behaviour.

Partition pruning and as_at/frame.complete resolution happen here, entirely at
plan time. All data reading is native: this connector's only job is to build a
Manifest of FileEntry blob paths + footer stats and hand it to ParquetReadNode -
the same modern parquet path every other connector uses.

Case mapping: the binder lowercases every SQL table identifier before any
connector sees it (opteryx/planner/binder/dataset.py - `node.relation.lower()`),
but real Mabel blob paths carry their original case (e.g. `RAW/NVD/CVE_LIST`)
and GCS/Linux paths are case-sensitive. Two ways to recover the real path,
mutually exclusive:

- `preserve_sql_case=True` - trust the casing exactly as typed in SQL. Sets
  `requires_original_case` so the binder forwards the pre-lowering relation
  name (opteryx/planner/binder/dataset.py's `original_relation`); MabelTable
  uses it verbatim. No mapping to maintain, but the query has to spell the
  dataset with its real casing every time.
- `case_map=...` - an explicit dict or callable mapping the (already
  lowercased) dataset name to its real-cased path, for when the real casing
  isn't - or shouldn't have to be - reproduced in every query.

MabelTable raises rather than guessing when the configured strategy doesn't
cover the requested dataset. This is a workaround local to this connector, not
a fix to the underlying case-folding behaviour, which is a pre-existing
engine-wide issue outside this connector's scope.
"""

import datetime
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from opteryx.connectors import TableType
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.base.base_connector import BaseTable
from opteryx.connectors.capabilities import Diachronic
from opteryx.exceptions import DataError
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.types.schema import RelationSchema

PARQUET_SUFFIX = ".parquet"


class UnsupportedSegmentationError(DataError):
    """Raised when a Mabel dataset uses a segmentation other than `by_hour`."""

    def __init__(self, dataset: str, segment: str):
        self.dataset = dataset
        self.segment = segment
        super().__init__(
            f"'{dataset}' contains unsupported segmentation (found `{segment}`), "
            "only 'by_hour' segments are supported."
        )


def _extract_prefix(path: str, prefix: str) -> Optional[str]:
    """Return the `prefix...` path segment starting at `prefix`, up to the next `/`."""
    start_index = path.find(prefix)
    if start_index == -1:
        return None
    end_index = path.find("/", start_index)
    return path[start_index:end_index] if end_index != -1 else None


def _is_complete_and_not_ignored(control_blobs: List[str], as_at: str) -> bool:
    """True when `as_at`'s sibling control blobs mark it complete and not invalid."""
    complete_suffix = f"{as_at}/frame.complete"
    ignore_suffix = f"{as_at}/frame.ignore"
    complete = False
    ignore = False
    for blob in control_blobs:
        if complete_suffix in blob:
            complete = True
        elif ignore_suffix in blob:
            ignore = True
        if complete and ignore:
            break
    return complete and not ignore


def _resolve_as_at(data_blobs: List[str], control_blobs: List[str]) -> List[str]:
    """Return the data blobs for the newest `as_at_` snapshot that is complete and not ignored.

    "The last event in": the most recent as_at whose control blobs mark it complete
    wins outright; older, incomplete, or ignored as_ats are discarded, never merged
    with or fallen back through. A partition with no as_at_ folders at all has no
    snapshot versioning, so every data blob is valid.
    """
    as_ats = sorted(
        {_extract_prefix(blob, "as_at_") for blob in data_blobs if "as_at_" in blob},
        reverse=True,
    )
    if not as_ats:
        return data_blobs
    for as_at in as_ats:
        if _is_complete_and_not_ignored(control_blobs, as_at):
            return [blob for blob in data_blobs if as_at in blob]
    return []


def _resolve_day_partition(blob_names: List[str], dataset: str) -> List[str]:
    """Resolve a day's blob listing to the valid data blobs for that day.

    Splits control blobs (frame.complete/frame.ignore, anything non-parquet) from
    data blobs, enforces the by_hour-only segmentation rule, and resolves as_at_
    snapshots. When by_hour partitioning is present, every hour bucket found is
    resolved independently and the results are unioned - a point-in-time read
    that doesn't pin a specific hour covers the whole day, matching the legacy
    scheme's default full-day range behaviour.
    """
    control_blobs: List[str] = []
    data_blobs: List[str] = []

    for blob in blob_names:
        if blob.lower().endswith(PARQUET_SUFFIX):
            data_blobs.append(blob)
            if "/by_" in blob:
                segment = _extract_prefix(blob, "by_")
                if segment != "by_hour":
                    raise UnsupportedSegmentationError(dataset=dataset, segment=segment)
        else:
            control_blobs.append(blob)

    hour_labels = sorted(
        {
            _extract_prefix(blob, "hour=")
            for blob in data_blobs
            if "/by_hour/" in blob and _extract_prefix(blob, "hour=") is not None
        }
    )

    if hour_labels:
        selected: List[str] = []
        for hour_label in hour_labels:
            hour_blobs = [blob for blob in data_blobs if f"/by_hour/{hour_label}/" in blob]
            selected.extend(_resolve_as_at(hour_blobs, control_blobs))
        return selected

    return _resolve_as_at(data_blobs, control_blobs)


def _date_partition_path(dataset: str, date: datetime.date) -> str:
    return f"{dataset}/year_{date.year:04d}/month_{date.month:02d}/day_{date.day:02d}"


class MabelTable(BaseTable, Diachronic):
    """
    Plan-time table metadata provider for Mabel-partitioned datasets.

    Transient, created per query. Resolves the year/month/day[/by_hour] partition
    and as_at/frame.complete snapshot for the query's point-in-time, then builds a
    Manifest of FileEntry blob paths + footer stats for ParquetReadNode. Performs
    no data reading itself.
    """

    __mode__ = "Blob"
    __type__ = "MABEL"
    __synchronousity__ = "synchronous"

    supports_diachronic = True

    def __init__(
        self,
        dataset: str,
        filesystem,
        storage_type: str,
        case_map=None,
        preserve_sql_case: bool = False,
        original_relation: Optional[str] = None,
        **kwargs,
    ):
        BaseTable.__init__(self, dataset=dataset, **kwargs)
        Diachronic.__init__(self, **kwargs)

        self.filesystem = filesystem
        self.__type__ = storage_type
        self._manifest = None

        if preserve_sql_case:
            # requires_original_case tells the binder to forward the relation
            # name as typed, before its case-folding lowercase - use that
            # verbatim rather than the (already-lowercased) self.dataset.
            if original_relation is None:
                raise DatasetReadError(
                    "MabelConnector was configured with preserve_sql_case=True but no "
                    "original_relation was supplied - the connector's requires_original_case "
                    "flag must be set for the binder to forward it (see MabelConnector.__init__)."
                )
            self.dataset = original_relation.replace(".", "/")
        elif case_map is not None:
            # Authoritative: the real-cased path for this (already-lowercased)
            # dataset name. No fallback to a guessed path - a case_map that
            # doesn't cover this dataset is a configuration gap, not something
            # to paper over with a naive lowercase-to-path conversion that is
            # very likely wrong.
            resolved = case_map(self.dataset) if callable(case_map) else case_map.get(self.dataset)
            if resolved is None:
                raise DatasetReadError(
                    f"No case mapping registered for dataset '{self.dataset}' - "
                    "MabelConnector was configured with a case_map but this dataset isn't in it."
                )
            self.dataset = resolved
        elif self.dataset and "/" not in self.dataset:
            # Dotted SQL identifiers (e.g. "mabel_ws.orders") address a single-level
            # path when no separator is already present, matching FileSystemTable.
            self.dataset = self.dataset.replace(".", "/")

    def get_dataset_schema(self) -> RelationSchema:
        if self.schema is not None:
            return self.schema
        schema, _ = self.get_dataset_metadata()
        return schema

    def get_dataset_metadata(self) -> Tuple[RelationSchema, "Manifest"]:  # noqa: F821
        if self.schema is not None and self._manifest is not None:
            return self.schema, self._manifest

        from rugo.parquet import read_metadata_from_memoryview  # type: ignore[import]

        from opteryx.connectors._rugo_schema import rugo_to_relation_schema
        from opteryx.connectors.parquet_io.pool_reader import fetch_column_stats
        from opteryx.models.file_entry import FileEntry
        from opteryx.models.manifest import Manifest
        from opteryx.models.manifest_io import is_dataset_manifest

        at_value = self.at_date
        if at_value is None:
            at_value = datetime.datetime.now(datetime.UTC)
        partition_date = at_value.date() if isinstance(at_value, datetime.datetime) else at_value

        date_path = _date_partition_path(self.dataset, partition_date)
        blob_names = [
            name
            for name in self.filesystem.list_files(date_path, recursive=True)
            if not is_dataset_manifest(name)
        ]

        if not blob_names:
            raise DatasetReadError(
                f"No data available for '{self.dataset}' at {partition_date.isoformat()}."
            )

        data_blobs = _resolve_day_partition(blob_names, dataset=self.dataset)

        if not data_blobs:
            raise DatasetReadError(
                f"'{self.dataset}' has no complete partition for {partition_date.isoformat()} "
                "- no as_at snapshot is marked complete for that day."
            )

        stream = self.filesystem.open_input_stream(data_blobs[0])
        try:
            mv = stream.memoryview
            rugo_metadata = read_metadata_from_memoryview(mv)
            schema = rugo_to_relation_schema(rugo_metadata, schema_name=self.dataset)
        finally:
            stream.close()

        infos = self.filesystem.get_file_info(data_blobs)
        sizes: Dict[str, int] = {i.path: (getattr(i, "size", 0) or 0) for i in infos}

        file_entries = []
        for blob_name in data_blobs:
            file_size = sizes.get(blob_name, 0)
            try:
                record_count, footer_size, column_stats = fetch_column_stats(
                    blob_name, file_size=file_size or -1
                )
                if file_size == 0:
                    file_size = footer_size
                column_stats.bind_schema([col.name for col in schema.columns])
            except (OSError, ValueError, RuntimeError):
                record_count = 0
                column_stats = None

            file_entries.append(
                FileEntry(
                    file_path=blob_name,
                    file_format="PARQUET",
                    record_count=record_count,
                    file_size_in_bytes=file_size,
                    column_stats=column_stats,
                )
            )

        self.schema = schema
        self._manifest = Manifest(file_entries, schema)
        return self.schema, self._manifest

    def read_dataset(self, **kwargs):
        """
        Mabel datasets are always read via ParquetReadNode from the Manifest
        built by get_dataset_metadata(); this connector performs no data reads
        itself.
        """
        raise UnsupportedSyntaxError(
            "All Parquet scans use ParquetReadNode. MabelConnector data reads are not supported."
        )


class MabelConnector(BaseConnector):
    """
    Gateway connector for Mabel-partitioned datasets.

    Long-lived, cached by storage configuration. Creates transient MabelTable
    instances for each dataset query.
    """

    __mode__ = "Blob"

    supports_diachronic = True

    def __init__(
        self,
        filesystem,
        storage_type="FILESYSTEM",
        case_map=None,
        preserve_sql_case: bool = False,
        **kwargs,
    ):
        if case_map is not None and preserve_sql_case:
            raise ValueError(
                "MabelConnector: case_map and preserve_sql_case are mutually exclusive "
                "strategies for recovering real-cased paths - configure one, not both."
            )

        self.filesystem = filesystem
        self.storage_type = storage_type
        self.__type__ = storage_type
        self.case_map = case_map
        self.preserve_sql_case = preserve_sql_case
        # Read by the binder (opteryx/planner/binder/dataset.py) to decide whether
        # to forward the pre-lowering relation name into table_engine()'s kwargs.
        self.requires_original_case = preserve_sql_case

    def locate_object(self, name: str) -> Tuple[Optional[TableType], any]:
        """Mabel workspaces only support tables (no views, no catalog)."""
        return (TableType.Table, None)

    def table_engine(self, name: str, **kwargs):
        telemetry = kwargs.pop("telemetry", None)
        return MabelTable(
            dataset=name,
            filesystem=self.filesystem,
            storage_type=self.storage_type,
            case_map=self.case_map,
            preserve_sql_case=self.preserve_sql_case,
            telemetry=telemetry,
            **kwargs,
        )


def create_local_mabel_connector(**kwargs):
    """Create a MabelConnector for local storage."""
    from opteryx.connectors.io_systems import OpteryxLocalFileSystem

    filesystem = OpteryxLocalFileSystem()
    return MabelConnector(filesystem=filesystem, storage_type="LOCAL", **kwargs)


def create_gcs_mabel_connector(bucket=None, **kwargs):
    """Create a MabelConnector for Google Cloud Storage."""
    from opteryx.connectors.io_systems import OpteryxGcsFileSystem

    filesystem = OpteryxGcsFileSystem(bucket=bucket, **kwargs)
    return MabelConnector(filesystem=filesystem, storage_type="GCS", **kwargs)
