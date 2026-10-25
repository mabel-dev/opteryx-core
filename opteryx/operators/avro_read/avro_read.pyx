# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Avro Read Node

SQL Query Execution Plan Node for `READ_AVRO(path)` and for a catalog Scan of a table
whose data files are Avro: an Iceberg table or a folder of .avro files
(docs/ICEBERG_AVRO_ORC_DATA_FILES_DESIGN.md).

This node is PLANNING ONLY. Execution is the native Source
(src/cpp/engine/native_avro_scan_source.hpp, compiled by
opteryx.managers.execution.compiler._Compiler._compile_avro_scan): its decode pool
claims whole files and streams each one's batches through rugo's C++ Avro reader
while execution consumes them. There is no Python read path — `read_morsels` raises.

What this node fixes at plan time:
- `avro_files`: resolved at bind time (opteryx.planner.binder.dataset) from a glob or
  an exact path — length 1 for a non-glob path.
- `avro_physical_columns`: the optimizer's projection, as the file's top-level field
  names (pre-alias). No predicate is pushed into the reader.
- `avro_reader_schema`: READ_AVRO and a folder dataset: the first file's writer schema,
  fields matched by name. A catalog table's Scan: the TABLE's schema, one field per
  column named by its field id (opteryx.connectors.avro_io.table_reader_schema). Every file is read with it as
  the reader schema, so files whose schemas evolved resolve onto the bound relation
  (docs/AVRO_READER_DESIGN.md §19.3) and a file that cannot fails loud naming it.
- `native_file_locations()`: where each file is read from — the READ_JSONL rules for
  READ_AVRO; local files only for a catalog Scan (no authenticated remote reads yet).
"""

from opteryx.connectors.io_systems.native_locations import function_scan_urls
from opteryx.connectors.io_systems.native_locations import native_locations
from opteryx.connectors.io_systems.native_locations import remote_files
from opteryx.exceptions import NotSupportedError
from opteryx.models import QueryProperties

# BasePlanNode/ReaderNode/Morsel in scope via _operators.pyx include.


cdef class AvroReadNode(ReaderNode):
    """Read node for READ_AVRO(path), backed by rugo's Avro decoder."""

    cdef public list avro_files
    cdef public list avro_physical_columns  # pushed-down projection, pre-alias field names
    cdef public str avro_reader_schema
    cdef public object avro_credentialed_filesystem
    cdef public bint avro_manifest_scan  # a manifest-backed Scan, not READ_AVRO
    cdef public bint avro_require_field_ids  # every file's fields must carry a field-id

    def __init__(
        self,
        properties: QueryProperties,
        step,
        list avro_physical_columns,
        list avro_files,
        str avro_reader_schema,
        object avro_credentialed_filesystem,
        bint avro_manifest_scan,
        bint avro_require_field_ids,
    ) -> None:
        """A READ_AVRO FunctionDataset step, or a manifest-backed Scan of an Avro
        dataset. The physical planner supplies everything the reader needs: the files,
        the pruned columns translated to reader-schema field names (parallel to the
        step's columns), the reader schema, and (READ_AVRO `credentials =>` only) the
        filesystem that signs each file's URL."""
        ReaderNode.__init__(self, properties, step)
        self.avro_files = avro_files
        self.avro_physical_columns = avro_physical_columns
        self.avro_reader_schema = avro_reader_schema
        self.avro_credentialed_filesystem = avro_credentialed_filesystem
        self.avro_manifest_scan = avro_manifest_scan
        self.avro_require_field_ids = avro_require_field_ids

    @property
    def name(self) -> str:  # pragma: no cover
        return "Avro Reader"

    def native_file_locations(self) -> list:
        """Where the native Source reads each of `avro_files` from (READ_AVRO): "" for a local
        path (memory-mapped) or the URL to GET. The rule - never the platform's credentials -
        lives in `function_scan_urls`. A manifest-backed Scan has its own method,
        `native_scan_locations`."""
        return function_scan_urls("READ_AVRO", self.dataset, self.avro_files, self.avro_credentialed_filesystem)

    def native_scan_locations(self) -> tuple:
        """(urls, auth_header) for the native Source: where each of `avro_files` is read
        from and how a remote read authenticates (docs/REMOTE_AVRO_ORC_DATA_FILES_DESIGN.md).

        READ_AVRO: the anonymous / credentialed rules of `native_file_locations`, no
        header. A catalog table (Iceberg shape): the platform filesystem's own declaration -
        a bearer header (GCS) or signed URLs (S3) - via `native_locations`. A folder of
        .avro files stays local only: reading its schema at bind time downloads the whole
        first file (design doc §7, phase 2)."""
        if not self.avro_manifest_scan:
            return self.native_file_locations(), ""
        remote = remote_files(self.avro_files)
        if remote and not self.avro_require_field_ids:
            raise NotSupportedError(
                f"The Avro data file '{remote[0]}' of {self.relation} is remote; a folder of "
                ".avro files is read from local paths only."
            )
        filesystem = resolve_scan_filesystem(self.connector, self.avro_files)[0] if remote else None
        return native_locations(filesystem, self.avro_files, str(self.relation))

    def read_morsels(self):
        from opteryx.exceptions import InvalidInternalStateError

        raise InvalidInternalStateError(
            "AvroReadNode executes only as a native engine source "
            "(NativeAvroScanSource); it has no Python read path."
        )
