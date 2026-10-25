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
ORC Read Node

SQL Query Execution Plan Node for a manifest-backed Scan of ORC data files: an Iceberg
table, or a folder of .orc files (docs/ORC_READER_DESIGN.md).

This node is PLANNING ONLY. Execution is the native Source
(src/cpp/engine/native_orc_scan_source.hpp, compiled by
opteryx.managers.execution.compiler._Compiler._compile_orc_scan): its decode pool claims
stripes across every file and decodes each through rugo's C++ ORC reader while execution
consumes them. There is no Python read path - `read_morsels` raises.

What this node fixes at plan time:
- `orc_files`: the manifest's file list. Local, or (a catalog table) gs:// / s3:// read with
  the platform filesystem's own credential mechanism - see `native_scan_locations`.
- `orc_reader_columns`: what each projected column is to be read as, a
  (name, field_id, DrakenType ordinal, precision, scale, nullable) tuple.
- `orc_physical_columns`: the projection, as the reader-column names (parallel to the
  step's columns).
- `orc_match_by_id`: an Iceberg table's files are matched to its columns by their
  `iceberg.id` attribute; a folder's by name.
No predicate is pushed into the reader: both connectors decline them for an ORC dataset.
"""

from opteryx.connectors.io_systems.native_locations import function_scan_urls
from opteryx.connectors.io_systems.native_locations import native_locations
from opteryx.connectors.io_systems.native_locations import remote_files
from opteryx.exceptions import NotSupportedError
from opteryx.models import QueryProperties

# BasePlanNode/ReaderNode/Morsel in scope via _operators.pyx include.


cdef class OrcReadNode(ReaderNode):
    """Read node for a manifest-backed Scan of ORC files, backed by rugo's ORC decoder."""

    cdef public list orc_files
    cdef public list orc_reader_columns
    cdef public list orc_physical_columns
    cdef public bint orc_match_by_id
    cdef public object orc_credentialed_filesystem
    cdef public bint orc_function_scan   # READ_ORC('path'), not a manifest-backed Scan

    def __init__(
        self,
        properties: QueryProperties,
        step,
        list orc_physical_columns,
        list orc_files,
        list orc_reader_columns,
        bint orc_match_by_id,
        object orc_credentialed_filesystem,
        bint orc_function_scan,
    ) -> None:
        ReaderNode.__init__(self, properties, step)
        self.orc_credentialed_filesystem = orc_credentialed_filesystem
        self.orc_function_scan = orc_function_scan
        self.orc_files = orc_files
        self.orc_reader_columns = orc_reader_columns
        self.orc_physical_columns = orc_physical_columns
        self.orc_match_by_id = orc_match_by_id

    @property
    def name(self) -> str:  # pragma: no cover
        return "ORC Reader"

    def native_scan_locations(self) -> tuple:
        """(urls, auth_header) for the native Source: where each of `orc_files` is read
        from and how a remote read authenticates (docs/REMOTE_AVRO_ORC_DATA_FILES_DESIGN.md).

        READ_ORC (a FUNCTION scan): `function_scan_urls` - gs:// / s3:// read ANONYMOUSLY (or, with
        `credentials =>`, through a URL signed with the customer's secret), NEVER with the
        platform's credentials (docs/ANONYMOUS_REMOTE_READS_DESIGN.md §0); no header.

        A catalog table (matched by `iceberg.id`): the platform filesystem's own
        declaration - a bearer header (GCS) or signed URLs (S3) - via `native_locations`.
        A folder of .orc files stays local only: reading its schema at bind time
        downloads the whole first file (design doc §7, phase 2)."""
        if self.orc_function_scan:
            return function_scan_urls("READ_ORC", self.dataset, self.orc_files, self.orc_credentialed_filesystem), ""
        remote = remote_files(self.orc_files)
        if remote and not self.orc_match_by_id:
            raise NotSupportedError(
                f"The ORC data file '{remote[0]}' of {self.relation} is remote; a folder of "
                ".orc files is read from local paths only."
            )
        filesystem = resolve_scan_filesystem(self.connector, self.orc_files)[0] if remote else None
        return native_locations(filesystem, self.orc_files, str(self.relation))

    def read_morsels(self):
        from opteryx.exceptions import InvalidInternalStateError

        raise InvalidInternalStateError(
            "OrcReadNode executes only as a native engine source "
            "(NativeOrcScanSource); it has no Python read path."
        )
