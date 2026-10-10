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

SQL Query Execution Plan Node for `READ_AVRO(path)`.

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
- `avro_reader_schema`: the first file's writer schema. Every file is read with it as
  the reader schema, so files whose schemas evolved resolve onto the bound relation
  (docs/AVRO_READER_DESIGN.md §19.3) and a file that cannot fails loud naming it.
- `native_file_locations()`: where each file is read from — the READ_JSONL rules.
"""

from opteryx.exceptions import NotSupportedError
from opteryx.models import QueryProperties

# BasePlanNode/ReaderNode/Morsel in scope via _operators.pyx include.


cdef class AvroReadNode(ReaderNode):
    """Read node for READ_AVRO(path), backed by rugo's Avro decoder."""

    cdef public list avro_files
    cdef public list avro_physical_columns  # pushed-down projection, pre-alias field names
    cdef public str avro_reader_schema
    cdef public object avro_credentialed_filesystem

    def __init__(self, properties: QueryProperties, step, list avro_physical_columns) -> None:
        """A READ_AVRO FunctionDataset step. `avro_physical_columns` is the physical
        planner's translation of the step's pruned columns to field names."""
        ReaderNode.__init__(self, properties, step)
        self.avro_files = list(step.avro_files)
        self.avro_physical_columns = avro_physical_columns
        self.avro_reader_schema = step.avro_reader_schema
        self.avro_credentialed_filesystem = step.avro_credentialed_filesystem

    @property
    def name(self) -> str:  # pragma: no cover
        return "Avro Reader"

    def native_file_locations(self) -> list:
        """Where the native Source reads each of `avro_files` from: a list parallel to
        it holding "" for a local path (memory-mapped) or the URL to GET with no
        credentials. Raises NotSupportedError for anything else, before any byte is
        read.

        SECURITY: as READ_JSONL — READ_AVRO is a bare dataset function, so a
        user-supplied gs:// / s3:// path is NEVER read with this process's platform
        credentials. It is rewritten to its public object URL by the anonymous
        filesystems the binder read its header through; the store's own ACL decides.
        With `credentials =>` each file goes as a short-lived URL signed with the
        customer's stored secret (the binder checked every file against its SCOPE)."""
        if self.avro_credentialed_filesystem is not None:
            return [
                self.avro_credentialed_filesystem.rewrite_to_signed_url(file, expiry_seconds=900)
                for file in self.avro_files
            ]
        path = self.dataset
        protocol = path.split("://")[0] if "://" in path else ""
        if protocol == "":
            return ["" for _ in self.avro_files]
        if protocol in ("http", "https"):
            return list(self.avro_files)
        if protocol == "gs":
            from opteryx.connectors.io_systems.anonymous_gcs_filesystem import (
                anonymous_gcs_filesystem,
            )

            filesystem = anonymous_gcs_filesystem()
        elif protocol == "s3":
            from opteryx.connectors.io_systems.anonymous_s3_filesystem import (
                anonymous_s3_filesystem,
            )

            filesystem = anonymous_s3_filesystem()
        else:
            raise NotSupportedError(
                f"READ_AVRO('{path}'): '{protocol}://' is not a supported scheme; "
                "READ_AVRO reads local paths, http(s):// URLs and public gs:// / s3:// objects."
            )
        if filesystem.native_auth_header() is not None:
            raise NotSupportedError(f"READ_AVRO('{path}'): an authenticated read is not supported.")
        return [filesystem.rewrite_to_signed_url(file) for file in self.avro_files]

    def read_morsels(self):
        from opteryx.exceptions import InvalidInternalStateError

        raise InvalidInternalStateError(
            "AvroReadNode executes only as a native engine source "
            "(NativeAvroScanSource); it has no Python read path."
        )
