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
JSONL Read Node

SQL Query Execution Plan Node for `READ_JSONL(path)` and manifest-backed scans of
JSONL datasets.

This node is PLANNING ONLY. Execution is the native streaming Source
(src/cpp/engine/native_jsonl_scan_source.hpp, compiled by
opteryx.managers.execution.compiler._Compiler._compile_jsonl_scan): its own decode
pool cuts every file into newline-aligned chunks and decodes them through rugo's C++
JSONL path while execution consumes them. There is no Python read path —
`read_morsels` raises.

What this node fixes at plan time:
- `jsonl_files`: resolved at bind time (opteryx.planner.binder.dataset) from a glob
  or an exact path — length 1 for a non-glob path.
- `jsonl_physical_columns` / `jsonl_predicates`: the optimizer's projection and
  predicate pushdown, as physical (pre-alias) names and rugo (column, op, value)
  tuples. One physical column can feed several identities.
- `pinned_schema()`: the bind-time schema pinned onto every chunk, so a later
  chunk is parsed strictly as the bound types and a value that does not fit fails
  loud. A bound column whose key appears in NO record of a chunk is column drift
  and fails loud naming the file — except a nested `key->>'sub'` column, which a
  chunk may legitimately lack.
- `native_file_locations()`: where each file is read from. Local paths are
  mapped; `http(s)://` URLs and unauthenticated `gs://` / `s3://` objects are
  fetched whole over plain HTTP(S). Authenticated remote reads are not supported
  (architect ruling 2026-10-01) and are refused at plan time.
"""

from opteryx.exceptions import NotSupportedError
from opteryx.models import QueryProperties

# BasePlanNode/ReaderNode/Morsel in scope via _operators.pyx include.


cdef class JsonlReadNode(ReaderNode):
    """Read node for READ_JSONL(path), backed by rugo's JSONL decoder."""

    # Stage 4: resolved, sorted, non-empty list of files this scan reads --
    # length 1 for a plain (non-glob) path. See opteryx.planner.binder.dataset.
    cdef public list jsonl_files
    cdef public list jsonl_physical_columns  # pushed-down projection, pre-alias physical names
    # Pushed-down predicates as rugo (physical_column_name, op, value) tuples --
    # see opteryx.planner.physical_planner._translate_jsonl_predicates.
    cdef public list jsonl_predicates
    # Resolved READ_JSONL(... key => value) options (Stage 3), forwarded
    # unchanged to rugo on every chunk's decode; see opteryx.planner.binder.dataset.
    cdef public bint jsonl_fail_on_error
    cdef public bint jsonl_infer_schema
    cdef public long long jsonl_infer_sample_size

    def __init__(
        self,
        properties: QueryProperties,
        step,
        list jsonl_files,
        list jsonl_physical_columns,
        list jsonl_predicates,
    ) -> None:
        """A READ_JSONL FunctionDataset step, or a manifest-backed Scan of a JSONL
        dataset. The file list, the pushed-down projection (pre-alias physical
        names) and the predicates as rugo tuples are the physical planner's
        translation of either. Only READ_JSONL carries decode options."""
        ReaderNode.__init__(self, properties, step)
        self.jsonl_files = jsonl_files
        self.jsonl_physical_columns = jsonl_physical_columns
        self.jsonl_predicates = jsonl_predicates
        self.jsonl_fail_on_error = True
        self.jsonl_infer_schema = True
        self.jsonl_infer_sample_size = 5
        if step.node_type in steps_with("jsonl_fail_on_error"):
            if step.jsonl_fail_on_error is not None:
                self.jsonl_fail_on_error = step.jsonl_fail_on_error
            if step.jsonl_infer_schema is not None:
                self.jsonl_infer_schema = step.jsonl_infer_schema
            if step.jsonl_infer_sample_size is not None:
                self.jsonl_infer_sample_size = step.jsonl_infer_sample_size

    @property
    def name(self) -> str:  # pragma: no cover
        return "JSONL Reader"

    def native_file_locations(self) -> list:
        """Where the native Source reads each of `jsonl_files` from: a list parallel
        to it holding "" for a local path (memory-mapped) or the URL to GET with no
        credentials. Raises NotSupportedError for anything else, before any byte is
        read.

        SECURITY: READ_JSONL is a bare dataset function — any SQL text can name any
        path — so a user-supplied gs:// / s3:// path is NEVER read with this
        process's platform credentials. It is rewritten to its public object URL by
        the anonymous filesystems the binder already reads it through
        (anonymous_gcs_filesystem / anonymous_s3_filesystem), whose native contract
        guarantees no auth header; the store's own ACL decides the outcome. A
        connector-backed (catalog) scan reads through the connector's filesystem,
        which may hold platform credentials, so its remote files are refused rather
        than fetched anonymously or with those credentials."""
        credentialed = getattr(self.connector, "credentialed_filesystem", None)
        if credentialed is not None:
            # READ_JSONL(..., credentials => '<workspace>.<name>'): the binder built
            # this from a stored customer secret, checked every file against the
            # secret's SCOPE, and the filesystem checks again here. The native Source
            # sends no headers, so each file goes as a short-lived URL signed with
            # that secret - never with this process's own credentials. The URL's
            # query string carries the signature, which is why the HTTP client
            # never quotes a query string in an error.
            return [
                credentialed.rewrite_to_signed_url(file, expiry_seconds=900)
                for file in self.jsonl_files
            ]
        if getattr(self.connector, "filesystem", None) is not None:
            for path in self.jsonl_files:
                if "://" in path:
                    raise NotSupportedError(
                        f"JSONL dataset file '{path}' is remote; JSONL datasets are "
                        "read from local files only — authenticated remote JSONL reads "
                        "are not supported."
                    )
            return ["" for _ in self.jsonl_files]
        path = self.dataset
        protocol = path.split("://")[0] if "://" in path else ""
        if protocol == "":
            return ["" for _ in self.jsonl_files]
        if protocol in ("http", "https"):
            return list(self.jsonl_files)
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
                f"READ_JSONL('{path}'): '{protocol}://' is not a supported scheme; "
                "READ_JSONL reads local paths, http(s):// URLs and public gs:// / s3:// objects."
            )
        if filesystem.native_auth_header() is not None:
            raise NotSupportedError(
                f"READ_JSONL('{path}'): an authenticated read is not supported."
            )
        return [filesystem.rewrite_to_signed_url(file) for file in self.jsonl_files]

    def pinned_schema(self) -> dict:
        """The bind-time schema pinned onto every chunk, ``{physical_name:
        str(ColumnType)}`` -- the platform's own type spelling, which rugo's
        declared-type vocabulary reads verbatim. Computed from the (physical name,
        expected column) pairs, so a physical column feeding several identities is
        declared once. Predicate-only columns are not typed here: rugo evaluates a
        pushed predicate on the raw token during the map build."""
        return {
            physical_name: str(expected.schema_column.column_type)
            for physical_name, expected in zip(self.jsonl_physical_columns, self.columns or [])
        }

    def read_morsels(self):
        from opteryx.exceptions import InvalidInternalStateError

        raise InvalidInternalStateError(
            "JsonlReadNode executes only as a native engine source "
            "(NativeJsonlScanSource); it has no Python read path."
        )
