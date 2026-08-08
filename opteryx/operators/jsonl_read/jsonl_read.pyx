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

SQL Query Execution Plan Node for `READ_JSONL(path)`.

The file is fetched via the same filesystem resolution Parquet scans use
(opteryx.connectors.io_systems.create_filesystem, keyed off the path's
protocol), split into newline-aligned chunks so no JSONL record is ever split,
and each chunk is decoded independently through rugo.jsonl.read_jsonl -- one
Morsel per chunk, streamed out of read_morsels() rather than buffering the
whole file into a single giant Morsel.

Stage 2: the optimizer's projection/predicate pushdown (see
opteryx/planner/optimizer/strategies/projection_pushdown.py and
predicate_pushdown.py) narrows `jsonl_physical_columns` to the columns
actually referenced and populates `jsonl_predicates` with pushable
(column-vs-literal) filters; both are passed to rugo on every chunk's decode.
A predicate that filters out every row of a chunk is a legitimate zero-row
result (decode_chunk returns None for it), not an error -- that chunk simply
contributes nothing.

rugo infers each chunk's schema independently (there is no working
explicit_schema override yet to pin every chunk to the schema resolved at
bind time), so every decoded chunk is validated -- by physical column NAME,
since rugo's projected-column output order is not guaranteed to match the
request order -- against the bind-time schema before being emitted; a
mismatch fails loud rather than silently emitting wrongly-typed or misaligned
columns.

Stage 4: `path` (a glob or an exact path) is resolved at bind time
(opteryx.planner.binder.dataset) into `jsonl_files`, a sorted, non-empty list
of matched file paths -- length 1 for a non-glob path, so there is no separate
single-file code path here. read_morsels() iterates that list sequentially
(no cross-file parallelism), opening and chunking each file exactly as before;
the same per-chunk schema validation applied within one file is applied
identically across files, so a file whose decoded columns disagree with the
bind-time schema fails loud, naming that file.
"""

from opteryx.exceptions import DatasetReadError
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
    cdef object _filesystem

    def __init__(self, properties: QueryProperties, **parameters) -> None:
        ReaderNode.__init__(self, properties=properties, **parameters)
        self.jsonl_files = list(parameters.get("jsonl_files") or [])
        self.jsonl_physical_columns = list(parameters.get("jsonl_physical_columns") or [])
        self.jsonl_predicates = list(parameters.get("jsonl_predicates") or [])
        jsonl_fail_on_error = parameters.get("jsonl_fail_on_error")
        self.jsonl_fail_on_error = True if jsonl_fail_on_error is None else jsonl_fail_on_error
        jsonl_infer_schema = parameters.get("jsonl_infer_schema")
        self.jsonl_infer_schema = True if jsonl_infer_schema is None else jsonl_infer_schema
        jsonl_infer_sample_size = parameters.get("jsonl_infer_sample_size")
        self.jsonl_infer_sample_size = 5 if jsonl_infer_sample_size is None else jsonl_infer_sample_size
        self._filesystem = None

    @property
    def name(self) -> str:  # pragma: no cover
        return "JSONL Reader"

    def to_mermaid(self, nid):  # pragma: no cover
        mermaid = f'NODE_{nid}[("**{self.name.upper()}**<br />'
        mermaid += f"{self.dataset}<br />"
        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '")]'

    cdef object _ensure_filesystem(self):
        if self._filesystem is None:
            # Dataset Scans attach a connector table that already holds the
            # RIGHT filesystem (platform credentials for gs:// catalog data).
            # The protocol-sniffing below is READ_JSONL's path, where a
            # user-supplied gs:// URL must NEVER use platform credentials.
            connector_filesystem = getattr(self.connector, "filesystem", None)
            if connector_filesystem is not None:
                self._filesystem = connector_filesystem
                return self._filesystem
            path = self.dataset
            protocol = path.split("://")[0] if "://" in path else ""
            if protocol in ("gs", "gcs"):
                # SECURITY: must mirror the bind-time choice in opteryx.planner.binder.
                # dataset's READ_JSONL branch exactly -- READ_JSONL never uses this
                # process's platform GCS credentials for a user-supplied path, at bind
                # time or execution time. See anonymous_gcs_filesystem's docstring.
                from opteryx.connectors.io_systems.anonymous_gcs_filesystem import (
                    anonymous_gcs_filesystem,
                )

                self._filesystem = anonymous_gcs_filesystem()
            else:
                from opteryx.connectors.io_systems import create_filesystem

                self._filesystem = create_filesystem(protocol)
        return self._filesystem

    def read_morsels(self):
        """Source-side morsel iterator driven by the push pipeline engine.

        Streams one Morsel per newline-aligned chunk, across every file in
        `jsonl_files` in order (sequential fan-out, Stage 4 -- a plain
        non-glob path is simply a one-file list, so this is the only code
        path for both cases).
        """
        from opteryx.connectors.jsonl_io import decode_chunk
        from opteryx.connectors.jsonl_io import iter_newline_chunks

        filesystem = self._ensure_filesystem()

        expected_columns = self.columns or []
        expected_physical_names = self.jsonl_physical_columns
        predicates = self.jsonl_predicates

        # physical (pre-alias) name -> expected LogicalColumn, for order-independent
        # correlation of decoded chunk vectors against the bind-time/pushed-down
        # schema -- rugo's projected-column output order is not guaranteed to match
        # the `columns=` request order, so this must be name-keyed, not positional.
        physical_to_expected = dict(zip(expected_physical_names, expected_columns))

        for path in self.jsonl_files:
            file_obj = filesystem.open_input_file(path)
            try:
                data = file_obj.memoryview
                file_schema_validated = False
                for chunk in iter_newline_chunks(data):
                    if len(chunk) == 0:
                        continue

                    # An EMPTY projection means "this query reads no columns"
                    # (COUNT(*), or a projection of only constants), NOT "a file with
                    # zero columns". rugo answers a `columns=[]` request with the
                    # chunk's FULL column set, so BOTH the per-file probe below and
                    # the per-chunk check further down would compare those real
                    # columns against an empty expectation and reject every file,
                    # including a single non-glob one -- the bug this branch fixes.
                    #
                    # Emit the same shape the parquet scan's equivalent path emits: a
                    # genuine ZERO-COLUMN morsel whose row count rides on
                    # `zero_col_rows`, which is what `select([])` produces (draken's
                    # cxx_morsel_ops.h) and exactly the contract UngroupedAggSink's
                    # CountStar reads -- see parquet_read.pyx's `_next_cxx` ("No output
                    # columns ... Emit a genuine ZERO-COLUMN morsel"). Building
                    # `Morsel.from_vectors([], [])` here instead would report
                    # num_rows == 0 and silently turn COUNT(*) into 0, which is worse
                    # than the loud failure this replaces.
                    #
                    # Skipping the drift checks is sound rather than merely convenient:
                    # with nothing projected, no column of this file is read into the
                    # result, so no disagreement between files (or between chunks) can
                    # change the answer. Both checks still fire for every query that
                    # projects at least one column -- a glob over genuinely divergent
                    # files is unaffected.
                    #
                    # The probe's None-ambiguity does not arise here: with no requested
                    # columns there is no "none of them exist in this chunk" case left
                    # to distinguish, so None can only mean `predicates` filtered every
                    # row out -- a legitimate zero-row chunk, skipped like any other.
                    if not expected_physical_names:
                        count_morsel = decode_chunk(
                            chunk,
                            expected_physical_names,
                            predicates,
                            fail_on_error=self.jsonl_fail_on_error,
                            infer_schema=self.jsonl_infer_schema,
                            infer_sample_size=self.jsonl_infer_sample_size,
                        )
                        if count_morsel is None:
                            continue

                        result_morsel = count_morsel.select([])

                        # `result_morsel.nbytes` is 0 (no columns); report the decoded
                        # chunk's size, which is the work this read actually did.
                        self.readings["rows_read"] += result_morsel.num_rows
                        self.readings["bytes_processed"] += count_morsel.nbytes

                        yield result_morsel
                        continue

                    if not file_schema_validated:
                        # Stage 4: decode_chunk(..., predicates) returns None both when
                        # every row is filtered out AND when none of the requested
                        # columns exist in this chunk at all -- indistinguishable from
                        # its return value alone. A predicate-free probe of this file's
                        # first chunk resolves that ambiguity once per file, so a file
                        # whose columns don't match the bind-time schema (e.g. a
                        # different file matched by this glob) fails loud naming this
                        # file, instead of silently contributing zero rows.
                        probe_morsel = decode_chunk(
                            chunk,
                            expected_physical_names,
                            None,
                            fail_on_error=self.jsonl_fail_on_error,
                            infer_schema=self.jsonl_infer_schema,
                            infer_sample_size=self.jsonl_infer_sample_size,
                        )
                        if probe_morsel is None:
                            # Two different situations reach here, and the probe's
                            # return value alone cannot tell them apart: this chunk
                            # has RECORDS but none of the expected columns (real
                            # schema drift -- fail loud), or this chunk has NO
                            # RECORDS at all (blank/whitespace-only lines -- an
                            # empty file, which is not an error). One more
                            # projection-free decode of the same chunk separates
                            # them: with no columns requested, rugo returns
                            # whatever the chunk holds, so None can only mean
                            # "no records". It costs a second decode only on a
                            # path that was previously an unconditional raise.
                            unprojected_morsel = decode_chunk(
                                chunk,
                                None,
                                None,
                                fail_on_error=self.jsonl_fail_on_error,
                                infer_schema=self.jsonl_infer_schema,
                                infer_sample_size=self.jsonl_infer_sample_size,
                            )
                            if unprojected_morsel is None:
                                # Record-less chunk: contributes no rows, and
                                # leaves this file unvalidated so the next chunk
                                # that does hold records is still probed.
                                continue
                            raise DatasetReadError(
                                f"READ_JSONL('{path}'): none of the expected columns "
                                f"{sorted(expected_physical_names)} (from the bind-time schema, "
                                "resolved from the first file in this glob's matched-file set) "
                                "were found in this file."
                            )
                        probe_names = {
                            n.decode("utf-8") if isinstance(n, bytes) else n
                            for n in probe_morsel.column_names
                        }
                        if probe_names != set(expected_physical_names):
                            raise DatasetReadError(
                                f"READ_JSONL('{path}'): this file's columns {sorted(probe_names)} "
                                f"do not match the expected {sorted(expected_physical_names)} from "
                                "the bind-time schema (resolved from the first file in this glob's "
                                "matched-file set)."
                            )
                        file_schema_validated = True

                    chunk_morsel = decode_chunk(
                        chunk,
                        expected_physical_names,
                        predicates,
                        fail_on_error=self.jsonl_fail_on_error,
                        infer_schema=self.jsonl_infer_schema,
                        infer_sample_size=self.jsonl_infer_sample_size,
                    )
                    if chunk_morsel is None:
                        # Every row in this chunk was filtered out by `predicates` --
                        # a legitimate zero-row result, not a decode failure. This
                        # chunk simply contributes nothing.
                        continue

                    chunk_names = {
                        n.decode("utf-8") if isinstance(n, bytes) else n
                        for n in chunk_morsel.column_names
                    }
                    if chunk_names != set(expected_physical_names):
                        raise DatasetReadError(
                            f"READ_JSONL('{path}'): a chunk decoded columns {sorted(chunk_names)}, "
                            f"expected {sorted(expected_physical_names)} from the bind-time schema. "
                            "rugo infers each chunk's schema independently, so this file's "
                            "columns are not uniform enough for chunked streaming, or it "
                            "does not match the schema resolved from the first file in a "
                            "glob's matched-file set."
                        )

                    names = []
                    vectors = []
                    for physical_name in expected_physical_names:
                        vector = chunk_morsel.column(physical_name.encode("utf-8"))
                        expected_column = physical_to_expected[physical_name].schema_column
                        if vector.type != expected_column.column_type.physical:
                            raise DatasetReadError(
                                f"READ_JSONL('{path}'): column '{physical_name}' decoded as "
                                f"{vector.type!r} in this chunk but {expected_column.column_type.physical!r} "
                                "at bind time. rugo infers each chunk's schema independently "
                                "from its own sample rows, so this file's columns are not "
                                "uniform enough for chunked streaming, or it does not match "
                                "the schema resolved from the first file in a glob's "
                                "matched-file set."
                            )
                        names.append(expected_column.identity)
                        vectors.append(vector)

                    result_morsel = Morsel.from_vectors(names, vectors)

                    self.readings["columns_read"] += len(result_morsel.column_names)
                    self.readings["rows_read"] += result_morsel.num_rows
                    self.readings["bytes_processed"] += result_morsel.nbytes

                    yield result_morsel
            finally:
                file_obj.close()
