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

The bind-time schema is PINNED onto every chunk (2026-09-17): each projected
column's bound type, spelled as `str(ColumnType)`, is passed to rugo as its
`explicit_schema`, so rugo parses the column strictly as that type instead of
re-inferring it from the chunk's own sample rows. A value that does not fit
fails loud from rugo naming the column, row and value; a column a chunk lacks
comes back typed and all-null. Decoded vectors are correlated back to the
plan by physical column NAME, since rugo's output order is not the request
order. Before this, a column that was null for the first rows of a later
chunk drifted to VARCHAR and failed the whole query, and every file paid an
extra projection-free decode of its first chunk just to check names.

Stage 4: `path` (a glob or an exact path) is resolved at bind time
(opteryx.planner.binder.dataset) into `jsonl_files`, a sorted, non-empty list
of matched file paths -- length 1 for a non-glob path, so there is no separate
single-file code path here. read_morsels() iterates that list sequentially
(no cross-file parallelism). A bound column whose key appears in NO record of
a chunk is column drift (a file in the glob that lacks the column) and fails
loud naming the file and the columns -- NDJSON semantics would otherwise read
it as a column of NULLs. A key that is merely sparse is present on some
record and is fine.
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

        # The bind-time schema, spelled the way rugo's declared-type vocabulary reads
        # it (the platform's own `str(ColumnType)`), computed ONCE and pinned onto
        # every chunk of every file. Predicate-only columns are not typed here:
        # rugo evaluates a pushed predicate on the raw token during the map build.
        explicit_schema = {
            physical_name: str(expected.schema_column.column_type)
            for physical_name, expected in physical_to_expected.items()
        }

        for path in self.jsonl_files:
            file_obj = filesystem.open_input_file(path)
            try:
                data = file_obj.memoryview
                for chunk in iter_newline_chunks(data):
                    if len(chunk) == 0:
                        continue

                    # An EMPTY projection means "this query reads no columns"
                    # (COUNT(*), or a projection of only constants), NOT "a file with
                    # zero columns". Emit the same shape the parquet scan's equivalent
                    # path emits: a genuine ZERO-COLUMN morsel whose row count rides on
                    # `zero_col_rows`, which is what `select([])` produces (draken's
                    # cxx_morsel_ops.h) and exactly the contract UngroupedAggSink's
                    # CountStar reads -- see parquet_read.pyx's `_next_cxx` ("No output
                    # columns ... Emit a genuine ZERO-COLUMN morsel"). Building
                    # `Morsel.from_vectors([], [])` here instead would report
                    # num_rows == 0 and silently turn COUNT(*) into 0, which is worse
                    # than the loud failure this replaces.
                    #
                    # With nothing projected there is nothing to pin and no column of
                    # this file reaches the result, so no disagreement between files
                    # can change the answer; None can only mean `predicates` filtered
                    # every row out -- a legitimate zero-row chunk, skipped like any other.
                    if not expected_physical_names:
                        count_morsel, _ = decode_chunk(
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

                    try:
                        chunk_morsel, absent_columns = decode_chunk(
                            chunk,
                            expected_physical_names,
                            predicates,
                            fail_on_error=self.jsonl_fail_on_error,
                            infer_schema=self.jsonl_infer_schema,
                            infer_sample_size=self.jsonl_infer_sample_size,
                            explicit_schema=explicit_schema,
                        )
                    except ValueError as err:
                        # rugo's declared-type mismatch: the message already names the
                        # column, row and value; this adds the file. Not flow control --
                        # the read is over, this is the error that ends it.
                        raise DatasetReadError(
                            f"READ_JSONL('{path}'): a value does not fit the schema resolved "
                            f"at bind time (from the first file in this glob's matched-file "
                            f"set). {err}"
                        ) from err
                    if chunk_morsel is None:
                        # Every row in this chunk was filtered out by `predicates` --
                        # a legitimate zero-row result, not a decode failure. This
                        # chunk simply contributes nothing.
                        continue

                    if absent_columns:
                        # A bound column whose key appears in NO record of this chunk is
                        # column drift: a file in the glob that does not have the column
                        # at all (a key that is merely sparse is present on SOME record
                        # and does not trip this). Fail loud naming the file and columns
                        # rather than emit a column of NULLs -- the same decision the
                        # per-chunk name check took before pinning, kept deliberately.
                        raise DatasetReadError(
                            f"READ_JSONL('{path}'): the expected columns {sorted(absent_columns)} "
                            "(from the bind-time schema, resolved from the first file in this "
                            "glob's matched-file set) are absent from every record in a chunk "
                            "of this file."
                        )

                    names = []
                    vectors = []
                    for physical_name in expected_physical_names:
                        # Every expected column is present: declared columns are always
                        # built by rugo (typed, all-null when the chunk lacks the key), and
                        # each carries its declared type -- a mismatch raised above.
                        vector = chunk_morsel.column(physical_name.encode("utf-8"))
                        names.append(physical_to_expected[physical_name].schema_column.identity)
                        vectors.append(vector)

                    result_morsel = Morsel.from_vectors(names, vectors)

                    self.readings["columns_read"] += len(result_morsel.column_names)
                    self.readings["rows_read"] += result_morsel.num_rows
                    self.readings["bytes_processed"] += result_morsel.nbytes

                    yield result_morsel
            finally:
                file_obj.close()
