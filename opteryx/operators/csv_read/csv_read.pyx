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
CSV Read Node

SQL Query Execution Plan Node for `READ_CSV(path)`.

Each file is fetched via the same filesystem resolution Parquet/JSONL scans
use (opteryx.connectors.io_systems.create_filesystem, keyed off the path's
protocol) and decoded through rugo.csv.read_csv in one pass -- unlike
JsonlReadNode, there is no newline-chunked streaming here: rugo's CSV reader
has no chunked entry point to stream from (it already applies the pushed-down
projection/predicates natively in one pass over the whole buffer and returns
exactly one Morsel), so read_morsels() yields one Morsel per file rather than
one per chunk. See opteryx.connectors.csv_io's module docstring.

Because each file's schema is independently sniffed by rugo (there is no
schema pinned across files the way Parquet's footer is authoritative),
every file's decoded columns/types are validated -- by physical column NAME,
since rugo's projected-column output order is not guaranteed to match the
request order -- against the bind-time schema before being emitted; a
mismatch fails loud rather than silently emitting wrongly-typed or misaligned
columns. `path` (a glob or an exact path) is resolved at bind time
(opteryx.planner.binder.dataset) into `csv_files`, a sorted, non-empty list
of matched file paths -- length 1 for a non-glob path, so there is no
separate single-file code path here.
"""

from opteryx.exceptions import DatasetReadError
from opteryx.models import QueryProperties

# BasePlanNode/ReaderNode/Morsel in scope via _operators.pyx include.


cdef class CsvReadNode(ReaderNode):
    """Read node for READ_CSV(path), backed by rugo's CSV decoder."""

    # Resolved, sorted, non-empty list of files this scan reads -- length 1
    # for a plain (non-glob) path. See opteryx.planner.binder.dataset.
    cdef public list csv_files
    cdef public list csv_physical_columns  # pushed-down projection, pre-alias physical names
    # Pushed-down predicates as rugo (physical_column_name, op, value) tuples --
    # see opteryx.planner.physical_planner._translate_csv_predicates.
    cdef public list csv_predicates
    # Resolved READ_CSV(... key => value) options, forwarded unchanged to rugo
    # on every file's decode; see opteryx.planner.binder.dataset.
    cdef public str csv_separator
    cdef public bint csv_has_header_row
    cdef public bint csv_fail_on_error
    cdef public long long csv_infer_sample_size
    cdef object _filesystem

    def __init__(self, properties: QueryProperties, **parameters) -> None:
        ReaderNode.__init__(self, properties=properties, **parameters)
        self.csv_files = list(parameters.get("csv_files") or [])
        self.csv_physical_columns = list(parameters.get("csv_physical_columns") or [])
        self.csv_predicates = list(parameters.get("csv_predicates") or [])
        csv_separator = parameters.get("csv_separator")
        self.csv_separator = "," if csv_separator is None else csv_separator
        csv_has_header_row = parameters.get("csv_has_header_row")
        self.csv_has_header_row = True if csv_has_header_row is None else csv_has_header_row
        csv_fail_on_error = parameters.get("csv_fail_on_error")
        self.csv_fail_on_error = True if csv_fail_on_error is None else csv_fail_on_error
        csv_infer_sample_size = parameters.get("csv_infer_sample_size")
        self.csv_infer_sample_size = 5 if csv_infer_sample_size is None else csv_infer_sample_size
        self._filesystem = None

    @property
    def name(self) -> str:  # pragma: no cover
        return "CSV Reader"

    def to_mermaid(self, nid):  # pragma: no cover
        mermaid = f'NODE_{nid}[("**{self.name.upper()}**<br />'
        mermaid += f"{self.dataset}<br />"
        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '")]'

    cdef object _ensure_filesystem(self):
        if self._filesystem is None:
            path = self.dataset
            protocol = path.split("://")[0] if "://" in path else ""
            if protocol in ("gs", "gcs"):
                # SECURITY: must mirror the bind-time choice in opteryx.planner.binder.
                # dataset's READ_CSV branch exactly -- READ_CSV never uses this
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

        Streams one Morsel per file in `csv_files`, in order (sequential
        fan-out -- a plain non-glob path is simply a one-file list, so this
        is the only code path for both cases).
        """
        from opteryx.connectors.csv_io import read_csv_file

        filesystem = self._ensure_filesystem()

        expected_columns = self.columns or []
        expected_physical_names = self.csv_physical_columns
        predicates = self.csv_predicates

        # physical (pre-alias) name -> expected LogicalColumn, for order-independent
        # correlation of a decoded file's vectors against the bind-time/pushed-down
        # schema -- rugo's projected-column output order is not guaranteed to match
        # the `columns=` request order, so this must be name-keyed, not positional.
        physical_to_expected = dict(zip(expected_physical_names, expected_columns))

        for path in self.csv_files:
            file_obj = filesystem.open_input_file(path)
            try:
                morsel = read_csv_file(
                    file_obj.memoryview,
                    columns=expected_physical_names,
                    predicates=predicates,
                    delimiter=self.csv_separator,
                    has_header=self.csv_has_header_row,
                    fail_on_error=self.csv_fail_on_error,
                    infer_sample_size=self.csv_infer_sample_size,
                )
            except RuntimeError as err:
                raise DatasetReadError(f"READ_CSV('{path}'): {err}") from err
            finally:
                file_obj.close()

            file_names = {
                n.decode("utf-8") if isinstance(n, bytes) else n
                for n in morsel.column_names
            }
            if file_names != set(expected_physical_names):
                raise DatasetReadError(
                    f"READ_CSV('{path}'): this file's columns {sorted(file_names)} "
                    f"do not match the expected {sorted(expected_physical_names)} from "
                    "the bind-time schema (resolved from the first file in this glob's "
                    "matched-file set)."
                )

            names = []
            vectors = []
            for physical_name in expected_physical_names:
                vector = morsel.column(physical_name.encode("utf-8"))
                expected_column = physical_to_expected[physical_name].schema_column
                if vector.type != expected_column.column_type.physical:
                    raise DatasetReadError(
                        f"READ_CSV('{path}'): column '{physical_name}' decoded as "
                        f"{vector.type!r} in this file but {expected_column.column_type.physical!r} "
                        "at bind time. rugo infers each file's schema independently "
                        "from its own sample rows, so this file's columns are not "
                        "uniform enough, or it does not match the schema resolved "
                        "from the first file in a glob's matched-file set."
                    )
                names.append(expected_column.identity)
                vectors.append(vector)

            result_morsel = Morsel.from_vectors(names, vectors)

            self.readings["columns_read"] += len(result_morsel.column_names)
            self.readings["rows_read"] += result_morsel.num_rows
            self.readings["bytes_processed"] += result_morsel.nbytes

            yield result_morsel
