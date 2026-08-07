# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Skene Read Node

Scan operator for `.skene` datasets. One `.skene` file IS one row group:
libskene's read_morsel reconstructs the whole file as a single draken Morsel
(zero copy across the boundary — the vectors in the emitted Morsel are the
buffers skene rebuilt), so this reader is one morsel per file, streamed in
manifest order.

Projection is pushed natively: skene's per-column contiguous extents mean an
unprojected column's bytes are never interpreted (whole-file bytes are still
fetched in this phase — the footer-extent ranged-read path is the native scan
source's job, not this operator's).

Predicates are NOT pushed in this phase — FileSystemTable.can_push declines
for skene datasets, so filters stay above the scan (a missed optimization,
never a dropped predicate). The footer's statistics/zone-map/bloom pruning
arrives with the native scan source.

Schema is not inferred and not sampled: every file's footer carries the exact
DrakenType + LogicalType per column, and every decoded file is validated
against the bind-time schema by name and physical type — a divergent file in
a dataset fails loud, naming the file.
"""

from libcpp.memory cimport shared_ptr
from libcpp.string cimport string

from draken.morsels.cxx_morsel cimport CxxMorsel

from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import QueryProperties

# BasePlanNode/ReaderNode/Morsel/morsel_to_cxx in scope via _operators.pyx include.


cdef class SkeneReadNode(ReaderNode):
    """Read node for skene datasets, backed by libskene."""

    # Manifest-ordered list of .skene files this scan reads.
    cdef public list skene_files
    # Pushed-down projection: physical (in-file) column names, parallel to
    # self.columns. Empty means COUNT(*)-style zero-column reads.
    cdef public list skene_physical_columns
    # Pushed predicates, lowered by compiler._compile_scan through the shared
    # rewrite chain. Pushed predicates are REMOVED from the plan, so applying
    # this per morsel is a correctness obligation, not an optimization.
    cdef public object compiled_predicate
    cdef object _filesystem

    def __init__(self, properties: QueryProperties, **parameters) -> None:
        ReaderNode.__init__(self, properties=properties, **parameters)
        self.skene_files = list(parameters.get("skene_files") or [])
        self.skene_physical_columns = list(parameters.get("skene_physical_columns") or [])
        self.compiled_predicate = None  # set at plan time by compiler._compile_scan
        self._filesystem = None

    @property
    def name(self) -> str:  # pragma: no cover
        return "Skene Reader"

    def to_mermaid(self, nid):  # pragma: no cover
        mermaid = f'NODE_{nid}[("**{self.name.upper()}**<br />'
        mermaid += f"{self.dataset}<br />"
        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '")]'

    cdef object _ensure_filesystem(self):
        if self._filesystem is None:
            # Dataset Scans attach a connector table that already holds the
            # right filesystem (platform credentials for gs:// catalog data).
            connector_filesystem = getattr(self.connector, "filesystem", None)
            if connector_filesystem is not None:
                self._filesystem = connector_filesystem
            else:
                from opteryx.connectors.io_systems import create_filesystem

                path = self.dataset
                protocol = path.split("://")[0] if "://" in path else ""
                self._filesystem = create_filesystem(protocol)
        return self._filesystem

    cdef Morsel _rename_to_identities(self, Morsel file_morsel, dict identity_by_physical, str path):
        """Validate column types against the bind-time schema and rename the
        columns to their planner identities on the CxxMorsel, returning a
        FRESH Cxx-backed wrapper over the renamed morsel.

        Name-keyed, not positional — like rugo, the reader's output order is
        not part of the contract this operator relies on. The re-wrap is a
        struct copy only (buffers stay shared): the Morsel wrapper caches its
        name list at wrap time, so an in-place rename under the original
        wrapper would leave its column_names stale.
        """
        cdef shared_ptr[CxxMorsel] sp = morsel_to_cxx(file_morsel)
        cdef CxxMorsel* mp = sp.get()
        cdef size_t i
        for i in range(mp.names.size()):
            decoded_name = mp.names[i].decode("utf-8")
            expected_column = identity_by_physical.get(decoded_name)
            if expected_column is None:
                raise DatasetReadError(
                    f"skene scan '{path}': decoded unexpected column "
                    f"'{decoded_name}' — not in the projected set "
                    f"{sorted(identity_by_physical)}."
                )
            # .value: DrakenType is a nanobind enum — it compares equal only to
            # itself, never to a bare int, so the C-side tag must be compared
            # against the enum's integer value.
            if <int>mp.columns[i].view.type != <int>expected_column.column_type.physical.value:
                raise DatasetReadError(
                    f"skene scan '{path}': column '{decoded_name}' is "
                    f"type {<int>mp.columns[i].view.type} in this file but "
                    f"{expected_column.column_type.physical!r} at bind time "
                    "(schema read from the dataset's first file). This "
                    "dataset's files do not share one schema."
                )
            identity = expected_column.identity
            if isinstance(identity, str):
                identity = identity.encode("utf-8")
            mp.names[i] = <string>identity
        return cxx_to_morsel(sp)

    def read_morsels(self):
        """One Morsel per .skene file, in manifest order."""
        import skene as _skene

        from opteryx.expression.evaluator import execute_bytecode
        from opteryx.expression.evaluator.evaluation import (
            filter_morsel_c_native as _filter_morsel_c_native,
        )

        filesystem = self._ensure_filesystem()

        expected_columns = self.columns or []
        physical_names = self.skene_physical_columns

        for path in self.skene_files:
            file_obj = filesystem.open_input_file(path)
            try:
                data = file_obj.memoryview

                if not physical_names:
                    if self.compiled_predicate is not None:
                        # A pushed predicate references at least one column, and
                        # projection pushdown always keeps predicate columns in
                        # the scan's column set — an empty projection alongside
                        # a predicate is an invariant break, and counting
                        # unfiltered rows here would be a silent wrong answer.
                        raise InvalidInternalStateError(
                            f"skene scan '{path}': pushed predicate with an "
                            "empty projection — predicate columns missing from "
                            "the scan's column set."
                        )
                    # An EMPTY projection is "this query reads no columns"
                    # (COUNT(*)), not "a file with zero columns": emit a genuine
                    # ZERO-COLUMN morsel whose row count rides on zero_col_rows
                    # (select([])) — the contract CountStar reads. Reading one
                    # real column bounds the work; the footer's row_count alone
                    # cannot build a morsel.
                    metadata = _skene.read_metadata(data)
                    footer_columns = metadata["columns"]
                    narrow = [footer_columns[0]["name"]] if footer_columns else None
                    count_morsel = _skene.read_morsel(data, columns=narrow)
                    result_morsel = count_morsel.select([])

                    self.readings["rows_read"] += result_morsel.num_rows
                    self.readings["bytes_processed"] += len(data)

                    yield result_morsel
                    continue

                try:
                    file_morsel = _skene.read_morsel(data, columns=physical_names)
                except _skene.SkeneError as err:
                    # A missing column names a file that diverges from the
                    # bind-time schema (resolved from the first file).
                    raise DatasetReadError(f"skene scan '{path}': {err}") from err

                # The morsel is Cxx-backed and the engine runs on the Cxx
                # substrate — keep it there. Validate types and rename the
                # columns to their planner identities directly on the
                # CxxMorsel (we exclusively own this fresh instance), instead
                # of materializing PyObject wrappers and rebuilding via
                # from_vectors: that round-trip was pure boundary waste.
                identity_by_physical = {
                    physical_name: expected.schema_column
                    for physical_name, expected in zip(physical_names, expected_columns)
                }
                result_morsel = self._rename_to_identities(
                    <Morsel>file_morsel, identity_by_physical, path
                )

                self.readings["columns_read"] += len(physical_names)
                self.readings["rows_read"] += result_morsel.num_rows
                self.readings["bytes_processed"] += len(data)

                if self.compiled_predicate is not None:
                    # Same application as the parquet scan: c-native filter
                    # first (no PyObject columns), VM + filter_mask fallback
                    # for anything the native path declines.
                    filtered = _filter_morsel_c_native(self.compiled_predicate, result_morsel)
                    if filtered is None:
                        filtered = result_morsel.filter_mask(
                            execute_bytecode(self.compiled_predicate, result_morsel)
                        )
                    result_morsel = <Morsel>filtered
                    if result_morsel.num_rows == 0:
                        # Every row of this file was filtered out — a legitimate
                        # empty result; the file contributes nothing.
                        continue

                yield result_morsel
            finally:
                file_obj.close()
