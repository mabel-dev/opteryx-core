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

NOTE — the execution path for a projecting skene scan is now
src/cpp/engine/native_skene_scan_source.hpp (zero-Python, parallel: workers
claim row groups from an atomic counter and decode independently). This operator
remains the plan-node carrier (it holds skene_files / skene_read_schema_columns
for the compiler) and still SERVES the bare zero-projection COUNT(*) shape,
which needs the materialized path's genuine zero-column morsel. read_morsels()
below is therefore the declining-shape path, not the main one.

Scan operator for `.skene` datasets. A `.skene` file holds one or more ROW
GROUPS, and libskene reconstructs one row group at a time as a draken Morsel
(zero copy across the boundary — the vectors in the emitted Morsel are the
buffers skene rebuilt), so this reader is one morsel per ROW GROUP, streamed in
manifest order and then row group order.

Projection is pushed natively: skene's per-column contiguous extents mean an
unprojected column's bytes are never interpreted (whole-file bytes are still
fetched in this phase — the footer-extent ranged-read path is the native scan
source's job, not this operator's).

Predicates ARE pushed for skene (architect ruling, 2026-08-21 — reversing the
earlier decline), but NOT into THIS operator: they are applied by
NativeSkeneScanSource, inside its decode workers. `can_push` accepting means
the pushdown strategy CONSUMES the Filter node, so a pushed predicate that
reached this reader would be silently dropped and the answer would be wrong.
This reader therefore refuses one outright (see read_morsels) rather than
ignoring it — and the compiler never routes one here, because the read set of
a scan with predicates is non-empty by construction, which is exactly the
condition that selects the native Source.

FILE-level pruning is unchanged and happens at plan time either way: the
manifest pruning strategy prunes from `node.predicates` and from parent Filter
nodes (footer min/max ordinals in the manifest bounds), consuming neither.
Zone-map/bloom ROW-GROUP skipping is still to come, in the native Source.

Schema is not inferred and not sampled: every file's footer carries the exact
DrakenType + LogicalType per column, and every decoded file is validated
against the bind-time schema by name and physical type — a divergent file in
a dataset fails loud, naming the file. The single sanctioned exception is a
scan-declared INT64→TIMESTAMP64 retag (TimestampCastSinkStrategy sinking a
`col::TIMESTAMP[unit]` into the scan): an allowlist of one verbatim retag,
matching NativeSkeneScanSource, not a loosening of the check.
"""

from libc.stdint cimport uint32_t
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string

from draken.morsels.cxx_morsel cimport CxxMorsel
from draken.morsels.cxx_morsel cimport cxx_column_retag_timestamp64

from opteryx.exceptions import DatasetReadError
from opteryx.models import QueryProperties

# TimestampUnit enum-name → draken unit code (logical_type.h TimestampUnit),
# mirroring the compiler's `_TS_UNIT_TO_INT`; microseconds is the same default.
_TS_UNIT_TO_CODE = {"SECONDS": 0, "MILLISECONDS": 1, "MICROSECONDS": 2, "NANOSECONDS": 3}


cdef inline unsigned char _timestamp_unit_code(object schema_column):
    """draken unit code for a column the plan declares TIMESTAMP64."""
    logical = schema_column.column_type.logical
    if logical is None or logical.unit is None:
        return 2
    return _TS_UNIT_TO_CODE.get(logical.unit.name, 2)

# BasePlanNode/ReaderNode/Morsel/morsel_to_cxx in scope via _operators.pyx include.


cdef class SkeneReadNode(ReaderNode):
    """Read node for skene datasets, backed by libskene."""

    # Manifest-ordered list of .skene files this scan reads.
    cdef public list skene_files
    # The schema columns this scan DECODES: projection ∪ pushed-predicate
    # columns (physical name = schema_column.name; see _skene_scan_config).
    # Empty means COUNT(*)-style zero-column reads with no predicates.
    cdef public list skene_read_schema_columns
    cdef object _filesystem

    def __init__(self, properties: QueryProperties, **parameters) -> None:
        ReaderNode.__init__(self, properties=properties, **parameters)
        self.skene_files = list(parameters.get("skene_files") or [])
        self.skene_read_schema_columns = list(parameters.get("skene_read_schema_columns") or [])
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
            file_type = <int>mp.columns[i].view.type
            bound_type = <int>expected_column.column_type.physical.value
            if file_type != bound_type:
                # The ONE permitted divergence, matching NativeSkeneScanSource:
                # the plan declares TIMESTAMP64 for a column this file stores as
                # INT64, because TimestampCastSinkStrategy sank a
                # `col::TIMESTAMP[unit]` into the scan. INT64 and TIMESTAMP64
                # share the same 8-byte payload and these units keep the integer
                # verbatim, so this is a pure retag — not schema drift. Every
                # other mismatch still fails loud.
                # DRAKEN_* are the C-level tags cimported by the enclosing
                # _operators.pyx (this file is include'd into it).
                if not (bound_type == <int>DRAKEN_TIMESTAMP64
                        and file_type == <int>DRAKEN_INT64):
                    raise DatasetReadError(
                        f"skene scan '{path}': column '{decoded_name}' is "
                        f"type {file_type} in this file but "
                        f"{expected_column.column_type.physical!r} at bind time "
                        "(schema read from the dataset's first file). This "
                        "dataset's files do not share one schema."
                    )
                if not cxx_column_retag_timestamp64(
                    mp, <uint32_t>i, _timestamp_unit_code(expected_column)
                ):
                    raise DatasetReadError(
                        f"skene scan '{path}': column '{decoded_name}' could not "
                        "be retagged to TIMESTAMP64 — the decoded column is "
                        "unowned or not INT64."
                    )
            identity = expected_column.identity
            if isinstance(identity, str):
                identity = identity.encode("utf-8")
            mp.names[i] = <string>identity
        return cxx_to_morsel(sp)

    def read_morsels(self):
        """One Morsel per ROW GROUP, in manifest order then row group order."""
        import skene as _skene

        if self.predicates:
            # can_push accepts for skene, so a pushed predicate has had its Filter
            # node removed from the plan — this reader applying nothing would
            # return unfiltered rows, silently. The compiler routes every scan
            # with predicates to NativeSkeneScanSource (their columns make the read
            # set non-empty, which is that path's admission test), so reaching here
            # means that routing broke. Fail, do not scan.
            raise DatasetReadError(
                f"skene scan '{self.dataset}': {len(self.predicates)} pushed "
                "predicate(s) reached the materialized reader, which cannot apply "
                "them — NativeSkeneScanSource is the only skene path with a "
                "reader-side filter."
            )

        filesystem = self._ensure_filesystem()

        read_schema_columns = self.skene_read_schema_columns
        physical_names = [sc.name for sc in read_schema_columns]
        identity_by_physical = {sc.name: sc for sc in read_schema_columns}
        projection_identities = [c.schema_column.identity for c in (self.columns or [])]
        # Predicate-only columns must not leave the scan: after filtering,
        # select back down to exactly the plan's projection. For COUNT(*)
        # WHERE this yields the genuine zero-column morsel whose (filtered)
        # row count rides on zero_col_rows — the CountStar contract.
        needs_select = len(read_schema_columns) != len(projection_identities)

        for path in self.skene_files:
            file_obj = filesystem.open_input_file(path)
            try:
                data = file_obj.memoryview

                # The FILE footer, once per file: cheap (no row group footer, no
                # section directory) and it is what says how many row groups
                # there are to iterate.
                metadata = _skene.read_metadata(data)
                row_group_count = len(metadata["row_groups"])

                for row_group in range(row_group_count):
                    if not physical_names:
                        # An EMPTY projection is "this query reads no columns"
                        # (COUNT(*)), not "a row group with zero columns": emit a
                        # genuine ZERO-COLUMN morsel whose row count rides on
                        # zero_col_rows (select([])) — the contract CountStar
                        # reads. Reading one real column bounds the work; the
                        # footer's row_count alone cannot build a morsel.
                        footer_columns = metadata["columns"]
                        narrow = [footer_columns[0]["name"]] if footer_columns else None
                        count_morsel = _skene.read_morsel(data, row_group, columns=narrow)
                        result_morsel = count_morsel.select([])

                        self.readings["rows_read"] += result_morsel.num_rows

                        yield result_morsel
                        continue

                    try:
                        file_morsel = _skene.read_morsel(
                            data, row_group, columns=physical_names
                        )
                    except _skene.SkeneError as err:
                        # A missing column names a file that diverges from the
                        # bind-time schema (resolved from the first file).
                        raise DatasetReadError(
                            f"skene scan '{path}' row group {row_group}: {err}"
                        ) from err

                    # The morsel is Cxx-backed and the engine runs on the Cxx
                    # substrate — keep it there. Validate types and rename the
                    # columns to their planner identities directly on the
                    # CxxMorsel (we exclusively own this fresh instance), instead
                    # of materializing PyObject wrappers and rebuilding via
                    # from_vectors: that round-trip was pure boundary waste.
                    result_morsel = self._rename_to_identities(
                        <Morsel>file_morsel, identity_by_physical, path
                    )

                    self.readings["columns_read"] += len(physical_names)
                    self.readings["rows_read"] += result_morsel.num_rows

                    if needs_select:
                        result_morsel = result_morsel.select(projection_identities)

                    yield result_morsel

                # Counted once per FILE, not once per row group: it is the bytes
                # the scan is responsible for, and adding len(data) per row group
                # would multiply it by the packing factor.
                self.readings["bytes_processed"] += len(data)
            finally:
                file_obj.close()
