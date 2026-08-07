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

from opteryx.exceptions import DatasetReadError
from opteryx.models import QueryProperties

# BasePlanNode/ReaderNode/Morsel in scope via _operators.pyx include.


cdef class SkeneReadNode(ReaderNode):
    """Read node for skene datasets, backed by libskene."""

    # Manifest-ordered list of .skene files this scan reads.
    cdef public list skene_files
    # Pushed-down projection: physical (in-file) column names, parallel to
    # self.columns. Empty means COUNT(*)-style zero-column reads.
    cdef public list skene_physical_columns
    cdef object _filesystem

    def __init__(self, properties: QueryProperties, **parameters) -> None:
        ReaderNode.__init__(self, properties=properties, **parameters)
        self.skene_files = list(parameters.get("skene_files") or [])
        self.skene_physical_columns = list(parameters.get("skene_physical_columns") or [])
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

    def read_morsels(self):
        """One Morsel per .skene file, in manifest order."""
        import skene as _skene

        filesystem = self._ensure_filesystem()

        expected_columns = self.columns or []
        physical_names = self.skene_physical_columns

        for path in self.skene_files:
            file_obj = filesystem.open_input_file(path)
            try:
                data = file_obj.memoryview

                if not physical_names:
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
                file_morsel.materialize()

                names = []
                vectors = []
                for physical_name, expected in zip(physical_names, expected_columns):
                    vector = file_morsel.column(physical_name.encode("utf-8"))
                    expected_column = expected.schema_column
                    if vector.type != expected_column.column_type.physical:
                        raise DatasetReadError(
                            f"skene scan '{path}': column '{physical_name}' is "
                            f"{vector.type!r} in this file but "
                            f"{expected_column.column_type.physical!r} at bind time "
                            "(schema read from the dataset's first file). This "
                            "dataset's files do not share one schema."
                        )
                    names.append(expected_column.identity)
                    vectors.append(vector)

                result_morsel = Morsel.from_vectors(names, vectors)

                self.readings["columns_read"] += len(names)
                self.readings["rows_read"] += result_morsel.num_rows
                self.readings["bytes_processed"] += result_morsel.nbytes

                yield result_morsel
            finally:
                file_obj.close()
