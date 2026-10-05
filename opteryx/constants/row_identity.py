# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Physical row address columns.

A scan asked for row identity emits two extra columns that are not in the
relation's schema and are not read from the data file - they describe WHERE
each row came from, which is the coordinate `delete_rows`/`merge_commit`
address rows by:

    (data-file path, file-local zero-based ordinal in physical row order)

Both are INT64. A file index would fit UINT32 comfortably, but
`vector_from_sequence` has no unsigned constructors, so an unsigned declaration
would be a type the scan cannot actually build. Widen to unsigned only once
that gap is closed.

`$file` carries an index into the scan's own ordered file list rather than the
path itself: the scan already holds that list, so the index is exact and free,
where a per-row path string would be a string payload dragged through a join
for no information gain. The consumer maps index -> path through the same list.

MERGE, UPDATE, DELETE and OPTIMIZE (carrying a vector index) ask for these. They
are deliberately NOT a general SQL surface - the names are unspellable in user SQL
by convention (a leading `$` marks engine-internal columns), and the binder only
adds them when the planner set `emit_row_identity` on the Scan.

The native parquet scan (NativeParquetScanSource) appends them after its read set,
numbered from the footer's row-group row counts and the rows each decode kept
(rugo's MorselRef::kept_rows), so deletes, page pruning and the worker prefilter
all keep the address exact.

⚠️ A scan projecting `$ordinal` must run SINGLE-PASS. The two-pass late
materialization path renumbers rows between its passes, so a row's position no
longer equals its file ordinal - the ordinal it produced would address a
different row. The compiler never routes a row-identity scan to the latmat Source;
do not relax that without making pass 2 carry the ordinal itself.
"""

# The column names, as they appear in the Scan's schema and in the synthesized
# SQL the MERGE planner builds.
ROW_IDENTITY_FILE = "$file"
ROW_IDENTITY_ORDINAL = "$ordinal"

ROW_IDENTITY_COLUMNS = (ROW_IDENTITY_FILE, ROW_IDENTITY_ORDINAL)

# What OPTIMIZE names the two when the relation has a vector index: the compaction sink
# records each written row's origin under these (natively, never written to a file) to
# carry the inputs' vectors into the outputs' index files (docs/VECTOR_INDEX_DESIGN.md §5.6).
COMPACTION_ORIGIN_FILE = "$carry_file"
COMPACTION_ORIGIN_ORDINAL = "$carry_ordinal"
