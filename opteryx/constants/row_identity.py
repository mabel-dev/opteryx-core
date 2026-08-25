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

Only MERGE asks for these today. They are deliberately NOT a general SQL
surface - the names are unspellable in user SQL by convention (a leading `$`
marks engine-internal columns), and `visit_scan` only materialises them when
the planner set `emit_row_identity` on the Scan.

⚠️ A scan projecting `$ordinal` must run SINGLE-PASS. The two-pass late
materialization path renumbers rows between its passes, so a row's position no
longer equals its file ordinal - the ordinal it produced would address a
different row. `parquet_read.pyx` gates on this; do not relax it without making
pass 2 carry the ordinal itself.
"""

# The column names, as they appear in the Scan's schema and in the synthesized
# SQL the MERGE planner builds.
ROW_IDENTITY_FILE = "$file"
ROW_IDENTITY_ORDINAL = "$ordinal"

ROW_IDENTITY_COLUMNS = (ROW_IDENTITY_FILE, ROW_IDENTITY_ORDINAL)
