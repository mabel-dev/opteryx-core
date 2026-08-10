# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
ANALYZE for catalog-backed datasets.

The local-filesystem path (``_analyze.py``) owns the whole statistics
computation itself — listing blobs, running the native per-file kernels,
writing the dataset's single manifest. A catalog-backed dataset works the
opposite way round: the catalog already owns its manifest, its snapshot
chain, and the very same native statistics pipeline (its
``build_parquet_manifest_entry_from_bytes`` runs the identical draken
kernels), so ANALYZE here is a thin delegation to
``SimpleDataset.refresh_manifest`` rather than a second implementation of
statistics collection.

`refresh_manifest` re-reads every file in the dataset's current manifest,
recomputes its statistics, and commits a new snapshot
(``operation_type="statistics-refresh"``) — it never rewrites, merges, or
splits the underlying data files. That is deliberately a different, lighter
operation than compaction (``DatasetCompactor.compact()``).
"""

from __future__ import annotations

from typing import Optional
from typing import Sequence

from opteryx.exceptions import UnsupportedSyntaxError


def analyze_table_catalog(
    table_engine, columns: Optional[Sequence[str]], author: Optional[str] = None
) -> int:
    """Refresh a catalog-backed dataset's statistics via the catalog itself.

    Returns the number of datasets analyzed (always 1 — the catalog refreshes
    the dataset as one unit and does not report a per-file count, unlike the
    local path which returns its own file count).
    """
    if columns:
        # Rejected before any refresh work starts. The catalog's stats builder
        # has no column-subset concept — it always recomputes every column of
        # every file — so honouring FOR COLUMNS here would mean silently doing
        # something other than what the SQL asked for.
        raise UnsupportedSyntaxError(
            "**ANALYZE TABLE** ... FOR COLUMNS is not supported for this dataset; "
            "**ANALYZE TABLE** <table> (without FOR COLUMNS) refreshes statistics "
            "for all columns."
        )

    table_engine.table.refresh_manifest(agent="opteryx-analyze", author=author)
    return 1
