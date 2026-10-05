# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Whether to search one data file through its vector index or exactly (ruled 2026-10-04).

Both paths are costed in estimated seconds, bytes AND CPU, and the cheaper one is used:

    index:  the index file      (requests x latency + bytes / throughput)
            + scoring the vectors it reads            (rows x dims x score)
            + the candidates' row groups              (requests, bytes)
            + embedding the k rows the query returns  (k x embed)
    exact:  the read set over every row group         (requests, bytes)
            + embedding every row, one worker per row group  (rows x embed / parallel)
            + scoring them                            (rows x dims x score)

What both paths pay alike (the data file's footer, embedding the query) is left out.
The constants are measured (opteryx/config.py VECTOR_COST_*). Compressed bytes per
column are estimated from the manifest: the file's bytes in proportion to the columns'
in-memory sizes. A file a figure is missing for is not costed - it keeps its index,
and the plan says so.
"""

import math
from dataclasses import dataclass
from typing import Optional

from opteryx import config

# A parquet fetch block is this many row groups (rugo's DEFAULT_ROW_GROUPS_PER_BLOCK):
# one request per projected block.
_ROW_GROUPS_PER_BLOCK = 4
# The index reader's largest single request (vector_index_file.hpp).
_INDEX_REQUEST_BYTES = 16 << 20


@dataclass(frozen=True)
class FileCost:
    index_seconds: float
    exact_seconds: float

    @property
    def use_index(self) -> bool:
        return self.index_seconds <= self.exact_seconds


def _transfer(requests: float, nbytes: float, remote: bool) -> float:
    if remote:
        return (requests * config.VECTOR_COST_REMOTE_REQUEST_SECONDS
                + nbytes / config.VECTOR_COST_REMOTE_BYTES_PER_SECOND)
    return (requests * config.VECTOR_COST_LOCAL_REQUEST_SECONDS
            + nbytes / config.VECTOR_COST_LOCAL_BYTES_PER_SECOND)


def embed_seconds_per_row(embedder: str) -> float:
    """The measured one-thread cost of embedding a row with `embedder`; refused when there
    is none, never guessed."""
    cost = config.VECTOR_COST_EMBED_SECONDS_PER_ROW.get(embedder)
    if cost is None:
        from opteryx.exceptions import InvalidConfigurationError

        raise InvalidConfigurationError(
            config_item="VECTOR_COST_EMBED_SECONDS_PER_ROW",
            provided_value=embedder,
            valid_value_description=(
                f"a measured per-row embedding cost for '{embedder}' - the vector index cost "
                f"model has one for {sorted(config.VECTOR_COST_EMBED_SECONDS_PER_ROW)} only"
            ),
        )
    return cost


def file_cost(
    *,
    remote: bool,
    rows: Optional[int],
    row_groups: Optional[int],
    file_bytes: int,
    file_uncompressed: Optional[int],
    read_uncompressed: Optional[int],
    index_bytes: int,
    index_footer_bytes: int,
    k: int,
    nprobe: int,
    clusters: int,
    dimensions: int,
    embed_per_row: float,
    workers: int,
) -> Optional[FileCost]:
    """Both paths' estimated seconds for one indexed data file, or None when a figure the
    estimate needs is unknown."""
    if (not rows or not row_groups or not file_bytes or not file_uncompressed
            or read_uncompressed is None):
        return None
    read_bytes = file_bytes * min(1.0, read_uncompressed / file_uncompressed)
    blocks = math.ceil(row_groups / _ROW_GROUPS_PER_BLOCK)
    score = config.VECTOR_COST_SCORE_SECONDS_PER_ROW_DIM * dimensions

    exact = (_transfer(blocks, read_bytes, remote)
             + rows * embed_per_row / min(workers, row_groups)
             + rows * score)

    # nprobe 0 reads every block; a probe reads its share of the clusters (K as the build
    # chose it: the definition's, else round(sqrt(rows))).
    body = index_bytes - index_footer_bytes
    if nprobe > 0:
        k_clusters = clusters if clusters > 0 else max(1, round(math.sqrt(rows)))
        share = min(1.0, nprobe / k_clusters)
    else:
        share = 1.0
    index_read = body * share
    candidate_groups = min(k, row_groups)
    index = (_transfer(1 + math.ceil(index_read / _INDEX_REQUEST_BYTES),
                       index_footer_bytes + index_read, remote)
             + rows * share * score
             + _transfer(min(candidate_groups, blocks), read_bytes * candidate_groups / row_groups,
                         remote)
             + min(k, rows) * embed_per_row)
    return FileCost(index_seconds=index, exact_seconds=exact)
