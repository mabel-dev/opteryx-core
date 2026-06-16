# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Exchange (hash-partition / shuffle) operator — M4 central scheduler.

Repartitions input rows into N bins by the hash of the partition-key columns, so
every instance of a group key routes to exactly one bin (bins are row-disjoint
and group-disjoint). Downstream per-bin aggregation therefore needs NO cross-bin
merge — which is the whole point: it removes the serial `merge()` Amdahl term that
caps round-robin+merge at high cardinality.

Separation of concerns (Volcano-style exchange): this operator owns the partition
*logic*; the central scheduler owns the worker *threads*. The routing hash and the
counting-partition live in the draken kernel (`Morsel.partition_by_hash`) — for a
single string key it folds the german-string slot `hash32` (no arena re-hash); for
multi-column / fixed-width keys it mixes the per-column hashes.

See docs/M4_CENTRAL_SCHEDULER_DESIGN.md §11.
"""

from opteryx.operators import BasePlanNode


class ExchangeNode(BasePlanNode):
    def __init__(self, properties=None, **parameters):
        super().__init__(properties=properties, **parameters)
        # Identities of the partition-key columns (the group-by columns).
        self.partition_columns = list(parameters["partition_columns"])

    @property
    def name(self):  # pragma: no cover
        return "Exchange"

    @property
    def config(self):  # pragma: no cover
        return f"HASH PARTITION ({', '.join(map(str, self.partition_columns))})"

    def partition(self, morsel, n_bins):
        """Return `n_bins` row-disjoint sub-morsels, rows routed by
        `hash(partition_columns) & (n_bins - 1)`. `n_bins` must be a power of two.
        Identical keys always land in the same bin; NULL keys collide to one bin.
        """
        return morsel.partition_by_hash(self.partition_columns, n_bins)

    def _push_impl(self, morsel):  # pragma: no cover
        # Not wired into the push pipeline yet — the parallel scheduler drives
        # partition() directly (integration depth #1). Plan-insertion as a 1→N
        # fan-out node is integration depth #2 (see the design doc).
        raise NotImplementedError(
            "ExchangeNode is driven by the parallel scheduler, not the push pipeline"
        )
