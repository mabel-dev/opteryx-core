# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Marker + gate for aggregate (GROUP BY / aggregate function) pushdown into a scan.

`supports_aggregate_pushdown` means: ONE read of this relation produces the
COMPLETE aggregate over the whole relation. That is what lets
`AggregateScanPushdownStrategy` REMOVE the local Aggregate node — there is no
partial result left to combine. A source that splits a relation across files,
partitions or workers cannot make this promise and must leave the flag False;
a per-source partial aggregate with a local combine is a different design.

`can_push_aggregate(groups, aggregates)` is asked per plan shape: the connector
declines any key or function it cannot render with the engine's exact
semantics (result type, NULL handling, string ordering). Declining leaves the
plan untouched; accepting a shape the connector then cannot build is a bug.
"""

from typing import Any, List


class AggregatePushable:
    supports_aggregate_pushdown: bool = False

    def can_push_aggregate(self, groups: List[Any], aggregates: List[Any]) -> bool:
        """`groups` are the GROUP BY key expressions (empty for an ungrouped
        aggregate); `aggregates` the AGGREGATOR nodes. True only when the whole
        shape can be answered remotely with the engine's semantics."""
        return False
