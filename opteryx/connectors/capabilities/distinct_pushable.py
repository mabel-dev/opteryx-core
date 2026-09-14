# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Marker + gate for DISTINCT pushdown into a scan.

Same contract as AggregatePushable: `supports_distinct_pushdown` promises that
ONE read of the relation yields the rows already deduplicated over the scan's
projected columns, so the local Distinct node can be removed.
"""

from typing import Any, List


class DistinctPushable:
    supports_distinct_pushdown: bool = False

    def can_push_distinct(self, columns: List[Any]) -> bool:
        """`columns` are the scan's projected columns the DISTINCT applies over.
        True only when every one can be a remote DISTINCT key with the engine's
        equality semantics."""
        return False
