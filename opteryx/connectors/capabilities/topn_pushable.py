# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Marker + gate for top-N (ORDER BY ... LIMIT n) pushdown into a scan.

`TopNScanPushdownStrategy` matches a HeapSort reading directly from a Scan and
asks the scan's connector, through `can_push_topn`, whether it can honour the
sort spec. A connector that says yes receives the spec on the scan node
(`topn_order_by`, `topn_limit`) and must return AT MOST the rows the spec
selects, or a superset of them: the HeapSort above is retained and makes the
canonical cut, so the pushed top-N is an optimisation, never the answer.

Each connector's `can_push_topn` encodes what ITS reader can order by. The
parquet reader sorts on ONE physical column; a SQL server takes any key list.
"""

from typing import Any, List, Tuple

from opteryx.expression import NodeType


class TopNPushable:
    supports_topn_pushdown: bool = False

    def can_push_topn(self, order_by: List[Tuple[Any, bool]]) -> bool:
        """`order_by` is the HeapSort's list of (expression, ascending). Return
        True only when every key can be honoured by this reader."""
        return False


def single_physical_column_topn(order_by) -> bool:
    """The parquet reader's rule: exactly one key, a plain column reference with
    a bound physical name and identity (the read path resolves it by name).
    Shared by every table whose scan is served by ParquetReadNode."""
    if not order_by or len(order_by) != 1:
        return False
    expression, _ascending = order_by[0]
    if expression.node_type != NodeType.IDENTIFIER:
        return False
    schema_column = expression.schema_column
    if schema_column is None:
        return False
    return bool(schema_column.name) and bool(schema_column.identity)
