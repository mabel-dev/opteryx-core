# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parquet-only decoder utilities.
"""

from typing import Optional
from typing import Tuple
from typing import Union

import opteryx.compiled.rugo.parquet as parquet_meta
import pyarrow
from opteryx.compiled.rugo.converters.orso import rugo_to_orso_schema
from opteryx.compiled.structures.memory_view_stream import MemoryViewStream
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import Node
from orso.tools import random_string
from pyarrow import parquet


def filter_records(filters: Optional[list], table: pyarrow.Table) -> pyarrow.Table:
    """
    Apply residual filters to a PyArrow table after read-time pushdown.
    """
    from opteryx.expression import evaluate
    from opteryx.expression import evaluate_and_append
    from opteryx.expression import get_all_nodes_of_type

    if isinstance(filters, list) and filters:
        filter_copy = [f.copy() for f in filters]
        root = filter_copy.pop()

        if root.left.node_type == NodeType.IDENTIFIER:
            root.left.schema_column.identity = root.left.source_column
        if root.right.node_type == NodeType.IDENTIFIER:
            root.right.schema_column.identity = root.right.source_column

        while filter_copy:
            right = filter_copy.pop()
            if right.left.node_type == NodeType.IDENTIFIER:
                right.left.schema_column.identity = right.left.source_column
            if right.right.node_type == NodeType.IDENTIFIER:
                right.right.schema_column.identity = right.right.source_column
            root = Node(
                NodeType.AND,
                left=root,
                right=right,
                schema_column=Node("schema_column", identity=random_string()),
            )
    else:
        root = filters

    function_evaluations = get_all_nodes_of_type(root, select_nodes=(NodeType.FUNCTION,))
    if function_evaluations:
        table = evaluate_and_append(function_evaluations, table)

    mask = evaluate(root, table)
    return table.filter(mask)


def parquet_decoder(
    buffer: Union[memoryview, bytes],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    force_read: bool = False,
    use_threads: bool = False,
) -> Tuple[int, int, int, pyarrow.Table]:
    """
    Decode Parquet data from an in-memory buffer.
    """
    if just_schema:
        if isinstance(buffer, memoryview):
            metadata = parquet_meta.read_metadata_from_memoryview(
                buffer, schema_only=True, max_row_groups=1, include_statistics=False
            )
        else:
            metadata = parquet_meta.read_metadata_from_memoryview(
                memoryview(buffer), schema_only=True, max_row_groups=1, include_statistics=False
            )
        return rugo_to_orso_schema(metadata, "parquet")

    if isinstance(buffer, memoryview):
        rmeta = parquet_meta.read_metadata_from_memoryview(buffer)
    else:
        rmeta = parquet_meta.read_metadata_from_memoryview(memoryview(buffer))

    if rmeta.get("row_groups"):
        schema_names = [c["name"] for c in rmeta["row_groups"][0]["columns"]]
    else:
        schema_names = []

    num_rows = rmeta.get("num_rows")
    num_columns = rmeta.get("num_columns") or len(schema_names)

    dnf_filter, processed_selection = (
        PredicatePushable.to_dnf(selection) if selection else (None, None)
    )

    projection_set = set(p.source_column for p in projection or [])
    filter_columns = {
        c.value for c in get_all_nodes_of_type(processed_selection, (NodeType.IDENTIFIER,))
    }
    selected_columns = list(projection_set.union(filter_columns).intersection(schema_names))

    if not selected_columns:
        selected_columns = None

    if isinstance(buffer, memoryview):
        buffer = MemoryViewStream(buffer)

    table = parquet.read_table(
        buffer,
        columns=selected_columns,
        pre_buffer=False,
        filters=dnf_filter,
        use_threads=use_threads,
        use_pandas_metadata=False,
    )

    if processed_selection:
        table = filter_records(processed_selection, table)

    return (
        num_rows,
        num_columns,
        0,
        table,
    )
