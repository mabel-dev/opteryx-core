# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Sort Node

This is a SQL Query Execution Plan Node.

This node orders a dataset
"""

import pyarrow as pa
from orso.types import OrsoTypes
from pyarrow import Table
from pyarrow import concat_tables

from opteryx import EOS
from opteryx.exceptions import ColumnNotFoundError
from opteryx.expression import NodeType
from opteryx.expression import evaluate_and_append
from opteryx.models import QueryProperties

from . import BasePlanNode

CHUNK_SIZE = 65536


class SortNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.order_by = parameters.get("order_by", [])
        self.morsels = []

    @property
    def config(self):  # pragma: no cover
        return ", ".join([f"{i[0].value} {i[1][0:3].upper()}" for i in self.order_by])

    @property
    def name(self):  # pragma: no cover
        return "Sort"

    def execute(self, morsel: Table, **kwargs) -> Table:
        morsel = self.ensure_arrow_table(morsel)

        if morsel != EOS:
            if morsel.num_rows > 0:
                self.morsels.append(morsel)
            yield None
            return

        if len(self.morsels) == 0:
            yield EOS
            return

        table = concat_tables(self.morsels, promote_options="permissive")

        mapped_order = []
        evaluations = []

        for column, direction in self.order_by:
            if column.node_type == NodeType.LITERAL and column.type == OrsoTypes.INTEGER:
                # we have an index rather than a column name, it's a natural
                # number but the list of column names is zero-based, so we
                # subtract one
                column_name = table.column_names[int(column.value) - 1]
                mapped_order.append(
                    (
                        column_name,
                        direction,
                    )
                )
            else:
                if column.node_type != NodeType.IDENTIFIER:
                    evaluations.append(column)
                try:
                    mapped_order.append(
                        (
                            column.schema_column.identity,
                            direction,
                        )
                    )
                except ColumnNotFoundError as cnfe:  # pragma: no cover
                    raise ColumnNotFoundError(
                        f"`ORDER BY` must reference columns as they appear in the `SELECT` clause. {cnfe}"
                    )

        if evaluations:
            table = evaluate_and_append(evaluations, table)

        # Arrow cannot sort dictionary-encoded columns; decode any that appear in sort order
        for col_name, _ in mapped_order:
            col_idx = table.schema.get_field_index(col_name)
            if col_idx >= 0 and pa.types.is_dictionary(table.schema.field(col_name).type):
                decoded = table.column(col_name).cast(table.schema.field(col_name).type.value_type)
                table = table.set_column(col_idx, col_name, decoded)

        table = table.sort_by(mapped_order)

        num_rows = table.num_rows
        for start in range(0, num_rows, CHUNK_SIZE):
            yield table.slice(start, min(CHUNK_SIZE, num_rows - start))

        yield EOS
