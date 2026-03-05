# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Projection Node

This is a SQL Query Execution Plan Node.

This Node eliminates columns that are not needed in a Relation. This is also the Node
that performs column renames.
"""

import pyarrow

from opteryx import EOS
from opteryx.draken.morsels.morsel import Morsel
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate_and_append
from opteryx.models import QueryProperties

from . import BasePlanNode


class ProjectionNode(BasePlanNode):
    is_stateless = True

    def __init__(self, properties: QueryProperties, **parameters):
        """
        Attribute Projection, remove unwanted columns and performs column renames.
        """
        BasePlanNode.__init__(self, properties=properties, **parameters)

        projection = parameters["projection"] + parameters.get("order_by_columns", [])

        self.projection = []
        for column in projection:
            self.projection.append(column.schema_column.identity)

        self.evaluations = [
            column for column in projection if column.node_type != NodeType.IDENTIFIER
        ]

        self.columns = parameters["projection"]

    @property
    def config(self):  # pragma: no cover
        from opteryx.managers.expression import format_expression

        return ", ".join(format_expression(col) for col in self.columns)

    @property
    def name(self):  # pragma: no cover
        return "Projection"

    def _count_emitted_constant_literals(self, morsel: Morsel) -> int:
        emitted = 0
        for statement in self.evaluations:
            if statement.node_type != NodeType.LITERAL:
                continue
            identity = statement.schema_column.identity
            try:
                col = morsel.column(
                    identity if isinstance(identity, bytes) else identity.encode("utf-8")
                )
            except Exception:
                continue
            if col.__class__.__name__ == "ConstantVector":
                emitted += 1
        return emitted

    def _execute_morsel_projection(self, morsel: Morsel):
        morsel = evaluate_and_append(self.evaluations, morsel)
        emitted = self._count_emitted_constant_literals(morsel)
        if emitted:
            self.readings["draken_constant_columns_emitted"] += emitted
        return morsel.select(self.projection)

    def execute(self, morsel: pyarrow.Table, **kwargs) -> pyarrow.Table:
        if morsel == EOS:
            yield EOS
            return

        # Keep Draken morsels native when possible to preserve constant vectors.
        if isinstance(morsel, Morsel):
            yield self._execute_morsel_projection(morsel)
            return

        table = self.ensure_arrow_table(morsel)

        # Extend constant-native projection to Arrow inputs when literals are present
        # by hopping through Morsel instead of Arrow literal expansion.
        if any(statement.node_type == NodeType.LITERAL for statement in self.evaluations):
            if any(getattr(column, "num_chunks", 0) > 1 for column in table.columns):
                table = table.combine_chunks()
            try:
                yield self._execute_morsel_projection(Morsel.from_arrow(table))
                return
            except Exception:
                # Defensive fallback: preserve pre-existing Arrow behavior.
                pass

        table = evaluate_and_append(self.evaluations, table)
        yield table.select(self.projection)
