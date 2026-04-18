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

from typing import Generator, Optional
from collections.abc import Iterable

from opteryx.compiled.draken.encoding import DRAKEN_ENCODING_CONSTANT
from opteryx.compiled.draken.morsels.morsel cimport Morsel
from opteryx.expression import NodeType
from opteryx.expression import evaluate_and_append
from opteryx.models import QueryProperties

from opteryx import EOS

from . import BasePlanNode

_DATA_FORMAT = "draken"


class ProjectionNode(BasePlanNode):

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
        from opteryx.expression import format_expression

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
            if getattr(col, "encoding", None) == DRAKEN_ENCODING_CONSTANT:
                emitted += 1
        return emitted

    def _execute_morsel_projection(self, morsel: Morsel):
        morsel = evaluate_and_append(self.evaluations, morsel)
        emitted = self._count_emitted_constant_literals(morsel)
        if emitted:
            self.readings["draken_constant_columns_emitted"] += emitted
        return morsel.select(self.projection)

    def execute(self, Morsel morsel):
        if morsel == EOS:
            return

        # Handle both single Morsel and Iterable of Morsels (from streaming)
        if isinstance(morsel, Morsel):
            morsels = (morsel,)
        elif isinstance(morsel, Iterable):
            morsels = morsel
        else:  # pragma: no cover
            yield None
            return

        for chunk in morsels:
            if chunk is EOS or chunk.num_rows == 0:
                continue
            yield self._execute_morsel_projection(chunk)
