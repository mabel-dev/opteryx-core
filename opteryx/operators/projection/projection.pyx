# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

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

from draken.encoding import DRAKEN_ENCODING_CONSTANT
from opteryx.expression import NodeType
from opteryx.expression import evaluate_and_append
from opteryx.models import QueryProperties

# BasePlanNode in scope via textual include from _operators.pyx.


cdef class ProjectionNode(BasePlanNode):
    cdef public list projection
    cdef public list evaluations

    def __init__(self, properties=None, **parameters):
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

    def _count_emitted_constant_literals(self, morsel):
        emitted = 0
        for statement in self.evaluations:
            if statement.node_type != NodeType.LITERAL:
                continue
            identity = statement.schema_column.identity
            try:
                col = morsel.column(identity)
            except Exception:
                continue
            if getattr(col, "encoding", None) == DRAKEN_ENCODING_CONSTANT:
                emitted += 1
        return emitted

    def _execute_morsel_projection(self, morsel):
        morsel = evaluate_and_append(self.evaluations, morsel)
        emitted = self._count_emitted_constant_literals(morsel)
        if emitted:
            self.readings["draken_constant_columns_emitted"] = \
                self.readings.get("draken_constant_columns_emitted", 0) + emitted
        return morsel.select(self.projection)

    cdef void _dispatch_push(self, Morsel morsel) except *:
        if morsel is _EOS_SENTINEL:
            self._emit_cdef(morsel)
            return

        # Single-morsel case is the only path the push pipeline uses; the
        # legacy iterable-of-morsels handling came from streaming from old
        # scan APIs and is no longer reachable here.
        if morsel.num_rows == 0:
            return
        self._emit_cdef(self._execute_morsel_projection(morsel))
