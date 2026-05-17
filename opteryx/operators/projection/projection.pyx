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

from draken.core.buffers cimport DrakenVector
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.float64_vector cimport Float64Vector
from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.integer_vector cimport IntegerVector
from draken.vectors.string_vector cimport StringVector
from opteryx.expression import NodeType
# evaluate_and_append_draken in scope from _operators evaluator includes
from opteryx.models import QueryProperties

# BasePlanNode in scope via textual include from _operators.pyx.


cdef inline bint _is_constant_vector(object vec) noexcept:
    cdef DrakenVector* uv
    if isinstance(vec, Float64Vector):
        uv = (<Float64Vector>vec).unified()
    elif isinstance(vec, Int64Vector):
        uv = (<Int64Vector>vec).unified()
    elif isinstance(vec, IntegerVector):
        uv = (<IntegerVector>vec).unified()
    elif isinstance(vec, BoolVector):
        uv = (<BoolVector>vec).unified()
    elif isinstance(vec, StringVector):
        uv = (<StringVector>vec).unified()
    else:
        return False
    return uv.data_length == 1


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

    cdef Py_ssize_t _count_emitted_constant_literals(self, Morsel morsel) except -1:
        cdef Py_ssize_t emitted = 0
        for statement in self.evaluations:
            if statement.node_type != NodeType.LITERAL:
                continue
            identity = statement.schema_column.identity
            try:
                col = morsel.column(identity)
            except Exception:
                continue
            if _is_constant_vector(col):
                emitted += 1
        return emitted

    cdef Morsel _execute_morsel_projection(self, Morsel morsel):
        cdef Py_ssize_t emitted
        morsel = evaluate_and_append_draken(self.evaluations, morsel)
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
