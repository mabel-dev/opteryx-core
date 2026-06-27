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
from draken.vectors.vector cimport Vector
from opteryx.expression import NodeType
from opteryx.expression.evaluator import compile_eval_nodes, execute_and_append
from opteryx.models import QueryProperties

# BasePlanNode in scope via textual include from _operators.pyx.


cdef inline bint _is_constant_vector(Vector vec) noexcept:
    """Telemetry-only observation of constant layout. Do not use for dispatch.

    Reads the unified view's data_length; constant layout means
    data_length == 1 regardless of vector type.
    """
    if vec is None:
        return False
    return vec.unified().data_length == 1


cdef class ProjectionNode(BasePlanNode):
    cdef public list projection
    cdef public list _compiled_evals
    cdef public set _literal_identities

    def __init__(self, properties=None, **parameters):
        """
        Attribute Projection, remove unwanted columns and performs column renames.
        """
        BasePlanNode.__init__(self, properties=properties, **parameters)

        # Both `projection` and `order_by_columns` may arrive as None (not just
        # absent): the optimizer treats a node's column lists as "iterable or None"
        # (projection_pushdown.py), and the physical planner forwards a None
        # `order_by_columns` verbatim. This fires e.g. on COUNT(*) over a subquery,
        # where pushdown leaves the inner Project with no ORDER BY columns. Normalise
        # both to empty lists — None means "no columns here", never a passthrough.
        proj = parameters["projection"] or []
        projection = proj + (parameters.get("order_by_columns") or [])

        self.projection = []
        for column in projection:
            self.projection.append(column.schema_column.identity)

        eval_nodes = [column for column in projection if column.node_type != NodeType.IDENTIFIER]
        self._compiled_evals = compile_eval_nodes(eval_nodes)
        self._literal_identities = {
            column.schema_column.identity
            for column in eval_nodes
            if column.node_type == NodeType.LITERAL
        }

        self.columns = proj

    @property
    def config(self):  # pragma: no cover
        from opteryx.expression import format_expression

        return ", ".join(format_expression(col) for col in self.columns)

    @property
    def name(self):  # pragma: no cover
        return "Projection"

    cdef Py_ssize_t _count_emitted_constant_literals(self, Morsel morsel) except -1:
        cdef Py_ssize_t emitted = 0
        for identity in self._literal_identities:
            try:
                col = morsel.column(identity)
            except Exception:
                continue
            if _is_constant_vector(col):
                emitted += 1
        return emitted

    cdef Morsel _execute_morsel_projection(self, Morsel morsel):
        cdef Py_ssize_t emitted
        morsel = execute_and_append(self._compiled_evals, morsel)
        emitted = self._count_emitted_constant_literals(morsel)
        if emitted:
            self.readings["draken_constant_columns_emitted"] = \
                self.readings.get("draken_constant_columns_emitted", 0) + emitted
        return morsel.select(self.projection)

    cpdef void _push_impl(self, Morsel morsel) except *:
        # Body runs GIL-held: the base nogil `_dispatch_push` decodes the C++
        # carrier and calls this, surfacing any exception via the ErrCtx path.
        if morsel is _EOS_SENTINEL:
            self.emit(morsel)
            return

        # Single-morsel case is the only path the push pipeline uses; the
        # legacy iterable-of-morsels handling came from streaming from old
        # scan APIs and is no longer reachable here.
        if morsel.num_rows == 0:
            return
        self.emit(self._execute_morsel_projection(morsel))
