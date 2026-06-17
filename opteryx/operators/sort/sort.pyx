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
Sort Node

This is a SQL Query Execution Plan Node.

This node orders a dataset using a permutation-based sort (C++ std::sort /
std::stable_sort with memcmp tiebreak) over Draken morsels. Dictionary-encoded
columns are ORDER BY-correct (codes are remapped to value rank before sorting,
with AVX2/NEON SIMD acceleration for uint8 codes).
"""

from typing import Generator, Optional
from opteryx.compiled.morsel_ops.sort import morsel_sort
from opteryx.exceptions import ColumnNotFoundError
from opteryx.expression import NodeType
from opteryx.expression.evaluator import compile_eval_nodes, execute_and_append
from opteryx.models import QueryProperties

# BasePlanNode in scope via textual include from _operators.pyx.


cdef class SortNode(BasePlanNode):
    cdef public list order_by
    cdef public list _morsels
    cdef public list _compiled_evals

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.order_by = parameters.get("order_by", [])
        self._morsels = []
        eval_nodes = [col for col, _ in self.order_by if col.node_type != NodeType.IDENTIFIER]
        self._compiled_evals = compile_eval_nodes(eval_nodes)

    @property
    def config(self):  # pragma: no cover
        return ", ".join(
            f"{col.value} {'ASC' if ascending else 'DESC'}"
            for col, ascending in self.order_by
        )

    @property
    def name(self):  # pragma: no cover
        return "Sort"

    cpdef void _push_impl(self, Morsel morsel) except *:
        # Body runs GIL-held: the base nogil `_dispatch_push` decodes the C++
        # carrier and calls this, surfacing any exception via the ErrCtx path.
        if morsel is not _EOS_SENTINEL:
            if morsel.num_rows > 0:
                self._morsels.append(morsel)
            return

        if not self._morsels:
            self.emit(_EOS_SENTINEL)
            return

        combined = Morsel.combine(self._morsels)

        column_names = []
        ascending_flags = []

        for column, ascending in self.order_by:
            try:
                identity = column.schema_column.identity
                column_names.append(identity)
            except ColumnNotFoundError as cnfe:  # pragma: no cover
                raise ColumnNotFoundError(
                    f"`ORDER BY` must reference columns as they appear in the `SELECT` clause. {cnfe}"
                ) from cnfe
            ascending_flags.append(bool(ascending))

        if self._compiled_evals:
            combined = execute_and_append(self._compiled_evals, combined)

        perm = morsel_sort(combined, column_names, ascending_flags)
        # take() returns a NEW reordered morsel; it does not mutate in place.
        # The result must be reassigned or the sort permutation is silently lost.
        combined = combined.take(perm)

        self.emit(combined)
        self.emit(_EOS_SENTINEL)
