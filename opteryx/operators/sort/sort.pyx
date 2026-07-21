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

from opteryx.expression import NodeType
from opteryx.expression.evaluator import compile_eval_nodes

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

    cdef BasePlanNode make_worker(self):
        # SPEC: order_by + compiled evals — read-only at run time, shared by
        # reference (no recompile). STATE: a fresh `_morsels` accumulator (and the
        # base `readings`/counters via `_copy_worker_base`).
        cdef SortNode w = SortNode.__new__(SortNode)
        self._copy_worker_base(w)
        w.order_by = self.order_by
        w._compiled_evals = self._compiled_evals
        w._morsels = []
        return w

    @property
    def config(self):  # pragma: no cover
        return ", ".join(
            f"{col.value} {'ASC' if ascending else 'DESC'}"
            for col, ascending in self.order_by
        )

    @property
    def name(self):  # pragma: no cover
        return "Sort"
