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
Heap Sort Node (Top-N Sort)

This is a SQL Query Execution Plan Node.

Despite the name, this was never a Heap Sort algorithm but an incremental
Top-N sorter that worked chunk-wise for efficiency.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
HeapSortNode branch, which reads `.limit`/`.order_by` off this class and
compiles them into the engine's set_topn_sink). This class is plan-time
config only.
"""

from opteryx.exceptions import ColumnNotFoundError
from opteryx.expression import NodeType
from opteryx.expression.evaluator import compile_eval_nodes

# BasePlanNode in scope via textual include from _operators.pyx.


cdef class HeapSortNode(BasePlanNode):
    cdef public list order_by
    cdef public object limit
    cdef public list _compiled_evals

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.order_by = parameters.get("order_by", [])
        self.limit = parameters.get("limit", -1)

        for column, _ in self.order_by:
            try:
                column.schema_column.identity
            except ColumnNotFoundError as cnfe:
                raise ColumnNotFoundError(
                    f"`ORDER BY` must reference columns from `SELECT`. {cnfe}"
                ) from cnfe

        eval_nodes = [col for col, _ in self.order_by if col.node_type != NodeType.IDENTIFIER]
        self._compiled_evals = compile_eval_nodes(eval_nodes)

    cdef BasePlanNode make_worker(self):
        # SPEC: order_by + limit + compiled evals — all derived once and
        # read-only at run time, shared by reference (no recompile).
        cdef HeapSortNode w = HeapSortNode.__new__(HeapSortNode)
        self._copy_worker_base(w)
        w.order_by = self.order_by
        w.limit = self.limit
        w._compiled_evals = self._compiled_evals
        return w

    @property
    def config(self):  # pragma: no cover
        order = ", ".join(
            f"{col.schema_column.name} {'ASC' if ascending else 'DESC'}"
            for col, ascending in self.order_by
        )
        return f"LIMIT = {self.limit}, ORDER = {order}"

    @property
    def name(self):  # pragma: no cover
        return "Heap Sort"
