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

Execution is 100% native (see opteryx/managers/execution/compiler.py's
SortNode branch, which reads `.order_by` off this class and compiles it into
the engine's set_sort_sink; `_sort_spec` materializes a computed ORDER BY key
itself). This class is plan-time config only.
"""

# BasePlanNode in scope via textual include from _operators.pyx.


cdef class SortNode(BasePlanNode):
    cdef public list order_by

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.order_by = parameters.get("order_by", [])

    @property
    def config(self):  # pragma: no cover
        return ", ".join(
            f"{col.value} {'ASC' if ascending else 'DESC'}"
            for col, ascending in self.order_by
        )

    @property
    def name(self):  # pragma: no cover
        return "Sort"
