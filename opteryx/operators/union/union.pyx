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
Union Node

This is a SQL Query Execution Plan Node.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
UnionNode branch, which reads `.column_ids` off this class). This class is
plan-time config only.
"""

# BasePlanNode in scope via _operators.pyx include.


cdef class UnionNode(BasePlanNode):
    cdef public list column_ids
    cdef public list schema

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.columns = parameters.get("columns", [])
        self.column_ids = [c.schema_column.identity for c in self.columns]
        self.schema = None

    @property
    def name(self):  # pragma: no cover
        return "Union"

    @property
    def config(self):  # pragma: no cover
        return ""
