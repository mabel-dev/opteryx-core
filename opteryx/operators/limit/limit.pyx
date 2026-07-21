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
Limit Node

This is a SQL Query Execution Plan Node.

This Node performs the LIMIT and the OFFSET steps

Execution is 100% native (see opteryx/managers/execution/compiler.py's
LimitNode branch, which reads `.limit`/`.offset` off this class and compiles
them into the engine's add_limit). This class is plan-time config only.
"""

# BasePlanNode in scope via textual include from _operators.pyx.


cdef class LimitNode(BasePlanNode):
    cdef public object limit
    cdef public object offset

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.limit = parameters.get("limit", float("inf"))
        self.offset = parameters.get("offset", 0)

    @property
    def name(self):  # pragma: no cover
        return "LIMIT"

    @property
    def config(self):  # pragma: no cover
        return str(self.limit) + " OFFSET " + str(self.offset)
