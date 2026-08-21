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
Scalar Subquery Guard Node

This is a SQL Query Execution Plan Node.

Runtime cardinality enforcement for an uncorrelated scalar subquery whose
single-row property the planner could not prove statically: >1 row raises the
SQL-standard cardinality violation, 0 rows yields the NULL a scalar subquery
is defined to be.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
ScalarGuardNode branch, which buffers the subquery pipeline and reads it back
through the engine's ScalarGuardSource — native_scalar_guard.hpp). This class
is plan-time config only.
"""

# BasePlanNode in scope via textual include from _operators.pyx.


cdef class ScalarGuardNode(BasePlanNode):

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

    @property
    def name(self):  # pragma: no cover
        return "SCALAR GUARD"

    @property
    def config(self):  # pragma: no cover
        return ""
