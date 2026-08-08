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
Distinct Node

This is a SQL Query Execution Plan Node.

This Node eliminates duplicate records.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
DistinctNode branch, which reads `._distinct_on`/`._distinct_on_exprs`/
`.distinct_ndv_estimate` off this class and compiles them into the engine's
set_distinct_sink). This class is plan-time config only.
"""

# BasePlanNode in scope via _operators.pyx include.


cdef class DistinctNode(BasePlanNode):
    cdef public object _distinct_on
    # The unreduced DISTINCT ON expression nodes (schema_column, node_type,
    # parameters intact) — parallel to GroupedAggregateHashedNode.groups.
    # Needed by the native compiler to materialize a computed DISTINCT ON key
    # (e.g. `DISTINCT ON (payload->'x')`) that the stream doesn't already
    # carry; `_distinct_on` alone (bare identities) throws that expression
    # tree away before the compiler ever sees it.
    cdef public object _distinct_on_exprs
    # Planner distinct-count estimate (int or None) — consumed by the native
    # plan compiler to gate DistinctSink's parvi front set.
    cdef public object distinct_ndv_estimate

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self._distinct_on_exprs = parameters.get("on")
        self._distinct_on = parameters.get("on")
        if self._distinct_on:
            self._distinct_on = [
                col.schema_column.identity for col in self._distinct_on
            ]
        self.distinct_ndv_estimate = parameters.get("distinct_ndv_estimate")

    @property
    def config(self):  # pragma: no cover
        return ""

    @property
    def name(self):  # pragma: no cover
        return "Distinction"

