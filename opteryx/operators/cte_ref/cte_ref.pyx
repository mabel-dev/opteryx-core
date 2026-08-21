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
CTE Reference Node

One reference to a shared, materialize-once CTE. The referenced body is a
physical plan of its own (PhysicalPlan.shared_ctes); the plan compiler lowers
the body ONCE into a producer pipeline ending in a buffer-append sink, and
lowers each of these nodes into a pipeline that reads that buffer and
selects/renames the body's output columns to this reference's own identities
(`cte_column_map`: reference identity -> body output identity).

This node never executes: like every plan node it is configuration for the
compiler. There is no read_morsels here on purpose — a Python execution path
for it must not exist.
"""


cdef class CteRefNode(BasePlanNode):  # pragma: no cover
    """A leaf reading the shared result of a multiply-referenced CTE."""
    cdef public object cte_key
    cdef public object cte_name
    cdef public object cte_column_map
    cdef public object alias
    cdef public object schema

    def __init__(self, properties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.cte_key = parameters.get("cte_key")
        self.cte_name = parameters.get("cte_name")
        self.cte_column_map = parameters.get("cte_column_map") or {}
        self.alias = parameters.get("alias")
        self.schema = parameters.get("schema")
        self.columns = parameters.get("columns", [])

    @property
    def name(self):  # pragma: no cover
        """Friendly name for this step"""
        return "CTE Reference"

    @property
    def config(self):
        """Additional details for this step"""
        return f"({self.cte_name or self.cte_key} AS {self.alias})"

    def __repr__(self):  # pragma: no cover
        return f"<{self.__class__.__name__} {self.cte_name} AS {self.alias}>"
