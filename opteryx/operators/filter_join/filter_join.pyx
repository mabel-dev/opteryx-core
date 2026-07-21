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
Filter Join Node

LEFT SEMI and LEFT ANTI join implementations for IN / NOT IN subquery rewrites
and INTERSECT / EXCEPT set operations.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
_compile_join, which reads `.join_type`/`.left_columns`/`.right_columns` off
this class for the "left semi"/"left anti"/"left anti null-aware" modes).
This class is plan-time config only.
"""

# BasePlanNode/JoinNode in scope via _operators.pyx include.


cdef class FilterJoinNode(JoinNode):
    cdef public str join_type
    cdef public object using
    cdef public list left_columns
    cdef public list right_columns

    def __init__(self, properties=None, **parameters):
        self.join_type = parameters["type"]
        JoinNode.__init__(self, properties=properties, **parameters)
        self.on = parameters.get("on")
        self.using = parameters.get("using")

        self.left_columns = parameters.get("left_columns")
        self.left_readers = parameters.get("left_readers")

        self.right_columns = parameters.get("right_columns")
        self.right_readers = parameters.get("right_readers")

    @property
    def name(self):  # pragma: no cover
        return self.join_type.replace(" ", "_")

    @property
    def config(self) -> str:  # pragma: no cover
        from opteryx.expression import format_expression

        if self.on:
            return f"{self.join_type.upper()} JOIN ({format_expression(self.on, True)})"
        if self.using:
            return f"{self.join_type.upper()} JOIN (USING {','.join(map(format_expression, self.using))})"
        return f"{self.join_type.upper()}"
