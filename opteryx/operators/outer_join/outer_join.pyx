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
Outer Join Node

LEFT / RIGHT / FULL OUTER JOIN plan-time configuration.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
_compile_join, which reads `.join_type`/`.left_columns`/`.right_columns` off
this class): "left outer" lowers to JoinMode::LeftOuter, "right outer" was
already rewritten to a swapped-leg left outer by the optimizer's join_rewriter,
and "full outer" lowers to JoinMode::FullOuter — LEFT OUTER probing with
build-side match tracking plus an UnmatchedBuildSource tail pipeline (see
native_join2.hpp). This class is plan-time config only; the old Cython
per-row build/probe implementation was deleted when full outer went native.
"""

# BasePlanNode/JoinNode in scope via _operators.pyx include.


cdef class OuterJoinNode(JoinNode):
    cdef public str join_type
    cdef public object using
    cdef public list left_columns
    cdef public list right_columns

    def __init__(self, properties=None, **parameters):
        # Ensure `join_type` exists before the base initializer accesses `self.name`
        self.join_type = parameters["type"]
        JoinNode.__init__(self, properties=properties, **parameters)
        self.on = parameters.get("on")
        self.using = parameters.get("using")

        self.left_columns = parameters.get("left_columns")
        self.left_readers = parameters.get("left_readers") or []

        self.right_columns = parameters.get("right_columns")
        self.right_readers = parameters.get("right_readers") or []

        self.left_relation_names = parameters.get("left_relation_names") or []
        self.right_relation_names = parameters.get("right_relation_names") or []

        self.columns = parameters.get("columns")

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
