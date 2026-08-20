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
    # Correlated NON-equality predicate lifted out of an EXISTS subquery
    # (decorrelate_subquery, the post-bind optimizer strategy; TPC-H Q21). None for the ordinary shape. The compiler
    # feeds it to the native probe, where it gates the existence test per candidate
    # pair — it is NOT a post-join filter.
    cdef public object residual
    # JoinOrderingStrategy's build-side exchange decision (compiler.py's
    # _compile_swapped_semi_anti). A cdef class has no __dict__, so a slot the
    # __init__ does not copy is silently ABSENT — which is how this decision was
    # made, counted in telemetry, shown in EXPLAIN, and never executed: at TPC-H
    # SF100 Q21 hash-built 600M lineitem rows it had decided to stream.
    cdef public bint swap_build_side

    def __init__(self, properties=None, **parameters):
        self.join_type = parameters["type"]
        JoinNode.__init__(self, properties=properties, **parameters)
        self.on = parameters.get("on")
        self.using = parameters.get("using")
        self.residual = parameters.get("residual")
        self.swap_build_side = parameters.get("swap_build_side", False)

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
