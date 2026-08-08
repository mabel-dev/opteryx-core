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
Inner (Nested Loop) Join Node

Every join whose ON clause contains a non-equi comparator lands here — pure
theta (`a > b`), mixed equi+theta (`a = b AND c > d`), and `!=` alike. The
optimizer's join_ordering strategy rewrites the logical join type to
"nested loop" for all of them (see _contains_non_equi_comparator), so this is
the ONLY node that carries a non-equi join.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
_compile_join, which reads `.join_type`/`.left_columns`/`.right_columns`/`.on`
off this class): "nested_loop" lowers to JoinMode::Inner, and the `on`
comparison becomes a `residual` applied as a post-join filter over the combined
layout. A pure theta join has no equi conjunct to key on, so it compiles to a
ZERO-KEY inner join (every build row shares one empty key -> cartesian) with the
residual doing the real work. This class is plan-time config only; the old
Cython bloom-filter + hash-index build/probe implementation was deleted when the
push path went dead.
"""

# BasePlanNode/JoinNode in scope via _operators.pyx include.


cdef class NestedLoopJoinNode(JoinNode):
    cdef public list left_columns
    cdef public list right_columns

    join_type = "nested_loop"

    def __init__(self, properties=None, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.left_columns = parameters.get("left_columns")
        self.right_columns = parameters.get("right_columns")

    @property
    def name(self):  # pragma: no cover
        return "Nested Loop Join"

    @property
    def config(self):  # pragma: no cover
        return "draken"
