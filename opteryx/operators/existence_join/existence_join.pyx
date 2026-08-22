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
Existence Join Node

The sibling of FilterJoinNode: the same existence test, EMITTED rather than
applied. Every probe row survives and the verdict is appended as a BOOL column,
which is what a SELECT-list `EXISTS` / `IN` reads. Used for
`SELECT ..., EXISTS (SELECT ...) ...` — see decorrelate_subquery.

Execution is 100% native and shares FilterJoinNode's operator: compiler.py's
_compile_join maps "left existence"/"left existence anti" onto the same
JoinMode as "left semi"/"left anti" and sets SemiAntiProbeOperator's
`emit_existence`. This class is plan-time config only.

It is a SEPARATE join type from "left semi"/"left anti" on purpose. An
optimizer strategy that recognises a semi join is entitled to assume it REMOVES
rows — pushing it below something, or reordering around it, on that basis. This
join keeps every row, so being unrecognised by those strategies is the correct
outcome, not a missed optimization.
"""

# BasePlanNode/JoinNode in scope via _operators.pyx include.


cdef class ExistenceJoinNode(JoinNode):
    cdef public str join_type
    cdef public object using
    cdef public list left_columns
    cdef public list right_columns
    # Correlated NON-equality residual, exactly as FilterJoinNode carries it: it
    # gates the existence test per candidate pair inside the probe. The verdict it
    # produces is the one flagged here, so a correlated non-equality EXISTS in a
    # SELECT list needs no separate machinery.
    cdef public object residual
    # The output column the verdict is written to — a BOOL column appended after
    # the probe's own columns. The projection above reads it by this identity.
    cdef public object existence_column
    # Is the flag THREE-valued? `EXISTS` is not; a projected `IN`/`NOT IN` is
    # (UNKNOWN when the probe key is NULL, or when nothing matched and the build
    # side held a NULL). See SemiAntiProbeOperator::existence_three_valued.
    cdef public bint existence_three_valued

    def __init__(self, properties=None, **parameters):
        self.join_type = parameters["type"]
        JoinNode.__init__(self, properties=properties, **parameters)
        self.on = parameters.get("on")
        self.using = parameters.get("using")
        self.residual = parameters.get("residual")
        self.existence_column = parameters.get("existence_column")
        self.existence_three_valued = parameters.get("existence_three_valued", False)

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
        return f"{self.join_type.upper()}"
