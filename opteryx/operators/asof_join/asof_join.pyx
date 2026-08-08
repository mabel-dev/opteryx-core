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
ASOF Join Node

ASOF (As-Of) joins match each left row to the nearest right row by value
(typically a timestamp) rather than by exact equality. Standard usage:

    ASOF JOIN quotes MATCH_CONDITION(trades.ts >= quotes.ts) USING (symbol)

For each left row, the match is the right row whose ASOF column value is the
closest that satisfies the inequality — i.e. the most recent quote at or
before the trade time, within the same symbol partition. LEFT semantics: every
left row is emitted, with NULL right columns when nothing matches.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
_compile_asof_join, reached from _compile_join on `join_type == "asof"`, which
reads `.asof_left_column`/`.asof_right_column`/`.asof_op` and the USING keys off
this class). This class is plan-time config only; the old Cython buffer-sort-
bisect implementation was deleted when the push path went dead. The set of
supported MATCH_CONDITION operators is now the compiler's to enforce.
"""

# BasePlanNode / JoinNode in scope via _operators.pyx include.


cdef class AsofJoinNode(JoinNode):
    cdef public object asof_left_column
    cdef public object asof_right_column
    cdef public object asof_op
    cdef public list left_columns
    cdef public list right_columns

    join_type = "asof"

    def __init__(self, properties=None, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.asof_left_column  = parameters.get("asof_left_column")
        self.asof_right_column = parameters.get("asof_right_column")
        self.asof_op           = parameters.get("asof_op")

        # The optional USING equi-partition keys. These arrive from the binder's
        # extract_join_fields, which already appends `schema_column.identity` —
        # they are column IDENTITIES, not bound nodes. Every other join operator
        # (inner, nested loop, filter) consumes them directly; do the same.
        #
        # They MUST be stored under `left_columns`/`right_columns`: the native
        # compiler's _compile_asof_join reads those names off this node to build
        # the build/probe key indices. Storing them under any other name makes
        # USING silently vanish and the join degrade to an unpartitioned ASOF.
        self.left_columns  = list(parameters.get("left_columns") or [])
        self.right_columns = list(parameters.get("right_columns") or [])

        if not self.asof_left_column or not self.asof_right_column or not self.asof_op:
            raise ValueError(
                "AsofJoinNode requires asof_left_column, asof_right_column, and asof_op"
            )

    @property
    def name(self):  # pragma: no cover
        return "ASOF Join"

    @property
    def config(self):  # pragma: no cover
        op_map = {"Lt": "<", "LtEq": "<=", "Gt": ">", "GtEq": ">="}
        op_sym = op_map.get(self.asof_op, self.asof_op)
        base = f"MATCH_CONDITION({self.asof_left_column} {op_sym} {self.asof_right_column})"
        if self.left_columns:
            # Identities are bytes — decode for display.
            names = ", ".join(
                c.decode("utf8") if isinstance(c, bytes) else str(c)
                for c in self.left_columns
            )
            base += f" USING ({names})"
        return base
