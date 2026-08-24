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
Band Join Node

An equi-join whose ON clause also bounds ONE build-side column both above and
below by values from the probe side:

    ON f.client = l.client
       AND l.event_time <= f.flow_start
       AND l.event_time  > f.flow_start - INTERVAL '20' SECOND

Executed as the equality alone, the join emits every pair within each equi group
and the band discards >99% of them one node up (measured: 2.55 BILLION rows out
for a 4.8M-row answer). Executed here, each equi group's build rows are kept
sorted by the banded column and a probe row emits the contiguous slice between
two bisects, so the discarded pairs are never formed.

JoinOrderingStrategy is the ONLY producer of this join type — it owns the single
retype decision that chooses between "band" and "nested loop" — and it attaches
the descriptor this class carries. Execution is 100% native (see
opteryx/managers/execution/compiler.py's _compile_band_join, which reads
`.band_column`/`.band_lower`/`.band_upper` and their inclusivity off this class).
This class is plan-time config only.

⛔ The band conjuncts are CONSUMED by the range, never also applied as a residual.
Recognition admits nothing in the ON clause but the equi keys and exactly these
two conjuncts, which is what makes consuming them exactly equivalent.
"""

# BasePlanNode / JoinNode in scope via _operators.pyx include.


cdef class BandJoinNode(JoinNode):
    cdef public object band_column
    cdef public object band_column_name
    cdef public object band_lower
    cdef public object band_upper
    cdef public bint band_lower_closed
    cdef public bint band_upper_closed
    cdef public list left_columns
    cdef public list right_columns

    join_type = "band"

    def __init__(self, properties=None, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        # The BUILD-side column the sorted runs are ordered by — a column IDENTITY
        # (bytes), matching how left_columns/right_columns carry join keys.
        self.band_column = parameters.get("band_column")
        # EXPLAIN only — never used to resolve anything. See join_ordering.
        self.band_column_name = parameters.get("band_column_name")
        # The two bounds, as bound expression NODES over the probe leg. They stay
        # expressions here: the compiler materialises them as synthetic probe
        # columns, the same way _compile_asof_join materialises a coercion cast.
        self.band_lower = parameters.get("band_lower")
        self.band_upper = parameters.get("band_upper")
        # Whether each end is CLOSED. `<=` and `>` in the example above make the
        # upper end closed and the lower end open; swapping them shifts the answer
        # by exactly the rows sitting ON a boundary.
        self.band_lower_closed = bool(parameters.get("band_lower_closed"))
        self.band_upper_closed = bool(parameters.get("band_upper_closed"))

        self.left_columns  = list(parameters.get("left_columns") or [])
        self.right_columns = list(parameters.get("right_columns") or [])

        if self.band_column is None or self.band_lower is None or self.band_upper is None:
            raise ValueError(
                "BandJoinNode requires band_column and both band_lower and band_upper"
            )
        if not self.left_columns or not self.right_columns:
            raise ValueError("BandJoinNode requires equi-join keys on both legs")

    @property
    def name(self):  # pragma: no cover
        return "Band Join"

    @property
    def config(self):  # pragma: no cover
        column = self.band_column_name
        if column is None:
            column = self.band_column
        if isinstance(column, bytes):
            column = column.decode("utf8")
        return "%s %s lower, upper%s" % (
            column,
            "IN [" if self.band_lower_closed else "IN (",
            "]" if self.band_upper_closed else ")",
        )
