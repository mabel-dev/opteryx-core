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
Window Node — ranking window functions (ROW_NUMBER / RANK / DENSE_RANK).

Two execution paths:

* Streaming (no ORDER BY) — a per-partition running counter is kept across morsels
  and appended as ROW_NUMBER. Used internally by the INTERSECT/EXCEPT ALL rewrite,
  where the partition key is every projected column and any distinct numbering of
  identical rows is correct. Single ROW_NUMBER only.

* Blocking (ORDER BY present) — the user-facing path. All input is buffered, sorted
  by (partition keys, order keys), and a single pass assigns:
      ROW_NUMBER  — 1..n within each partition
      RANK        — 1-based, ties (equal order key) share, next skips      (1,1,3)
      DENSE_RANK  — 1-based, ties share, next does not skip                (1,1,2)
  The computed numbers are scattered back to input order via the sort permutation,
  so the operator preserves input row order (SQL does not guarantee an order without
  a top-level ORDER BY, but preserving input order is least-surprising and matches
  the aggregate-window path).

Partition / order equality is by the engine's 64-bit row hash (hash-only identity,
the same contract GROUP BY and the hash joins rely on).

Execution is 100% native (see opteryx/managers/execution/compiler.py's
WindowNode branch, which reads `._partition_columns`/`._order_columns`/
`._order_ascending`/`._functions` off this class). This class is plan-time
config only.
"""

from opteryx.exceptions import UnsupportedSyntaxError

# Ranking-function kind codes (avoid string compares in the hot loop).
cdef int _RANK_ROW_NUMBER = 0
cdef int _RANK_RANK = 1
cdef int _RANK_DENSE_RANK = 2

cdef dict _KIND_CODES = {
    "ROW_NUMBER": _RANK_ROW_NUMBER,
    "RANK": _RANK_RANK,
    "DENSE_RANK": _RANK_DENSE_RANK,
}


cdef class WindowNode(BasePlanNode):
    cdef public list _partition_columns   # partition-key column identities (bytes)
    cdef public list _order_columns       # order-key column identities (bytes)
    cdef public list _order_ascending     # bool per order column
    cdef public list _functions           # list of (kind_code:int, output_identity:bytes)
    cdef public bint _blocking            # ORDER BY present -> buffer + sort

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        partition_by = parameters.get("partition_by") or []
        self._partition_columns = [col.schema_column.identity for col in partition_by]

        order_by = parameters.get("order_by") or []
        self._order_columns = [col.schema_column.identity for col, _ in order_by]
        self._order_ascending = [bool(asc) for _, asc in order_by]

        functions = parameters.get("window_functions") or []
        self._functions = []
        for kind, output_identity in functions:
            if kind not in _KIND_CODES:
                raise UnsupportedSyntaxError(f"Unsupported window function '{kind}'.")
            self._functions.append((_KIND_CODES[kind], output_identity))

        self._blocking = len(self._order_columns) > 0

        if not self._blocking:
            # Streaming path only handles a single ROW_NUMBER over a partition.
            if not self._partition_columns:
                raise UnsupportedSyntaxError(
                    "ROW_NUMBER without ORDER BY requires a PARTITION BY."
                )
            if len(self._functions) != 1 or self._functions[0][0] != _RANK_ROW_NUMBER:
                raise UnsupportedSyntaxError(
                    "Only ROW_NUMBER() OVER (PARTITION BY ...) is supported without ORDER BY."
                )

    @property
    def name(self):  # pragma: no cover
        return "Window"

    @property
    def config(self):  # pragma: no cover
        return "ranking OVER (PARTITION BY ... ORDER BY ...)" if self._blocking else "ROW_NUMBER OVER (PARTITION BY ...)"

