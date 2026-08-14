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
Window Node — ranking (ROW_NUMBER / RANK / DENSE_RANK) and navigation
(LAG / LEAD) window functions.

One execution path: WindowSink (src/cpp/engine/native_sort.hpp) is a pipeline
breaker. All input is buffered, sorted by (partition keys ASC, order keys with
their direction), and a single pass assigns:
    ROW_NUMBER  — 1..n within each partition
    RANK        — 1-based, ties (equal order key) share, next skips      (1,1,3)
    DENSE_RANK  — 1-based, ties share, next does not skip                (1,1,2)
    LAG/LEAD    — the argument column's value from the row `offset` before/after
                  the current row within its partition; NULL when that row falls
                  outside the partition
Output is emitted in sorted order (the compiler pins the downstream pipeline to
dop 1 to preserve it). A no-ORDER-BY window — used internally by the
INTERSECT/EXCEPT ALL rewrite, single ROW_NUMBER only — compiles to the same sink
with an empty order-key suffix; there is no separate streaming path.

Partition boundaries and order-ties are decided by exact value comparison
(win_keys_equal: memcmp for strings, numeric compare otherwise; two NULLs are
equal, so NULL partition keys form one real partition). Not a hash.

When WindowTopKFusionStrategy fuses a downstream `rank <= K` filter and the shape
qualifies (single ROW_NUMBER, one fixed-width ORDER BY key), the compiler swaps in
WindowTopKSink instead — per-partition top-K heaps, no global sort, no ordering
promise on its output.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
WindowNode branch, which reads `._partition_columns`/`._order_columns`/
`._order_ascending`/`._functions` off this class). This class is plan-time
config only.
"""

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.operators.window.helpers import WINDOW_FUNCTIONS

# Kind codes come from the window-function registry (the single Python-side
# source of truth; the C++ WinFn enum in native_sort.hpp mirrors it by hand).
cdef dict _KIND_CODES = dict(WINDOW_FUNCTIONS)
cdef int _RANK_ROW_NUMBER = WINDOW_FUNCTIONS["ROW_NUMBER"]


cdef class WindowNode(BasePlanNode):
    cdef public list _partition_columns   # partition-key column identities (bytes)
    cdef public list _order_columns       # order-key column identities (bytes)
    cdef public list _order_ascending     # bool per order column
    # _functions entries: (kind_code:int, output_identity:bytes,
    # arg_identity:bytes|None, offset:int) — the last two are only meaningful
    # for the navigation kinds (LAG/LEAD).
    cdef public list _functions
    cdef public bint _has_order_by        # ORDER BY present in the OVER clause
    cdef public long long _top_k          # WindowTopKFusionStrategy hint; -1 = unset

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        partition_by = parameters.get("partition_by") or []
        self._partition_columns = [col.schema_column.identity for col in partition_by]

        order_by = parameters.get("order_by") or []
        self._order_columns = [col.schema_column.identity for col, _ in order_by]
        self._order_ascending = [bool(asc) for _, asc in order_by]

        # window_functions entries: (kind, output_identity, arg_node|None, offset).
        # The arg NODE stays in node.parameters for the compiler (a computed
        # argument is projected to a stream column there); this class keeps its
        # bound IDENTITY, which is what the sink resolves columns by.
        functions = parameters.get("window_functions") or []
        self._functions = []
        for kind, output_identity, arg_node, offset in functions:
            if kind not in _KIND_CODES:
                raise UnsupportedSyntaxError(f"Unsupported window function '{kind}'. **ROW_NUMBER**, **RANK**, **DENSE_RANK**, **LAG** and **LEAD** are the supported window functions.")
            arg_identity = None if arg_node is None else arg_node.schema_column.identity
            self._functions.append((_KIND_CODES[kind], output_identity, arg_identity, int(offset)))

        self._has_order_by = len(self._order_columns) > 0

        # Set only by WindowTopKFusionStrategy, and only when there is exactly one
        # ranking output — a fused `WHERE <rank> <= K` filter. Keep only the rows
        # whose rank is <= top_k before gathering/emitting (still computes an exact
        # rank for every row first: RANK/DENSE_RANK ties can only be resolved once
        # every row's rank is known).
        top_k = parameters.get("top_k")
        self._top_k = int(top_k) if top_k is not None else -1

        if not self._has_order_by:
            # The no-ORDER-BY shape is only produced by the INTERSECT/EXCEPT ALL
            # rewrite: a single ROW_NUMBER over a partition.
            if not self._partition_columns:
                raise UnsupportedSyntaxError(
                    "ROW_NUMBER without **ORDER BY** requires a **PARTITION BY**. Add one, or give the window an **ORDER BY**."
                )
            if len(self._functions) != 1 or self._functions[0][0] != _RANK_ROW_NUMBER:
                raise UnsupportedSyntaxError(
                    "Only ROW_NUMBER() **OVER** (**PARTITION BY** ...) is supported without **ORDER BY**."
                )

    @property
    def name(self):  # pragma: no cover
        return "Window"

    @property
    def config(self):  # pragma: no cover
        return "window OVER (PARTITION BY ... ORDER BY ...)" if self._has_order_by else "ROW_NUMBER OVER (PARTITION BY ...)"
