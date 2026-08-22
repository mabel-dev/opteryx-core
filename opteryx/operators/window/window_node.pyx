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
Window Node — ranking (ROW_NUMBER / RANK / DENSE_RANK / NTILE / PERCENT_RANK /
CUME_DIST), navigation (LAG / LEAD) and value (FIRST_VALUE / LAST_VALUE /
NTH_VALUE) window functions.

One execution path: WindowSink (src/cpp/engine/native_sort.hpp) is a pipeline
breaker. All input is buffered, sorted by (partition keys ASC, order keys with
their direction), and a single pass assigns:
    ROW_NUMBER   — 1..n within each partition
    RANK         — 1-based, ties (equal order key) share, next skips     (1,1,3)
    DENSE_RANK   — 1-based, ties share, next does not skip               (1,1,2)
    NTILE(k)     — 1..k, the partition split into k contiguous buckets as
                   evenly as it divides; the first (n mod k) buckets take one
                   row more than the rest
    PERCENT_RANK — (RANK - 1) / (partition rows - 1), FLOAT64 in [0, 1]; 0 for
                   a one-row partition
    CUME_DIST    — rows through the current row's LAST TIED PEER / partition
                   rows, FLOAT64 in (0, 1]
    LAG/LEAD     — the argument column's value from the row `offset` before/after
                   the current row within its partition; NULL when that row falls
                   outside the partition
    FIRST_VALUE/LAST_VALUE/NTH_VALUE
                 — the argument column's value from the partition's first, last,
                   or nth row (1-based); NULL when the partition has no such row.
                   Computed over the WHOLE ordered partition — see
                   VALUE_FUNCTIONS in opteryx/operators/window/helpers.py
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
    # arg_identity:bytes|None, offset:int). `arg_identity` is set only for the
    # kinds that read a value from another row (GATHERED_FUNCTIONS); `offset` is
    # the kind's single constant integer parameter — LAG/LEAD's row shift,
    # NTILE's bucket count, NTH_VALUE's 1-based position — and unused otherwise.
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
                # The supported set is listed FROM the registry, so this message
                # cannot drift out of date the way a hand-written list did.
                _supported = ", ".join(f"**{name}**" for name in sorted(_KIND_CODES))
                raise UnsupportedSyntaxError(f"Unsupported window function '{kind}'. The supported window functions are {_supported}.")
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


from opteryx.operators.window.helpers import FRAMED_AGGREGATE_FUNCTIONS

cdef dict _AGG_KIND_CODES = dict(FRAMED_AGGREGATE_FUNCTIONS)


cdef class FramedWindowNode(BasePlanNode):
    """SUM/COUNT/AVG/MIN/MAX OVER (PARTITION BY ... ORDER BY ... ROWS|RANGE BETWEEN ...).

    A separate node from WindowNode — see native_window_frame.hpp's header comment for
    why a framed aggregate (a sliding-window reduction, per-function OUTPUT TYPE) is a
    different computation from ranking/navigation (one value per row from the sorted
    order itself, always INT64 or the LAG/LEAD argument's own type).

    Plan-time config only; execution is 100% native (FramedWindowSink,
    native_window_frame.hpp), read off this class by compiler.py's
    FramedWindowNode branch (``._partition_columns``/``._order_columns``/
    ``._order_ascending``/``._functions``).
    """
    cdef public list _partition_columns   # partition-key column identities (bytes)
    cdef public list _order_columns       # order-key column identities (bytes)
    cdef public list _order_ascending     # bool per order column
    # _functions entries: (kind_code:int, output_identity:bytes, arg_node:Node|None,
    # frame:tuple) — mirrors WindowNode's `_functions` shape (kind/identity/arg/extra).
    # `arg_node` is kept as the NODE (not yet resolved to a column identity): a
    # computed argument (e.g. `SUM(a + b) OVER (...)`) is projected to a stream
    # column by the compiler, same as WindowNode's navigation argument. Each
    # function's OUTPUT type is resolved by the compiler too (`_layout_type`/`_cts`,
    # off `node.columns` — the pre-minted SchemaColumns the binder registered), not
    # carried here: it depends on the ARGUMENT's bound type, which only the binder
    # (via `_aggregate_return_type`) and the compiler's type-tracking machinery need
    # to agree on, not this plan-time config class.
    cdef public list _functions

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        partition_by = parameters.get("partition_by") or []
        self._partition_columns = [col.schema_column.identity for col in partition_by]

        order_by = parameters.get("order_by") or []
        if not order_by:
            raise UnsupportedSyntaxError(
                "A window **FRAME** (ROWS/RANGE BETWEEN ...) requires an **ORDER BY** in its **OVER** (...) clause."
            )
        self._order_columns = [col.schema_column.identity for col, _ in order_by]
        self._order_ascending = [bool(asc) for _, asc in order_by]

        functions = parameters.get("window_functions") or []
        self._functions = []
        for kind, output_identity, arg_node, frame in functions:
            if kind not in _AGG_KIND_CODES:
                raise UnsupportedSyntaxError(
                    f"Unsupported framed window function '{kind}'. **SUM**, **COUNT**, **AVG**, **MIN** and **MAX** are the supported window aggregate functions."
                )
            self._functions.append(
                (_AGG_KIND_CODES[kind], output_identity, arg_node, frame)
            )
        if not self._functions:
            raise UnsupportedSyntaxError("a framed window node with no functions")

    @property
    def name(self):  # pragma: no cover
        return "Framed Window"

    @property
    def config(self):  # pragma: no cover
        return "window OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE BETWEEN ...)"
