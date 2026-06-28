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

BasePlanNode, Morsel, Vector, DRAKEN_INT64 and _EOS_SENTINEL are in scope via the
textual include from _operators.pyx.
"""

from libcpp.unordered_map cimport unordered_map
from libc.stdint cimport uint8_t, uint32_t, uint64_t, int64_t
from libc.string cimport memcpy

from draken.vectors.vector cimport from_decoded

from opteryx.compiled.morsel_ops.sort import morsel_sort
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import QueryProperties

cdef extern from "core/alloc.h" nogil:
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil

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
    cdef public list _morsels             # buffered input (blocking path)
    cdef unordered_map[uint64_t, int64_t] _counts   # streaming path: partition hash -> count

    cdef bint is_partition_parallel(self):
        # Streaming ROW_NUMBER/window keeps a single GLOBAL running counter per
        # partition (`_counts`); splitting the input across workers would split the
        # sequence and change the answer. Serial/merge-only — never fanned out.
        return False

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
        self._morsels = []

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

    cpdef void _push_impl(self, Morsel morsel) except *:
        # Body runs GIL-held: the base nogil `_dispatch_push` decodes the C++
        # carrier and calls this, surfacing any exception via the ErrCtx path.
        if self._blocking:
            self._push_blocking(morsel)
        else:
            self._push_streaming(morsel)

    # ------------------------------------------------------------------ streaming
    cdef void _push_streaming(self, Morsel morsel) except *:
        if morsel is _EOS_SENTINEL:
            self.emit(morsel)
            return
        if morsel.num_rows == 0:
            return

        cdef Py_ssize_t n = morsel.num_rows
        cdef uint64_t[::1] h = morsel.hash(self._partition_columns)
        cdef int64_t* rn_buf = <int64_t*>draken_malloc(<size_t>n * sizeof(int64_t))
        if rn_buf == NULL:
            raise MemoryError()

        cdef Py_ssize_t i
        cdef int64_t c
        with nogil:
            for i in range(n):
                c = self._counts[h[i]] + 1
                self._counts[h[i]] = c
                rn_buf[i] = c

        cdef Vector rn_vec = from_decoded(<void*>rn_buf, NULL, <uint32_t>n, DRAKEN_INT64)
        morsel.append_vector(self._functions[0][1], rn_vec)
        self.emit(morsel)

    # ------------------------------------------------------------------- blocking
    cdef void _push_blocking(self, Morsel morsel) except *:
        if morsel is not _EOS_SENTINEL:
            if morsel.num_rows > 0:
                self._morsels.append(morsel)
            return

        if not self._morsels:
            self.emit(_EOS_SENTINEL)
            return

        combined = Morsel.combine(self._morsels)
        self._morsels = []
        cdef Py_ssize_t n = combined.num_rows

        # Sort by partition keys then order keys; the permutation maps sorted
        # position -> original row index.
        sort_cols = list(self._partition_columns) + list(self._order_columns)
        sort_asc = [True] * len(self._partition_columns) + list(self._order_ascending)
        cdef int[::1] perm = morsel_sort(combined, sort_cols, sort_asc)

        cdef bint has_partition = len(self._partition_columns) > 0
        cdef uint64_t[::1] order_h = combined.hash(self._order_columns)
        # When there is no PARTITION BY the whole input is one partition; point
        # part_h at a real array (order_h) so the memoryview is never None — it is
        # never read because has_partition gates every access.
        cdef uint64_t[::1] part_h = (
            combined.hash(self._partition_columns) if has_partition else order_h
        )

        # Compute all three numberings in a single pass over sorted order, scattering
        # each into input-order position so the emitted morsel keeps input order.
        cdef int64_t* rn = <int64_t*>draken_malloc(<size_t>n * sizeof(int64_t))
        cdef int64_t* rk = <int64_t*>draken_malloc(<size_t>n * sizeof(int64_t))
        cdef int64_t* dr = <int64_t*>draken_malloc(<size_t>n * sizeof(int64_t))
        if rn == NULL or rk == NULL or dr == NULL:
            draken_free(rn); draken_free(rk); draken_free(dr)
            raise MemoryError()

        cdef Py_ssize_t si
        cdef uint32_t oi
        cdef int64_t cur_rn = 0, cur_rk = 0, cur_dr = 0
        cdef uint64_t prev_part = 0, prev_order = 0
        cdef uint64_t cur_part, cur_order
        cdef bint first = True
        with nogil:
            for si in range(n):
                oi = <uint32_t>perm[si]
                cur_part = part_h[oi] if has_partition else 0
                cur_order = order_h[oi]
                if first or (has_partition and cur_part != prev_part):
                    cur_rn = 1
                    cur_rk = 1
                    cur_dr = 1
                    first = False
                else:
                    cur_rn += 1
                    if cur_order != prev_order:
                        cur_rk = cur_rn
                        cur_dr += 1
                    # equal order key -> tie: rank and dense_rank unchanged
                rn[oi] = cur_rn
                rk[oi] = cur_rk
                dr[oi] = cur_dr
                prev_part = cur_part
                prev_order = cur_order

        # Emit one column per requested window function.
        cdef int kind
        cdef object out_identity
        cdef int64_t* src
        cdef int64_t* out_buf
        cdef Vector out_vec
        for kind, out_identity in self._functions:
            if kind == _RANK_ROW_NUMBER:
                src = rn
            elif kind == _RANK_RANK:
                src = rk
            else:
                src = dr
            out_buf = <int64_t*>draken_malloc(<size_t>n * sizeof(int64_t))
            if out_buf == NULL:
                draken_free(rn); draken_free(rk); draken_free(dr)
                raise MemoryError()
            memcpy(out_buf, src, <size_t>n * sizeof(int64_t))
            out_vec = from_decoded(<void*>out_buf, NULL, <uint32_t>n, DRAKEN_INT64)
            combined.append_vector(out_identity, out_vec)

        draken_free(rn)
        draken_free(rk)
        draken_free(dr)

        self.emit(combined)
        self.emit(_EOS_SENTINEL)
