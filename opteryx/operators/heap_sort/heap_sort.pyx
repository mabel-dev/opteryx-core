# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from array import array
from collections.abc import Iterable

from opteryx.vectors.vector_ranking import vector_exact_search_top_k
from opteryx.exceptions import ColumnNotFoundError
from opteryx.expression import NodeType
from opteryx.expression.evaluator import compile_eval_nodes, execute_and_append
from opteryx.models import QueryProperties

# Licensed under the Apache License, Version 2.0 (the "License");
from opteryx.tracing.event_recorder import record_event as _trace_record
from opteryx.vectors.vector_types import get_vector_source_identifier
from opteryx.vectors.vector_types import node_is_numeric_vector
from opteryx.vectors.vector_types import node_is_vector_query_expression

# BasePlanNode in scope via textual include from _operators.pyx.

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy

from draken.vectors.vector cimport Vector
from draken.core.buffers cimport (
    DrakenVector, DrakenStringArena, DrakenStringSlot, DrakenType,
    str_prefix4, str_compare,
    DRAKEN_VARCHAR, DRAKEN_NVARCHAR, DRAKEN_VARBINARY,
    DRAKEN_INT8, DRAKEN_INT16, DRAKEN_INT32, DRAKEN_INT64, DRAKEN_DECIMAL,
    DRAKEN_FLOAT32, DRAKEN_FLOAT64, DRAKEN_DATE32, DRAKEN_TIMESTAMP64,
    DRAKEN_TIME32, DRAKEN_TIME64, DRAKEN_BOOL,
)

# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Heap Sort Node (Top-N Sort)

This is a SQL Query Execution Plan Node.

This query plan node maintains a sorted table of the top-N records seen so far
based on the provided ORDER BY clause. Despite the name, this is not a Heap Sort
algorithm, but an incremental Top-N sorter that works chunk-wise for efficiency.

This is faster, particularly when working with large datasets even though we're now
sorting smaller chunks over and over again.
"""



# ── Module-level C helpers ────────────────────────────────────────────────────

cdef inline bint _is_descending(bint ascending) noexcept:
    """Return True if the sort direction is descending."""
    return not ascending


cdef inline bint _is_nearest_neighbor_order(str function_name, bint ascending) noexcept:
    """Return True if this vector ordering corresponds to nearest-neighbour semantics."""
    cdef bint descending = not ascending
    return (function_name == "COSINE_DISTANCE" and not descending) or (
        function_name == "COSINE_SIMILARITY" and descending
    )


cdef int _compare_rows_vectors(
    Py_ssize_t left_index,
    Py_ssize_t right_index,
    list vectors,
    list directions,
) except *:
    """
    Compare two rows across an ordered list of vectors using native methods only.
    Returns -1, 0, or 1. Nulls sort last. No Python materialization.
    All vectors MUST support is_null_at() and compare_at() methods.
    """
    cdef Py_ssize_t i, ncols = len(vectors)
    cdef bint descending, left_null, right_null
    cdef Vector vector
    cdef int cmp_result

    for i in range(ncols):
        vector = vectors[i]
        descending = <bint>directions[i]

        left_null = vector.is_null_at(left_index)
        right_null = vector.is_null_at(right_index)

        if left_null and right_null:
            continue
        if left_null:
            return 1
        if right_null:
            return -1

        cmp_result = vector.compare_at(left_index, right_index)

        if cmp_result == 0:
            continue

        return -cmp_result if descending else cmp_result

    return 0


cdef bint _mh_sift_down(
    int32_t* buf,
    Py_ssize_t pos,
    Py_ssize_t size,
    list vectors,
    list directions,
) except False:
    """Sift down in a worst-first heap of int32 row indices."""
    cdef Py_ssize_t left, right, worst
    cdef int32_t tmp
    while True:
        left = (pos << 1) + 1
        right = left + 1
        worst = pos
        if left < size and _compare_rows_vectors(buf[left], buf[worst], vectors, directions) > 0:
            worst = left
        if right < size and _compare_rows_vectors(buf[right], buf[worst], vectors, directions) > 0:
            worst = right
        if worst == pos:
            break
        tmp = buf[pos]; buf[pos] = buf[worst]; buf[worst] = tmp
        pos = worst
    return True


cdef bint _mh_sift_up(
    int32_t* buf,
    Py_ssize_t pos,
    list vectors,
    list directions,
) except False:
    """Sift up in a worst-first heap of int32 row indices."""
    cdef Py_ssize_t parent
    cdef int32_t tmp
    while pos > 0:
        parent = (pos - 1) >> 1
        if _compare_rows_vectors(buf[pos], buf[parent], vectors, directions) > 0:
            tmp = buf[pos]; buf[pos] = buf[parent]; buf[parent] = tmp
            pos = parent
        else:
            break
    return True


cdef object _compressed_top_k(
    int64_t[::1] compressed,
    Py_ssize_t k,
    bint descending,
    int64_t null_val,
):
    """
    Single-pass, GIL-free top-k on a compressed int64 column.

    Maintains a worst-first heap of take_count entries:
      ASC  → max-heap (root = k-th smallest = worst kept)
      DESC → min-heap (root = k-th largest = worst kept)

    After the scan the heap is insertion-sorted to produce a fully ordered
    result.  Up to (k - valid_selected) null row indices are appended last.

    Returns an array('i') of row indices.
    """
    cdef Py_ssize_t n = compressed.shape[0]
    cdef Py_ssize_t i, heap_size = 0, null_count = 0, ni = 0, count
    cdef Py_ssize_t pos, parent_pos, left, right, worst
    cdef Py_ssize_t valid_count, take_count, needed
    cdef int64_t val, parent_val, worst_val, tmp_val
    cdef int32_t tmp_idx
    cdef int64_t* heap_vals = NULL
    cdef int32_t* heap_idxs = NULL
    cdef int32_t* null_buf = NULL
    cdef int[::1] rv

    with nogil:
        for i in range(n):
            if compressed[i] == null_val:
                null_count += 1

    valid_count = n - null_count
    take_count = k if k < valid_count else valid_count
    if take_count == 0:
        return array("i")

    heap_vals = <int64_t*>PyMem_Malloc(take_count * sizeof(int64_t))
    heap_idxs = <int32_t*>PyMem_Malloc(take_count * sizeof(int32_t))
    # Allocate null_count + 1 to guarantee a non-NULL pointer even when null_count == 0,
    # keeping the nogil scan branch unconditional.
    null_buf  = <int32_t*>PyMem_Malloc((null_count + 1) * sizeof(int32_t))
    if heap_vals == NULL or heap_idxs == NULL or null_buf == NULL:
        PyMem_Free(heap_vals)
        PyMem_Free(heap_idxs)
        PyMem_Free(null_buf)
        raise MemoryError()

    try:
        with nogil:
            for i in range(n):
                val = compressed[i]
                if val == null_val:
                    null_buf[ni] = <int32_t>i
                    ni += 1
                    continue

                if heap_size < take_count:
                    # Fill phase: insert and sift up.
                    heap_vals[heap_size] = val
                    heap_idxs[heap_size] = <int32_t>i
                    heap_size += 1
                    pos = heap_size - 1
                    while pos > 0:
                        parent_pos = (pos - 1) >> 1
                        parent_val = heap_vals[parent_pos]
                        if (not descending and heap_vals[pos] > parent_val) or \
                           (descending     and heap_vals[pos] < parent_val):
                            tmp_val = heap_vals[pos]
                            heap_vals[pos] = parent_val
                            heap_vals[parent_pos] = tmp_val
                            tmp_idx = heap_idxs[pos]
                            heap_idxs[pos] = heap_idxs[parent_pos]
                            heap_idxs[parent_pos] = tmp_idx
                            pos = parent_pos
                        else:
                            break
                else:
                    # Replace phase: only if this row beats the heap root (worst kept).
                    worst_val = heap_vals[0]
                    if (not descending and val >= worst_val) or \
                       (descending     and val <= worst_val):
                        continue
                    heap_vals[0] = val
                    heap_idxs[0] = <int32_t>i
                    pos = 0
                    while True:
                        left  = (pos << 1) + 1
                        right = left + 1
                        worst = pos
                        if left < heap_size:
                            if (not descending and heap_vals[left] > heap_vals[worst]) or \
                               (descending     and heap_vals[left] < heap_vals[worst]):
                                worst = left
                        if right < heap_size:
                            if (not descending and heap_vals[right] > heap_vals[worst]) or \
                               (descending     and heap_vals[right] < heap_vals[worst]):
                                worst = right
                        if worst == pos:
                            break
                        tmp_val = heap_vals[pos]
                        heap_vals[pos] = heap_vals[worst]
                        heap_vals[worst] = tmp_val
                        tmp_idx = heap_idxs[pos]
                        heap_idxs[pos] = heap_idxs[worst]
                        heap_idxs[worst] = tmp_idx
                        pos = worst

        # Insertion-sort the heap entries by value (k is small).
        with nogil:
            for i in range(1, heap_size):
                tmp_val = heap_vals[i]
                tmp_idx = heap_idxs[i]
                pos = i
                while pos > 0:
                    if (not descending and heap_vals[pos - 1] > tmp_val) or \
                       (descending     and heap_vals[pos - 1] < tmp_val):
                        heap_vals[pos] = heap_vals[pos - 1]
                        heap_idxs[pos] = heap_idxs[pos - 1]
                        pos -= 1
                    else:
                        break
                heap_vals[pos] = tmp_val
                heap_idxs[pos] = tmp_idx

        result = array("i", b"\x00" * (k * 4))
        rv = result
        memcpy(&rv[0], heap_idxs, heap_size * sizeof(int32_t))
        count = heap_size
        needed = k - heap_size
        if needed > 0 and null_count > 0:
            needed = needed if needed < null_count else null_count
            for i in range(needed):
                rv[count] = null_buf[i]
                count += 1
        return result[:count] if count < k else result

    finally:
        PyMem_Free(heap_vals)
        PyMem_Free(heap_idxs)
        PyMem_Free(null_buf)


cdef object _compressed_threshold_candidates(
    int64_t[::1] compressed,
    Py_ssize_t k,
    bint descending,
    int64_t null_val,
):
    """
    Returns all valid row indices whose compressed value meets or beats the k-th
    order statistic (threshold).  May return more than k entries on ties.
    Returns None if fewer than k valid rows exist.
    Used to pre-filter candidates before a full multi-key sort.
    """
    cdef Py_ssize_t n = compressed.shape[0]
    cdef Py_ssize_t i, valid_count = 0
    cdef int64_t val, threshold

    with nogil:
        for i in range(n):
            if compressed[i] != null_val:
                valid_count += 1

    if valid_count < k:
        return None

    top_k = _compressed_top_k(compressed, k, descending, null_val)
    if len(top_k) < k:
        return None

    # After insertion-sort: top_k[-1] is the k-th best (worst of the kept).
    threshold = compressed[<Py_ssize_t>top_k[k - 1]]

    candidates = []
    for i in range(n):
        val = compressed[i]
        if val == null_val:
            continue
        if (not descending and val <= threshold) or (descending and val >= threshold):
            candidates.append(i)
    return candidates if len(candidates) >= k else None


cdef inline int _compare_single_vector(
    Py_ssize_t left, Py_ssize_t right, Vector vector, bint descending
) except? -2:
    """Compare two values at given indices in a vector. Returns -1, 0, 1."""
    cdef int cmp = vector.compare_at(left, right)
    return -cmp if descending else cmp


cdef void _sift_up_single_vector(
    int32_t* buf, Py_ssize_t pos, Vector vector, bint descending
) except *:
    """Sift up in a worst-first heap of row indices."""
    cdef Py_ssize_t parent
    cdef int32_t tmp
    while pos > 0:
        parent = (pos - 1) >> 1
        if _compare_single_vector(buf[pos], buf[parent], vector, descending) > 0:
            tmp = buf[pos]; buf[pos] = buf[parent]; buf[parent] = tmp
            pos = parent
        else:
            break


cdef void _sift_down_single_vector(
    int32_t* buf, Py_ssize_t pos, Py_ssize_t size, Vector vector, bint descending
) except *:
    """Sift down in a worst-first heap of row indices."""
    cdef Py_ssize_t left, right, worst
    cdef int32_t tmp
    while True:
        left = (pos << 1) + 1
        right = left + 1
        worst = pos
        if left < size and _compare_single_vector(buf[left], buf[worst], vector, descending) > 0:
            worst = left
        if right < size and _compare_single_vector(buf[right], buf[worst], vector, descending) > 0:
            worst = right
        if worst == pos:
            break
        tmp = buf[pos]; buf[pos] = buf[worst]; buf[worst] = tmp
        pos = worst


# ── German-string single-key top-N (prefix normalized key) ──────────────────
# Sort string columns on an int64 key built straight from the slot's big-endian
# 4-byte prefix (str_prefix4 — already in the slot, no byte re-extraction, no
# to_pylist). The top-k heap compares those int64 keys (register compare, dense
# array, no slot loads); only on a prefix-key TIE does it fall back to a full
# str_compare. So high-prefix-entropy columns (SearchPhrase) fly, and low-entropy
# ones (URL → all "http") degrade gracefully to str_compare — never wrong.

cdef inline void _build_string_prefix_keys(DrakenVector* dv, int64_t* out) noexcept nogil:
    # out[i] = signed-int64 key whose order == lexicographic order of the first
    # 4 bytes. prefix4 is big-endian (unsigned-lex); flip the sign bit so signed
    # int64 compare matches. Null rows get a don't-care key (handled via validity).
    cdef DrakenStringArena* sa = <DrakenStringArena*>dv.data
    cdef const uint32_t* sel = dv.selection
    cdef DrakenStringSlot* slots = sa.slots
    cdef uint32_t n = dv.length
    cdef uint32_t i
    cdef uint64_t u
    for i in range(n):
        u = (<uint64_t>str_prefix4(&slots[sel[i]])) << 32
        out[i] = <int64_t>(u ^ <uint64_t>0x8000000000000000ULL)


cdef inline bint _row_is_null(DrakenVector* dv, Py_ssize_t i) noexcept nogil:
    if dv.validity == NULL:
        return False
    return ((dv.validity[i >> 3] >> (i & 7)) & 1) == 0


cdef inline int _cmp_string_keyed(
    int64_t* keys, DrakenStringSlot* slots, const uint8_t* arena, const uint32_t* sel,
    Py_ssize_t a, Py_ssize_t b, bint descending
) noexcept nogil:
    """ASC lexicographic comparison via the prefix key, str_compare on key-tie;
    negated for descending. Same -1/0/1 convention as _compare_single_vector."""
    cdef int c
    cdef int64_t ka = keys[a]
    cdef int64_t kb = keys[b]
    if ka != kb:
        c = -1 if ka < kb else 1
    else:
        c = str_compare(&slots[sel[a]], arena, &slots[sel[b]], arena)
    return -c if descending else c


cdef void _sift_up_string_keyed(
    int32_t* buf, Py_ssize_t pos, int64_t* keys, DrakenStringSlot* slots,
    const uint8_t* arena, const uint32_t* sel, bint descending
) noexcept nogil:
    cdef Py_ssize_t parent
    cdef int32_t tmp
    while pos > 0:
        parent = (pos - 1) >> 1
        if _cmp_string_keyed(keys, slots, arena, sel, buf[pos], buf[parent], descending) > 0:
            tmp = buf[pos]; buf[pos] = buf[parent]; buf[parent] = tmp
            pos = parent
        else:
            break


cdef void _sift_down_string_keyed(
    int32_t* buf, Py_ssize_t pos, Py_ssize_t size, int64_t* keys, DrakenStringSlot* slots,
    const uint8_t* arena, const uint32_t* sel, bint descending
) noexcept nogil:
    cdef Py_ssize_t left, right, worst
    cdef int32_t tmp
    while True:
        left = (pos << 1) + 1
        right = left + 1
        worst = pos
        if left < size and _cmp_string_keyed(keys, slots, arena, sel, buf[left], buf[worst], descending) > 0:
            worst = left
        if right < size and _cmp_string_keyed(keys, slots, arena, sel, buf[right], buf[worst], descending) > 0:
            worst = right
        if worst == pos:
            break
        tmp = buf[pos]; buf[pos] = buf[worst]; buf[worst] = tmp
        pos = worst


# ── Numeric single-key top-N (NO compress — sort on the raw field values) ────
# Numeric values are already order-comparable, so there is no key to build:
# read data[selection[i]] in its native width and compare in-register. No
# to_pylist, no int64 key array, no transform pass. Matches draken_vector_
# compare_at exactly (signed int order for integer family; NaN-highest, -0==+0
# total order for floats).

cdef inline int64_t _num_read_i64(void* data, DrakenType t, uint32_t idx) noexcept nogil:
    if t == DRAKEN_INT64 or t == DRAKEN_TIMESTAMP64 or t == DRAKEN_TIME64 or t == DRAKEN_DECIMAL:
        return (<int64_t*>data)[idx]
    elif t == DRAKEN_INT32 or t == DRAKEN_DATE32 or t == DRAKEN_TIME32:
        return <int64_t>(<int32_t*>data)[idx]
    elif t == DRAKEN_INT16:
        return <int64_t>(<int16_t*>data)[idx]
    elif t == DRAKEN_INT8:
        return <int64_t>(<int8_t*>data)[idx]
    else:  # DRAKEN_BOOL
        return <int64_t>((<uint8_t*>data)[idx] & 1)


cdef inline double _num_read_f64(void* data, DrakenType t, uint32_t idx) noexcept nogil:
    if t == DRAKEN_FLOAT64:
        return (<double*>data)[idx]
    else:  # DRAKEN_FLOAT32
        return <double>(<float*>data)[idx]


cdef inline int _num_cmp_f64(double a, double b) noexcept nogil:
    # Total order matching draken: NaN == NaN and sorts highest; -0.0 == +0.0.
    if a != a:
        return 0 if b != b else 1
    if b != b:
        return -1
    return -1 if a < b else (1 if a > b else 0)


cdef inline int _cmp_num(
    void* data, DrakenType t, bint is_float, const uint32_t* sel,
    Py_ssize_t a, Py_ssize_t b, bint descending
) noexcept nogil:
    cdef int c
    cdef int64_t ia, ib
    if is_float:
        c = _num_cmp_f64(_num_read_f64(data, t, sel[a]), _num_read_f64(data, t, sel[b]))
    else:
        ia = _num_read_i64(data, t, sel[a])
        ib = _num_read_i64(data, t, sel[b])
        c = -1 if ia < ib else (1 if ia > ib else 0)
    return -c if descending else c


cdef void _sift_up_num(
    int32_t* buf, Py_ssize_t pos, void* data, DrakenType t, bint is_float,
    const uint32_t* sel, bint descending
) noexcept nogil:
    cdef Py_ssize_t parent
    cdef int32_t tmp
    while pos > 0:
        parent = (pos - 1) >> 1
        if _cmp_num(data, t, is_float, sel, buf[pos], buf[parent], descending) > 0:
            tmp = buf[pos]; buf[pos] = buf[parent]; buf[parent] = tmp
            pos = parent
        else:
            break


cdef void _sift_down_num(
    int32_t* buf, Py_ssize_t pos, Py_ssize_t size, void* data, DrakenType t, bint is_float,
    const uint32_t* sel, bint descending
) noexcept nogil:
    cdef Py_ssize_t left, right, worst
    cdef int32_t tmp
    while True:
        left = (pos << 1) + 1
        right = left + 1
        worst = pos
        if left < size and _cmp_num(data, t, is_float, sel, buf[left], buf[worst], descending) > 0:
            worst = left
        if right < size and _cmp_num(data, t, is_float, sel, buf[right], buf[worst], descending) > 0:
            worst = right
        if worst == pos:
            break
        tmp = buf[pos]; buf[pos] = buf[worst]; buf[worst] = tmp
        pos = worst


# ── Multi-key top-N (native per-column compare, short-circuit; NO compare_at) ─
# One comparator that reads each ORDER BY column's native value in nogil and
# short-circuits on the first discriminating column — numeric via _num_*, string
# via the prefix key + str_compare tiebreak. Replaces the per-column compare_at
# (Python-boundary) calls in _compare_rows_vectors. Null ordering matches
# _compare_rows_vectors exactly: nulls sort last, direction-independent.

cdef struct ColMeta:
    DrakenVector*      dv
    DrakenType         dtype
    int                kind        # 0 = int-family, 1 = float, 2 = string
    bint               desc
    int64_t*           pkeys       # string prefix keys (kind==2) else NULL
    DrakenStringSlot*  slots       # kind==2
    const uint8_t*     arena       # kind==2
    const uint32_t*    sel
    void*              data        # numeric data ptr (kind 0/1)


cdef inline int _cmp_multi(ColMeta* cols, Py_ssize_t ncols, Py_ssize_t a, Py_ssize_t b) noexcept nogil:
    cdef Py_ssize_t c
    cdef ColMeta* m
    cdef int cc
    cdef bint ln, rn
    cdef int64_t ia, ib, ka, kb
    for c in range(ncols):
        m = &cols[c]
        ln = _row_is_null(m.dv, a)
        rn = _row_is_null(m.dv, b)
        if ln and rn:
            continue
        if ln:
            return 1          # null sorts last (direction-independent)
        if rn:
            return -1
        if m.kind == 2:
            ka = m.pkeys[a]; kb = m.pkeys[b]
            if ka != kb:
                cc = -1 if ka < kb else 1
            else:
                cc = str_compare(&m.slots[m.sel[a]], m.arena, &m.slots[m.sel[b]], m.arena)
        elif m.kind == 1:
            cc = _num_cmp_f64(_num_read_f64(m.data, m.dtype, m.sel[a]),
                              _num_read_f64(m.data, m.dtype, m.sel[b]))
        else:
            ia = _num_read_i64(m.data, m.dtype, m.sel[a])
            ib = _num_read_i64(m.data, m.dtype, m.sel[b])
            cc = -1 if ia < ib else (1 if ia > ib else 0)
        if cc != 0:
            return -cc if m.desc else cc
    return 0


cdef void _sift_up_multi(int32_t* buf, Py_ssize_t pos, ColMeta* cols, Py_ssize_t ncols) noexcept nogil:
    cdef Py_ssize_t parent
    cdef int32_t tmp
    while pos > 0:
        parent = (pos - 1) >> 1
        if _cmp_multi(cols, ncols, buf[pos], buf[parent]) > 0:
            tmp = buf[pos]; buf[pos] = buf[parent]; buf[parent] = tmp
            pos = parent
        else:
            break


cdef void _sift_down_multi(int32_t* buf, Py_ssize_t pos, Py_ssize_t size, ColMeta* cols, Py_ssize_t ncols) noexcept nogil:
    cdef Py_ssize_t left, right, worst
    cdef int32_t tmp
    while True:
        left = (pos << 1) + 1
        right = left + 1
        worst = pos
        if left < size and _cmp_multi(cols, ncols, buf[left], buf[worst]) > 0:
            worst = left
        if right < size and _cmp_multi(cols, ncols, buf[right], buf[worst]) > 0:
            worst = right
        if worst == pos:
            break
        tmp = buf[pos]; buf[pos] = buf[worst]; buf[worst] = tmp
        pos = worst


# ── Node ──────────────────────────────────────────────────────────────────────

_EXACT_COMPRESS_VECTOR_TYPES = frozenset({
    "Vector", "BoolVector",
})


cdef class HeapSortNode(BasePlanNode):
    cdef public list order_by
    cdef public object limit
    cdef public bint vector_topk_candidate
    cdef public list mapped_order
    cdef public object _uniform_direction
    cdef public dict _compress_cache
    cdef public list _chunk_buffer
    cdef public list _compiled_evals

    _NULL_COMPRESSED = -(1 << 63)  # INT64_MIN — same sentinel used by compress_into
    _USEARCH_ENABLED = False
    _USEARCH_MIN_ROWS = 2048

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.order_by = parameters.get("order_by", [])
        self.limit = parameters.get("limit", -1)
        self.vector_topk_candidate = bool(parameters.get("vector_topk_candidate", False))

        self.mapped_order = []
        for column, direction in self.order_by:
            try:
                self.mapped_order.append((column.schema_column.identity, direction))
            except ColumnNotFoundError as cnfe:
                raise ColumnNotFoundError(
                    f"`ORDER BY` must reference columns from `SELECT`. {cnfe}"
                ) from cnfe

        any_desc = False
        all_desc = True
        for _, ascending in self.mapped_order:
            if not ascending:
                any_desc = True
            else:
                all_desc = False
        self._uniform_direction = True if all_desc else (False if not any_desc else None)

        self._compress_cache = {}
        self._chunk_buffer = []
        eval_nodes = [col for col, _ in self.order_by if col.node_type != NodeType.IDENTIFIER]
        self._compiled_evals = compile_eval_nodes(eval_nodes)

    def _is_exact_compressible_vector(self, vector) -> bool:
        name = vector.__class__.__name__
        result = self._compress_cache.get(name)
        if result is None:
            result = name in _EXACT_COMPRESS_VECTOR_TYPES
            self._compress_cache[name] = result
        return result

    @property
    def config(self):  # pragma: no cover
        order = ", ".join(
            f"{col.schema_column.name} {'ASC' if ascending else 'DESC'}"
            for col, ascending in self.order_by
        )
        return f"LIMIT = {self.limit}, ORDER = {order}"

    @property
    def name(self):  # pragma: no cover
        return "Heap Sort"

    cdef void _dispatch_push(self, Morsel morsel) except *:
        cdef Py_ssize_t chunk_rows

        if morsel is _EOS_SENTINEL:
            if not self._chunk_buffer:
                self._emit_cdef(_EOS_SENTINEL)
                return
            table = Morsel.combine(self._chunk_buffer)
            if self.mapped_order:
                table = self._top_n(table)
            self._emit_cdef(table)
            self._emit_cdef(_EOS_SENTINEL)
            return

        chunk_rows = morsel.num_rows
        if chunk_rows == 0:
            return

        chunk = self._top_n(morsel)
        if chunk.num_rows > 0:
            self._chunk_buffer.append(chunk)

    cdef Morsel _materialize_rows(self, Morsel morsel, row_indices):
        if not row_indices:
            return morsel.empty()
        return morsel.take(row_indices)

    cdef Morsel _ensure_order_expressions_evaluated(self, Morsel morsel):
        return execute_and_append(self._compiled_evals, morsel)

    def _top_n(self, morsel):
        cdef Py_ssize_t k, row_idx, i, j, heap_size = 0
        cdef int32_t* heap_buf = NULL
        cdef int32_t pivot_idx, tmp
        cdef int[::1] rv

        vector_ranked = self._vector_top_n(morsel)
        if vector_ranked is not None:
            return vector_ranked

        morsel = self._ensure_order_expressions_evaluated(morsel)

        k = min(<Py_ssize_t>self.limit, <Py_ssize_t>morsel.num_rows)
        if k == 0:
            return morsel.empty()

        if len(self.mapped_order) == 1:
            return self._top_n_single_key(morsel, k)

        # Native multi-column comparator (no compare_at); returns None if any key
        # column is an unsupported type, falling back to the paths below.
        keyed = self._top_n_multi_key_keyed(morsel, k)
        if keyed is not None:
            return keyed

        if self._uniform_direction is not None:
            return self._top_n_multi_key_uniform(morsel, k, descending=<bint>self._uniform_direction)

        key_vectors = [morsel.column(column) for column, _ in self.mapped_order]
        directions  = [_is_descending(direction) for _, direction in self.mapped_order]

        heap_buf = <int32_t*>PyMem_Malloc(k * sizeof(int32_t))
        if heap_buf == NULL:
            raise MemoryError()
        try:
            for row_idx in range(<Py_ssize_t>morsel.num_rows):
                if heap_size < k:
                    heap_buf[heap_size] = <int32_t>row_idx
                    heap_size += 1
                    _mh_sift_up(heap_buf, heap_size - 1, key_vectors, directions)
                elif _compare_rows_vectors(row_idx, heap_buf[0], key_vectors, directions) < 0:
                    heap_buf[0] = <int32_t>row_idx
                    _mh_sift_down(heap_buf, 0, heap_size, key_vectors, directions)

            # Insertion-sort the heap by row ordering.
            for i in range(1, heap_size):
                pivot_idx = heap_buf[i]
                j = i
                while j > 0 and _compare_rows_vectors(heap_buf[j - 1], pivot_idx, key_vectors, directions) > 0:
                    heap_buf[j] = heap_buf[j - 1]
                    j -= 1
                heap_buf[j] = pivot_idx

            top_indices = array("i", b"\x00" * (heap_size * 4))
            rv = top_indices
            memcpy(&rv[0], heap_buf, heap_size * sizeof(int32_t))
        finally:
            PyMem_Free(heap_buf)
        return self._materialize_rows(morsel, top_indices)

    cdef Morsel _top_n_multi_key_keyed(self, Morsel morsel, Py_ssize_t k):
        cdef Py_ssize_t ncols = len(self.mapped_order)
        cdef Py_ssize_t n = morsel.num_rows
        cdef Py_ssize_t c, i, j, heap_size = 0
        cdef int32_t pivot_idx
        cdef int[::1] rv
        cdef DrakenVector* dv
        cdef DrakenType t
        cdef Vector vec
        cdef DrakenStringArena* sa
        cdef bint supported = True
        cdef ColMeta* cols = <ColMeta*>PyMem_Malloc(ncols * sizeof(ColMeta))
        cdef int32_t* heap_buf = NULL
        if cols == NULL:
            raise MemoryError()
        for c in range(ncols):
            cols[c].pkeys = NULL

        vectors = [morsel.column(col) for col, _ in self.mapped_order]
        try:
            for c in range(ncols):
                vec = vectors[c]
                dv = vec.unified()
                t = dv.type
                cols[c].dv = dv
                cols[c].dtype = t
                cols[c].desc = _is_descending(self.mapped_order[c][1])
                cols[c].sel = dv.selection
                if t == DRAKEN_VARCHAR or t == DRAKEN_NVARCHAR or t == DRAKEN_VARBINARY:
                    cols[c].kind = 2
                    sa = <DrakenStringArena*>dv.data
                    cols[c].slots = sa.slots
                    cols[c].arena = sa.arena
                    cols[c].data = NULL
                    cols[c].pkeys = <int64_t*>PyMem_Malloc(n * sizeof(int64_t))
                    if cols[c].pkeys == NULL:
                        raise MemoryError()
                    _build_string_prefix_keys(dv, cols[c].pkeys)
                elif t == DRAKEN_FLOAT32 or t == DRAKEN_FLOAT64:
                    cols[c].kind = 1
                    cols[c].data = dv.data
                elif (t == DRAKEN_INT8 or t == DRAKEN_INT16 or t == DRAKEN_INT32 or t == DRAKEN_INT64
                      or t == DRAKEN_DECIMAL or t == DRAKEN_DATE32 or t == DRAKEN_TIMESTAMP64
                      or t == DRAKEN_TIME32 or t == DRAKEN_TIME64 or t == DRAKEN_BOOL):
                    cols[c].kind = 0
                    cols[c].data = dv.data
                else:
                    supported = False
                    break

            if not supported:
                return None

            heap_buf = <int32_t*>PyMem_Malloc(k * sizeof(int32_t))
            if heap_buf == NULL:
                raise MemoryError()

            with nogil:
                for i in range(n):
                    if heap_size < k:
                        heap_buf[heap_size] = <int32_t>i
                        heap_size += 1
                        _sift_up_multi(heap_buf, heap_size - 1, cols, ncols)
                    elif _cmp_multi(cols, ncols, i, heap_buf[0]) < 0:
                        heap_buf[0] = <int32_t>i
                        _sift_down_multi(heap_buf, 0, heap_size, cols, ncols)

                for i in range(1, heap_size):
                    pivot_idx = heap_buf[i]
                    j = i
                    while j > 0 and _cmp_multi(cols, ncols, heap_buf[j - 1], pivot_idx) > 0:
                        heap_buf[j] = heap_buf[j - 1]
                        j -= 1
                    heap_buf[j] = pivot_idx

            top_indices = array("i", b"\x00" * (heap_size * 4))
            rv = top_indices
            memcpy(&rv[0], heap_buf, heap_size * sizeof(int32_t))
            return self._materialize_rows(morsel, top_indices)
        finally:
            for c in range(ncols):
                if cols[c].pkeys != NULL:
                    PyMem_Free(cols[c].pkeys)
            PyMem_Free(cols)
            PyMem_Free(heap_buf)

    cdef Morsel _top_n_single_key(self, Morsel morsel, Py_ssize_t k):
        cdef bint descending
        cdef Py_ssize_t take_count

        column_name, direction = self.mapped_order[0]
        descending = _is_descending(direction)
        vector = morsel.column(column_name)

        # German-string prefix-keyed fast path (string columns only; returns None otherwise).
        string_path = self._top_n_single_key_string(morsel, vector, descending, k)
        if string_path is not None:
            return string_path

        # Numeric direct-value fast path (no compress; returns None for non-numeric).
        numeric_path = self._top_n_single_key_numeric(morsel, vector, descending, k)
        if numeric_path is not None:
            return numeric_path

        fast_path = self._top_n_single_key_compressed(morsel, vector, descending, k)
        if fast_path is not None:
            return fast_path

        return self._top_n_single_key_vector(morsel, vector, descending, k)

    cdef Morsel _top_n_single_key_numeric(self, Morsel morsel, Vector vector, bint descending, Py_ssize_t k):
        cdef DrakenVector* dv = vector.unified()
        cdef DrakenType t = dv.type
        cdef bint is_float = (t == DRAKEN_FLOAT32 or t == DRAKEN_FLOAT64)
        cdef bint is_int = (
            t == DRAKEN_INT8 or t == DRAKEN_INT16 or t == DRAKEN_INT32 or t == DRAKEN_INT64
            or t == DRAKEN_DECIMAL or t == DRAKEN_DATE32 or t == DRAKEN_TIMESTAMP64
            or t == DRAKEN_TIME32 or t == DRAKEN_TIME64 or t == DRAKEN_BOOL
        )
        if not (is_float or is_int):
            return None
        cdef Py_ssize_t n = morsel.num_rows
        if n == 0 or k <= 0:
            return None

        cdef void* data = dv.data
        cdef const uint32_t* sel = dv.selection

        cdef int32_t* heap_buf = <int32_t*>PyMem_Malloc(k * sizeof(int32_t))
        if heap_buf == NULL:
            raise MemoryError()

        cdef Py_ssize_t heap_size = 0, i, j, count
        cdef int32_t pivot_idx
        cdef int[::1] rv
        try:
            with nogil:
                for i in range(n):
                    if _row_is_null(dv, i):
                        continue
                    if heap_size < k:
                        heap_buf[heap_size] = <int32_t>i
                        heap_size += 1
                        _sift_up_num(heap_buf, heap_size - 1, data, t, is_float, sel, descending)
                    elif _cmp_num(data, t, is_float, sel, i, heap_buf[0], descending) < 0:
                        heap_buf[0] = <int32_t>i
                        _sift_down_num(heap_buf, 0, heap_size, data, t, is_float, sel, descending)

                for i in range(1, heap_size):
                    pivot_idx = heap_buf[i]
                    j = i
                    while j > 0 and _cmp_num(data, t, is_float, sel, heap_buf[j - 1], pivot_idx, descending) > 0:
                        heap_buf[j] = heap_buf[j - 1]
                        j -= 1
                    heap_buf[j] = pivot_idx

            result = array("i", b"\x00" * (k * 4))
            rv = result
            memcpy(&rv[0], heap_buf, heap_size * sizeof(int32_t))
            count = heap_size
            if count < k:
                for i in range(n):
                    if count >= k:
                        break
                    if _row_is_null(dv, i):
                        rv[count] = <int32_t>i
                        count += 1
            return self._materialize_rows(morsel, result[:count] if count < k else result)
        finally:
            PyMem_Free(heap_buf)

    cdef Morsel _top_n_single_key_string(self, Morsel morsel, Vector vector, bint descending, Py_ssize_t k):
        cdef DrakenVector* dv = vector.unified()
        if not (dv.type == DRAKEN_VARCHAR or dv.type == DRAKEN_NVARCHAR or dv.type == DRAKEN_VARBINARY):
            return None
        cdef Py_ssize_t n = morsel.num_rows
        if n == 0 or k <= 0:
            return None

        cdef DrakenStringArena* sa = <DrakenStringArena*>dv.data
        cdef DrakenStringSlot* slots = sa.slots
        cdef const uint8_t* arena = sa.arena
        cdef const uint32_t* sel = dv.selection

        cdef int64_t* keys = <int64_t*>PyMem_Malloc(n * sizeof(int64_t))
        cdef int32_t* heap_buf = <int32_t*>PyMem_Malloc(k * sizeof(int32_t))
        if keys == NULL or heap_buf == NULL:
            PyMem_Free(keys)
            PyMem_Free(heap_buf)
            raise MemoryError()

        cdef Py_ssize_t heap_size = 0, i, j, count
        cdef int32_t pivot_idx
        cdef int[::1] rv
        try:
            with nogil:
                _build_string_prefix_keys(dv, keys)
                for i in range(n):
                    if _row_is_null(dv, i):
                        continue
                    if heap_size < k:
                        heap_buf[heap_size] = <int32_t>i
                        heap_size += 1
                        _sift_up_string_keyed(heap_buf, heap_size - 1, keys, slots, arena, sel, descending)
                    elif _cmp_string_keyed(keys, slots, arena, sel, i, heap_buf[0], descending) < 0:
                        heap_buf[0] = <int32_t>i
                        _sift_down_string_keyed(heap_buf, 0, heap_size, keys, slots, arena, sel, descending)

                # Insertion-sort the heap into fully ordered output.
                for i in range(1, heap_size):
                    pivot_idx = heap_buf[i]
                    j = i
                    while j > 0 and _cmp_string_keyed(keys, slots, arena, sel, heap_buf[j - 1], pivot_idx, descending) > 0:
                        heap_buf[j] = heap_buf[j - 1]
                        j -= 1
                    heap_buf[j] = pivot_idx

            result = array("i", b"\x00" * (k * 4))
            rv = result
            memcpy(&rv[0], heap_buf, heap_size * sizeof(int32_t))
            count = heap_size
            if count < k:
                for i in range(n):
                    if count >= k:
                        break
                    if _row_is_null(dv, i):
                        rv[count] = <int32_t>i
                        count += 1
            return self._materialize_rows(morsel, result[:count] if count < k else result)
        finally:
            PyMem_Free(keys)
            PyMem_Free(heap_buf)

    cdef Morsel _top_n_single_key_vector(self, Morsel morsel, Vector vector, bint descending, Py_ssize_t k):
        """Top-k on single column using native vector comparators only. No Python fallback."""
        cdef Py_ssize_t n = morsel.num_rows
        cdef Py_ssize_t i, heap_size = 0, pivot_idx, j, count
        cdef int32_t* heap_buf = <int32_t*>PyMem_Malloc(k * sizeof(int32_t))
        cdef int[::1] rv

        if heap_buf == NULL:
            raise MemoryError()

        try:
            for i in range(n):
                if vector.is_null_at(i):
                    continue

                if heap_size < k:
                    heap_buf[heap_size] = <int32_t>i
                    heap_size += 1
                    _sift_up_single_vector(heap_buf, heap_size - 1, vector, descending)
                elif _compare_single_vector(i, heap_buf[0], vector, descending) < 0:
                    heap_buf[0] = <int32_t>i
                    _sift_down_single_vector(heap_buf, 0, heap_size, vector, descending)

            # Insertion-sort heap
            for i in range(1, heap_size):
                pivot_idx = heap_buf[i]
                j = i
                while j > 0 and _compare_single_vector(heap_buf[j - 1], pivot_idx, vector, descending) > 0:
                    heap_buf[j] = heap_buf[j - 1]
                    j -= 1
                heap_buf[j] = pivot_idx

            result = array("i", b"\x00" * (k * 4))
            rv = result
            memcpy(&rv[0], heap_buf, heap_size * sizeof(int32_t))
            count = heap_size
            if count < k:
                for i in range(n):
                    if count >= k:
                        break
                    if vector.is_null_at(i):
                        rv[count] = <int32_t>i
                        count += 1

            return self._materialize_rows(morsel, result[:count] if count < k else result)
        finally:
            PyMem_Free(heap_buf)

    cdef Morsel _top_n_multi_key_uniform(self, Morsel morsel, Py_ssize_t k, bint descending):
        vectors = [morsel.column(col) for col, _ in self.mapped_order]
        directions = [_is_descending(direction) for _, direction in self.mapped_order]

        candidate_indices = self._candidate_indices_from_first_key(morsel, k, descending)
        search_space = (
            candidate_indices if candidate_indices is not None else range(morsel.num_rows)
        )

        return self._heap_top_k_multi_vector(morsel, vectors, directions, k, search_space)

    cdef Morsel _heap_top_k_multi_vector(self, Morsel morsel, list vectors, list directions, Py_ssize_t k, search_space):
        """Top-k multi-column using vector comparator, no Python materialization."""
        cdef Py_ssize_t heap_size = 0, idx, i, j
        cdef int32_t* heap_buf = <int32_t*>PyMem_Malloc(k * sizeof(int32_t))
        cdef int32_t pivot_idx
        cdef int[::1] rv

        if heap_buf == NULL:
            raise MemoryError()

        try:
            for idx in search_space:
                if heap_size < k:
                    heap_buf[heap_size] = <int32_t>idx
                    heap_size += 1
                    _mh_sift_up(heap_buf, heap_size - 1, vectors, directions)
                elif _compare_rows_vectors(idx, heap_buf[0], vectors, directions) < 0:
                    heap_buf[0] = <int32_t>idx
                    _mh_sift_down(heap_buf, 0, heap_size, vectors, directions)

            # Insertion-sort heap
            for i in range(1, heap_size):
                pivot_idx = heap_buf[i]
                j = i
                while j > 0 and _compare_rows_vectors(heap_buf[j - 1], pivot_idx, vectors, directions) > 0:
                    heap_buf[j] = heap_buf[j - 1]
                    j -= 1
                heap_buf[j] = pivot_idx

            result = array("i", b"\x00" * (heap_size * 4))
            rv = result
            memcpy(&rv[0], heap_buf, heap_size * sizeof(int32_t))
            return self._materialize_rows(morsel, result)
        finally:
            PyMem_Free(heap_buf)

    cdef _candidate_indices_from_first_key(self, Morsel morsel, Py_ssize_t k, bint descending):
        cdef int64_t[::1] compressed

        first_column = self.mapped_order[0][0]
        first_vector = morsel.column(first_column)
        if not self._is_exact_compressible_vector(first_vector):
            return None
        try:
            compressed = first_vector.compress()
        except Exception:
            return None
        return _compressed_threshold_candidates(compressed, k, descending, -(1 << 63))

    cdef _top_n_single_key_compressed(self, Morsel morsel, Vector vector, bint descending, Py_ssize_t k):
        cdef int64_t[::1] compressed

        if not self._is_exact_compressible_vector(vector):
            return None
        try:
            compressed = vector.compress()
        except Exception:
            return None
        if compressed.shape[0] != morsel.num_rows:
            return None
        selected = _compressed_top_k(compressed, k, descending, -(1 << 63))
        return self._materialize_rows(morsel, selected)

    def _vector_top_n(self, morsel):
        cdef Py_ssize_t take_count, row_index, i, j, dims
        cdef bint nearest_neighbor_order, descending

        if len(self.order_by) != 1:
            return None

        order_expression, direction = self.order_by[0]
        if order_expression.node_type != NodeType.FUNCTION:
            return None
        if order_expression.value not in ("COSINE_SIMILARITY", "COSINE_DISTANCE"):
            return None
        if len(order_expression.parameters) != 2:
            return None

        source_node, query_node = order_expression.parameters
        source_identifier = get_vector_source_identifier(source_node)
        if source_identifier is None:
            return None
        if not node_is_numeric_vector(source_node) or not node_is_vector_query_expression(
            query_node
        ):
            return None

        query_vector = self._resolve_query_vector(query_node)
        if query_vector is None or query_vector.size == 0:
            return None

        # Convert query vector to float32 buffer (fail-fast if invalid)
        query_floats = self._validate_numeric_sequence(query_vector)
        if query_floats is None:
            return None

        dims = len(query_floats)
        cdef float* query_buf = <float*>malloc(dims * sizeof(float))
        if query_buf == NULL:
            raise MemoryError("Failed to allocate query vector buffer")

        cdef float[::1] query_view = <float[:dims]>query_buf
        for i in range(dims):
            query_view[i] = query_floats[i]

        source_keys = [
            getattr(source_identifier.schema_column, "identity", None),
            getattr(source_identifier, "source_column", None),
            getattr(source_identifier.schema_column, "name", None),
        ]
        source_vector = None
        for source_key in source_keys:
            if not source_key:
                continue
            try:
                source_vector = morsel.column(source_key if isinstance(source_key, bytes) else source_key.encode())
                break
            except Exception:
                continue
        if source_vector is None:
            return None

        # Pass 1: Filter and count valid rows (no Python materialization yet)
        source_indices_list = []
        valid_rows_list = []
        for row_index in range(<Py_ssize_t>morsel.num_rows):
            if source_vector.is_null_at(row_index):
                continue
            # Get row vector value (materializes one row, not all)
            row_vector = self._validate_numeric_sequence(source_vector[row_index])
            if row_vector is None:
                continue
            if len(row_vector) != dims:
                continue
            valid_rows_list.append(row_vector)
            source_indices_list.append(row_index)

        cdef Py_ssize_t n_valid = len(valid_rows_list)
        if n_valid == 0:
            return None

        # Pass 2: Materialize into contiguous buffers
        cdef float* dense_buf = <float*>malloc(n_valid * dims * sizeof(float))
        cdef int64_t* row_ids_buf = <int64_t*>malloc(n_valid * sizeof(int64_t))
        if dense_buf == NULL or row_ids_buf == NULL:
            if dense_buf != NULL: free(dense_buf)
            if row_ids_buf != NULL: free(row_ids_buf)
            raise MemoryError("Failed to allocate vector buffers")

        cdef float[:, ::1] dense_view = <float[:n_valid, :dims]>dense_buf
        cdef int64_t[::1] row_ids_view = <int64_t[:n_valid]>row_ids_buf

        for i in range(n_valid):
            row_ids_view[i] = source_indices_list[i]
            row_floats = valid_rows_list[i]
            # Copy row vector into dense matrix (already validated)
            for j in range(dims):
                dense_view[i, j] = row_floats[j]

        self.readings["vector_topk_candidate_rows"] += n_valid
        take_count = min(<Py_ssize_t>self.limit, n_valid)
        if take_count == 0:
            free(dense_buf)
            free(row_ids_buf)
            return morsel.empty()

        nearest_neighbor_order = _is_nearest_neighbor_order(order_expression.value, direction)

        # Usearch optimization
        if (
            self._USEARCH_ENABLED
            and self.vector_topk_candidate
            and n_valid >= self._USEARCH_MIN_ROWS
            and nearest_neighbor_order
        ):
            try:
                from opteryx.compiled.nanobind import usearch_native

                index = usearch_native.UsearchIndex(
                    dimensions=dims,
                    capacity=n_valid,
                    metric="cos",
                    expansion_add=16,
                    expansion_search=16,
                )
                self.readings["feature_vector_topk_usearch"] += 1
                self.readings["vector_topk_usearch_rows_indexed"] += n_valid
                index.add_batch(row_ids_view, dense_view)
                found_ids, _ = index.search(query_view, take_count)
                if found_ids:
                    free(dense_buf)
                    free(row_ids_buf)
                    return self._materialize_rows(morsel, [int(row_id) for row_id in found_ids])
            except Exception:
                self.readings["feature_vector_topk_usearch_fallbacks"] += 1
            finally:
                pass  # Buffers are freed later if not returned here

        # Exact search
        try:
            from opteryx.compiled.nanobind import vector_search

            if nearest_neighbor_order:
                found_ids, _ = vector_search.exact_search_cosine(
                    query_view,
                    row_ids_view,
                    dense_view,
                    take_count,
                )
                self.readings["feature_vector_topk_exact"] += 1
                if found_ids:
                    result = self._materialize_rows(morsel, [int(row_id) for row_id in found_ids])
                else:
                    result = morsel.empty()
                free(dense_buf)
                free(row_ids_buf)
                return result

            # Score and rank manually (Non-nearest neighbor case or complex sort)
            scores_mv = vector_search.score_cosine(query_view, dense_view)
            self.readings["feature_vector_topk_exact"] += 1
        except Exception:
            free(dense_buf)
            free(row_ids_buf)
            return None

        # Clean up buffers before final ranking (scores_mv is independent)
        free(dense_buf)
        free(row_ids_buf)
        free(query_buf)

        # Normalise scores to Python list
        if getattr(scores_mv, 'tolist', None) is not None:
            scores = scores_mv.tolist()
        else:
            scores = list(scores_mv)

        # Use vector_ranking module for top-k selection and ranking
        metric = order_expression.value  # "COSINE_SIMILARITY" or "COSINE_DISTANCE"
        top_indices = vector_exact_search_top_k(
            similarity_scores=scores,
            source_row_indices=source_indices_list,
            k=take_count,
            metric=metric,
        )

        return self._materialize_rows(morsel, top_indices)

    @staticmethod
    def _validate_numeric_sequence(value):
        """
        Validate and convert sequence to list of floats without NumPy.

        Returns list of floats or None if invalid.
        Fails fast on: non-sequences, non-numeric elements, NaN, infinity.
        """
        try:
            # Check it's sized/iterable
            n = len(value)
        except TypeError:
            return None

        if n == 0:
            return None

        result = []
        for item in value:
            # Type coercion: bool, int, float → float
            if item is None:
                return None
            elif isinstance(item, bool):
                f = float(1.0 if item else 0.0)
            elif isinstance(item, (int, float)):
                f = float(item)
            else:
                # Try generic conversion
                try:
                    f = float(item)
                except (TypeError, ValueError):
                    return None

            # Reject NaN and infinity
            if f != f or f == float('inf') or f == float('-inf'):
                return None

            result.append(f)

        return result

    def _resolve_query_vector(self, query_node):
        if query_node.node_type == NodeType.LITERAL:
            return self._validate_numeric_sequence(query_node.value)
        if (
            query_node.node_type == NodeType.FUNCTION
            and query_node.value == "EMBED"
            and len(query_node.parameters) == 1
            and query_node.parameters[0].node_type == NodeType.LITERAL
        ):
            from opteryx.vectors.embeddings import embed_text_matrix

            embedded = embed_text_matrix([query_node.parameters[0].value])
            if not embedded or not embedded[0]:
                return None
            return self._validate_numeric_sequence(embedded[0])
        return None
