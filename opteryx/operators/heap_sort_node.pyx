# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True
# cython: infer_types=True

import heapq
from collections.abc import Iterable

import numpy
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.exceptions import ColumnNotFoundError
from opteryx.expression import NodeType
from opteryx.expression import evaluate_and_append
from opteryx.models import QueryProperties

# Licensed under the Apache License, Version 2.0 (the "License");
from opteryx.tracing.event_recorder import record_event as _trace_record
from opteryx.vectors.vector_types import get_vector_source_identifier
from opteryx.vectors.vector_types import node_is_numeric_vector
from opteryx.vectors.vector_types import node_is_vector_query_expression

from opteryx import EOS

from . import BasePlanNode

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport int32_t, int64_t

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


_DATA_FORMAT = "arrow,draken"


# ── Module-level C helpers ────────────────────────────────────────────────────

cdef inline bint _is_descending(object direction) noexcept:
    """Return True if the sort direction is descending."""
    if isinstance(direction, bool):
        return not <bint>direction
    return str(direction).upper().startswith("DESC")


cdef inline bint _is_nearest_neighbor_order(str function_name, object direction) noexcept:
    """Return True if this vector ordering corresponds to nearest-neighbour semantics."""
    cdef bint descending = _is_descending(direction)
    return (function_name == "COSINE_DISTANCE" and not descending) or (
        function_name == "COSINE_SIMILARITY" and descending
    )


cdef int _compare_rows_py(
    Py_ssize_t left_index,
    Py_ssize_t right_index,
    list key_values,
    list directions,
) except *:
    """
    Compare two rows across an ordered list of (values, descending) pairs.
    Returns -1, 0, or 1.  Nulls sort last.
    """
    cdef Py_ssize_t i, ncols = len(key_values)
    cdef bint descending, left_null, right_null
    cdef object values, left_value, right_value

    for i in range(ncols):
        values = key_values[i]
        left_value = (<list>values)[left_index]
        right_value = (<list>values)[right_index]
        descending = <bint>directions[i]

        left_null = left_value is None
        right_null = right_value is None
        if left_null and right_null:
            continue
        if left_null:
            return 1
        if right_null:
            return -1
        if left_value == right_value:
            continue
        if descending:
            return -1 if left_value > right_value else 1
        return -1 if left_value < right_value else 1
    return 0


cdef bint _mh_sift_down(
    int32_t* buf,
    Py_ssize_t pos,
    Py_ssize_t size,
    list key_values,
    list directions,
) except False:
    """Sift down in a worst-first heap of int32 row indices."""
    cdef Py_ssize_t left, right, worst
    cdef int32_t tmp
    while True:
        left = (pos << 1) + 1
        right = left + 1
        worst = pos
        if left < size and _compare_rows_py(buf[left], buf[worst], key_values, directions) > 0:
            worst = left
        if right < size and _compare_rows_py(buf[right], buf[worst], key_values, directions) > 0:
            worst = right
        if worst == pos:
            break
        tmp = buf[pos]; buf[pos] = buf[worst]; buf[worst] = tmp
        pos = worst
    return True


cdef bint _mh_sift_up(
    int32_t* buf,
    Py_ssize_t pos,
    list key_values,
    list directions,
) except False:
    """Sift up in a worst-first heap of int32 row indices."""
    cdef Py_ssize_t parent
    cdef int32_t tmp
    while pos > 0:
        parent = (pos - 1) >> 1
        if _compare_rows_py(buf[pos], buf[parent], key_values, directions) > 0:
            tmp = buf[pos]; buf[pos] = buf[parent]; buf[parent] = tmp
            pos = parent
        else:
            break
    return True


cdef list _compressed_top_k(
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

    Returns a Python list of int row indices.
    """
    cdef Py_ssize_t n = compressed.shape[0]
    cdef Py_ssize_t i, heap_size = 0, null_count = 0, ni = 0
    cdef Py_ssize_t pos, parent_pos, left, right, worst
    cdef Py_ssize_t valid_count, take_count, needed
    cdef int64_t val, parent_val, worst_val, tmp_val
    cdef int32_t tmp_idx
    cdef int64_t* heap_vals = NULL
    cdef int32_t* heap_idxs = NULL
    cdef int32_t* null_buf = NULL

    with nogil:
        for i in range(n):
            if compressed[i] == null_val:
                null_count += 1

    valid_count = n - null_count
    take_count = k if k < valid_count else valid_count
    if take_count == 0:
        return []

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

        result = [<int>heap_idxs[i] for i in range(heap_size)]
        needed = k - heap_size
        if needed > 0 and null_count > 0:
            for i in range(needed if needed < null_count else null_count):
                result.append(<int>null_buf[i])
        return result

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


# ── Node ──────────────────────────────────────────────────────────────────────

class HeapSortNode(BasePlanNode):
    _NULL_COMPRESSED = -(1 << 63)  # INT64_MIN — same sentinel used by compress_into
    _USEARCH_ENABLED = False
    _USEARCH_MIN_ROWS = 2048
    _EXACT_COMPRESS_VECTOR_TYPES = frozenset(
        {
            "BoolVector",
            "Date32Vector",
            "Float64Vector",
            "Int8Vector",
            "Int16Vector",
            "Int32Vector",
            "Int64Vector",
            "StringVector",
            "TimeVector",
            "TimestampVector",
            "UInt8Vector",
            "UInt16Vector",
            "UInt32Vector",
            "UInt64Vector",
        }
    )

    @classmethod
    def _is_exact_compressible_vector(cls, vector) -> bool:
        return vector.__class__.__name__ in cls._EXACT_COMPRESS_VECTOR_TYPES

    def __init__(self, properties: QueryProperties, **parameters):
        super().__init__(properties=properties, **parameters)
        self.order_by = parameters.get("order_by", [])
        self.limit = parameters.get("limit", -1)
        self.vector_topk_candidate = parameters.get("vector_topk_candidate", False)

        self.mapped_order = []
        for column, direction in self.order_by:
            try:
                self.mapped_order.append((column.schema_column.identity, direction))
            except ColumnNotFoundError as cnfe:
                raise ColumnNotFoundError(
                    f"`ORDER BY` must reference columns from `SELECT`. {cnfe}"
                ) from cnfe
        self.table = None
        self._chunk_buffer = []

    @property
    def config(self):  # pragma: no cover
        order = ", ".join(
            f"{col.schema_column.name} {dir[:3].upper()}" for col, dir in self.order_by
        )
        return f"LIMIT = {self.limit}, ORDER = {order}"

    @property
    def name(self):  # pragma: no cover
        return "Heap Sort"

    def execute(self, morsel):
        morsel = self.ensure_draken_morsel(morsel)

        if morsel is EOS:
            if self.table is None and not self._chunk_buffer:
                return

            if self.limit and self.limit > 0 and self.mapped_order:
                if self._chunk_buffer:
                    self.table = Morsel.combine(self._chunk_buffer)
                    self.table = self._top_n(self.table)
                elif self.table is not None:
                    self.table = self._top_n(self.table)
            elif (self.limit is None or self.limit <= 0) and self.mapped_order:
                self.table = self._sort_morsel(self.table)

            yield self.table
            return

        if isinstance(morsel, Morsel):
            morsels = (morsel,)
        elif isinstance(morsel, Iterable):
            morsels = morsel
        else:  # pragma: no cover
            _trace_record(
                "operator_execute",
                operator_name=self.name,
                operator_id=self.identity,
                duration_ns=0,
                rows_in=getattr(morsel, "num_rows", 0) if morsel is not EOS else 0,
                rows_out=getattr(self.table, "num_rows", 0) if self.table is not None else 0,
                produced_rows=bool(
                    self.table is not None and getattr(self.table, "num_rows", 0) > 0
                ),
            )
            yield None
            return

        cdef Py_ssize_t chunk_rows
        for chunk in morsels:
            chunk_rows = chunk.num_rows if chunk is not EOS else 0
            if chunk is EOS or chunk_rows == 0:
                continue

            if self.limit and self.limit > 0:
                chunk = self._top_n(chunk)
                if chunk.num_rows > 0:
                    self._chunk_buffer.append(chunk)
            else:
                if self.table is None:
                    self.table = chunk
                else:
                    self.table.append(chunk)

        yield None

    def _sorted_indices(self, morsel):
        cdef Py_ssize_t i, n = morsel.num_rows
        cdef bint reverse

        if not self.mapped_order:
            return list(range(n))

        indices = list(range(n))
        for column_name, direction in reversed(self.mapped_order):
            values = morsel.column(column_name.encode()).to_pylist()
            reverse = _is_descending(direction)

            non_null = []
            nulls = []
            for i in range(n):
                (nulls if values[i] is None else non_null).append(i)
            non_null.sort(key=lambda idx: values[idx], reverse=reverse)
            indices = non_null + nulls
        return indices

    def _materialize_rows(self, morsel, list row_indices):
        if not row_indices:
            return morsel.empty()
        return morsel.take(row_indices)

    def _sort_morsel(self, morsel):
        return self._materialize_rows(morsel, self._sorted_indices(morsel))

    def _ensure_order_expressions_evaluated(self, morsel):
        existing_columns = {
            name.decode("utf-8") if isinstance(name, bytes) else name
            for name in morsel.column_names
        }
        evaluations = []
        for column, _ in self.order_by:
            if column.node_type == NodeType.IDENTIFIER:
                continue
            identity = getattr(column.schema_column, "identity", None)
            if identity in existing_columns:
                continue
            evaluations.append(column)

        if not evaluations:
            return morsel

        return evaluate_and_append(evaluations, morsel)

    def _top_n(self, morsel):
        cdef Py_ssize_t k, row_idx, i, j, heap_size = 0
        cdef int32_t* heap_buf = NULL
        cdef int32_t pivot_idx, tmp

        if self.limit is None or self.limit <= 0:
            return morsel

        vector_ranked = self._vector_top_n(morsel)
        if vector_ranked is not None:
            return vector_ranked

        morsel = self._ensure_order_expressions_evaluated(morsel)

        k = min(<Py_ssize_t>self.limit, <Py_ssize_t>morsel.num_rows)
        if k == 0:
            return morsel.empty()

        if not self.mapped_order:
            return self._materialize_rows(morsel, list(range(k)))

        if len(self.mapped_order) == 1:
            return self._top_n_single_key(morsel, k)

        uniform_direction = self._uniform_direction()
        if uniform_direction is not None:
            return self._top_n_multi_key_uniform(morsel, k, descending=<bint>uniform_direction)

        key_vectors = [morsel.column(column.encode()) for column, _ in self.mapped_order]
        key_values  = [vec.to_pylist() for vec in key_vectors]
        directions  = [_is_descending(direction) for _, direction in self.mapped_order]

        heap_buf = <int32_t*>PyMem_Malloc(k * sizeof(int32_t))
        if heap_buf == NULL:
            raise MemoryError()
        try:
            for row_idx in range(<Py_ssize_t>morsel.num_rows):
                if heap_size < k:
                    heap_buf[heap_size] = <int32_t>row_idx
                    heap_size += 1
                    _mh_sift_up(heap_buf, heap_size - 1, key_values, directions)
                elif _compare_rows_py(row_idx, heap_buf[0], key_values, directions) < 0:
                    heap_buf[0] = <int32_t>row_idx
                    _mh_sift_down(heap_buf, 0, heap_size, key_values, directions)

            # Insertion-sort the heap by row ordering.
            for i in range(1, heap_size):
                pivot_idx = heap_buf[i]
                j = i
                while j > 0 and _compare_rows_py(heap_buf[j - 1], pivot_idx, key_values, directions) > 0:
                    heap_buf[j] = heap_buf[j - 1]
                    j -= 1
                heap_buf[j] = pivot_idx

            top_indices = [<int>heap_buf[i] for i in range(heap_size)]
        finally:
            PyMem_Free(heap_buf)
        return self._materialize_rows(morsel, top_indices)

    def _uniform_direction(self):
        cdef bint any_desc = False, all_desc = True
        cdef bint d
        for _, direction in self.mapped_order:
            d = _is_descending(direction)
            if d:
                any_desc = True
            else:
                all_desc = False
        if all_desc:
            return True
        if not any_desc:
            return False
        return None

    def _top_n_single_key(self, morsel, Py_ssize_t k):
        cdef bint descending
        cdef Py_ssize_t take_count

        column_name, direction = self.mapped_order[0]
        descending = _is_descending(direction)
        vector = morsel.column(column_name.encode())

        fast_path = self._top_n_single_key_compressed(morsel, vector, descending, k)
        if fast_path is not None:
            return fast_path

        values = vector.to_pylist()
        non_null_indices = []
        null_indices = []
        for i, value in enumerate(values):
            (null_indices if value is None else non_null_indices).append(i)
        take_count = k if k < len(non_null_indices) else len(non_null_indices)

        if descending:
            top_indices = heapq.nlargest(take_count, non_null_indices, key=values.__getitem__)
        else:
            top_indices = heapq.nsmallest(take_count, non_null_indices, key=values.__getitem__)

        if len(top_indices) < k and null_indices:
            top_indices.extend(null_indices[: k - len(top_indices)])

        return self._materialize_rows(morsel, top_indices)

    def _top_n_multi_key_uniform(self, morsel, Py_ssize_t k, bint descending):
        key_values = [morsel.column(column.encode()).to_pylist() for column, _ in self.mapped_order]
        candidate_indices = self._candidate_indices_from_first_key(morsel, k, descending)
        search_space = (
            candidate_indices if candidate_indices is not None else range(morsel.num_rows)
        )

        if descending:
            def row_key(index):
                return tuple((values[index] is not None, values[index]) for values in key_values)
            top_indices = heapq.nlargest(k, search_space, key=row_key)
        else:
            def row_key(index):
                return tuple((values[index] is None, values[index]) for values in key_values)
            top_indices = heapq.nsmallest(k, search_space, key=row_key)

        return self._materialize_rows(morsel, top_indices)

    def _candidate_indices_from_first_key(self, morsel, Py_ssize_t k, bint descending):
        cdef int64_t[::1] compressed

        first_column = self.mapped_order[0][0]
        first_vector = morsel.column(first_column.encode())
        if not self._is_exact_compressible_vector(first_vector):
            return None
        try:
            compressed = first_vector.compress()
        except Exception:
            return None
        return _compressed_threshold_candidates(compressed, k, descending, -(1 << 63))

    def _top_n_single_key_compressed(self, morsel, vector, bint descending, Py_ssize_t k):
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
        cdef Py_ssize_t take_count, row_index
        cdef bint nearest_neighbor_order, descending

        if self.limit is None or self.limit <= 0 or len(self.order_by) != 1:
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

        source_keys = [
            getattr(source_identifier.schema_column, "identity", None),
            getattr(source_identifier, "source_column", None),
            getattr(source_identifier.schema_column, "name", None),
        ]
        source_values = None
        for source_key in source_keys:
            if not source_key:
                continue
            try:
                source_values = morsel.column(source_key.encode()).to_pylist()
                break
            except Exception:
                continue
        if source_values is None:
            return None

        dense_rows = []
        source_indices = []
        for row_index in range(<Py_ssize_t>len(source_values)):
            row_vector = self._coerce_numeric_vector(source_values[row_index])
            if row_vector is None:
                continue
            if row_vector.shape[0] != query_vector.shape[0]:
                continue
            dense_rows.append(row_vector)
            source_indices.append(row_index)

        if not dense_rows:
            return None

        dense_vectors = numpy.vstack(dense_rows).astype(numpy.float32, copy=False)
        self.readings["vector_topk_candidate_rows"] += dense_vectors.shape[0]
        take_count = min(<Py_ssize_t>self.limit, <Py_ssize_t>dense_vectors.shape[0])
        if take_count == 0:
            return morsel.empty()

        query_vector = numpy.ascontiguousarray(query_vector, dtype=numpy.float32)
        dense_vectors = numpy.ascontiguousarray(dense_vectors, dtype=numpy.float32)
        row_ids = numpy.asarray(source_indices, dtype=numpy.int64)
        nearest_neighbor_order = _is_nearest_neighbor_order(order_expression.value, direction)

        if (
            self._USEARCH_ENABLED
            and self.vector_topk_candidate
            and dense_vectors.shape[0] >= self._USEARCH_MIN_ROWS
            and nearest_neighbor_order
        ):
            try:
                from opteryx.compiled.nanobind import usearch_native

                index = usearch_native.UsearchIndex(
                    dimensions=query_vector.shape[0],
                    capacity=dense_vectors.shape[0],
                    metric="cos",
                    expansion_add=16,
                    expansion_search=16,
                )
                self.readings["feature_vector_topk_usearch"] += 1
                self.readings["vector_topk_usearch_rows_indexed"] += dense_vectors.shape[0]
                index.add_batch(row_ids, dense_vectors)
                found_ids, _ = index.search(query_vector, take_count)
                if found_ids:
                    return self._materialize_rows(morsel, [int(row_id) for row_id in found_ids])
            except Exception:
                self.readings["feature_vector_topk_usearch_fallbacks"] += 1

        try:
            from opteryx.compiled.nanobind import vector_search

            if nearest_neighbor_order:
                found_ids, _ = vector_search.exact_search_cosine(
                    query_vector,
                    row_ids,
                    dense_vectors,
                    take_count,
                )
                self.readings["feature_vector_topk_exact"] += 1
                if found_ids:
                    return self._materialize_rows(morsel, [int(row_id) for row_id in found_ids])
                return morsel.empty()

            scores = numpy.asarray(
                vector_search.score_cosine(query_vector, dense_vectors), dtype=numpy.float32
            )
        except Exception:
            return None

        self.readings["feature_vector_topk_exact"] += 1
        scores = numpy.nan_to_num(scores, nan=0.0, posinf=0.0, neginf=0.0)
        if order_expression.value == "COSINE_DISTANCE":
            scores = 1.0 - numpy.clip(scores, -1.0, 1.0)

        descending = _is_descending(direction)
        if take_count < scores.shape[0]:
            if descending:
                candidate_indices = numpy.argpartition(-scores, take_count - 1)[:take_count]
            else:
                candidate_indices = numpy.argpartition(scores, take_count - 1)[:take_count]
        else:
            candidate_indices = numpy.arange(scores.shape[0], dtype=numpy.int64)

        if descending:
            order = numpy.lexsort((row_ids[candidate_indices], -scores[candidate_indices]))
        else:
            order = numpy.lexsort((row_ids[candidate_indices], scores[candidate_indices]))
        ranked_dense_indices = candidate_indices[order]

        top_indices = [
            int(source_indices[int(index)]) for index in ranked_dense_indices[:take_count]
        ]
        return self._materialize_rows(morsel, top_indices)

    @staticmethod
    def _coerce_numeric_vector(value):
        try:
            vector = numpy.asarray(value, dtype=numpy.float32)
        except (TypeError, ValueError):
            return None
        if vector.ndim != 1:
            return None
        return vector

    def _resolve_query_vector(self, query_node):
        if query_node.node_type == NodeType.LITERAL:
            return self._coerce_numeric_vector(query_node.value)
        if (
            query_node.node_type == NodeType.FUNCTION
            and query_node.value == "EMBED"
            and len(query_node.parameters) == 1
            and query_node.parameters[0].node_type == NodeType.LITERAL
        ):
            from opteryx.vectors.embeddings import embed_text_matrix

            embedded = embed_text_matrix([query_node.parameters[0].value])
            if embedded.size == 0:
                return None
            return self._coerce_numeric_vector(embedded[0])
        return None
