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
Outer Join Node - Draken-Native Morsel-Based Operations

This is a SQL Query Execution Plan Node.

PyArrow has LEFT/RIGHT/FULL OUTER JOIN implementations, but they error when the
relations being joined contain STRUCT or ARRAY columns so we've written our own
OUTER JOIN implementations.
"""

import time

from libc.stdint cimport uint8_t, uint64_t, int64_t, int32_t
from libc.string cimport memset
from libcpp.vector cimport vector
from cpython.mem cimport PyMem_Malloc, PyMem_Free

from draken.vectors.bool_vector cimport BoolVector, bool_vector_from_bits
from draken.morsels.morsel cimport Morsel
from draken.morsels.morsel cimport align_tables

from opteryx.compiled.structures.bloom_filter import create_bloom_filter_morsel
from opteryx.compiled.structures.bloom_filter import bloom_filter_check_morsel
from opteryx.compiled.structures.carchar_index cimport CarcharJoinIndexWrapper
from opteryx.compiled.morsel_ops.null_filter cimport non_null_row_indices
from opteryx.models import QueryProperties

# EOS sentinel available as _EOS_SENTINEL via the umbrella unit.

# BasePlanNode/JoinNode in scope via _operators.pyx include.


# Telemetry: number of times the outer-join bloom-filter Draken fast-path was applied.
BLOOM_FASTPATH_COUNTER = 0

CHUNK_SIZE: int = 50_000


cdef class _JoinChunkBuffers:
    """
    RAII wrapper for the two fixed-size int32 index buffers used during join
    materialisation. Cython generators do not support yield inside try/finally,
    so buffer ownership is managed here via __dealloc__ instead.
    """
    cdef int32_t* left_buf
    cdef int32_t* right_buf

    def __cinit__(self):
        self.left_buf = <int32_t*>PyMem_Malloc(CHUNK_SIZE * sizeof(int32_t))
        self.right_buf = <int32_t*>PyMem_Malloc(CHUNK_SIZE * sizeof(int32_t))
        if self.left_buf == NULL or self.right_buf == NULL:
            raise MemoryError()

    def __dealloc__(self):
        if self.left_buf != NULL:
            PyMem_Free(self.left_buf)
            self.left_buf = NULL
        if self.right_buf != NULL:
            PyMem_Free(self.right_buf)
            self.right_buf = NULL


cdef class _JoinFlags:
    """
    RAII wrapper for a byte-per-row flag array used to track matched rows.
    Freed via __dealloc__ for the same reason as _JoinChunkBuffers.
    """
    cdef uint8_t* flags
    cdef Py_ssize_t n

    def __cinit__(self, Py_ssize_t n):
        cdef Py_ssize_t alloc = n if n > 0 else 1
        self.n = n
        self.flags = <uint8_t*>PyMem_Malloc(alloc)
        if self.flags == NULL:
            raise MemoryError()
        memset(self.flags, 0, alloc)

    def __dealloc__(self):
        if self.flags != NULL:
            PyMem_Free(self.flags)
            self.flags = NULL


cpdef CarcharJoinIndexWrapper _build_probe_hash_map(Morsel morsel, list join_columns):
    cdef CarcharJoinIndexWrapper ht = CarcharJoinIndexWrapper()
    cdef vector[int64_t] non_null_indices_vec = non_null_row_indices(morsel, join_columns)
    cdef const int64_t* non_null_ptr = non_null_indices_vec.data()
    cdef Py_ssize_t n_non_null = <Py_ssize_t>non_null_indices_vec.size()
    cdef uint64_t[::1] row_hashes = morsel.hash(join_columns)
    cdef Py_ssize_t i

    for i in range(n_non_null):
        ht.insert_row(row_hashes[non_null_ptr[i]], non_null_ptr[i])

    return ht


cpdef CarcharJoinIndexWrapper _build_side_hash_map(Morsel morsel, list join_columns):
    cdef CarcharJoinIndexWrapper ht = CarcharJoinIndexWrapper()
    cdef vector[int64_t] non_null_indices_vec = non_null_row_indices(morsel, join_columns)
    cdef const int64_t* non_null_ptr = non_null_indices_vec.data()
    cdef Py_ssize_t n_non_null = <Py_ssize_t>non_null_indices_vec.size()
    cdef uint64_t[::1] row_hashes = morsel.hash(join_columns)
    cdef int64_t i, row_idx

    for i in range(n_non_null):
        row_idx = non_null_ptr[i]
        ht.insert_row(row_hashes[row_idx], row_idx)

    return ht


def left_join(
    Morsel left_morsel,
    Morsel right_morsel,
    list left_columns,
    list right_columns,
    object filter_index,
    CarcharJoinIndexWrapper left_hash,
    object columns=None,
):
    """
    Perform a LEFT OUTER JOIN using a prebuilt left-side hash map.

    Probes left_hash with each right row's hash to find matching left rows.
    Unmatched left rows are emitted with null right side (-1 index).

    Yields Morsel chunks of the joined result.
    """
    cdef _JoinChunkBuffers bufs = _JoinChunkBuffers()
    cdef _JoinFlags seen = _JoinFlags(0)  # replaced below after n_left is known
    cdef uint64_t[::1] right_hashes
    cdef uint8_t[::1] bit_results
    cdef vector[int64_t] left_rows
    cdef Py_ssize_t n_left, n_right, i, j, buf_pos
    cdef int32_t l
    cdef BoolVector mask

    n_left = len(left_morsel)
    n_right = len(right_morsel)

    # Bloom filter pre-filter on right side
    if filter_index is not None:
        bit_results = bloom_filter_check_morsel(filter_index, right_morsel, right_columns)
        if bit_results is not None:
            mask = bool_vector_from_bits(&bit_results[0], NULL, n_right)
            right_morsel = right_morsel.filter_mask(mask)
            n_right = len(right_morsel)

        if n_right == 0:
            # Right side empty: all left rows unmatched, yield with null right
            buf_pos = 0
            for i in range(n_left):
                bufs.left_buf[buf_pos] = <int32_t>i
                bufs.right_buf[buf_pos] = -1
                buf_pos += 1
                if buf_pos == CHUNK_SIZE:
                    yield align_tables(
                        left_morsel, right_morsel,
                        <int32_t[:CHUNK_SIZE]>bufs.left_buf,
                        <int32_t[:CHUNK_SIZE]>bufs.right_buf,
                    )
                    buf_pos = 0
            if buf_pos > 0:
                yield align_tables(
                    left_morsel, right_morsel,
                    <int32_t[:buf_pos]>bufs.left_buf,
                    <int32_t[:buf_pos]>bufs.right_buf,
                )
            return

    # Track which left rows were matched
    seen = _JoinFlags(n_left)

    right_hashes = right_morsel.hash(right_columns)
    buf_pos = 0

    # Match phase: for each right row, find all matching left rows
    for i in range(n_right):
        left_rows = left_hash.rows_for(right_hashes[i])
        for j in range(left_rows.size()):
            l = <int32_t>left_rows[j]
            seen.flags[l] = 1
            bufs.left_buf[buf_pos] = l
            bufs.right_buf[buf_pos] = <int32_t>i
            buf_pos += 1
            if buf_pos == CHUNK_SIZE:
                yield align_tables(
                    left_morsel, right_morsel,
                    <int32_t[:CHUNK_SIZE]>bufs.left_buf,
                    <int32_t[:CHUNK_SIZE]>bufs.right_buf,
                )
                buf_pos = 0

    if buf_pos > 0:
        yield align_tables(
            left_morsel, right_morsel,
            <int32_t[:buf_pos]>bufs.left_buf,
            <int32_t[:buf_pos]>bufs.right_buf,
        )

    # Unmatched left rows: emit with null right side
    buf_pos = 0
    for i in range(n_left):
        if not seen.flags[i]:
            bufs.left_buf[buf_pos] = <int32_t>i
            bufs.right_buf[buf_pos] = -1
            buf_pos += 1
            if buf_pos == CHUNK_SIZE:
                yield align_tables(
                    left_morsel, right_morsel,
                    <int32_t[:CHUNK_SIZE]>bufs.left_buf,
                    <int32_t[:CHUNK_SIZE]>bufs.right_buf,
                )
                buf_pos = 0

    if buf_pos > 0:
        yield align_tables(
            left_morsel, right_morsel,
            <int32_t[:buf_pos]>bufs.left_buf,
            <int32_t[:buf_pos]>bufs.right_buf,
        )


def right_join(
    Morsel left_morsel,
    Morsel right_morsel,
    list left_columns,
    list right_columns,
    object filter_index,
    object left_hash,
    object columns=None,
):
    """
    Perform a RIGHT OUTER JOIN.

    Builds a local hash map from the left morsel, then probes it with each right
    row's hash. Unmatched right rows are emitted with null left side (-1 index).

    left_hash is accepted for interface compatibility but unused (right join builds
    its own local left hash map since self.left_hash is only populated for left outer).

    Yields Morsel chunks of the joined result.
    """
    cdef _JoinChunkBuffers bufs = _JoinChunkBuffers()
    cdef _JoinFlags seen = _JoinFlags(0)  # replaced below after n_right is known
    cdef uint64_t[::1] right_hashes
    cdef uint8_t[::1] bit_results
    cdef vector[int64_t] left_rows
    cdef Py_ssize_t n_left, n_right, i, j, buf_pos
    cdef int32_t l
    cdef CarcharJoinIndexWrapper left_hash_table
    cdef BoolVector mask

    n_left = len(left_morsel)
    n_right = len(right_morsel)

    # Bloom filter pre-filter on left side
    if filter_index is not None:
        bit_results = bloom_filter_check_morsel(filter_index, left_morsel, left_columns)
        if bit_results is not None:
            mask = bool_vector_from_bits(&bit_results[0], NULL, n_left)
            left_morsel = left_morsel.filter_mask(mask)
            n_left = len(left_morsel)

        if n_left == 0:
            # Left side empty: all right rows unmatched, yield with null left
            buf_pos = 0
            for i in range(n_right):
                bufs.left_buf[buf_pos] = -1
                bufs.right_buf[buf_pos] = <int32_t>i
                buf_pos += 1
                if buf_pos == CHUNK_SIZE:
                    yield align_tables(
                        left_morsel, right_morsel,
                        <int32_t[:CHUNK_SIZE]>bufs.left_buf,
                        <int32_t[:CHUNK_SIZE]>bufs.right_buf,
                    )
                    buf_pos = 0
            if buf_pos > 0:
                yield align_tables(
                    left_morsel, right_morsel,
                    <int32_t[:buf_pos]>bufs.left_buf,
                    <int32_t[:buf_pos]>bufs.right_buf,
                )
            return

    # Track which right rows were matched
    seen = _JoinFlags(n_right)

    left_hash_table = _build_probe_hash_map(left_morsel, left_columns)
    right_hashes = right_morsel.hash(right_columns)
    buf_pos = 0

    # Match phase: for each right row, find all matching left rows
    for i in range(n_right):
        left_rows = left_hash_table.rows_for(right_hashes[i])
        for j in range(left_rows.size()):
            l = <int32_t>left_rows[j]
            seen.flags[i] = 1
            bufs.left_buf[buf_pos] = l
            bufs.right_buf[buf_pos] = <int32_t>i
            buf_pos += 1
            if buf_pos == CHUNK_SIZE:
                yield align_tables(
                    left_morsel, right_morsel,
                    <int32_t[:CHUNK_SIZE]>bufs.left_buf,
                    <int32_t[:CHUNK_SIZE]>bufs.right_buf,
                )
                buf_pos = 0

    if buf_pos > 0:
        yield align_tables(
            left_morsel, right_morsel,
            <int32_t[:buf_pos]>bufs.left_buf,
            <int32_t[:buf_pos]>bufs.right_buf,
        )

    # Unmatched right rows: emit with null left side
    buf_pos = 0
    for i in range(n_right):
        if not seen.flags[i]:
            bufs.left_buf[buf_pos] = -1
            bufs.right_buf[buf_pos] = <int32_t>i
            buf_pos += 1
            if buf_pos == CHUNK_SIZE:
                yield align_tables(
                    left_morsel, right_morsel,
                    <int32_t[:CHUNK_SIZE]>bufs.left_buf,
                    <int32_t[:CHUNK_SIZE]>bufs.right_buf,
                )
                buf_pos = 0

    if buf_pos > 0:
        yield align_tables(
            left_morsel, right_morsel,
            <int32_t[:buf_pos]>bufs.left_buf,
            <int32_t[:buf_pos]>bufs.right_buf,
        )


def full_join(
    Morsel left_morsel,
    Morsel right_morsel,
    list left_columns,
    list right_columns,
    object filter_index=None,
    object left_hash=None,
    object columns=None,
):
    """
    Perform a FULL OUTER JOIN.

    Builds a hash map from the right morsel, probes it with each left row.
    All left rows are emitted (matched with right rows, or with null right side).
    Unmatched right rows are then emitted with null left side.

    filter_index and left_hash are accepted for interface compatibility but unused.

    Yields Morsel chunks of the joined result.
    """
    cdef _JoinChunkBuffers bufs = _JoinChunkBuffers()
    cdef _JoinFlags matched_right = _JoinFlags(0)  # replaced below
    cdef uint64_t[::1] left_hashes
    cdef vector[int64_t] right_rows
    cdef Py_ssize_t n_left, n_right, i, j, buf_pos
    cdef int32_t r
    cdef CarcharJoinIndexWrapper right_hash_table

    n_left = len(left_morsel)
    n_right = len(right_morsel)

    matched_right = _JoinFlags(n_right)
    right_hash_table = _build_probe_hash_map(right_morsel, right_columns)
    left_hashes = left_morsel.hash(left_columns)
    buf_pos = 0

    # Left pass: emit all left rows, matched or with null right
    for i in range(n_left):
        right_rows = right_hash_table.rows_for(left_hashes[i])
        if right_rows.size() > 0:
            for j in range(right_rows.size()):
                r = <int32_t>right_rows[j]
                matched_right.flags[r] = 1
                bufs.left_buf[buf_pos] = <int32_t>i
                bufs.right_buf[buf_pos] = r
                buf_pos += 1
                if buf_pos == CHUNK_SIZE:
                    yield align_tables(
                        left_morsel, right_morsel,
                        <int32_t[:CHUNK_SIZE]>bufs.left_buf,
                        <int32_t[:CHUNK_SIZE]>bufs.right_buf,
                    )
                    buf_pos = 0
        else:
            bufs.left_buf[buf_pos] = <int32_t>i
            bufs.right_buf[buf_pos] = -1
            buf_pos += 1
            if buf_pos == CHUNK_SIZE:
                yield align_tables(
                    left_morsel, right_morsel,
                    <int32_t[:CHUNK_SIZE]>bufs.left_buf,
                    <int32_t[:CHUNK_SIZE]>bufs.right_buf,
                )
                buf_pos = 0

    if buf_pos > 0:
        yield align_tables(
            left_morsel, right_morsel,
            <int32_t[:buf_pos]>bufs.left_buf,
            <int32_t[:buf_pos]>bufs.right_buf,
        )

    # Right pass: emit unmatched right rows with null left side
    buf_pos = 0
    for i in range(n_right):
        if not matched_right.flags[i]:
            bufs.left_buf[buf_pos] = -1
            bufs.right_buf[buf_pos] = <int32_t>i
            buf_pos += 1
            if buf_pos == CHUNK_SIZE:
                yield align_tables(
                    left_morsel, right_morsel,
                    <int32_t[:CHUNK_SIZE]>bufs.left_buf,
                    <int32_t[:CHUNK_SIZE]>bufs.right_buf,
                )
                buf_pos = 0

    if buf_pos > 0:
        yield align_tables(
            left_morsel, right_morsel,
            <int32_t[:buf_pos]>bufs.left_buf,
            <int32_t[:buf_pos]>bufs.right_buf,
        )


cdef class OuterJoinNode(JoinNode):
    cdef public str join_type
    cdef public object using
    cdef public list left_columns
    cdef public list right_columns
    cdef public list left_morsels
    cdef public list right_morsels
    cdef public object left_relation
    cdef public Morsel _left_morsel
    cdef public object left_hash
    cdef public object filter_index
    cdef public bint _build_phase

    def __init__(self, properties=None, **parameters):
        # Ensure `join_type` exists before the base initializer accesses `self.name`
        self.join_type = parameters["type"]
        JoinNode.__init__(self, properties=properties, **parameters)
        self.on = parameters.get("on")
        self.using = parameters.get("using")

        self.left_columns = parameters.get("left_columns")
        self.left_readers = parameters.get("left_readers") or []

        self.right_columns = parameters.get("right_columns")
        self.right_readers = parameters.get("right_readers") or []

        self.left_relation_names = parameters.get("left_relation_names") or []
        self.right_relation_names = parameters.get("right_relation_names") or []

        self.columns = parameters.get("columns")

        self.left_morsels = []
        self.right_morsels = []
        self.left_relation = None
        self.left_hash = None

        self.filter_index = None
        self._build_phase = True

    @property
    def name(self):  # pragma: no cover
        return self.join_type.replace(" ", "_")

    @property
    def config(self) -> str:  # pragma: no cover
        from opteryx.expression import format_expression

        if self.on:
            return f"{self.join_type.upper()} JOIN ({format_expression(self.on, True)})"
        if self.using:
            return f"{self.join_type.upper()} JOIN (USING {','.join(map(format_expression, self.using))})"
        return f"{self.join_type.upper()}"

    cpdef void push_left(self, Morsel morsel) except *:
        cdef long long start
        if morsel is _EOS_SENTINEL:
            if self.left_morsels:
                self._left_morsel = Morsel.combine(self.left_morsels)
                self.left_morsels = []
            else:
                self._left_morsel = Morsel.from_vectors({})
            self._left_morsel = self._apply_join_key_casts(self._left_morsel, is_left=True)
            if self.join_type == "left outer":
                start = time.monotonic_ns()
                self.left_hash = _build_side_hash_map(self._left_morsel, self.left_columns)
                if len(self._left_morsel) < 16_000_001:
                    start = time.monotonic_ns()
                    self.filter_index = create_bloom_filter_morsel(self._left_morsel, self.left_columns)
                    self.readings["time_build_bloom_filter"] += time.monotonic_ns() - start
                    self.readings["feature_bloom_filter"] += 1
            return
        if morsel is not None:
            self.left_morsels.append(morsel)

    cpdef void push_right(self, Morsel morsel) except *:
        cdef Py_ssize_t orig_rows
        cdef uint8_t[::1] bit_results
        cdef object pass_filter_index
        cdef Morsel right_morsel
        cdef Py_ssize_t eliminated_rows

        if morsel is _EOS_SENTINEL:
            pass_filter_index = self.filter_index
            if self.right_morsels:
                right_morsel = Morsel.combine(self.right_morsels)
                self.right_morsels = []
                if pass_filter_index is not None:
                    orig_rows = len(right_morsel)
                    bit_results = bloom_filter_check_morsel(self.filter_index, right_morsel, self.right_columns)
                    if bit_results is not None:
                        mask = bool_vector_from_bits(&bit_results[0], NULL, orig_rows)
                        right_morsel = right_morsel.filter_mask(mask)
                        eliminated_rows = orig_rows - len(right_morsel)
                        self.readings["rows_eliminated_by_bloom_filter"] += eliminated_rows
                        global BLOOM_FASTPATH_COUNTER
                        BLOOM_FASTPATH_COUNTER += 1
                        pass_filter_index = None
            else:
                right_morsel = Morsel.from_vectors({})

            right_morsel = self._apply_join_key_casts(right_morsel, is_left=False)
            left_morsel_for_join = self._left_morsel
            join_provider = providers.get(self.join_type)

            for result_morsel in join_provider(
                left_morsel=left_morsel_for_join,
                right_morsel=right_morsel,
                left_columns=self.left_columns,
                right_columns=self.right_columns,
                left_hash=self.left_hash,
                filter_index=pass_filter_index,
                columns=self.columns,
            ):
                if self.columns is not None:
                    candidates = [c.schema_column.identity for c in self.columns]
                    keep_columns = [c for c in candidates if c in result_morsel.column_names]
                    result_morsel = result_morsel.select(keep_columns)
                self.emit(result_morsel)
            self.emit(_EOS_SENTINEL)
            return

        if morsel is not None:
            self.right_morsels.append(morsel)


providers = {"left outer": left_join, "full outer": full_join, "right outer": right_join}
