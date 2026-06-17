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
Inner (Nested Loop) Join Node - Draken-Native

Hash-index approach: at left EOS, build an O(L) unordered_map<hash→[left_rows]>.
Each right morsel probes in O(R) — total join cost O(L + R×morsels), not O(L×R).
"""

from typing import Generator, Optional
import time
from array import array
from libc.stdint cimport uint8_t, int32_t, uint64_t
from libc.string cimport memcpy
from libcpp.vector cimport vector
from draken.vectors.bool_vector cimport BoolVector, bool_vector_from_bits
from draken.morsels.morsel cimport align_tables
from opteryx.models import QueryProperties


cdef extern from "operators/loop_join_kernels.hpp" namespace "opteryx::operators" nogil:
    cdef cppclass HashIndex:
        pass
    HashIndex* build_hash_index(const uint64_t* hashes, size_t n)
    void destroy_hash_index(HashIndex* idx)
    void probe_hash_index(
        const HashIndex* idx,
        const uint64_t* right_hashes,
        size_t nr,
        vector[int32_t]& out_left,
        vector[int32_t]& out_right,
    )

# EOS sentinel available as _EOS_SENTINEL via the umbrella unit.

# BasePlanNode/JoinNode in scope via _operators.pyx include.


cdef class _HashIndexHolder:
    """RAII wrapper for a C++ HashIndex*. __dealloc__ frees the C++ allocation."""
    cdef HashIndex* _ptr

    def __cinit__(self):
        self._ptr = NULL

    def __dealloc__(self):
        if self._ptr != NULL:
            destroy_hash_index(self._ptr)
            self._ptr = NULL


cdef object _bits_to_bool_vector(uint8_t[::1] bits, Py_ssize_t n):
    """Convert bit-packed uint8 memoryview to a Draken-native BOOL Vector (no Arrow).

    bool_vector_from_bits returns a draken_native.Vector (BOOL-typed), not the
    BoolVector shim — return it as object so no spurious conversion is forced.
    """
    if bits is None:
        return None
    return bool_vector_from_bits(&bits[0], NULL, n)


cdef Morsel _probe_hash_join_morsel(
    Morsel left_morsel,
    Morsel right_morsel,
    HashIndex* hash_idx,
    list right_columns,
):
    """
    Probe the pre-built hash index with one right morsel — O(R) per call.

    hash_idx is built once from the full combined left morsel at EOS.
    Returns None if there are no matching rows.
    """
    cdef Morsel rm = right_morsel
    if rm is None or hash_idx == NULL:
        return None

    cdef Py_ssize_t nr = rm.num_rows
    if nr == 0:
        return None

    cdef uint64_t[::1] right_hashes = rm.hash(right_columns)
    cdef vector[int32_t] left_idx_vec
    cdef vector[int32_t] right_idx_vec

    with nogil:
        probe_hash_index(hash_idx, &right_hashes[0], <size_t>nr, left_idx_vec, right_idx_vec)

    cdef Py_ssize_t nmatch = <Py_ssize_t>left_idx_vec.size()
    if nmatch == 0:
        return None

    cdef object left_arr = array('i', [0]) * nmatch
    cdef object right_arr = array('i', [0]) * nmatch
    cdef int32_t[::1] left_view = left_arr
    cdef int32_t[::1] right_view = right_arr
    memcpy(&left_view[0], left_idx_vec.data(), nmatch * sizeof(int32_t))
    memcpy(&right_view[0], right_idx_vec.data(), nmatch * sizeof(int32_t))

    return align_tables(left_morsel, rm, left_view, right_view)


cdef class NestedLoopJoinNode(JoinNode):
    cdef public list left_columns
    cdef public list right_columns
    cdef public Morsel left_morsel
    cdef public list left_morsels
    cdef public object left_filter
    cdef public object left_hash_index   # _HashIndexHolder or None
    cdef public bint _build_phase

    join_type = "nested_loop"

    def __init__(self, properties=None, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.left_columns = parameters.get("left_columns")
        self.right_columns = parameters.get("right_columns")

        self.left_morsel = None
        self.left_morsels = []

        self.left_filter = None
        self.left_hash_index = None
        self._build_phase = True

    @property
    def name(self):
        return "Nested Loop Join"

    @property
    def config(self):
        return "draken"

    cdef int push_left(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        cdef CxxMorsel* raw = m.get()
        cdef bint is_eos = (raw != NULL and raw.state == MorselState.END_OF_STREAM)
        with gil:
            try:
                if is_eos:
                    self._push_left_gil(_EOS_SENTINEL)
                else:
                    self._push_left_gil(cxx_to_morsel(m))
            except BaseException as exc:  # noqa: BLE001 — surfaced via ErrCtx
                self._stash_exc(exc, err)
        return err.code if err != NULL else 0

    cdef void _push_left_gil(self, Morsel morsel) except *:
        cdef long long start
        cdef uint64_t[::1] left_hashes
        cdef _HashIndexHolder holder
        cdef HashIndex* idx
        if morsel is _EOS_SENTINEL:
            self._build_complete = True
            if self.left_morsels:
                self.left_morsel = Morsel.combine(self.left_morsels)
                self.left_morsels = []
            else:
                self.left_morsel = None

            if self.left_morsel is not None and self.left_morsel.num_rows > 0:
                from opteryx.compiled.structures.bloom_filter import create_bloom_filter_morsel
                start = time.monotonic_ns()
                self.left_filter = create_bloom_filter_morsel(self.left_morsel, self.left_columns)
                self.readings["time_build_bloom_filter"] += time.monotonic_ns() - start
                self.readings["feature_bloom_filter"] += 1

                # Build hash index — O(L), done once. The bloom filter uses its
                # own internal hash; we hash separately here for the index.
                left_hashes = self.left_morsel.hash(self.left_columns)
                idx = build_hash_index(&left_hashes[0], <size_t>self.left_morsel.num_rows)
                if idx == NULL:
                    raise MemoryError("build_hash_index returned NULL")
                holder = _HashIndexHolder()
                holder._ptr = idx
                self.left_hash_index = holder
            return

        if morsel is not None:
            self.left_morsels.append(morsel)

    cdef int push_right(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil:
        cdef CxxMorsel* raw = m.get()
        cdef bint is_eos = (raw != NULL and raw.state == MorselState.END_OF_STREAM)
        with gil:
            try:
                if is_eos:
                    self._push_right_gil(_EOS_SENTINEL)
                else:
                    self._push_right_gil(cxx_to_morsel(m))
            except BaseException as exc:  # noqa: BLE001 — surfaced via ErrCtx
                self._stash_exc(exc, err)
        return err.code if err != NULL else 0

    cdef void _push_right_gil(self, Morsel morsel) except *:
        cdef long long start
        cdef Py_ssize_t eliminated_rows
        cdef Morsel morsel_filtered
        cdef Morsel result
        cdef _HashIndexHolder holder
        self._require_build_complete()
        if morsel is _EOS_SENTINEL:
            self.emit(_EOS_SENTINEL)
            return

        if self.left_morsel is None or self.left_morsel.num_rows == 0 or morsel.num_rows == 0:
            return

        if self.left_filter is not None:
            from opteryx.compiled.structures.bloom_filter import bloom_filter_check_morsel
            start = time.monotonic_ns()
            bit_results = bloom_filter_check_morsel(self.left_filter, morsel, self.right_columns)
            self.readings["time_bloom_filtering"] += time.monotonic_ns() - start

            if bit_results is not None:
                filter_mask = _bits_to_bool_vector(bit_results, morsel.num_rows)
                morsel_filtered = morsel.filter_mask(filter_mask)
                eliminated_rows = morsel.num_rows - morsel_filtered.num_rows
                self.readings["rows_eliminated_by_bloom_filter"] += eliminated_rows
                morsel = morsel_filtered

        holder = self.left_hash_index
        if morsel.num_rows > 0 and holder is not None:
            result = _probe_hash_join_morsel(
                self.left_morsel, morsel, holder._ptr, self.right_columns
            )
            if result is not None:
                self.emit(result)
