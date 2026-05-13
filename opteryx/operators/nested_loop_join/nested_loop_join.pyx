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

REFACTORED: 100% Draken-native, zero Arrow in execution hot path.
Uses Draken Morsel buffering, Morsel.hash() for hashing, Draken-native alignment.
"""

from typing import Generator, Optional
import time
from array import array
from libc.stdint cimport uint8_t, int32_t, uint64_t
from libc.string cimport memcpy
from libcpp.vector cimport vector
from draken.vectors.bool_vector cimport BoolVector, bool_vector_from_bits
from draken.morsels.align cimport align_tables
from opteryx.models import QueryProperties

# EOS sentinel available as _EOS_SENTINEL via the umbrella unit.

# BasePlanNode/JoinNode in scope via _operators.pyx include.


# Helper to convert bit-packed results memoryview to BoolVector (avoids cdef in method)
cdef BoolVector _bits_to_bool_vector(uint8_t[::1] bits, Py_ssize_t n):
    """Convert bit-packed uint8 memoryview to BoolVector (Draken-native, no Arrow)."""
    if bits is None:
        return None
    return bool_vector_from_bits(&bits[0], NULL, n)


# Nested loop join kernel - pure Draken implementation
cdef Morsel _nested_loop_join_morsel(Morsel left_morsel, Morsel right_morsel, list left_columns, list right_columns):
    """
    Perform a nested loop join on Draken Morsels and return the aligned result.

    Uses native Morsel.hash() to compute row hashes, then matches under nogil
    using C++ vector<int32_t> for accumulation. Returns None if no matches.
    """
    cdef Morsel lm = left_morsel
    cdef Morsel rm = right_morsel

    if lm is None or rm is None:
        return None

    cdef Py_ssize_t nl = lm.num_rows
    cdef Py_ssize_t nr = rm.num_rows

    if nl == 0 or nr == 0:
        return None

    # Get hash values for both sides (Draken-native)
    cdef uint64_t[::1] left_hashes = lm.hash(left_columns)
    cdef uint64_t[::1] right_hashes = rm.hash(right_columns)

    cdef vector[int32_t] left_idx_vec
    cdef vector[int32_t] right_idx_vec
    cdef Py_ssize_t i, j
    cdef uint64_t left_hash, right_hash

    # Nested loop join: smaller side outer for better cache locality.
    with nogil:
        if nl <= nr:
            for i in range(nl):
                left_hash = left_hashes[i]
                for j in range(nr):
                    if left_hash == right_hashes[j]:
                        left_idx_vec.push_back(<int32_t>i)
                        right_idx_vec.push_back(<int32_t>j)
        else:
            for j in range(nr):
                right_hash = right_hashes[j]
                for i in range(nl):
                    if right_hash == left_hashes[i]:
                        left_idx_vec.push_back(<int32_t>i)
                        right_idx_vec.push_back(<int32_t>j)

    cdef Py_ssize_t nmatch = <Py_ssize_t>left_idx_vec.size()
    if nmatch == 0:
        return None

    cdef object left_arr = array('i', [0]) * nmatch
    cdef object right_arr = array('i', [0]) * nmatch
    cdef int32_t[::1] left_view = left_arr
    cdef int32_t[::1] right_view = right_arr
    memcpy(&left_view[0], left_idx_vec.data(), nmatch * sizeof(int32_t))
    memcpy(&right_view[0], right_idx_vec.data(), nmatch * sizeof(int32_t))

    return align_tables(lm, rm, left_view, right_view)


cdef class NestedLoopJoinNode(JoinNode):
    cdef public list left_columns
    cdef public list right_columns
    cdef public Morsel left_morsel
    cdef public list left_morsels
    cdef public object left_filter
    cdef public bint _build_phase

    join_type = "nested_loop"

    def __init__(self, properties=None, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.left_columns = parameters.get("left_columns")
        self.right_columns = parameters.get("right_columns")

        self.left_morsel = None
        self.left_morsels = []

        self.left_filter = None
        self._build_phase = True

    @property
    def name(self):
        return "Nested Loop Join"

    @property
    def config(self):
        return "draken"

    cpdef void push_left(self, Morsel morsel) except *:
        cdef long long start
        if morsel is _EOS_SENTINEL:
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
            return
        if morsel is not None:
            self.left_morsels.append(morsel)

    cpdef void push_right(self, Morsel morsel) except *:
        cdef long long start
        cdef Py_ssize_t eliminated_rows
        cdef Morsel morsel_filtered
        cdef Morsel result
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

        if morsel.num_rows > 0:
            result = _nested_loop_join_morsel(
                self.left_morsel, morsel, self.left_columns, self.right_columns
            )
            if result is not None:
                self.emit(result)
