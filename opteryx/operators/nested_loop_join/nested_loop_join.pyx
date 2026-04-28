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
from libc.stdint cimport uint8_t, int32_t, uint64_t
from draken.vectors.bool_vector cimport BoolVector, bool_vector_from_bits
from draken.morsels.align cimport align_tables
from opteryx.models import QueryProperties

from opteryx import EOS, EMPTY

from . import JoinNode


# Helper to convert bit-packed results memoryview to BoolVector (avoids cdef in method)
cdef BoolVector _bits_to_bool_vector(uint8_t[::1] bits, Py_ssize_t n):
    """Convert bit-packed uint8 memoryview to BoolVector (Draken-native, no Arrow)."""
    if bits is None:
        return None
    return bool_vector_from_bits(&bits[0], NULL, n)


# Helper to align morsels using index tuples (converts to Draken memoryviews)
cdef Morsel _align_morsels_with_tuples(Morsel left_morsel, Morsel right_morsel, object left_indexes, object right_indexes):
    """Align two morsels using index tuples (Draken-native, no Arrow)."""
    cdef object left_arr = array('i', left_indexes) if left_indexes else array('i', [])
    cdef object right_arr = array('i', right_indexes) if right_indexes else array('i', [])
    cdef int32_t[::1] left_view = left_arr
    cdef int32_t[::1] right_view = right_arr
    return align_tables(left_morsel, right_morsel, left_view, right_view)


# Nested loop join kernel - pure Draken implementation
cdef tuple _nested_loop_join_morsel(Morsel left_morsel, Morsel right_morsel, list left_columns, list right_columns):
    """
    Perform a nested loop join on Draken Morsels.

    Uses native Morsel.hash() to compute row hashes, enabling pure Draken flow.
    No Arrow table conversion, no buffer access patterns - direct hash-based join.

    Inputs:
        left_morsel: Left Morsel (build side)
        right_morsel: Right Morsel (probe side)
        left_columns: Column identities for left join keys
        right_columns: Column identities for right join keys

    Returns:
        (left_indexes, right_indexes) tuples (as Int32Buffer for alignment)
    """
    cdef Morsel lm = left_morsel
    cdef Morsel rm = right_morsel

    if lm is None or rm is None:
        return (), ()

    cdef Py_ssize_t nl = lm.num_rows
    cdef Py_ssize_t nr = rm.num_rows

    if nl == 0 or nr == 0:
        return (), ()

    # Get hash values for both sides (Draken-native)
    cdef uint64_t[::1] left_hashes = lm.hash(left_columns)
    cdef uint64_t[::1] right_hashes = rm.hash(right_columns)

    cdef list left_indexes = []
    cdef list right_indexes = []

    cdef Py_ssize_t i, j
    cdef uint64_t left_hash, right_hash

    # Nested loop join: smaller side outer for better cache locality
    if nl <= nr:
        for i in range(nl):
            left_hash = left_hashes[i]
            for j in range(nr):
                if left_hash == right_hashes[j]:
                    left_indexes.append(i)
                    right_indexes.append(j)
    else:
        for j in range(nr):
            right_hash = right_hashes[j]
            for i in range(nl):
                if right_hash == left_hashes[i]:
                    left_indexes.append(i)
                    right_indexes.append(j)

    return tuple(left_indexes), tuple(right_indexes)


class NestedLoopJoinNode(JoinNode):
    join_type = "nested_loop"

    def __init__(self, properties: QueryProperties, **parameters):
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

    def execute(self, Morsel morsel):

        if self._build_phase:
            if morsel == EOS:
                self._build_phase = False

                # Combine buffered morsels (Draken-native, no concat_tables)
                if self.left_morsels:
                    self.left_morsel = Morsel.combine(self.left_morsels)
                    self.left_morsels = []
                else:
                    self.left_morsel = None

                # Skip join key casts — Draken-native path

                # Build bloom filter using Morsel.hash() (Draken-native)
                if self.left_morsel is not None and self.left_morsel.num_rows > 0:
                    from opteryx.compiled.structures.bloom_filter import create_bloom_filter_morsel
                    start = time.monotonic_ns()
                    self.left_filter = create_bloom_filter_morsel(self.left_morsel, self.left_columns)
                    self.readings["time_build_bloom_filter"] += time.monotonic_ns() - start
                    self.readings["feature_bloom_filter"] += 1

            else:
                if morsel is not None and morsel != EMPTY:
                    self.left_morsels.append(morsel)

            yield None
            return

        else:
            if morsel == EOS:
                yield EOS
                return

            if self.left_morsel is None or self.left_morsel.num_rows == 0 or morsel.num_rows == 0:
                left_indexes = ()
                right_indexes = ()
            else:
                # Apply bloom filter
                if self.left_filter is not None:
                    from opteryx.compiled.structures.bloom_filter import bloom_filter_check_morsel
                    start = time.monotonic_ns()
                    bit_results = bloom_filter_check_morsel(self.left_filter, morsel, self.right_columns)
                    self.readings["time_bloom_filtering"] += time.monotonic_ns() - start

                    if bit_results is not None:
                        # Convert bit-packed results directly to BoolVector
                        filter_mask = _bits_to_bool_vector(bit_results, morsel.num_rows)
                        morsel_filtered = morsel.filter_mask(filter_mask)
                        eliminated_rows = morsel.num_rows - morsel_filtered.num_rows
                        self.readings["rows_eliminated_by_bloom_filter"] += eliminated_rows
                        morsel = morsel_filtered

                # Skip join key casts — Draken-native path

                # Perform nested loop join
                if morsel.num_rows > 0:
                    left_indexes, right_indexes = _nested_loop_join_morsel(
                        self.left_morsel, morsel, self.left_columns, self.right_columns
                    )
                else:
                    left_indexes = ()
                    right_indexes = ()

            # Draken-native morsel alignment
            if left_indexes and right_indexes:
                yield _align_morsels_with_tuples(self.left_morsel, morsel, left_indexes, right_indexes)
            else:
                # Empty join result
                yield None
