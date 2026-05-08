# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Cross Join Node - Draken-Native Implementation (Session 46)

This is a SQL Query Execution Plan Node.

This performs a CROSS JOIN - CROSS JOIN is not natively supported by PyArrow so this is written
here rather than calling the join() functions.

REFACTORED (Session 46): Draken-native Cartesian product
- Replaced NumPy index generation with Draken-native build_cartesian_indices
- Eliminated PyArrow table alignment in hot path in favor of Morsel.take
- Removed NumPy imports and dependency in hot paths
"""

from typing import Generator, Optional

from libc.stdint cimport int32_t, int64_t
from libc.stdlib cimport malloc, free

from draken.vectors.int64_vector cimport from_sequence as int64_from_sequence
from draken.vectors.int64_vector cimport from_rle_builder as int64_from_rle_builder

from opteryx.models import QueryProperties

from opteryx import EOS, EMPTY

from . import JoinNode


cpdef tuple build_cartesian_indices(int64_t left_rows, int64_t right_rows):
    """
    Build row indices for a Cartesian product (CROSS JOIN).

    Left index is RLE-encoded (left_rows runs of length right_rows each).
    Right index is dense ([0..right_rows-1] repeated left_rows times).

    Returns:
        tuple of (Int64Vector, Int64Vector) — left (RLE) and right (dense) row indices
    """
    cdef int64_t total_rows = left_rows * right_rows
    cdef int64_t i, j
    cdef int64_t* left_run_vals
    cdef int32_t* left_run_lens
    cdef Int64Vector left_vec
    cdef IntBuffer right_indices_buf
    cdef const int64_t[::1] right_mv
    cdef Int64Vector right_vec

    if total_rows == 0:
        return (Int64Vector(0), Int64Vector(0))

    left_run_vals = <int64_t*>malloc(left_rows * sizeof(int64_t))
    left_run_lens = <int32_t*>malloc(left_rows * sizeof(int32_t))
    if left_run_vals == NULL or left_run_lens == NULL:
        free(left_run_vals)
        free(left_run_lens)
        raise MemoryError()

    for i in range(left_rows):
        left_run_vals[i] = i
        left_run_lens[i] = <int32_t>right_rows

    left_vec = int64_from_rle_builder(left_run_vals, left_run_lens, <size_t>left_rows)
    free(left_run_vals)
    free(left_run_lens)

    right_indices_buf = IntBuffer(total_rows)
    for i in range(left_rows):
        for j in range(right_rows):
            right_indices_buf.append(j)

    right_mv = right_indices_buf.get_buffer()
    right_vec = int64_from_sequence(right_mv)
    right_vec._arrow_data_buf = right_indices_buf

    return (left_vec, right_vec)

INTERNAL_BATCH_SIZE: int = 10000  # config
MAX_JOIN_SIZE: int = 1_000_000  # config

def _cross_join(left_morsel: Morsel, right_morsel: Morsel) -> Generator[Morsel, None, None]:
    """
    A cross join is the cartesian product of two tables.
    Draken-native implementation using Morsel.take().
    """

    # Optimization for COUNT(*) queries
    # Note: identity for $COUNT(*) is a known constant
    encoded_count_identity = b"$COUNT(*)"
    if left_morsel.column_names == [encoded_count_identity] and right_morsel.column_names == [encoded_count_identity]:
        left_count = left_morsel.column(encoded_count_identity)[0]
        right_count = right_morsel.column(encoded_count_identity)[0]

        from draken.vectors.int64_vector import from_sequence
        res = Morsel.from_vectors(
            [encoded_count_identity],
            [from_sequence([left_count * right_count])]
        )
        yield res
        return

    if left_morsel.column_names == [encoded_count_identity]:
        left_count = left_morsel.column(encoded_count_identity)[0]
        for _ in range(left_count):
            yield right_morsel.copy()
        return

    if right_morsel.column_names == [encoded_count_identity]:
        right_count = right_morsel.column(encoded_count_identity)[0]
        for _ in range(right_count):
            yield left_morsel.copy()
        return

    cdef int left_rows = left_morsel.num_rows
    cdef int right_rows = right_morsel.num_rows

    if left_rows == 0 or right_rows == 0:
        # Return empty morsel with combined schema
        res = left_morsel.copy()
        res._empty_inplace()
        for col_name in right_morsel.column_names:
            if col_name not in res.column_names:
                res.append_vector(col_name, right_morsel.column(col_name).slice(0, 0))
        yield res
        return

    # Generate Cartesian product indices using Draken-native helper
    left_indices, right_indices = build_cartesian_indices(left_rows, right_rows)

    # Take rows from both morsels to create the join result
    res_morsel = left_morsel.copy().take(left_indices)

    # Take from right
    right_taken = right_morsel.copy().take(right_indices)

    # Merge columns
    left_names = set(left_morsel.column_names)
    for col_name in right_morsel.column_names:
        if col_name not in left_names:
            res_morsel.append_vector(col_name, right_taken.column(col_name))

    yield res_morsel

class CrossJoinNode(JoinNode):
    """
    Implements a SQL CROSS JOIN (Draken-native)
    """

    join_type = "cross"

    def __init__(self, properties: QueryProperties, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.source = parameters.get("column")

        # JoinNode expects these to be set for label_join_legs
        self.left_readers = parameters.get("left_readers")
        self.right_readers = parameters.get("right_readers")
        self.left_relation_names = parameters.get("left_relation_names") or []
        self.right_relation_names = parameters.get("right_relation_names") or []

        self.left_morsels = []
        self.right_morsels = []
        self.left_table = None  # Now stores a combined Morsel
        self.hash_set = CarcharSetWrapper()

        self.continue_executing = True
        self._build_phase = True

    @property
    def name(self):  # pragma: no cover
        return "Cross Join"

    @property
    def config(self):  # pragma: no cover
        return f"CROSS JOIN"

    def execute(self, Morsel morsel):

        if not self.continue_executing:
            yield None
            return

        if self._build_phase:
            if morsel == EOS:
                self._build_phase = False
                if self.left_morsels:
                    self.left_table = Morsel.combine(self.left_morsels)
                    self.left_morsels = []
                else:
                    # Left side has no rows — cross product is always empty.
                    self.left_table = None
                yield None
            else:
                if morsel is not None and morsel != EMPTY and len(morsel) > 0:
                    self.left_morsels.append(morsel)
                yield None
            return

        else:
            if morsel == EOS:
                if self.left_table is None:
                    yield EOS
                    return
                if self.right_morsels:
                    right_table = Morsel.combine(self.right_morsels)
                    self.right_morsels = []
                else:
                    yield EOS
                    return

                yield from _cross_join(self.left_table, right_table)
                yield EOS
            else:
                if morsel is not None and morsel != EMPTY and len(morsel) > 0:
                    self.right_morsels.append(morsel)
                yield None
