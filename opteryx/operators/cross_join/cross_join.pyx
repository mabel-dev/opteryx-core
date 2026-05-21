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

from libc.stdint cimport int64_t, uint32_t
from libc.stdlib cimport malloc, free

from draken.vectors.integer64_vector cimport from_decoded as int64_from_decoded
from draken.vectors.integer64_vector cimport make_int64_dict_only

from opteryx.models import QueryProperties

# EOS sentinel available as _EOS_SENTINEL via the umbrella unit.

# BasePlanNode/JoinNode in scope via _operators.pyx include.


cpdef tuple build_cartesian_indices(int64_t left_rows, int64_t right_rows):
    """
    Build row indices for a Cartesian product (CROSS JOIN).

    Left index is dictionary-encoded (unique values [0..left_rows-1], codes expand runs).
    Right index is dense ([0..right_rows-1] repeated left_rows times).

    Returns:
        tuple of (Integer64Vector dict-encoded, Integer64Vector dense) — left and right row indices
    """
    cdef int64_t total_rows = left_rows * right_rows
    cdef int64_t i, j
    cdef Integer64Vector left_vec, right_vec
    cdef uint32_t* codes = NULL
    cdef int64_t* dict_vals = NULL
    cdef int64_t* rvals = NULL

    if total_rows == 0:
        return (Integer64Vector(0), Integer64Vector(0))

    # dict values: [0, 1, ..., left_rows-1]
    dict_vals = <int64_t*>malloc(<size_t>left_rows * sizeof(int64_t))
    if dict_vals == NULL:
        raise MemoryError()

    # codes[i * right_rows + j] = i  (same run value repeated right_rows times)
    codes = <uint32_t*>malloc(<size_t>total_rows * sizeof(uint32_t))
    if codes == NULL:
        free(dict_vals)
        raise MemoryError()

    # Right side: draken-allocated buffer, ownership transferred via from_decoded.
    rvals = <int64_t*>malloc(<size_t>total_rows * sizeof(int64_t))
    if rvals == NULL:
        free(codes)
        free(dict_vals)
        raise MemoryError()

    with nogil:
        for i in range(left_rows):
            dict_vals[i] = i

        for i in range(left_rows):
            for j in range(right_rows):
                codes[i * right_rows + j] = <uint32_t>i

        for i in range(left_rows):
            for j in range(right_rows):
                rvals[i * right_rows + j] = j

    # Transfer ownership of rvals before the fallible left-side construction;
    # if make_int64_dict_only raises, right_vec frees rvals on dealloc.
    right_vec = int64_from_decoded(<void*>rvals, NULL, <size_t>total_rows)

    # make_int64_dict_only copies both codes and dict_vals internally.
    left_vec = make_int64_dict_only(
        codes, <Py_ssize_t>total_rows,
        dict_vals, <Py_ssize_t>left_rows, NULL
    )
    free(codes)
    free(dict_vals)

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

        from draken.interop.vector_sequence import vector_from_sequence
        res = Morsel.from_vectors(
            [encoded_count_identity],
            [vector_from_sequence([left_count * right_count])]
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

    cdef Py_ssize_t left_rows = left_morsel.num_rows
    cdef Py_ssize_t right_rows = right_morsel.num_rows

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

cdef class CrossJoinNode(JoinNode):
    """
    Implements a SQL CROSS JOIN (Draken-native)
    """

    cdef public object source
    cdef public list left_morsels
    cdef public list right_morsels
    cdef public Morsel left_table
    cdef public CarcharSetWrapper hash_set
    cdef public bint continue_executing
    cdef public bint _build_phase

    join_type = "cross"

    def __init__(self, properties=None, **parameters):
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

    cpdef void push_left(self, Morsel morsel) except *:
        if not self.continue_executing:
            return
        if morsel is _EOS_SENTINEL:
            if self.left_morsels:
                self.left_table = Morsel.combine(self.left_morsels)
                self.left_morsels = []
            else:
                self.left_table = None
            return
        if morsel is not None and len(morsel) > 0:
            self.left_morsels.append(morsel)

    cpdef void push_right(self, Morsel morsel) except *:
        cdef Morsel right_table
        if not self.continue_executing:
            return
        if morsel is _EOS_SENTINEL:
            if self.left_table is None:
                self.emit(_EOS_SENTINEL)
                return
            if self.right_morsels:
                right_table = Morsel.combine(self.right_morsels)
                self.right_morsels = []
            else:
                self.emit(_EOS_SENTINEL)
                return
            for chunk in _cross_join(self.left_table, right_table):
                self.emit(chunk)
            self.emit(_EOS_SENTINEL)
            return
        if morsel is not None and len(morsel) > 0:
            self.right_morsels.append(morsel)
