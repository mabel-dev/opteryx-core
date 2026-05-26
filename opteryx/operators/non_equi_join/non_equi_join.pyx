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
Non-Equi Join Node - Pure Draken

This is a SQL Query Execution Plan Node.

This implements non-equi joins (comparisons other than equality) using:
- a vectorized outer loop: for each left row, call the Draken typed scalar comparison
  on the entire right vector, producing a bit-packed BoolVector in one SIMD-eligible pass
- direct bit extraction from the BoolVector (no Python indexing in the inner loop)
- CInt32Buffer for index accumulation (no Python list allocation)
- Draken-native morsel alignment

"""

from libc.stdint cimport int32_t, uint8_t
from libc.string cimport memcpy
from libcpp.vector cimport vector as cppvector
from array import array

from draken.morsels.morsel cimport Morsel, align_tables
from draken.vectors.bool_vector cimport BoolVector
from draken.core.buffers cimport DrakenVector

from opteryx.models import QueryProperties


cdef extern from "operators/loop_join_kernels.hpp" namespace "opteryx::operators" nogil:
    void non_equi_emit_indices(
        int32_t left_index,
        const uint8_t* data_bits,
        const uint8_t* null_bits,
        size_t right_rows,
        cppvector[int32_t]& out_left,
        cppvector[int32_t]& out_right,
    )

# EOS sentinel available as _EOS_SENTINEL via the umbrella unit.

# BasePlanNode/JoinNode in scope via _operators.pyx include.


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

cdef Morsel _align_morsels_from_vec(Morsel left_morsel, Morsel right_morsel,
                                     const cppvector[int32_t]& left_vec,
                                     const cppvector[int32_t]& right_vec):
    """Align two morsels using C++ vector indices via a typed int32 array view."""
    cdef Py_ssize_t n = <Py_ssize_t>left_vec.size()
    cdef object left_arr  = array('i', [0]) * n
    cdef object right_arr = array('i', [0]) * n
    cdef int32_t[::1] left_view  = left_arr
    cdef int32_t[::1] right_view = right_arr
    if n > 0:
        memcpy(&left_view[0],  left_vec.data(),  n * sizeof(int32_t))
        memcpy(&right_view[0], right_vec.data(), n * sizeof(int32_t))
    return align_tables(left_morsel, right_morsel, left_view, right_view)


cdef BoolVector _compare_right_vec_with_scalar(object right_vec, object left_val, object comparison_op, bint swapped):
    """
    Call the correct typed Draken scalar comparison on right_vec.

    When swapped=True the predicate was originally  left_val OP right_vec,
    but we stored the join with sides flipped, so we invert ordered operators.
    """
    cdef object op = comparison_op

    if swapped:
        if op == "Gt":
            op = "Lt"
        elif op == "GtEq":
            op = "LtEq"
        elif op == "Lt":
            op = "Gt"
        elif op == "LtEq":
            op = "GtEq"
        # NotEq is symmetric — no inversion needed

    if op == "NotEq":
        return right_vec.not_equals(left_val)
    if op == "Gt":
        return right_vec.greater_than(left_val)
    if op == "GtEq":
        return right_vec.greater_than_or_equals(left_val)
    if op == "Lt":
        return right_vec.less_than(left_val)
    if op == "LtEq":
        return right_vec.less_than_or_equals(left_val)

    raise ValueError(f"Unsupported comparison operator: {comparison_op}")


# ---------------------------------------------------------------------------
# Core kernel
# ---------------------------------------------------------------------------

cdef Morsel _non_equi_nested_loop_join_kernel(
    Morsel left_morsel,
    Morsel right_morsel,
    object left_column,
    object right_column,
    str comparison_op,
    bint swapped,
):
    """
    Non-equi join kernel.

    Complexity: O(L * R) rows, but the inner per-right-row work is a single
    bit extraction — the heavy comparison is done once per left row as a
    vectorized Draken operation that can exploit SIMD internally.

    Null handling:
    - Left nulls:  skipped before the comparison call (no allocation wasted).
    - Right nulls: the Draken comparison methods propagate NULLs into the
                   BoolVector validity bitmap.  We honour that by checking the
                   null_bitmap in the inner bit-extraction loop.
    """
    cdef Py_ssize_t left_rows  = left_morsel.num_rows
    cdef Py_ssize_t right_rows = right_morsel.num_rows

    if left_rows == 0 or right_rows == 0:
        return None

    cdef object left_vec  = left_morsel.column(left_column)
    cdef object right_vec = right_morsel.column(right_column)

    cdef cppvector[int32_t] left_idx_vec
    cdef cppvector[int32_t] right_idx_vec

    cdef Py_ssize_t i
    cdef int32_t i32
    cdef object left_val
    cdef BoolVector mask
    cdef DrakenVector* mask_uv
    cdef uint8_t* data_bits
    cdef uint8_t* null_bits
    cdef size_t right_rows_sz = <size_t>right_rows

    for i in range(left_rows):
        left_val = left_vec[i]
        if left_val is None:
            continue

        # One vectorized call covering all right_rows — SIMD-eligible inside Draken.
        mask = _compare_right_vec_with_scalar(right_vec, left_val, comparison_op, swapped)
        mask_uv   = mask.unified()
        data_bits = <uint8_t*> mask_uv.data
        null_bits = mask_uv.validity  # NULL means all rows are valid
        i32 = <int32_t>i

        # Inner loop: bit-extraction in C++ (popcount-driven scan, nogil).
        with nogil:
            non_equi_emit_indices(
                i32, data_bits, null_bits, right_rows_sz,
                left_idx_vec, right_idx_vec,
            )

    if left_idx_vec.size() == 0:
        return None

    return _align_morsels_from_vec(left_morsel, right_morsel, left_idx_vec, right_idx_vec)


# ---------------------------------------------------------------------------
# Node
# ---------------------------------------------------------------------------

cdef class NonEquiJoinNode(JoinNode):
    cdef public object left_column
    cdef public object right_column
    cdef public str comparison_op
    cdef public Morsel left_morsel
    cdef public list left_morsels
    cdef public bint _build_phase

    join_type = "non equi"

    def __init__(self, properties=None, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.left_column  = parameters.get("on").get("left").schema_column.identity
        self.right_column = parameters.get("on").get("right").schema_column.identity
        self.comparison_op = parameters.get("on").get("value")

        self.left_morsel  = None
        self.left_morsels = []
        self._build_phase = True

        valid_ops = ("NotEq", "Gt", "GtEq", "Lt", "LtEq")
        if self.comparison_op not in valid_ops:
            raise ValueError(f"Unsupported comparison operator: {self.comparison_op}")

    @property
    def name(self):  # pragma: no cover
        return "Non-Equi Join"

    @property
    def config(self):  # pragma: no cover
        op_symbols = {
            "NotEq": "!=",
            "Gt":    ">",
            "GtEq":  ">=",
            "Lt":    "<",
            "LtEq":  "<=",
        }
        op_symbol = op_symbols.get(self.comparison_op, self.comparison_op)
        return f"{self.left_column} {op_symbol} {self.right_column}"

    cpdef void push_left(self, Morsel morsel) except *:
        if morsel is _EOS_SENTINEL:
            if self.left_morsels:
                self.left_morsel = Morsel.combine(self.left_morsels)
                self.left_morsels = []
            return
        if morsel is not None:
            self.left_morsels.append(morsel)

    cpdef void push_right(self, Morsel morsel) except *:
        if morsel is _EOS_SENTINEL:
            self.emit(_EOS_SENTINEL)
            return

        if self.left_morsel is None or self.left_morsel.num_rows == 0 or morsel.num_rows == 0:
            return

        left_column  = self.left_column
        right_column = self.right_column
        swapped = False

        if left_column in morsel.column_names:
            left_column, right_column = right_column, left_column
            swapped = True

        cdef Morsel aligned = _non_equi_nested_loop_join_kernel(
            self.left_morsel,
            morsel,
            left_column,
            right_column,
            self.comparison_op,
            swapped,
        )

        if aligned is not None:
            self.emit(aligned)
