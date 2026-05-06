# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

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
from array import array

from draken.morsels.morsel cimport Morsel
from draken.morsels.align cimport align_tables
from draken.vectors.bool_vector cimport BoolVector
from draken.core.buffers cimport DrakenFixedBuffer

from opteryx.models import QueryProperties

from opteryx import EOS, EMPTY

from . import JoinNode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

cdef Morsel _align_morsels(Morsel left_morsel, Morsel right_morsel,
                            object left_list, object right_list):
    """Align two morsels using index lists, converting to typed array for zero-copy view."""
    cdef object left_arr  = array('i', left_list)
    cdef object right_arr = array('i', right_list)
    cdef int32_t[::1] left_view  = left_arr
    cdef int32_t[::1] right_view = right_arr
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

cdef tuple _non_equi_nested_loop_join_kernel(
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
        return None, None

    cdef object left_vec  = left_morsel.column(left_column)
    cdef object right_vec = right_morsel.column(right_column)

    cdef list left_list  = []
    cdef list right_list = []

    cdef Py_ssize_t i, j
    cdef object left_val
    cdef BoolVector mask
    cdef DrakenFixedBuffer* mask_ptr
    cdef uint8_t* data_bits
    cdef uint8_t* null_bits

    for i in range(left_rows):
        left_val = left_vec[i]
        if left_val is None:
            continue

        # One vectorized call covering all right_rows — SIMD-eligible inside Draken.
        mask = _compare_right_vec_with_scalar(right_vec, left_val, comparison_op, swapped)
        mask_ptr  = mask.ptr
        data_bits = <uint8_t*> mask_ptr.data
        null_bits = mask_ptr.null_bitmap  # NULL means all rows are valid

        # Inner loop: only bit extraction — no Python object access.
        if null_bits == NULL:
            # Fast path: no nulls in the comparison result.
            for j in range(right_rows):
                if (data_bits[j >> 3] >> (j & 7)) & 1:
                    left_list.append(i)
                    right_list.append(j)
        else:
            # Null-aware path: skip positions where the validity bit is 0.
            for j in range(right_rows):
                if ((null_bits[j >> 3] >> (j & 7)) & 1) and \
                   ((data_bits[j >> 3] >> (j & 7)) & 1):
                    left_list.append(i)
                    right_list.append(j)

    if not left_list:
        return None, None

    return left_list, right_list


# ---------------------------------------------------------------------------
# Node
# ---------------------------------------------------------------------------

class NonEquiJoinNode(JoinNode):
    join_type = "non equi"

    def __init__(self, properties: QueryProperties, **parameters):
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

    def execute(self, Morsel morsel):

        if self._build_phase:
            if morsel == EOS:
                self._build_phase = False
                if self.left_morsels:
                    self.left_morsel = Morsel.combine(self.left_morsels)
                    self.left_morsels = []
            else:
                if morsel is not None and morsel != EMPTY:
                    self.left_morsels.append(morsel)
            yield None
            return

        if morsel == EOS:
            yield EOS
            return

        if self.left_morsel is None or self.left_morsel.num_rows == 0 or morsel.num_rows == 0:
            yield None
            return

        left_column  = self.left_column
        right_column = self.right_column
        swapped = False

        if left_column in morsel.column_names:
            left_column, right_column = right_column, left_column
            swapped = True

        left_list, right_list = _non_equi_nested_loop_join_kernel(
            self.left_morsel,
            morsel,
            left_column,
            right_column,
            self.comparison_op,
            swapped,
        )

        if left_list is not None:
            yield _align_morsels(self.left_morsel, morsel, left_list, right_list)
        else:
            yield None
