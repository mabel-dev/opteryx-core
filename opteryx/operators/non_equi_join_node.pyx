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

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Non-Equi Join Node - Pure Draken

This is a SQL Query Execution Plan Node.

This implements non-equi joins (comparisons other than equality) using:
- a scalar outer loop
- direct Draken vector scalar comparison methods on the inner side
- Draken-native morsel alignment

No PyArrow is used in this operator.
"""

from cpython.array cimport array
from libc.stdint cimport int32_t

from opteryx.compiled.draken.morsels.morsel cimport Morsel
from opteryx.compiled.draken.morsels.align cimport align_tables
from opteryx.models import QueryProperties

from opteryx import EOS, EMPTY

from . import JoinNode

_DATA_FORMAT = "draken"


cdef Morsel _align_morsels_with_tuples(
    Morsel left_morsel,
    Morsel right_morsel,
    object left_indexes,
    object right_indexes,
):
    """Align two morsels using Python index tuples/lists via Draken memoryviews."""
    cdef object left_arr = array('i', left_indexes) if left_indexes else array('i', [])
    cdef object right_arr = array('i', right_indexes) if right_indexes else array('i', [])
    cdef int32_t[::1] left_view = left_arr
    cdef int32_t[::1] right_view = right_arr
    return align_tables(left_morsel, right_morsel, left_view, right_view)


def _compare_vector_with_scalar(vector, value, comparison_op, swapped=False):
    """
    Compare a Draken vector against a scalar using direct vector scalar methods.

    When `swapped` is True, the original predicate was `value OP vector`, but the
    comparison is being evaluated as `vector ? value`, so ordered operators must
    be inverted.
    """
    if swapped:
        if comparison_op == "Gt":
            comparison_op = "Lt"
        elif comparison_op == "GtEq":
            comparison_op = "LtEq"
        elif comparison_op == "Lt":
            comparison_op = "Gt"
        elif comparison_op == "LtEq":
            comparison_op = "GtEq"

    if comparison_op == "NotEq":
        return vector.not_equals(value)
    if comparison_op == "Gt":
        return vector.greater_than(value)
    if comparison_op == "GtEq":
        return vector.greater_than_or_equals(value)
    if comparison_op == "Lt":
        return vector.less_than(value)
    if comparison_op == "LtEq":
        return vector.less_than_or_equals(value)

    raise ValueError(f"Unsupported comparison operator: {comparison_op}")


def _non_equi_nested_loop_join(
    left_morsel,
    right_morsel,
    left_column,
    right_column,
    comparison_op,
    swapped=False,
):
    """
    Perform a non-equi join using:
    - scalar outer loop
    - vectorized inner comparison via direct Draken scalar comparison methods

    Returns:
        Tuple of (left_indices, right_indices) as Python tuples for alignment.
    """
    left_rows = left_morsel.num_rows
    right_rows = right_morsel.num_rows

    if left_rows == 0 or right_rows == 0:
        return (), ()

    left_vec = left_morsel.column(left_column.encode("utf-8"))
    right_vec = right_morsel.column(right_column.encode("utf-8"))

    left_indexes = []
    right_indexes = []

    for i in range(left_rows):
        left_val = left_vec[i]
        if left_val is None:
            continue

        comparison_mask = _compare_vector_with_scalar(
            right_vec, left_val, comparison_op, swapped=swapped
        )

        for j in range(right_rows):
            if comparison_mask[j]:
                right_val = right_vec[j]
                if right_val is None:
                    continue
                left_indexes.append(i)
                right_indexes.append(j)

    return tuple(left_indexes), tuple(right_indexes)


class NonEquiJoinNode(JoinNode):
    join_type = "non equi"

    def __init__(self, properties: QueryProperties, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.left_column = parameters.get("on").get("left").schema_column.identity
        self.right_column = parameters.get("on").get("right").schema_column.identity
        self.comparison_op = parameters.get("on").get("value")

        self.left_morsel = None
        self.left_morsels = []
        self._build_phase = True

        valid_ops = ["NotEq", "Gt", "GtEq", "Lt", "LtEq"]
        if self.comparison_op not in valid_ops:
            raise ValueError(f"Unsupported comparison operator: {self.comparison_op}")

    @property
    def name(self):  # pragma: no cover
        return "Non-Equi Join"

    @property
    def config(self):  # pragma: no cover
        op_symbols = {
            "NotEq": "!=",
            "Gt": ">",
            "GtEq": ">=",
            "Lt": "<",
            "LtEq": "<=",
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

        right_morsel = morsel

        left_column = self.left_column
        right_column = self.right_column
        swapped = False

        if left_column.encode("utf-8") in right_morsel.column_names:
            left_column, right_column = right_column, left_column
            swapped = True

        left_indexes, right_indexes = _non_equi_nested_loop_join(
            self.left_morsel,
            right_morsel,
            left_column,
            right_column,
            self.comparison_op,
            swapped=swapped,
        )

        if left_indexes and right_indexes:
            yield _align_morsels_with_tuples(
                self.left_morsel, right_morsel, left_indexes, right_indexes
            )
        else:
            yield None
