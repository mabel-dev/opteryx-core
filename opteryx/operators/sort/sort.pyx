# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Sort Node

This is a SQL Query Execution Plan Node.

This node orders a dataset using a permutation-based sort (C++ std::sort /
std::stable_sort with memcmp tiebreak) over Draken morsels. Dictionary-encoded
columns are ORDER BY-correct (codes are remapped to value rank before sorting,
with AVX2/NEON SIMD acceleration for uint8 codes).
"""

from typing import Generator, Optional
from opteryx.compiled.morsel_ops.sort import morsel_sort
from opteryx.exceptions import ColumnNotFoundError
from opteryx.expression import NodeType
from opteryx.expression import evaluate_and_append
from opteryx.models import QueryProperties

from opteryx import EOS

from . import BasePlanNode


class SortNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.order_by = parameters.get("order_by", [])
        self._morsels = []

    @property
    def config(self):  # pragma: no cover
        return ", ".join(
            f"{col.value} {'ASC' if ascending else 'DESC'}"
            for col, ascending in self.order_by
        )

    @property
    def name(self):  # pragma: no cover
        return "Sort"

    def execute(self, Morsel morsel):

        if morsel is not EOS:
            if morsel.num_rows > 0:
                self._morsels.append(morsel)
            yield None
            return

        if not self._morsels:
            return

        combined = Morsel.combine(self._morsels)

        column_names = []
        ascending_flags = []
        evaluations = []

        for column, ascending in self.order_by:
            if column.node_type != NodeType.IDENTIFIER:
                evaluations.append(column)
            try:
                identity = column.schema_column.identity
                column_names.append(identity)
            except ColumnNotFoundError as cnfe:  # pragma: no cover
                raise ColumnNotFoundError(
                    f"`ORDER BY` must reference columns as they appear in the `SELECT` clause. {cnfe}"
                ) from cnfe

            ascending_flags.append(bool(ascending))

        if evaluations:
            combined = evaluate_and_append(evaluations, combined)

        perm = morsel_sort(combined, column_names, ascending_flags)
        combined.take(perm)

        yield combined
