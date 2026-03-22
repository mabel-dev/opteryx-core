# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Sort Node

This is a SQL Query Execution Plan Node.

This node orders a dataset using a permutation-based LSD radix sort over Draken
morsels.  Dictionary-encoded columns are ORDER BY-correct (codes are remapped to
value rank before sorting, with AVX2/NEON SIMD acceleration for uint8 codes).
"""

from orso.types import OrsoTypes

from opteryx import EOS
from opteryx.compiled.morsel_ops.sort import morsel_sort
from opteryx.exceptions import ColumnNotFoundError
from opteryx.expression import NodeType
from opteryx.expression import evaluate_and_append
from opteryx.models import QueryProperties

from . import BasePlanNode

_DATA_FORMAT = "arrow,draken"


class SortNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.order_by = parameters.get("order_by", [])
        self._morsel = None

    @property
    def config(self):  # pragma: no cover
        return ", ".join([f"{i[0].value} {i[1][0:3].upper()}" for i in self.order_by])

    @property
    def name(self):  # pragma: no cover
        return "Sort"

    def execute(self, morsel, **kwargs):
        morsel = self.ensure_draken_morsel(morsel)

        if morsel is not EOS:
            if morsel.num_rows > 0:
                if self._morsel is None:
                    self._morsel = morsel
                else:
                    self._morsel.append(morsel)
            yield None
            return

        if self._morsel is None:
            yield EOS
            return

        column_names = []
        ascending_flags = []
        evaluations = []

        for column, direction in self.order_by:
            if column.node_type == NodeType.LITERAL and column.type == OrsoTypes.INTEGER:
                # ORDER BY <position> — natural number, 1-based
                col_name = self._morsel.column_names[int(column.value) - 1]
                column_names.append(col_name if isinstance(col_name, bytes) else col_name.encode())
            else:
                if column.node_type != NodeType.IDENTIFIER:
                    evaluations.append(column)
                try:
                    identity = column.schema_column.identity
                    column_names.append(
                        identity if isinstance(identity, bytes) else identity.encode()
                    )
                except ColumnNotFoundError as cnfe:  # pragma: no cover
                    raise ColumnNotFoundError(
                        f"`ORDER BY` must reference columns as they appear in the `SELECT` clause. {cnfe}"
                    ) from cnfe

            asc = not str(direction).upper().startswith("DESC")
            ascending_flags.append(asc)

        if evaluations:
            self._morsel = evaluate_and_append(evaluations, self._morsel)

        perm = morsel_sort(self._morsel, column_names, ascending_flags)
        self._morsel.take(list(perm))

        yield self._morsel
        yield EOS
