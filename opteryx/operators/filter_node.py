# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Selection Node

This is a SQL Query Execution Plan Node.

This node is responsible for applying filters to datasets.
"""

from opteryx import EOS
from opteryx.expression import NodeType
from opteryx.expression import format_expression
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import QueryProperties

from . import BasePlanNode


class FilterNode(BasePlanNode):
    is_stateless = True

    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.filter = parameters.get("filter")

        self.function_evaluations = get_all_nodes_of_type(
            self.filter,
            select_nodes=(NodeType.FUNCTION,),
        )

    @property
    def config(self):  # pragma: no cover
        return format_expression(self.filter)

    @property
    def name(self):  # pragma: no cover
        return "Filter"

    def execute(self, morsel, **kwargs):
        from opteryx.draken.morsels.morsel import Morsel
        from opteryx.expression.evaluator import evaluate_and_append_draken
        from opteryx.expression.evaluator import evaluate_draken

        if morsel is EOS:
            yield EOS
            return

        if not isinstance(morsel, Morsel):
            morsel = Morsel.from_arrow(morsel)

        if self.function_evaluations:
            morsel = evaluate_and_append_draken(self.function_evaluations, morsel)

        mask = evaluate_draken(self.filter, morsel)
        filtered = morsel.filter_mask(mask)

        if filtered.num_rows > 0:
            yield filtered
        else:
            yield morsel.slice(0, 0)
