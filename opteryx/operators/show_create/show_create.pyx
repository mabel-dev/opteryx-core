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
Show Create Node

This is a SQL Query Execution Plan Node.
"""

from typing import Generator, Optional
from opteryx.exceptions import DatasetNotFoundError, UnsupportedSyntaxError
from opteryx.models import QueryProperties

# BasePlanNode/JoinNode in scope via _operators.pyx include.


class ShowCreateNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        self.object_type = parameters.get("object_type")
        self.object_name = parameters.get("object_name")
        # Bound by visit_show, which authorizes the read first. Never derived
        # here - deriving it locally is what let this run unauthorized.
        self.connector = parameters.get("connector")

    @property
    def name(self):  # pragma: no cover
        return "Show"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel):
        # VIEW is the only object type that reaches here: plan_show_create_query
        # rejects every other form by name at plan time.
        from opteryx.connectors import TableType

        object_type, _ = self.connector.locate_object(self.object_name)
        if object_type != TableType.View:
            raise DatasetNotFoundError(dataset=self.object_name, connector="VIEW")

        view_definition = self.connector.get_view(self.object_name)
        vectors = [
            vector_from_sequence([self.object_name], dtype=_draken_native.DrakenType.VARCHAR),
            vector_from_sequence([view_definition.statement], dtype=_draken_native.DrakenType.VARCHAR),
        ]
        morsel = Morsel.from_vectors([self.object_name, "create_statement"], vectors)
        yield morsel
