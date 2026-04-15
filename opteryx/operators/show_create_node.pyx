# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Show Create Node

This is a SQL Query Execution Plan Node.
"""

from typing import Generator, Optional
from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.exceptions import DatasetNotFoundError, UnsupportedSyntaxError
from opteryx.models import QueryProperties
from opteryx.types import OrsoTypes

from . import BasePlanNode

_DATA_FORMAT = "draken"


class ShowCreateNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        self.object_type = parameters.get("object_type")
        self.object_name = parameters.get("object_name")

    @property
    def name(self):  # pragma: no cover
        return "Show"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel):
        if self.object_type == "VIEW":
            from opteryx.planner.views import is_view
            from opteryx.planner.views import view_as_sql

            if is_view(self.object_name):
                print("SHOW CREATE VIEW", self.object_name)
                view_sql = view_as_sql(self.object_name)
                vectors = [
                    vector_from_sequence([self.object_name], dtype=OrsoTypes.VARCHAR),
                    vector_from_sequence([view_sql], dtype=OrsoTypes.VARCHAR),
                ]
                morsel = Morsel.from_vectors([self.object_name, "create_statement"], vectors)
                yield morsel
                return

            raise DatasetNotFoundError(dataset=self.object_name, connector="VIEW")

        raise UnsupportedSyntaxError("Invalid SHOW statement")
