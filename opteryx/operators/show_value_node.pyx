# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Show Variables Node

This is a SQL Query Execution Plan Node.
"""

from typing import Generator

from opteryx.compiled.draken.interop.arrow import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.exceptions import SqlError
from opteryx.models import QueryProperties
from opteryx.types import OrsoTypes

from opteryx import EOS

from . import ReaderNode

_DATA_FORMAT = "draken"


class ShowValueNode(ReaderNode):
    def __init__(self, properties: QueryProperties, **parameters):
        ReaderNode.__init__(self, properties=properties, **parameters)

        self.key = parameters.get("key")
        self.kind = parameters.get("kind")
        self.value = parameters.get("value")

        if self.kind == "PARAMETER":
            if self.value[0] == "@":
                raise SqlError("PARAMETERS cannot start with '@'")
            self.key = self.value
            self.value = properties.variables[self.value]

    @property
    def name(self):  # pragma: no cover
        return "Show Value"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel):
        vectors = [
            vector_from_sequence([self.key], dtype=OrsoTypes.VARCHAR),
            vector_from_sequence([str(self.value)], dtype=OrsoTypes.VARCHAR),
        ]
        morsel = Morsel.from_vectors(["name", "value"], vectors)
        yield morsel
