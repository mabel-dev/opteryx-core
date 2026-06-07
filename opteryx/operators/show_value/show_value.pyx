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
Show Variables Node

This is a SQL Query Execution Plan Node.
"""

from typing import Generator

from opteryx.exceptions import SqlError
from opteryx.models import QueryProperties

# EOS sentinel in scope as _EOS_SENTINEL via the umbrella unit.


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
            vector_from_sequence([self.key], dtype=_draken_native.DrakenType.VARCHAR),
            vector_from_sequence([str(self.value)], dtype=_draken_native.DrakenType.VARCHAR),
        ]
        morsel = Morsel.from_vectors(["name", "value"], vectors)
        yield morsel
