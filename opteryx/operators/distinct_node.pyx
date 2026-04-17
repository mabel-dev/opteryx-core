# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Distinct Node

This is a SQL Query Execution Plan Node.

This Node eliminates duplicate records.
"""

from typing import Generator, Optional
from opteryx.compiled.draken import Morsel
from opteryx.models import QueryProperties

from opteryx import EOS

from . import BasePlanNode

_DATA_FORMAT = "draken"


class DistinctNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self._distinct_on = parameters.get("on")
        if self._distinct_on:
            # Convert column identities to bytes for Draken morsel
            self._distinct_on = [
                col.schema_column.identity.encode("utf-8") for col in self._distinct_on
            ]
        self._hash_set = None
        self.at_least_one_yielded = False

    @property
    def config(self):  # pragma: no cover
        return ""

    @property
    def name(self):  # pragma: no cover
        return "Distinction"

    def execute(self, morsel):
        from opteryx.compiled.morsel_ops.distinct import distinct
        from opteryx.compiled.structures.carchar_set import CarcharSetWrapper

        if self._hash_set is None:
            self._hash_set = CarcharSetWrapper()

        if morsel == EOS:
            return

        for chunk in [morsel]:
            distinct(chunk, self._hash_set, columns=self._distinct_on)

            if len(chunk) > 0 or not self.at_least_one_yielded:
                yield chunk

            self.at_least_one_yielded = True
