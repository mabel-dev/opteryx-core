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
        self._set_variant = parameters.get("set_variant", "carchar")
        self._hash_set = None
        self.at_least_one_yielded = False
        self._promoted = False  # Track if we've promoted from parvi to carchar

    @property
    def config(self):  # pragma: no cover
        return ""

    @property
    def name(self):  # pragma: no cover
        return "Distinction"

    def execute(self, morsel):
        from opteryx.compiled.morsel_ops.distinct import distinct
        from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
        from opteryx.compiled.structures.parvi_set import ParviSetWrapper

        if self._hash_set is None:
            if self._set_variant == "parvi" and not self._promoted:
                self._hash_set = ParviSetWrapper()
            else:
                self._hash_set = CarcharSetWrapper()

        if morsel == EOS:
            return

        for chunk in [morsel]:
            distinct(chunk, self._hash_set, columns=self._distinct_on)

            # If parvi is now full and we haven't promoted yet, prepare to promote
            # on the next morsel
            if isinstance(self._hash_set, ParviSetWrapper) and self._hash_set.full() and not self._promoted:
                # Promote to carchar for next morsel
                carchar_set = CarcharSetWrapper()
                self._hash_set = carchar_set
                self._promoted = True

            if len(chunk) > 0 or not self.at_least_one_yielded:
                yield chunk

            self.at_least_one_yielded = True
