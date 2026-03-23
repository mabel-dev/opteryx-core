# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Distinct Node

This is a SQL Query Execution Plan Node.

This Node eliminates duplicate records.
"""

from opteryx.draken import Morsel
from opteryx.models import QueryProperties

from opteryx import EOS

from . import BasePlanNode

_DATA_FORMAT = "arrow,draken"


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

    def execute(self, morsel, **kwargs):
        from opteryx.compiled.morsel_ops.distinct import distinct
        from opteryx.compiled.structures.carchar_set import CarcharSetWrapper

        if self._hash_set is None:
            self._hash_set = CarcharSetWrapper()

        if morsel == EOS:
            yield EOS
            return

        if not isinstance(morsel, Morsel):
            # e.g. Arrow Table produced by UnionNode — convert to Morsel
            converted = self.ensure_draken_morsel(morsel)
            if converted is EOS:
                yield EOS
                return
            # iter_from_arrow returns a generator; process each chunk
            import pyarrow

            if isinstance(converted, pyarrow.Table):
                converted = Morsel.iter_from_arrow(converted)
            for sub_morsel in converted if hasattr(converted, "__iter__") else [converted]:
                yield from self.execute(sub_morsel, **kwargs)
            return

        for chunk in [morsel]:
            distinct(chunk, self._hash_set, columns=self._distinct_on)

            if len(chunk) > 0 or not self.at_least_one_yielded:
                yield chunk

            self.at_least_one_yielded = True
