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
                col.schema_column.identity for col in self._distinct_on
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
            is_active_parvi = isinstance(self._hash_set, ParviSetWrapper) and not self._promoted
            promotion_seed = None
            if is_active_parvi and not self._hash_set.full():
                # Snapshot pre-chunk state so overflow replay can include all
                # new keys from this chunk, including those inserted pre-overflow.
                promotion_seed = CarcharSetWrapper()
                self._hash_set.drain_into_carchar(promotion_seed)

            overflow = distinct(chunk, self._hash_set, columns=self._distinct_on)

            # Promote only on real capacity overflow (unseen key at capacity).
            should_promote = overflow and is_active_parvi
            if should_promote:
                if promotion_seed is not None:
                    carchar_set = promotion_seed
                else:
                    # Already full at chunk start; current Parvi state is a valid seed.
                    parvi_set = self._hash_set
                    carchar_set = CarcharSetWrapper()
                    parvi_set.drain_into_carchar(carchar_set)
                self._hash_set = carchar_set
                self._promoted = True

                # Overflow path leaves chunk unchanged; replay yields all
                # previously-unseen rows for this chunk.
                distinct(chunk, self._hash_set, columns=self._distinct_on)

            if len(chunk) > 0 or not self.at_least_one_yielded:
                yield chunk

            self.at_least_one_yielded = True
