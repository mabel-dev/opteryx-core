# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Limit Node

This is a SQL Query Execution Plan Node.

This Node performs the LIMIT and the OFFSET steps
"""

from collections.abc import Iterable

from opteryx import EOS
from opteryx.draken.morsels.morsel import Morsel
from opteryx.models import QueryProperties

from . import BasePlanNode

_DATA_FORMAT = "draken"


class LimitNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.limit = parameters.get("limit", float("inf"))
        self.offset = parameters.get("offset", 0)

        self.remaining_rows = self.limit if self.limit is not None else float("inf")
        self.rows_left_to_skip = max(0, self.offset)

    @property
    def name(self):  # pragma: no cover
        return "LIMIT"

    @property
    def config(self):  # pragma: no cover
        return str(self.limit) + " OFFSET " + str(self.offset)

    def execute(self, morsel: Morsel, **kwargs) -> Morsel:
        morsel = self.ensure_draken_morsel(morsel)

        if morsel == EOS:
            yield EOS
            return

        # Handle both single Morsel and Iterable of Morsels (from streaming)
        if isinstance(morsel, Morsel):
            morsels = (morsel,)
        elif isinstance(morsel, Iterable):
            morsels = morsel
        else:  # pragma: no cover
            yield None
            return

        for chunk in morsels:
            if chunk is EOS or chunk.num_rows == 0:
                continue

            if self.rows_left_to_skip > 0:
                if self.rows_left_to_skip >= chunk.num_rows:
                    self.rows_left_to_skip -= chunk.num_rows
                    continue
                else:
                    chunk = chunk.slice(
                        offset=self.rows_left_to_skip,
                        length=chunk.num_rows - self.rows_left_to_skip,
                    )
                    self.rows_left_to_skip = 0

            if self.remaining_rows <= 0:
                continue

            if chunk.num_rows < self.remaining_rows:
                self.remaining_rows -= chunk.num_rows
                yield chunk
            else:
                rows_to_slice = self.remaining_rows
                self.remaining_rows = 0
                yield chunk.slice(offset=0, length=rows_to_slice)
                break
