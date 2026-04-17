# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Null Reader Node

This operator is used when a FILTER(FALSE) condition has been detected,
indicating that no rows can possibly match the predicate. Instead of reading
from the underlying connector, we short-circuit and return an empty table
with the correct schema.

This is more efficient than reading all rows and filtering them out.
"""

import logging
from typing import Generator


from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.compiled.draken.vectors.scalar_constructors import from_scalar
from opteryx.types.schema import RelationSchema

from opteryx import EOS

from . import BasePlanNode

_DATA_FORMAT = "arrow,draken"


logger = logging.getLogger(__name__)


class NullReaderNode(BasePlanNode):  # pragma: no cover
    """
    Returns an empty table with the correct schema.

    Used when contradictory predicates make the result empty.
    """

    def __init__(self, properties, **parameters):
        """Initialize NullReaderNode."""
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.columns = parameters.get("columns", [])
        self.relations = parameters.get("relations", [])
        self.schema = parameters.get("schema")

    def execute(self, morsel):
        """Return empty table with correct schema."""
        if morsel == EOS:
            yield None
            return

        # Try to build empty Morsel with correct schema
        # First try: use schema property if available
        if self.schema:
            try:
                empty_morsel = Morsel()
                for column in self.schema.columns:
                    # Create empty vector with correct type
                    vector = from_scalar(None, 0, dtype=column.arrow_field.type)
                    empty_morsel.append_vector(column.identity.encode(), vector)
                yield empty_morsel
                return
            except Exception as err:  # pragma: no cover - defensive fallback
                logger.debug(f"Unable to build schema-aware empty morsel: {err}")

        # Second try: use columns property if available
        if self.columns:
            empty_morsel = Morsel()
            for col in self.columns:
                col_name = col
                if hasattr(col, "identity"):
                    col_name = col.identity
                elif hasattr(col, "name"):
                    col_name = col.name

                # Default to null type for unknown columns
                vector = from_scalar(None, 0)
                empty_morsel.append_vector(str(col_name).encode(), vector)
            yield empty_morsel
            return

        # Fallback: return completely empty morsel
        yield Morsel()

    @property
    def name(self):  # pragma: no cover
        """Friendly name for this step"""
        return "Null Reader"

    @property
    def config(self):
        """Additional details for this step"""
        return "(empty table - contradictory predicates)"

    def __repr__(self):  # pragma: no cover
        return f"<{self.__class__.__name__}>"
