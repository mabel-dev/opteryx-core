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
Null Reader Node

This operator is used when a FILTER(FALSE) condition has been detected,
indicating that no rows can possibly match the predicate. Instead of reading
from the underlying connector, we short-circuit and return an empty table
with the correct schema.

This is more efficient than reading all rows and filtering them out.
"""

import logging
from typing import Generator


from draken.draken_native import vector_from_sequence as _vector_from_sequence
from opteryx.types.schema import RelationSchema

# EOS sentinel in scope as _EOS_SENTINEL via the umbrella unit.
# BasePlanNode in scope via _operators.pyx include.


logger = logging.getLogger(__name__)


cdef class NullReaderNode(BasePlanNode):  # pragma: no cover
    """
    Returns an empty table with the correct schema.

    Used when contradictory predicates make the result empty.
    """
    # `columns` is a BasePlanNode field; only the scan-specific extras here.
    cdef public object relations
    cdef public object schema

    def __init__(self, properties, **parameters):
        """Initialize NullReaderNode."""
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.columns = parameters.get("columns", [])
        self.relations = parameters.get("relations", [])
        self.schema = parameters.get("schema")

    def read_morsels(self):
        """Source-side iterator: yields a single empty morsel with the correct schema."""
        # Build empty Morsel with correct schema from columns property
        if self.columns:
            empty_morsel = Morsel()
            for col in self.columns:
                col_name = getattr(col, "identity", None) or getattr(col, "name", None) or col
                vector = _vector_from_sequence([])
                empty_morsel.append_vector(col_name if isinstance(col_name, bytes) else str(col_name).encode(), vector)
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
