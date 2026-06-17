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
Union Node

This is a SQL Query Execution Plan Node.
"""

from typing import Generator, Optional
from opteryx.models import QueryProperties

# BasePlanNode in scope via _operators.pyx include.


cdef class UnionNode(BasePlanNode):
    cdef public list column_ids
    cdef public list schema

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.columns = parameters.get("columns", [])
        self.column_ids = [c.schema_column.identity for c in self.columns]
        self.schema = None

    @property
    def name(self):  # pragma: no cover
        return "Union"

    @property
    def config(self):  # pragma: no cover
        return ""

    cpdef void _push_impl(self, Morsel morsel) except *:
        """Union receives one EOS per input leg. The pipeline compiler stamps
        the expected leg count via set_expected_input_closes; emit the single
        downstream EOS only after every leg has closed.

        Body runs GIL-held: the base nogil `_dispatch_push` decodes the C++
        carrier (recovering the EOS sentinel) and calls this."""
        if morsel is _EOS_SENTINEL:
            if self._record_input_close():
                self.emit(_EOS_SENTINEL)
            return

        if self.schema is None:
            self.schema = list(morsel.column_names)
        else:
            morsel = morsel.rename(self.schema)

        if morsel.num_columns != len(self.column_ids):
            morsel = morsel.select(self.schema[: len(self.column_ids)])
        morsel = morsel.rename(self.column_ids)
        self.emit(morsel.select(self.column_ids))
