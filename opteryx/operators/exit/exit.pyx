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
Exit Node

This is a SQL Query Execution Plan Node.

This does the final preparation before returning results to users.

This does two things that the projection node doesn't do:
    - renames columns from the internal names
    - removes all columns not being returned to the user

This node doesn't do any calculations, it is a pure Projection.
"""

from typing import Generator, Optional
from collections.abc import Iterable

from opteryx.exceptions import AmbiguousIdentifierError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import QueryProperties

# BasePlanNode in scope via textual include from _operators.pyx.


cdef class ExitNode(BasePlanNode):
    """Terminal operator of the push pipeline. Buffers formatted result
    morsels in `_pending`; the engine drains and yields them to the caller
    one at a time (streaming, not materialised)."""
    cdef public bint at_least_one
    cdef public list final_columns
    cdef public list final_names
    cdef public list _pending

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.at_least_one = False
        self._pending = []

        final_columns = []
        final_names = []
        for column in self.columns:
            final_columns.append(column.schema_column.identity)
            final_names.append(column.alias)

        if len(set(final_names)) != len(final_names):
            from collections import Counter

            duplicates = [name for name, count in Counter(final_names).items() if count > 1]
            raise AmbiguousIdentifierError(
                message=f"Query result contains multiple instances of the same column(s) - `{'`, `'.join(duplicates)}`"
            )

        self.final_columns = list(final_columns)
        self.final_names = final_names

    @property
    def config(self):  # pragma: no cover
        return None

    @property
    def name(self):  # pragma: no cover
        return "Exit"

    cpdef bint has_pending(self):
        return len(self._pending) > 0

    cpdef object pop_pending(self):
        return self._pending.pop(0)

    cdef void _dispatch_push(self, Morsel morsel) except *:
        if morsel is _EOS_SENTINEL:
            if not self.at_least_one:
                vectors = [_draken_native.vector_from_sequence([]) for _ in self.columns]
                empty = Morsel.from_vectors(self.final_names, vectors)
                self._pending.append(empty)
            return

        if morsel.num_rows == 0:
            return

        self.at_least_one = True

        morsel_column_names = morsel.column_names
        if not set(self.final_columns).issubset(morsel_column_names):  # pragma: no cover
            mapping = {
                name: int_name for name, int_name in zip(self.final_columns, self.final_names)
            }
            missing_references = {
                mapping.get(ref): ref
                for ref in self.final_columns
                if ref not in morsel_column_names
            }
            raise InvalidInternalStateError(
                f"The following fields were not in the resultset - {', '.join(missing_references.keys())}"
            )

        out = morsel.select(self.final_columns).rename(self.final_names)
        self._pending.append(out)
