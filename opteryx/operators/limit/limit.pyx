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
Limit Node

This is a SQL Query Execution Plan Node.

This Node performs the LIMIT and the OFFSET steps
"""

from typing import Generator, Optional
from collections.abc import Iterable

from opteryx.models import QueryProperties

# BasePlanNode in scope via textual include from _operators.pyx.


cdef class LimitNode(BasePlanNode):
    cdef public object limit
    cdef public object offset
    cdef public object remaining_rows
    cdef public object rows_left_to_skip

    def __init__(self, properties=None, **parameters):
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

    cpdef void _push_impl(self, Morsel morsel) except *:
        # Body runs GIL-held: the base nogil `_dispatch_push` decodes the C++
        # carrier (recovering the EOS sentinel) and calls this; it catches any
        # exception and surfaces it via the ErrCtx status path.
        if morsel is _EOS_SENTINEL:
            self.emit(morsel)
            return

        if morsel.num_rows == 0:
            return

        chunk = morsel

        if self.rows_left_to_skip > 0:
            if self.rows_left_to_skip >= chunk.num_rows:
                self.rows_left_to_skip -= chunk.num_rows
                return
            chunk = chunk.slice(
                offset=self.rows_left_to_skip,
                length=chunk.num_rows - self.rows_left_to_skip,
            )
            self.rows_left_to_skip = 0

        if self.remaining_rows <= 0:
            return

        if chunk.num_rows < self.remaining_rows:
            self.remaining_rows -= chunk.num_rows
            self.emit(chunk)
        else:
            rows_to_slice = self.remaining_rows
            self.remaining_rows = 0
            self.emit(chunk.slice(offset=0, length=rows_to_slice))
            # LIMIT reached — signal upstream to stop and emit terminal EOS
            # so downstream operators flush.
            if self._ctx is not None:
                self._ctx.terminate()
            self.emit(_EOS_SENTINEL)
