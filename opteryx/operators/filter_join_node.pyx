# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Filter Join Node

This is a SQL Query Execution Plan Node.

This module contains implementations for LEFT SEMI and LEFT ANTI joins.
These joins are used to filter rows from the left table based on the
presence or absence of matching rows in the right table.
"""

from typing import Generator, Optional
import time

from libc.stdint cimport int64_t, uint64_t

from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper
from opteryx.compiled.structures.buffers cimport IntBuffer, Int32Buffer
from opteryx.models import QueryProperties
from opteryx.compiled.draken.morsels.morsel cimport Morsel

from opteryx import EOS

from . import JoinNode

_DATA_FORMAT = "arrow"


cdef CarcharSetWrapper _build_filter_hash_set(Morsel morsel, list columns, CarcharSetWrapper seen_hashes):
    cdef Py_ssize_t num_rows = morsel.num_rows
    cdef Py_ssize_t row_idx
    cdef uint64_t[::1] row_hashes = morsel.hash(columns)

    if seen_hashes is None:
        seen_hashes = CarcharSetWrapper()

    for row_idx in range(num_rows):
        seen_hashes.insert(row_hashes[row_idx])

    return seen_hashes


cdef Morsel _semi_join_filter(Morsel relation, list join_columns, CarcharSetWrapper seen_hashes):
    cdef Py_ssize_t num_rows = relation.num_rows
    cdef Py_ssize_t row_idx
    cdef IntBuffer index_buffer = IntBuffer(num_rows)
    cdef uint64_t[::1] row_hashes = relation.hash(join_columns)

    for row_idx in range(num_rows):
        if seen_hashes.contains(row_hashes[row_idx]):
            index_buffer.append(row_idx)

    if index_buffer.size() > 0:
        return relation.take(index_buffer.to_int32_buffer())
    else:
        return relation.slice(0, 0)


cdef Morsel _anti_join_filter(Morsel relation, list join_columns, CarcharSetWrapper seen_hashes):
    cdef Py_ssize_t num_rows = relation.num_rows
    cdef Py_ssize_t row_idx
    cdef IntBuffer index_buffer = IntBuffer(num_rows)
    cdef uint64_t[::1] row_hashes = relation.hash(join_columns)

    for row_idx in range(num_rows):
        if not seen_hashes.contains(row_hashes[row_idx]):
            index_buffer.append(row_idx)

    if index_buffer.size() > 0:
        return relation.take(index_buffer.to_int32_buffer())
    else:
        return relation.slice(0, 0)


class FilterJoinNode(JoinNode):
    def __init__(self, properties: QueryProperties, **parameters):
        # Ensure `join_type` exists before the base initializer accesses `self.name`
        self.join_type = parameters["type"]
        JoinNode.__init__(self, properties=properties, **parameters)
        self.on = parameters.get("on")
        self.using = parameters.get("using")

        self.left_columns = parameters.get("left_columns")
        self.left_readers = parameters.get("left_readers")

        self.right_columns = parameters.get("right_columns")
        self.right_readers = parameters.get("right_readers")

        self.right_hash_set = CarcharSetWrapper()
        self._build_phase = True  # right side arrives first (plan reverses semi/anti joins)

    @property
    def name(self):  # pragma: no cover
        return self.join_type.replace(" ", "_")

    @property
    def config(self) -> str:  # pragma: no cover
        from opteryx.expression import format_expression

        if self.on:
            return f"{self.join_type.upper()} JOIN ({format_expression(self.on, True)})"
        if self.using:
            return f"{self.join_type.upper()} JOIN (USING {','.join(map(format_expression, self.using))})"
        return f"{self.join_type.upper()}"

    def execute(self, Morsel morsel):

        if self._build_phase:
            # Build phase: right side arrives first due to plan ordering for semi/anti joins
            if morsel == EOS:
                self._build_phase = False
                yield None
            else:
                morsel = self._apply_join_key_casts(morsel, is_left=False)
                start = time.monotonic_ns()
                self.right_hash_set = _build_filter_hash_set(morsel, self.right_columns, self.right_hash_set)
                self.readings["time_build_filter_hash_table"] += time.monotonic_ns() - start
                yield None
        else:
            # Probe phase: left side
            if morsel == EOS:
                yield EOS
                return
            morsel = self._apply_join_key_casts(morsel, is_left=True)
            if self.join_type == "left anti":
                yield _anti_join_filter(morsel, self.left_columns, self.right_hash_set)
            elif self.join_type == "left semi":
                yield _semi_join_filter(morsel, self.left_columns, self.right_hash_set)
