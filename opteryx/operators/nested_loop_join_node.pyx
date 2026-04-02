# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Inner (Nested Loop) Join Node

This is a SQL Query Execution Plan Node.

This is an implementation of a nested loop join, which is a simple join algorithm, it excels
when one of the relations is very small - in this situation it's many times faster than a hash
join as we don't need to create the hash table.

The Join Order Optimization Strategy will decide if this node should be used, based on the size.

This is a toy implementation, whilst it is used in production payloads we're playing with
milliseconds of performance difference between this and a hash join.
"""

from typing import Generator, Optional
import time

import numpy
import pyarrow
import pyarrow.compute as pc
from opteryx.compiled.joins import nested_loop_join
from opteryx.compiled.structures.bloom_filter import create_bloom_filter
from opteryx.models import QueryProperties
from opteryx.utils.arrow import align_tables
from pyarrow import Table

from opteryx import EOS

from . import JoinNode

_DATA_FORMAT = "arrow"


class NestedLoopJoinNode(JoinNode):
    join_type = "nested_loop"

    def __init__(self, properties: QueryProperties, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.left_columns = parameters.get("left_columns")
        self.right_columns = parameters.get("right_columns")

        self.left_relation = None
        self.left_buffer = []

        self.left_filter = None  # bloom filter for the left relation
        self._build_phase = True

    @property
    def name(self):  # pragma: no cover
        return "Nested Loop Join"

    @property
    def config(self):  # pragma: no cover
        return ""

    @staticmethod
    def _filter_null_join_keys(table: Table, join_columns):
        """
        SQL inner-join semantics treat NULL join keys as non-matching.
        Drop rows where any join key is NULL (or NaN in float columns).
        """
        if table is None or table.num_rows == 0 or not join_columns:
            return table

        mask = None
        for column in join_columns:
            if column not in table.column_names:
                continue
            column_data = table.column(column)
            column_mask = pc.is_valid(column_data)
            if pyarrow.types.is_floating(column_data.type):
                column_mask = pc.and_(column_mask, pc.invert(pc.is_nan(column_data)))
            mask = column_mask if mask is None else pc.and_(mask, column_mask)

        if mask is None:
            return table
        return table.filter(mask)

    def execute(self, morsel):
        morsel = self.ensure_arrow_table(morsel)

        if self._build_phase:
            if morsel == EOS:
                self._build_phase = False
                self.left_relation = pyarrow.concat_tables(self.left_buffer, promote_options="none")
                self.left_buffer.clear()
                self.left_relation = self._apply_join_key_casts(self.left_relation, is_left=True)
                self.left_relation = self._filter_null_join_keys(
                    self.left_relation, self.left_columns
                )

                # build a bloom filter for the left relation if it's small enough
                start = time.monotonic_ns()
                self.left_filter = create_bloom_filter(self.left_relation, self.left_columns)
                self.readings["time_build_bloom_filter"] += time.monotonic_ns() - start
                self.readings["feature_bloom_filter"] += 1

            else:
                self.left_buffer.append(morsel)
            yield None
            return

        else:
            if morsel == EOS:
                yield EOS
                return

            if self.left_relation.num_rows == 0 or morsel.num_rows == 0:
                left_indexes = numpy.array([], dtype=numpy.int64)
                right_indexes = numpy.array([], dtype=numpy.int64)
            else:
                if self.left_filter is not None:
                    # Filter the morsel using the bloom filter, it's a quick way to
                    # reduce the number of rows that need to be joined.
                    start = time.monotonic_ns()
                    _pcm = self.left_filter.possibly_contains_many(morsel, self.right_columns)
                    maybe_in_left = pyarrow.Array.from_buffers(
                        pyarrow.bool_(),
                        morsel.num_rows,
                        [None, pyarrow.py_buffer(_pcm)],
                    )
                    self.readings["time_bloom_filtering"] += time.monotonic_ns() - start

                    morsel = morsel.filter(maybe_in_left)
                    eliminated_rows = len(maybe_in_left) - morsel.num_rows
                    self.readings["rows_eliminated_by_bloom_filter"] += eliminated_rows

                morsel = self._apply_join_key_casts(morsel, is_left=False)
                morsel = self._filter_null_join_keys(morsel, self.right_columns)

                left_indexes, right_indexes = nested_loop_join(
                    self.left_relation, morsel, self.left_columns, self.right_columns
                )
            yield align_tables(self.left_relation, morsel, left_indexes, right_indexes)
