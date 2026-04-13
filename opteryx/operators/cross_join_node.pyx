# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Cross Join Node - Draken-Native Buffering (Session 42)

This is a SQL Query Execution Plan Node.

This performs a CROSS JOIN - CROSS JOIN is not natively supported by PyArrow so this is written
here rather than calling the join() functions

REFACTORED (Session 42): Draken-native Morsel buffering
- Buffer morsels instead of Arrow tables
- Morsel.combine() instead of pyarrow.concat_tables()

Note: CROSS JOIN UNNEST is implemented by the UnnestJoinNode
"""

from typing import Generator, Optional
import numpy
import pyarrow
from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.models import QueryProperties

from opteryx import EOS, EMPTY

from . import JoinNode

_DATA_FORMAT = "draken"


INTERNAL_BATCH_SIZE: int = 10000  # config
MAX_JOIN_SIZE: int = 1_000_000  # config


def _cartesian_product(*arrays):
    """
    Cartesian product of arrays creates every combination of the elements in the arrays
    """
    array_count = len(arrays)
    arr = numpy.empty([len(array) for array in arrays] + [array_count], dtype=numpy.int64)
    for i, array in enumerate(numpy.ix_(*arrays)):
        arr[..., i] = array
    return numpy.hsplit(arr.reshape(-1, array_count), array_count)


def _cross_join(left_table, right_table):
    """
    A cross join is the cartesian product of two tables - this usually isn't very
    useful, but it does allow you to the theta joins (non-equi joins)

    Arrow-based join algorithm (warm path, acceptable).
    """

    def _chunker(seq_1, seq_2, size: int = INTERNAL_BATCH_SIZE):
        for i in range(0, len(seq_1), size):
            yield memoryview(seq_1)[i : i + size], memoryview(seq_2)[i : i + size]

    from opteryx.utils.arrow import align_tables

    # Optimization for COUNT(*) queries
    if left_table.column_names == ["$COUNT(*)"] and right_table.column_names == ["$COUNT(*)"]:
        left_count = left_table["$COUNT(*)"][0].as_py()
        right_count = right_table["$COUNT(*)"][0].as_py()
        yield pyarrow.Table.from_pydict({"$COUNT(*)": [left_count * right_count]})
        return

    if left_table.column_names == ["$COUNT(*)"]:
        left_count = left_table["$COUNT(*)"][0].as_py()
        for _ in range(left_count):
            yield right_table
        return

    if right_table.column_names == ["$COUNT(*)"]:
        right_count = right_table["$COUNT(*)"][0].as_py()
        for _ in range(right_count):
            yield left_table
        return

    at_least_once = False
    left_schema = left_table.schema
    right_schema = right_table.schema

    # Iterate through left table in chunks of size INTERNAL_BATCH_SIZE
    for left_block in left_table.to_batches(max_chunksize=INTERNAL_BATCH_SIZE):
        # Convert the chunk to a table to retain column names
        left_block = pyarrow.Table.from_batches([left_block], schema=left_table.schema)

        # Create an array of row indices for each table
        left_array = numpy.arange(left_block.num_rows, dtype=numpy.int64)
        right_array = numpy.arange(right_table.num_rows, dtype=numpy.int64)

        # Calculate the cartesian product of the two arrays of row indices
        left_align, right_align = _cartesian_product(left_array, right_array)

        # Further break down the result into manageable chunks of size MAX_JOIN_SIZE
        for left_chunk, right_chunk in _chunker(
            left_align.flatten(), right_align.flatten(), MAX_JOIN_SIZE
        ):
            # Align the tables using the specified chunks of row indices
            table = align_tables(left_block, right_table, left_chunk, right_chunk)

            # Yield the resulting table to the caller
            yield table
            at_least_once = True

    if not at_least_once:
        fields = [pyarrow.field(name=f.name, type=f.type) for f in right_schema] + [
            pyarrow.field(name=f.name, type=f.type) for f in left_schema
        ]
        combined_schemas = pyarrow.schema(fields)
        yield pyarrow.Table.from_arrays(
            [pyarrow.array([]) for _ in combined_schemas], schema=combined_schemas
        )


class CrossJoinNode(JoinNode):
    """
    Implements a SQL CROSS JOIN
    """

    join_type = "cross"

    def __init__(self, properties: QueryProperties, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.source = parameters.get("column")

        self._left_relation = parameters.get("left_relation_names")
        self._right_relation = parameters.get("right_relation_names")

        # REFACTORED (Session 42): Buffer Morsels instead of Arrow tables
        self.left_morsels = []
        self.right_morsels = []
        self.left_table = None
        self.right_table = None
        self.hash_set = CarcharSetWrapper()

        self.continue_executing = True
        self._build_phase = True

    @property
    def name(self):  # pragma: no cover
        return "Cross Join"

    @property
    def config(self):  # pragma: no cover
        return f"CROSS JOIN"

    def execute(self, morsel):
        morsel = self.ensure_draken_morsel(morsel)

        if not self.continue_executing:
            yield None
            return

        if self._build_phase:
            if morsel == EOS:
                self._build_phase = False
                # REFACTORED (Session 42): Combine Morsels instead of Arrow tables
                if self.left_morsels:
                    left_morsel = Morsel.combine(self.left_morsels)
                    self.left_morsels = []
                    # Convert to Arrow for join algorithm (warm path, acceptable)
                    self.left_table = left_morsel.to_arrow()
                else:
                    self.left_table = pyarrow.table({})
            else:
                if morsel is not None and morsel != EMPTY:
                    self.left_morsels.append(morsel)
            yield None
            return

        else:
            if morsel == EOS:
                # REFACTORED (Session 42): Combine Morsels instead of Arrow tables
                if self.right_morsels:
                    right_morsel = Morsel.combine(self.right_morsels)
                    self.right_morsels = []
                    # Convert to Arrow for join algorithm (warm path, acceptable)
                    right_table = right_morsel.to_arrow()
                else:
                    right_table = pyarrow.table({})

                yield from _cross_join(self.left_table, right_table)
                yield EOS
            else:
                if morsel is not None and morsel != EMPTY:
                    self.right_morsels.append(morsel)
                yield None
