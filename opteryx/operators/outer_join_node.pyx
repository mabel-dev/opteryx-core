# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Outer Join Node - Draken-Native Buffering (Session 42)

This is a SQL Query Execution Plan Node.

PyArrow has LEFT/RIGHT/FULL OUTER JOIN implementations, but they error when the
relations being joined contain STRUCT or ARRAY columns so we've written our own
OUTER JOIN implementations.

REFACTORED: Draken-native Morsel buffering (Session 42)
- Buffer morsels instead of Arrow tables
- Morsel.combine() instead of pyarrow.concat_tables()
- Join functions operate on Arrow (warm path, acceptable)
"""

import time
from array import array
from typing import List

# Draken BoolVector helpers for converting bit-packed bloom results
from libc.stdint cimport uint8_t
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector, bool_vector_from_bits

# Telemetry: number of times the outer-join bloom-filter Draken fast-path was applied.
# Incremented when a probe morsel is filtered via the Draken bit-packed result path.
BLOOM_FASTPATH_COUNTER = 0

import pyarrow
from opteryx.compiled.joins import build_side_hash_map
from opteryx.compiled.joins import probe_side_hash_map
from opteryx.compiled.joins import right_join
from opteryx.compiled.structures.bloom_filter import create_bloom_filter
from opteryx.compiled.structures.buffers import IntBuffer, Int32Buffer
from opteryx.compiled.structures.hash_table import HashTable
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.models import QueryProperties
from opteryx.utils.arrow import align_tables

from opteryx import EOS, EMPTY

from . import JoinNode

_DATA_FORMAT = "draken"


CHUNK_SIZE: int = 50_000


def left_join(
    left_relation,
    right_relation,
    left_columns: List[str],
    right_columns: List[str],
    filter_index,
    left_hash,
    columns=None,
):
    """
    Perform a LEFT OUTER JOIN using a prebuilt hash map and optional filter.

    Yields:
        pyarrow.Table chunks of the joined result.
    """

    left_indexes = IntBuffer()
    right_indexes = IntBuffer()
    seen_flags = array("b", [0]) * left_relation.num_rows

    if filter_index:
        # We can just dispose of rows from the right relation that don't match
        # our bloom filter
        _pcm = filter_index.possibly_contains_many(right_relation, right_columns)
        possibly_matching_rows = pyarrow.Array.from_buffers(
            pyarrow.bool_(),
            right_relation.num_rows,
            [None, pyarrow.py_buffer(_pcm)],
        )
        right_relation = right_relation.filter(possibly_matching_rows)

        # If there's no matching rows in the right relation, we can exit early
        if right_relation.num_rows == 0:
            # Short circuit: no matching right rows at all
            for i in range(0, left_relation.num_rows, CHUNK_SIZE):
                chunk = list(range(i, min(i + CHUNK_SIZE, left_relation.num_rows)))
                yield align_tables(
                    source_table=left_relation,
                    append_table=right_relation.slice(0, 0),
                    source_indices=chunk,
                    append_indices=[None] * len(chunk),
                )
            return

    # Build the hash table of the right relation
    right_hash = probe_side_hash_map(right_relation, right_columns)

    for h, right_rows in right_hash.hash_table.items():
        left_rows = left_hash.get(h)
        if not left_rows:
            continue
        for l in left_rows:
            seen_flags[l] = 1
            left_indexes.extend([l] * len(right_rows))
            right_indexes.extend(right_rows)

    # Yield matching rows
    if left_indexes.size() > 0:
        yield align_tables(
            right_relation,
            left_relation,
            right_indexes.to_int32_buffer(),
            left_indexes.to_int32_buffer(),
        )

    # Emit unmatched left rows using null-filled right columns
    unmatched = [i for i, seen in enumerate(seen_flags) if not seen]

    if unmatched:
        unmatched_left = left_relation.take(pyarrow.array(unmatched))
        # Create a right-side table with zero rows, we do this because
        # we want arrow to do the heavy lifting of adding new columns to
        # the left relation, we do not want to add rows to the left
        # relation - arrow is faster at adding null columns that we can be.
        null_right = pyarrow.table(
            [pyarrow.nulls(0, type=field.type) for field in right_relation.schema],
            schema=right_relation.schema,
        )
        yield pyarrow.concat_tables([unmatched_left, null_right], promote_options="permissive")

    return


def full_join(
    left_relation,
    right_relation,
    left_columns: List[str],
    right_columns: List[str],
    columns=None,
    **kwargs,
):
    hash_table = HashTable()
    non_null_right_values = right_relation.select(right_columns).itercolumns()
    for i, value_tuple in enumerate(zip(*non_null_right_values)):
        hash_table.insert(abs(hash(value_tuple)), i)

    left_indexes = []
    right_indexes = []

    left_values = left_relation.select(left_columns).itercolumns()
    for i, value_tuple in enumerate(zip(*left_values)):
        rows = hash_table.get(abs(hash(value_tuple)))
        if rows:
            right_indexes.extend(rows)
            left_indexes.extend([i] * len(rows))
        else:
            right_indexes.append(None)
            left_indexes.append(i)

    for i in range(right_relation.num_rows):
        if i not in right_indexes:
            right_indexes.append(i)
            left_indexes.append(None)

    for i in range(0, len(left_indexes), CHUNK_SIZE):
        chunk_left_indexes = left_indexes[i : i + CHUNK_SIZE]
        chunk_right_indexes = right_indexes[i : i + CHUNK_SIZE]

        # Align this chunk and add the resulting table to our list
        yield align_tables(right_relation, left_relation, chunk_right_indexes, chunk_left_indexes)


class OuterJoinNode(JoinNode):
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

        self.columns = parameters.get("columns")

        # REFACTORED (Session 42): Morsel buffering instead of Arrow table buffering
        self.left_morsels = []
        self.right_morsels = []
        self.left_relation = None
        self.left_hash = None

        self.filter_index = None
        self._build_phase = True

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

    def execute(self, morsel):
        morsel = self.ensure_draken_morsel(morsel)
        # Cython-typed locals used for Draken-native bloom-filter checks.
        # Declared here at the top of the function so cdef is valid (not inside a nested block).
        cdef Py_ssize_t orig_rows
        cdef uint8_t[::1] bit_results
        cdef object pass_filter_index
        cdef object right_morsel_local
        cdef object left_morsel_local

        if self._build_phase:
            if morsel == EOS:
                self._build_phase = False
                # REFACTORED (Session 42): Combine Morsels instead of Arrow tables
                if self.left_morsels:
                    left_morsel = Morsel.combine(self.left_morsels)
                    self.left_morsels = []
                    # Keep the combined Morsel around for fast-path bloom creation and later use.
                    self._left_morsel = left_morsel
                    # Convert to Arrow for join algorithm (warm path, acceptable)
                    self.left_relation = left_morsel.to_arrow()
                else:
                    self._left_morsel = None
                    self.left_relation = pyarrow.table({})

                self.left_relation = self._apply_join_key_casts(self.left_relation, is_left=True)
                if self.join_type == "left outer":
                    start = time.monotonic_ns()
                    self.left_hash = build_side_hash_map(self.left_relation, self.left_columns)

                    if self.left_relation.num_rows < 16_000_001:
                        # Prefer a Morsel-based fast path when the combined left morsel is available.
                        # This avoids creating Arrow buffers in the hot path and keeps bloom checks
                        # fully Draken-native.
                        start = time.monotonic_ns()
                        if getattr(self, "_left_morsel", None) is not None:
                            # Fast Draken-native builder
                            from opteryx.compiled.structures.bloom_filter import create_bloom_filter_morsel
                            self.filter_index = create_bloom_filter_morsel(self._left_morsel, self.left_columns)
                        else:
                            # Fallback to Arrow-based builder
                            self.filter_index = create_bloom_filter(self.left_relation, self.left_columns)
                        self.readings["time_build_bloom_filter"] += time.monotonic_ns() - start
                        self.readings["feature_bloom_filter"] += 1
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

                    # Prefer to apply the bloom-filter at the Morsel level (native fast path).
                    # If we successfully apply the Draken bloom check we will convert the
                    # filtered morsel to Arrow and avoid constructing an Arrow mask via
                    # Array.from_buffers().
                    pass_filter_index = self.filter_index
                    if self.filter_index is not None:
                        try:
                            from opteryx.compiled.structures.bloom_filter import bloom_filter_check_morsel
                            # Use the C-typed locals declared at the top of this function.
                            orig_rows = right_morsel.num_rows
                            # bloom_filter_check_morsel returns a typed uint8_t[::1] memoryview or None.
                            bit_results = bloom_filter_check_morsel(self.filter_index, right_morsel, self.right_columns)
                            if bit_results is not None:
                                # Convert bit-packed results directly to BoolVector (Draken-native)
                                mask = bool_vector_from_bits(&bit_results[0], NULL, orig_rows)
                                right_morsel = right_morsel.filter_mask(mask)
                                eliminated_rows = orig_rows - right_morsel.num_rows
                                self.readings["rows_eliminated_by_bloom_filter"] += eliminated_rows
                                # Fast-path used — increment module-level counter for telemetry/tests
                                global BLOOM_FASTPATH_COUNTER
                                BLOOM_FASTPATH_COUNTER += 1
                                # We've applied bloom filtering at Morsel level, no need for provider to
                                # re-run the bloom check via Arrow buffers - suppress it.
                                pass_filter_index = None
                        except Exception:
                            # On any failure, fall back to Arrow-based path below.
                            pass

                    # Convert (possibly filtered) morsel to Arrow for the join warm-path.
                    right_relation = right_morsel.to_arrow()
                else:
                    right_relation = pyarrow.table({})

                right_relation = self._apply_join_key_casts(right_relation, is_left=False)

                join_provider = providers.get(self.join_type)

                for result_table in join_provider(
                    left_relation=self.left_relation,
                    right_relation=right_relation,
                    left_columns=self.left_columns,
                    right_columns=self.right_columns,
                    left_hash=self.left_hash,
                    filter_index=pass_filter_index,
                    columns=self.columns,
                ):
                    # Project down to only the needed columns if specified
                    if self.columns is not None:
                        candidates = [c.schema_column.identity for c in self.columns]
                        keep_columns = [c for c in candidates if c in result_table.schema.names]
                        result_table = result_table.select(keep_columns)
                    yield result_table
                yield EOS

            else:
                if morsel is not None and morsel != EMPTY:
                    self.right_morsels.append(morsel)
                yield None


providers = {"left outer": left_join, "full outer": full_join, "right outer": right_join}
