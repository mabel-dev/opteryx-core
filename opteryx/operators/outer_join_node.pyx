# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Outer Join Node - Draken-Native Morsel-Based Operations

This is a SQL Query Execution Plan Node.

PyArrow has LEFT/RIGHT/FULL OUTER JOIN implementations, but they error when the
relations being joined contain STRUCT or ARRAY columns so we've written our own
OUTER JOIN implementations.

REFACTORED (Session 51): Draken-native Morsel-based join functions
- Join functions accept Morsels instead of Arrow tables
- Morsel.combine() and align_tables_pyarray() for result alignment
- No Arrow conversions within join logic; Arrow only remains at execution boundaries
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
from opteryx.compiled.structures.bloom_filter import create_bloom_filter
from opteryx.compiled.structures.bloom_filter import bloom_filter_check_morsel
from opteryx.compiled.structures.buffers import IntBuffer
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.compiled.draken.morsels.align import align_tables_pyarray
from opteryx.models import QueryProperties

from opteryx import EOS, EMPTY

from . import JoinNode

_DATA_FORMAT = "draken"


CHUNK_SIZE: int = 50_000


cdef BoolVector _bits_to_bool_vector(uint8_t[::1] bits, Py_ssize_t n):
    """Convert bit-packed uint8 memoryview to BoolVector (Draken-native, no Arrow)."""
    if bits is None:
        return None
    return bool_vector_from_bits(&bits[0], NULL, n)


def left_join(
    left_morsel,
    right_morsel,
    left_columns: List[str],
    right_columns: List[str],
    filter_index,
    left_hash,
    columns=None,
):
    """
    Perform a LEFT OUTER JOIN using prebuilt hash map and optional filter.

    Accepts Morsels, returns Morsels.

    Yields:
        Morsel chunks of the joined result.
    """

    left_indexes = []
    right_indexes = []
    seen_flags = array("b", [0]) * len(left_morsel)

    if filter_index is not None:
        bit_results = bloom_filter_check_morsel(filter_index, right_morsel, right_columns)
        if bit_results is not None:
            mask = _bits_to_bool_vector(bit_results, len(right_morsel))
            right_morsel = right_morsel.filter_mask(mask)

        if len(right_morsel) == 0:
            for i in range(0, len(left_morsel), CHUNK_SIZE):
                chunk_end = min(i + CHUNK_SIZE, len(left_morsel))
                chunk_indices = list(range(i, chunk_end))
                null_indices = [-1] * len(chunk_indices)
                yield align_tables_pyarray(
                    left_morsel,
                    right_morsel,
                    chunk_indices,
                    null_indices,
                )
            return

    right_hash = probe_side_hash_map(right_morsel, right_columns)

    for h, right_rows in right_hash.hash_table.items():
        left_rows = left_hash.get(h)
        if not left_rows:
            continue
        for l in left_rows:
            seen_flags[l] = 1
            left_indexes.extend([l] * len(right_rows))
            right_indexes.extend(right_rows)

    if left_indexes:
        yield align_tables_pyarray(
            left_morsel,
            right_morsel,
            left_indexes,
            right_indexes,
        )

    unmatched = [i for i, seen in enumerate(seen_flags) if not seen]

    if unmatched:
        for i in range(0, len(unmatched), CHUNK_SIZE):
            chunk_end = min(i + CHUNK_SIZE, len(unmatched))
            chunk_indices = unmatched[i:chunk_end]
            null_indices = [-1] * len(chunk_indices)

            yield align_tables_pyarray(
                left_morsel,
                right_morsel,
                chunk_indices,
                null_indices,
            )

    return


def right_join(
    left_morsel,
    right_morsel,
    left_columns: List[str],
    right_columns: List[str],
    filter_index,
    left_hash,
    columns=None,
):
    """
    Perform a RIGHT OUTER JOIN using prebuilt hash map and optional filter.

    Accepts Morsels, returns Morsels.

    Yields:
        Morsel chunks of the joined result.
    """

    left_hash_table = probe_side_hash_map(left_morsel, left_columns)

    left_indexes = []
    right_indexes = []
    seen_flags = array("b", [0]) * len(right_morsel)

    if filter_index is not None:
        bit_results = bloom_filter_check_morsel(filter_index, left_morsel, left_columns)
        if bit_results is not None:
            mask = _bits_to_bool_vector(bit_results, len(left_morsel))
            left_morsel = left_morsel.filter_mask(mask)

        if len(left_morsel) == 0:
            for i in range(0, len(right_morsel), CHUNK_SIZE):
                chunk_end = min(i + CHUNK_SIZE, len(right_morsel))
                chunk_indices = list(range(i, chunk_end))
                null_indices = [-1] * len(chunk_indices)
                yield align_tables_pyarray(
                    left_morsel,
                    right_morsel,
                    null_indices,
                    chunk_indices,
                )
            return

    right_hashes = right_morsel.hash(right_columns)
    for i in range(len(right_morsel)):
        left_rows = left_hash_table.get(right_hashes[i])
        if left_rows:
            seen_flags[i] = 1
            for left_idx in left_rows:
                left_indexes.append(left_idx)
                right_indexes.append(i)

    if left_indexes:
        yield align_tables_pyarray(
            left_morsel,
            right_morsel,
            left_indexes,
            right_indexes,
        )

    unmatched = [i for i, seen in enumerate(seen_flags) if not seen]

    if unmatched:
        for i in range(0, len(unmatched), CHUNK_SIZE):
            chunk_end = min(i + CHUNK_SIZE, len(unmatched))
            chunk_indices = unmatched[i:chunk_end]
            null_indices = [-1] * len(chunk_indices)

            yield align_tables_pyarray(
                left_morsel,
                right_morsel,
                null_indices,
                chunk_indices,
            )

    return


def full_join(
    left_morsel,
    right_morsel,
    left_columns: List[str],
    right_columns: List[str],
    columns=None,
    **kwargs,
):
    """
    Perform a FULL OUTER JOIN.

    Accepts Morsels, returns Morsels.

    Yields:
        Morsel chunks of the joined result.
    """
    right_hash_table = probe_side_hash_map(right_morsel, right_columns)

    left_indexes = []
    right_indexes = []
    matched_right = set()

    left_hashes = left_morsel.hash(left_columns)
    for i in range(len(left_morsel)):
        right_rows = right_hash_table.get(left_hashes[i])
        if right_rows:
            for right_idx in right_rows:
                left_indexes.append(i)
                right_indexes.append(right_idx)
                matched_right.add(right_idx)
        else:
            left_indexes.append(i)
            right_indexes.append(-1)

    for i in range(len(right_morsel)):
        if i not in matched_right:
            left_indexes.append(-1)
            right_indexes.append(i)

    total_rows = len(left_indexes)
    chunk_start = 0

    while chunk_start < total_rows:
        chunk_end = min(chunk_start + CHUNK_SIZE, total_rows)
        chunk_left = left_indexes[chunk_start : chunk_end]
        chunk_right = right_indexes[chunk_start : chunk_end]

        yield align_tables_pyarray(
            left_morsel,
            right_morsel,
            chunk_left,
            chunk_right,
        )

        chunk_start = chunk_end


class OuterJoinNode(JoinNode):
    def __init__(self, properties: QueryProperties, **parameters):
        # Ensure `join_type` exists before the base initializer accesses `self.name`
        self.join_type = parameters["type"]
        JoinNode.__init__(self, properties=properties, **parameters)
        self.on = parameters.get("on")
        self.using = parameters.get("using")

        self.left_columns = parameters.get("left_columns")
        self.left_readers = parameters.get("left_readers") or []

        self.right_columns = parameters.get("right_columns")
        self.right_readers = parameters.get("right_readers") or []

        self.left_relation_names = parameters.get("left_relation_names") or []
        self.right_relation_names = parameters.get("right_relation_names") or []

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
        cdef object pass_filter_index = self.filter_index

        if self._build_phase:
            if morsel == EOS:
                self._build_phase = False
                if self.left_morsels:
                    left_morsel = Morsel.combine(self.left_morsels)
                    self.left_morsels = []
                    self._left_morsel = left_morsel
                else:
                    self._left_morsel = Morsel.from_vectors({})

                self.left_relation = self._apply_join_key_casts(self._left_morsel.to_arrow(), is_left=True)
                self._left_morsel = Morsel.from_arrow(self.left_relation)

                if self.join_type == "left outer":
                    start = time.monotonic_ns()
                    self.left_hash = build_side_hash_map(self._left_morsel, self.left_columns)

                    if len(self._left_morsel) < 16_000_001:
                        start = time.monotonic_ns()
                        self.filter_index = create_bloom_filter(self.left_relation, self.left_columns)
                        self.readings["time_build_bloom_filter"] += time.monotonic_ns() - start
                        self.readings["feature_bloom_filter"] += 1
            else:
                if morsel is not None and morsel != EMPTY:
                    self.left_morsels.append(morsel)
            yield None
            return

        if morsel == EOS:
            if self.right_morsels:
                right_morsel = Morsel.combine(self.right_morsels)
                self.right_morsels = []

                if pass_filter_index is not None:
                    orig_rows = len(right_morsel)
                    bit_results = bloom_filter_check_morsel(self.filter_index, right_morsel, self.right_columns)
                    if bit_results is not None:
                        mask = _bits_to_bool_vector(bit_results, orig_rows)
                        right_morsel = right_morsel.filter_mask(mask)
                        eliminated_rows = orig_rows - len(right_morsel)
                        self.readings["rows_eliminated_by_bloom_filter"] += eliminated_rows
                        global BLOOM_FASTPATH_COUNTER
                        BLOOM_FASTPATH_COUNTER += 1
                        pass_filter_index = None
            else:
                right_morsel = Morsel.from_vectors({})

            right_relation = self._apply_join_key_casts(right_morsel.to_arrow(), is_left=False)
            right_morsel = Morsel.from_arrow(right_relation)
            left_morsel_for_join = self._left_morsel

            join_provider = providers.get(self.join_type)

            for result_morsel in join_provider(
                left_morsel=left_morsel_for_join,
                right_morsel=right_morsel,
                left_columns=self.left_columns,
                right_columns=self.right_columns,
                left_hash=self.left_hash,
                filter_index=pass_filter_index,
                columns=self.columns,
            ):
                if self.columns is not None:
                    candidates = [c.schema_column.identity for c in self.columns]
                    keep_columns = [c for c in candidates if c in result_morsel.column_names]
                    result_morsel = result_morsel.select(keep_columns)
                yield result_morsel.to_arrow()
            yield EOS
        else:
            if morsel is not None and morsel != EMPTY:
                self.right_morsels.append(morsel)
            yield None


providers = {"left outer": left_join, "full outer": full_join, "right outer": right_join}
