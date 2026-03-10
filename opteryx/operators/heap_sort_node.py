# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Heap Sort Node (Top-N Sort)

This is a SQL Query Execution Plan Node.

This query plan node maintains a sorted table of the top-N records seen so far
based on the provided ORDER BY clause. Despite the name, this is not a Heap Sort
algorithm, but an incremental Top-N sorter that works chunk-wise for efficiency.

This is faster, particularly when working with large datasets even though we're now
sorting smaller chunks over and over again.
"""

import heapq
from collections.abc import Iterable
from functools import cmp_to_key

import numpy
import pyarrow

from opteryx import EOS
from opteryx.draken.interop.arrow import vector_from_sequence
from opteryx.draken.morsels.morsel import Morsel
from opteryx.exceptions import ColumnNotFoundError
from opteryx.models import QueryProperties

from . import BasePlanNode


class HeapSortNode(BasePlanNode):
    _NULL_COMPRESSED = numpy.iinfo(numpy.int64).min
    # Dictionary child type ids from third_party/mabel/draken/core/buffers.h.
    # We only treat exact integer/boolean dictionary keys as exact-compressible.
    _EXACT_DICTIONARY_CHILD_TYPES = frozenset({1, 2, 3, 4, 50})
    _EXACT_COMPRESS_VECTOR_TYPES = frozenset(
        {
            "BoolVector",
            "Date32Vector",
            "Float64Vector",
            "Int8Vector",
            "Int16Vector",
            "Int32Vector",
            "Int64Vector",
            "StringVector",
            "TimeVector",
            "TimestampVector",
            "UInt8Vector",
            "UInt16Vector",
            "UInt32Vector",
            "UInt64Vector",
        }
    )

    @classmethod
    def _is_exact_compressible_vector(cls, vector) -> bool:
        vector_type = vector.__class__.__name__
        if vector_type in cls._EXACT_COMPRESS_VECTOR_TYPES:
            return True
        if vector_type != "DictionaryVector":
            return False
        return getattr(vector, "dictionary_value_type", None) in cls._EXACT_DICTIONARY_CHILD_TYPES

    def __init__(self, properties: QueryProperties, **parameters):
        super().__init__(properties=properties, **parameters)
        self.order_by = parameters.get("order_by", [])
        self.limit = parameters.get("limit", -1)

        self.mapped_order = []
        for column, direction in self.order_by:
            try:
                self.mapped_order.append((column.schema_column.identity, direction))
            except ColumnNotFoundError as cnfe:
                raise ColumnNotFoundError(
                    f"`ORDER BY` must reference columns from `SELECT`. {cnfe}"
                ) from cnfe
        self.table = None

    @property
    def config(self):  # pragma: no cover
        order = ", ".join(
            f"{col.schema_column.name} {dir[:3].upper()}" for col, dir in self.order_by
        )
        return f"LIMIT = {self.limit}, ORDER = {order}"

    @property
    def name(self):  # pragma: no cover
        return "Heap Sort"

    def execute(self, morsel: pyarrow.Table, **kwargs):
        morsel = self.ensure_draken_morsel(morsel)

        _ = kwargs  # kwargs are part of the execution contract
        if morsel is EOS:
            if self.table is None:
                yield EOS
                return

            if (self.limit is None or self.limit <= 0) and self.mapped_order:
                self.table = self._sort_morsel(self.table)
            elif self.limit and self.limit > 0 and self.mapped_order:
                self.table = self._top_n(self.table)

            yield self.table
            yield EOS
            return

        if isinstance(morsel, Morsel):
            morsels = (morsel,)
        elif isinstance(morsel, Iterable):
            morsels = morsel
        else:  # pragma: no cover
            yield None
            return

        for chunk in morsels:
            if chunk is EOS or chunk.num_rows == 0:
                continue

            if self.limit and self.limit > 0:
                chunk = self._top_n(chunk)
                if self.table is None:
                    self.table = chunk
                else:
                    self.table.append(chunk)
                    self.table = self._top_n(self.table)
            else:
                if self.table is None:
                    self.table = chunk
                else:
                    self.table.append(chunk)

        yield None

    def _sorted_indices(self, morsel: Morsel) -> list[int]:
        if not self.mapped_order:
            return list(range(morsel.num_rows))

        indices = list(range(morsel.num_rows))
        for column_name, direction in reversed(self.mapped_order):
            values = morsel.column(column_name.encode()).to_pylist()
            reverse = direction.upper().startswith("DESC")

            non_null = [i for i in indices if values[i] is not None]
            nulls = [i for i in indices if values[i] is None]
            non_null.sort(key=lambda i: values[i], reverse=reverse)
            indices = non_null + nulls
        return indices

    def _materialize_rows(self, morsel: Morsel, row_indices: list[int]) -> Morsel:
        if not row_indices:
            return morsel.empty()

        names = morsel.column_names
        if len(row_indices) <= 4096:
            selection = numpy.asarray(row_indices, dtype=numpy.int32)
            vectors = []
            for name in names:
                vector = morsel.column(name)
                if vector.__class__.__name__ == "StringVector":
                    values = []
                    for row_index in row_indices:
                        single = vector.take(numpy.asarray([row_index], dtype=numpy.int32))
                        values.append(single.to_pylist()[0])
                    vectors.append(vector_from_sequence(values))
                else:
                    vectors.append(vector.take(selection))
            return Morsel.from_vectors(names, vectors)

        selection = numpy.asarray(row_indices, dtype=numpy.int32)
        vectors = []
        for name in names:
            vec = morsel.column(name)
            vectors.append(vec.take(selection))
        return Morsel.from_vectors(names, vectors)

    def _sort_morsel(self, morsel: Morsel) -> Morsel:
        return self._materialize_rows(morsel, self._sorted_indices(morsel))

    def _top_n(self, morsel: Morsel) -> Morsel:
        if self.limit is None or self.limit <= 0:
            return morsel

        k = min(self.limit, morsel.num_rows)
        if k == 0:
            return morsel.empty()

        if not self.mapped_order:
            return self._materialize_rows(morsel, list(range(k)))

        if len(self.mapped_order) == 1:
            return self._top_n_single_key(morsel, k)

        uniform_direction = self._uniform_direction()
        if uniform_direction is not None:
            return self._top_n_multi_key_uniform(morsel, k, descending=uniform_direction)

        key_vectors = [morsel.column(column.encode()) for column, _ in self.mapped_order]
        key_values = [vec.to_pylist() for vec in key_vectors]
        directions = [direction.upper().startswith("DESC") for _, direction in self.mapped_order]

        def compare_rows(left_index: int, right_index: int) -> int:
            for values, descending in zip(key_values, directions):
                left_value = values[left_index]
                right_value = values[right_index]

                left_null = left_value is None
                right_null = right_value is None
                if left_null and right_null:
                    continue
                if left_null:
                    return 1
                if right_null:
                    return -1
                if left_value == right_value:
                    continue
                if descending:
                    return -1 if left_value > right_value else 1
                return -1 if left_value < right_value else 1
            return 0

        class _WorstFirst:
            __slots__ = ("index",)

            def __init__(self, index: int):
                self.index = index

            def __lt__(self, other: "_WorstFirst"):
                # Invert comparison so the heap root is the worst current top-k row.
                return compare_rows(self.index, other.index) > 0

        heap: list[_WorstFirst] = []
        for row_index in range(morsel.num_rows):
            candidate = _WorstFirst(row_index)
            if len(heap) < k:
                heapq.heappush(heap, candidate)
                continue
            if compare_rows(row_index, heap[0].index) < 0:
                heapq.heapreplace(heap, candidate)

        top_indices = [item.index for item in heap]
        top_indices.sort(key=cmp_to_key(compare_rows))
        return self._materialize_rows(morsel, top_indices)

    def _uniform_direction(self) -> bool | None:
        directions = [direction.upper().startswith("DESC") for _, direction in self.mapped_order]
        if all(directions):
            return True
        if not any(directions):
            return False
        return None

    def _top_n_single_key(self, morsel: Morsel, k: int) -> Morsel:
        column_name, direction = self.mapped_order[0]
        descending = direction.upper().startswith("DESC")
        vector = morsel.column(column_name.encode())

        fast_path = self._top_n_single_key_compressed(morsel, vector, descending, k)
        if fast_path is not None:
            return fast_path

        values = vector.to_pylist()
        non_null_indices = [i for i, value in enumerate(values) if value is not None]
        null_indices = [i for i, value in enumerate(values) if value is None]
        take_count = min(k, len(non_null_indices))

        if descending:
            top_indices = heapq.nlargest(take_count, non_null_indices, key=values.__getitem__)
        else:
            top_indices = heapq.nsmallest(take_count, non_null_indices, key=values.__getitem__)

        if len(top_indices) < k and null_indices:
            top_indices.extend(null_indices[: k - len(top_indices)])

        return self._materialize_rows(morsel, top_indices)

    def _top_n_multi_key_uniform(self, morsel: Morsel, k: int, descending: bool) -> Morsel:
        key_values = [morsel.column(column.encode()).to_pylist() for column, _ in self.mapped_order]
        candidate_indices = self._candidate_indices_from_first_key(morsel, k, descending)
        search_space = (
            candidate_indices if candidate_indices is not None else range(morsel.num_rows)
        )

        if descending:

            def row_key(index: int):
                return tuple((values[index] is not None, values[index]) for values in key_values)

            top_indices = heapq.nlargest(k, search_space, key=row_key)
        else:

            def row_key(index: int):
                return tuple((values[index] is None, values[index]) for values in key_values)

            top_indices = heapq.nsmallest(k, search_space, key=row_key)

        return self._materialize_rows(morsel, top_indices)

    def _candidate_indices_from_first_key(
        self, morsel: Morsel, k: int, descending: bool
    ) -> list[int] | None:
        first_column = self.mapped_order[0][0]
        first_vector = morsel.column(first_column.encode())
        if not self._is_exact_compressible_vector(first_vector):
            return None

        try:
            compressed = numpy.asarray(first_vector.compress(), dtype=numpy.int64)
        except Exception:
            return None

        valid_mask = compressed != self._NULL_COMPRESSED
        valid_indices = numpy.nonzero(valid_mask)[0]
        valid_count = valid_indices.size
        if valid_count < k:
            return None

        valid_values = compressed[valid_indices]
        if descending:
            partition = numpy.argpartition(valid_values, valid_count - k)[-k:]
            threshold = valid_values[partition].min()
            return valid_indices[valid_values >= threshold].tolist()

        partition = numpy.argpartition(valid_values, k - 1)[:k]
        threshold = valid_values[partition].max()
        return valid_indices[valid_values <= threshold].tolist()

    def _top_n_single_key_compressed(
        self, morsel: Morsel, vector, descending: bool, k: int
    ) -> Morsel | None:
        if not self._is_exact_compressible_vector(vector):
            return None

        try:
            compressed = numpy.asarray(vector.compress(), dtype=numpy.int64)
        except Exception:
            return None

        if compressed.ndim != 1 or compressed.size != morsel.num_rows:
            return None

        valid_mask = compressed != self._NULL_COMPRESSED
        valid_indices = numpy.nonzero(valid_mask)[0]
        valid_count = valid_indices.size
        take_count = min(k, valid_count)
        selected: list[int] = []

        if take_count > 0:
            valid_values = compressed[valid_indices]
            if take_count < valid_count:
                if descending:
                    partition = numpy.argpartition(valid_values, valid_count - take_count)[
                        -take_count:
                    ]
                else:
                    partition = numpy.argpartition(valid_values, take_count - 1)[:take_count]
                chosen = valid_indices[partition]
            else:
                chosen = valid_indices

            sort_order = numpy.argsort(compressed[chosen], kind="mergesort")
            if descending:
                sort_order = sort_order[::-1]
            selected = chosen[sort_order].tolist()

        if len(selected) < k:
            null_indices = numpy.nonzero(~valid_mask)[0]
            needed = k - len(selected)
            if needed > 0 and null_indices.size:
                selected.extend(null_indices[:needed].tolist())

        return self._materialize_rows(morsel, selected)
