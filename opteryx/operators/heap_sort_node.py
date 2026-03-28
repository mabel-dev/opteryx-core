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
from opteryx.compiled.draken.interop.arrow import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.exceptions import ColumnNotFoundError
from opteryx.expression import NodeType
from opteryx.expression import evaluate_and_append
from opteryx.models import QueryProperties
from opteryx.vectors.vector_types import get_vector_source_identifier
from opteryx.vectors.vector_types import node_is_numeric_vector
from opteryx.vectors.vector_types import node_is_vector_query_expression

from opteryx import EOS

from . import BasePlanNode

_DATA_FORMAT = "arrow,draken"


class HeapSortNode(BasePlanNode):
    _NULL_COMPRESSED = numpy.iinfo(numpy.int64).min
    _USEARCH_ENABLED = False
    _USEARCH_MIN_ROWS = 2048
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

        to_arrow = getattr(vector, "to_arrow", None)
        if to_arrow is None:
            return False

        try:
            arrow_arr = to_arrow()
        except Exception:
            return False

        if not isinstance(arrow_arr, (pyarrow.Array, pyarrow.ChunkedArray)):
            return False
        if not pyarrow.types.is_dictionary(arrow_arr.type):
            return False

        value_type = arrow_arr.type.value_type
        return pyarrow.types.is_integer(value_type) or pyarrow.types.is_boolean(value_type)

    def __init__(self, properties: QueryProperties, **parameters):
        super().__init__(properties=properties, **parameters)
        self.order_by = parameters.get("order_by", [])
        self.limit = parameters.get("limit", -1)
        self.vector_topk_candidate = parameters.get("vector_topk_candidate", False)

        self.mapped_order = []
        for column, direction in self.order_by:
            try:
                self.mapped_order.append((column.schema_column.identity, direction))
            except ColumnNotFoundError as cnfe:
                raise ColumnNotFoundError(
                    f"`ORDER BY` must reference columns from `SELECT`. {cnfe}"
                ) from cnfe
        self.table = None
        self._chunk_buffer = []

    @property
    def config(self):  # pragma: no cover
        order = ", ".join(
            f"{col.schema_column.name} {dir[:3].upper()}" for col, dir in self.order_by
        )
        return f"LIMIT = {self.limit}, ORDER = {order}"

    @property
    def name(self):  # pragma: no cover
        return "Heap Sort"

    @staticmethod
    def _is_descending(direction) -> bool:
        if isinstance(direction, bool):
            return not direction
        return str(direction).upper().startswith("DESC")

    @staticmethod
    def _coerce_numeric_vector(value) -> numpy.ndarray | None:
        try:
            vector = numpy.asarray(value, dtype=numpy.float32)
        except (TypeError, ValueError):
            return None
        if vector.ndim != 1:
            return None
        return vector

    @staticmethod
    def _is_nearest_neighbor_order(function_name: str, direction) -> bool:
        descending = HeapSortNode._is_descending(direction)
        return (function_name == "COSINE_DISTANCE" and not descending) or (
            function_name == "COSINE_SIMILARITY" and descending
        )

    def execute(self, morsel: pyarrow.Table, **kwargs):
        morsel = self.ensure_draken_morsel(morsel)

        _ = kwargs  # kwargs are part of the execution contract
        if morsel is EOS:
            if self.table is None and not self._chunk_buffer:
                yield EOS
                return

            if self.limit and self.limit > 0 and self.mapped_order:
                if self._chunk_buffer:
                    combined = pyarrow.concat_tables(
                        [chunk.to_arrow() for chunk in self._chunk_buffer],
                        promote_options="permissive",
                    )
                    combined = combined.combine_chunks()
                    self.table = Morsel.from_arrow(combined)
                    self.table = self._top_n(self.table)
                elif self.table is not None:
                    self.table = self._top_n(self.table)
            elif (self.limit is None or self.limit <= 0) and self.mapped_order:
                self.table = self._sort_morsel(self.table)

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
                if chunk.num_rows > 0:
                    self._chunk_buffer.append(chunk)
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
            reverse = self._is_descending(direction)

            non_null = []
            nulls = []
            for i in indices:
                (nulls if values[i] is None else non_null).append(i)
            non_null.sort(key=lambda i: values[i], reverse=reverse)
            indices = non_null + nulls
        return indices

    def _materialize_rows(self, morsel: Morsel, row_indices: list[int]) -> Morsel:
        if not row_indices:
            return morsel.empty()

        names = morsel.column_names
        py_materialize_types = {"StringVector", "ArrayVector", "VectorVector"}
        selection = numpy.asarray(row_indices, dtype=numpy.int32)
        vectors = []
        for name in names:
            vector = morsel.column(name)
            use_python_materialization = (
                vector.__class__.__name__ in py_materialize_types
                or not hasattr(vector, "take")
                or getattr(vector, "dictionary_size", 0) > 0
            )
            if use_python_materialization:
                values = vector.to_pylist()
                vectors.append(
                    vector_from_sequence([values[row_index] for row_index in row_indices])
                )
            else:
                vectors.append(vector.take(selection))
        return Morsel.from_vectors(names, vectors)

    def _sort_morsel(self, morsel: Morsel) -> Morsel:
        return self._materialize_rows(morsel, self._sorted_indices(morsel))

    def _ensure_order_expressions_evaluated(self, morsel: Morsel) -> Morsel:
        existing_columns = {
            name.decode("utf-8") if isinstance(name, bytes) else name
            for name in morsel.column_names
        }
        evaluations = []
        for column, _ in self.order_by:
            if column.node_type == NodeType.IDENTIFIER:
                continue
            identity = getattr(column.schema_column, "identity", None)
            if identity in existing_columns:
                continue
            evaluations.append(column)

        if not evaluations:
            return morsel

        return evaluate_and_append(evaluations, morsel)

    def _top_n(self, morsel: Morsel) -> Morsel:
        if self.limit is None or self.limit <= 0:
            return morsel

        vector_ranked = self._vector_top_n(morsel)
        if vector_ranked is not None:
            return vector_ranked

        morsel = self._ensure_order_expressions_evaluated(morsel)

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
        directions = [self._is_descending(direction) for _, direction in self.mapped_order]

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
        directions = [self._is_descending(direction) for _, direction in self.mapped_order]
        if all(directions):
            return True
        if not any(directions):
            return False
        return None

    def _top_n_single_key(self, morsel: Morsel, k: int) -> Morsel:
        column_name, direction = self.mapped_order[0]
        descending = self._is_descending(direction)
        vector = morsel.column(column_name.encode())

        fast_path = self._top_n_single_key_compressed(morsel, vector, descending, k)
        if fast_path is not None:
            return fast_path

        values = vector.to_pylist()
        non_null_indices = []
        null_indices = []
        for i, value in enumerate(values):
            (null_indices if value is None else non_null_indices).append(i)
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

    def _vector_top_n(self, morsel: Morsel) -> Morsel | None:
        if self.limit is None or self.limit <= 0 or len(self.order_by) != 1:
            return None

        order_expression, direction = self.order_by[0]
        if order_expression.node_type != NodeType.FUNCTION:
            return None
        if order_expression.value not in ("COSINE_SIMILARITY", "COSINE_DISTANCE"):
            return None
        if len(order_expression.parameters) != 2:
            return None

        source_node, query_node = order_expression.parameters
        source_identifier = get_vector_source_identifier(source_node)
        if source_identifier is None:
            return None
        if not node_is_numeric_vector(source_node) or not node_is_vector_query_expression(
            query_node
        ):
            return None

        query_vector = self._resolve_query_vector(query_node)
        if query_vector is None or query_vector.size == 0:
            return None

        source_keys = [
            getattr(source_identifier.schema_column, "identity", None),
            getattr(source_identifier, "source_column", None),
            getattr(source_identifier.schema_column, "name", None),
        ]
        source_values = None
        for source_key in source_keys:
            if not source_key:
                continue
            try:
                source_values = morsel.column(source_key.encode()).to_pylist()
                break
            except Exception:
                continue
        if source_values is None:
            return None

        dense_rows: list[numpy.ndarray] = []
        source_indices: list[int] = []
        for row_index, value in enumerate(source_values):
            row_vector = self._coerce_numeric_vector(value)
            if row_vector is None:
                continue
            if row_vector.shape[0] != query_vector.shape[0]:
                continue
            dense_rows.append(row_vector)
            source_indices.append(row_index)

        if not dense_rows:
            return None

        dense_vectors = numpy.vstack(dense_rows).astype(numpy.float32, copy=False)
        self.readings["vector_topk_candidate_rows"] += dense_vectors.shape[0]
        take_count = min(self.limit, dense_vectors.shape[0])
        if take_count == 0:
            return morsel.empty()

        query_vector = numpy.ascontiguousarray(query_vector, dtype=numpy.float32)
        dense_vectors = numpy.ascontiguousarray(dense_vectors, dtype=numpy.float32)
        row_ids = numpy.asarray(source_indices, dtype=numpy.int64)
        nearest_neighbor_order = self._is_nearest_neighbor_order(order_expression.value, direction)

        if (
            self._USEARCH_ENABLED
            and self.vector_topk_candidate
            and dense_vectors.shape[0] >= self._USEARCH_MIN_ROWS
            and nearest_neighbor_order
        ):
            try:
                from opteryx.compiled.nanobind import usearch_native

                index = usearch_native.UsearchIndex(
                    dimensions=query_vector.shape[0],
                    capacity=dense_vectors.shape[0],
                    metric="cos",
                    expansion_add=16,
                    expansion_search=16,
                )
                self.readings["feature_vector_topk_usearch"] += 1
                self.readings["vector_topk_usearch_rows_indexed"] += dense_vectors.shape[0]
                index.add_batch(
                    row_ids,
                    dense_vectors,
                )
                found_ids, _ = index.search(
                    query_vector,
                    take_count,
                )
                if found_ids:
                    return self._materialize_rows(morsel, [int(row_id) for row_id in found_ids])
            except Exception:
                self.readings["feature_vector_topk_usearch_fallbacks"] += 1
                pass

        try:
            from opteryx.compiled.nanobind import vector_search

            if nearest_neighbor_order:
                found_ids, _ = vector_search.exact_search_cosine(
                    query_vector,
                    row_ids,
                    dense_vectors,
                    take_count,
                )
                self.readings["feature_vector_topk_exact"] += 1
                if found_ids:
                    return self._materialize_rows(morsel, [int(row_id) for row_id in found_ids])
                return morsel.empty()

            scores = numpy.asarray(
                vector_search.score_cosine(query_vector, dense_vectors), dtype=numpy.float32
            )
        except Exception:
            return None

        self.readings["feature_vector_topk_exact"] += 1
        scores = numpy.nan_to_num(scores, nan=0.0, posinf=0.0, neginf=0.0)
        if order_expression.value == "COSINE_DISTANCE":
            scores = 1.0 - numpy.clip(scores, -1.0, 1.0)

        descending = self._is_descending(direction)
        if take_count < scores.shape[0]:
            if descending:
                candidate_indices = numpy.argpartition(-scores, take_count - 1)[:take_count]
            else:
                candidate_indices = numpy.argpartition(scores, take_count - 1)[:take_count]
        else:
            candidate_indices = numpy.arange(scores.shape[0], dtype=numpy.int64)

        if descending:
            order = numpy.lexsort((row_ids[candidate_indices], -scores[candidate_indices]))
        else:
            order = numpy.lexsort((row_ids[candidate_indices], scores[candidate_indices]))
        ranked_dense_indices = candidate_indices[order]

        top_indices = [
            int(source_indices[int(index)]) for index in ranked_dense_indices[:take_count]
        ]
        return self._materialize_rows(morsel, top_indices)

    def _resolve_query_vector(self, query_node) -> numpy.ndarray | None:
        if query_node.node_type == NodeType.LITERAL:
            return self._coerce_numeric_vector(query_node.value)
        if (
            query_node.node_type == NodeType.FUNCTION
            and query_node.value == "EMBED"
            and len(query_node.parameters) == 1
            and query_node.parameters[0].node_type == NodeType.LITERAL
        ):
            from opteryx.vectors.embeddings import embed_text_matrix

            embedded = embed_text_matrix([query_node.parameters[0].value])
            if embedded.size == 0:
                return None
            return self._coerce_numeric_vector(embedded[0])
        return None
