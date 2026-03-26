# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from __future__ import annotations

from dataclasses import dataclass
from functools import cmp_to_key
from heapq import heappop
from heapq import heappush
from typing import Iterable
from typing import Iterator
from typing import Sequence

from opteryx.compiled.draken.interop.arrow import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel


def _normalize_column_name(column: str | bytes) -> bytes:
    if isinstance(column, bytes):
        return column
    return str(column).encode("utf-8")


@dataclass(frozen=True)
class SortKey:
    column: str | bytes
    direction: str = "ASC"


class ShuffleMergeOperation:
    """
    Plain merge operation for post-shuffle streams.

    This does not reorder rows; it concatenates streams in provided order.
    """

    @staticmethod
    def merge_streams(streams: Sequence[Iterable[Morsel] | Morsel]) -> Iterable[Morsel]:
        for stream in streams:
            if isinstance(stream, Morsel):
                if stream.num_rows > 0:
                    yield stream
                continue
            for morsel in stream:
                if morsel is None or morsel.num_rows == 0:
                    continue
                yield morsel


@dataclass
class _StreamCursor:
    source_id: int
    morsels: Iterator[Morsel]
    order_columns: list[bytes]

    schema_columns: list[bytes] | None = None
    row_index: int = 0
    row_count: int = 0
    global_row_ordinal: int = 0
    order_values: list[list] | None = None
    column_values: list[list] | None = None

    def _normalize_schema(self, morsel: Morsel) -> list[bytes]:
        return [
            name if isinstance(name, bytes) else str(name).encode("utf-8")
            for name in morsel.column_names
        ]

    def _load_next_non_empty_morsel(self, expected_schema: list[bytes] | None) -> bool:
        for morsel in self.morsels:
            if morsel is None or morsel.num_rows == 0:
                continue

            schema = self._normalize_schema(morsel)
            if expected_schema is not None and schema != expected_schema:
                raise ValueError("all input streams must share the same schema")

            self.schema_columns = schema
            self.row_index = 0
            self.row_count = morsel.num_rows
            self.order_values = [morsel.column(col).to_pylist() for col in self.order_columns]
            self.column_values = [morsel.column(col).to_pylist() for col in schema]
            return True

        self.row_index = 0
        self.row_count = 0
        self.order_values = None
        self.column_values = None
        return False

    def initialize(self, expected_schema: list[bytes] | None) -> bool:
        return self._load_next_non_empty_morsel(expected_schema)

    def current_order_value(self, sort_index: int):
        return self.order_values[sort_index][self.row_index]

    def append_current_row_to(self, buffers: list[list]) -> None:
        for col_index, values in enumerate(self.column_values):
            buffers[col_index].append(values[self.row_index])

    def advance(self, expected_schema: list[bytes]) -> bool:
        self.global_row_ordinal += 1
        self.row_index += 1
        if self.row_index < self.row_count:
            return True
        return self._load_next_non_empty_morsel(expected_schema)


@dataclass
class _HeapItem:
    cursor: _StreamCursor
    sorter: "ShuffleMergeSortOperation"

    def __lt__(self, other: "_HeapItem") -> bool:
        return self.sorter._compare_cursors(self.cursor, other.cursor) < 0


class ShuffleMergeSortOperation:
    """
    K-way merge for pre-sorted morsel streams.

    Each input stream must already be sorted by the same sort keys.
    """

    def __init__(self, order_by: list[SortKey | tuple[str | bytes, str]]):
        if not order_by:
            raise ValueError("order_by is required for merge sort")

        normalized: list[SortKey] = []
        for entry in order_by:
            if isinstance(entry, SortKey):
                normalized.append(
                    SortKey(column=_normalize_column_name(entry.column), direction=entry.direction)
                )
            else:
                column, direction = entry
                normalized.append(
                    SortKey(
                        column=_normalize_column_name(column), direction=str(direction or "ASC")
                    )
                )

        self.order_by = normalized
        self._order_columns = [key.column for key in self.order_by]
        self._descending = [key.direction.upper().startswith("DESC") for key in self.order_by]

    def _iter_morsels(self, stream: Iterable[Morsel] | Morsel) -> Iterator[Morsel]:
        if isinstance(stream, Morsel):
            yield stream
            return
        for morsel in stream:
            yield morsel

    def _compare_cursors(self, left: _StreamCursor, right: _StreamCursor) -> int:
        for index, _key in enumerate(self.order_by):
            left_value = left.current_order_value(index)
            right_value = right.current_order_value(index)

            left_is_null = left_value is None
            right_is_null = right_value is None
            if left_is_null and right_is_null:
                continue
            if left_is_null:
                return 1
            if right_is_null:
                return -1
            if left_value == right_value:
                continue

            descending = self._descending[index]
            if descending:
                return -1 if left_value > right_value else 1
            return -1 if left_value < right_value else 1

        if left.source_id != right.source_id:
            return -1 if left.source_id < right.source_id else 1
        if left.global_row_ordinal == right.global_row_ordinal:
            return 0
        return -1 if left.global_row_ordinal < right.global_row_ordinal else 1

    def _build_morsel(self, schema_columns: list[bytes], buffers: list[list]) -> Morsel:
        names = [name.decode("utf-8") for name in schema_columns]
        vectors = [vector_from_sequence(values) for values in buffers]
        return Morsel.from_vectors(names, vectors)

    def merge_sorted_streams_iter(
        self,
        streams: Sequence[Iterable[Morsel] | Morsel],
        *,
        limit: int | None = None,
        batch_size: int = 65536,
    ) -> Iterator[Morsel]:
        if limit is not None and limit < 0:
            raise ValueError("limit must be zero or positive")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if limit == 0:
            return

        expected_schema: list[bytes] | None = None
        cursors: list[_StreamCursor] = []
        for source_id, stream in enumerate(streams):
            cursor = _StreamCursor(
                source_id=source_id,
                morsels=self._iter_morsels(stream),
                order_columns=self._order_columns,
            )
            if cursor.initialize(expected_schema):
                if expected_schema is None:
                    expected_schema = cursor.schema_columns
                cursors.append(cursor)

        if not cursors:
            return

        heap: list[_HeapItem] = []
        for cursor in cursors:
            heappush(heap, _HeapItem(cursor=cursor, sorter=self))

        emitted = 0
        output_buffers = [[] for _ in expected_schema]
        while heap and (limit is None or emitted < limit):
            cursor = heappop(heap).cursor
            cursor.append_current_row_to(output_buffers)
            emitted += 1

            if emitted % batch_size == 0:
                yield self._build_morsel(expected_schema, output_buffers)
                output_buffers = [[] for _ in expected_schema]

            if cursor.advance(expected_schema):
                heappush(heap, _HeapItem(cursor=cursor, sorter=self))

        if output_buffers and output_buffers[0]:
            yield self._build_morsel(expected_schema, output_buffers)

    def merge_sorted_streams(
        self,
        streams: Sequence[Iterable[Morsel] | Morsel],
        *,
        limit: int | None = None,
        batch_size: int = 65536,
    ) -> Morsel | None:
        chunks = list(
            self.merge_sorted_streams_iter(
                streams,
                limit=limit,
                batch_size=batch_size,
            )
        )
        if not chunks:
            return None
        if len(chunks) == 1:
            return chunks[0]

        schema_columns = [
            name if isinstance(name, bytes) else str(name).encode("utf-8")
            for name in chunks[0].column_names
        ]
        combined = [[] for _ in schema_columns]
        for chunk in chunks:
            current_schema = [
                name if isinstance(name, bytes) else str(name).encode("utf-8")
                for name in chunk.column_names
            ]
            if current_schema != schema_columns:
                raise ValueError("merged chunks have inconsistent schema")
            for index, name in enumerate(schema_columns):
                combined[index].extend(chunk.column(name).to_pylist())

        return self._build_morsel(schema_columns, combined)

    def sort_single_stream(
        self,
        morsels: Sequence[Morsel],
        *,
        limit: int | None = None,
    ) -> Morsel | None:
        if limit is not None and limit < 0:
            raise ValueError("limit must be zero or positive")
        if limit == 0:
            return None

        filtered = [m for m in morsels if m is not None and m.num_rows > 0]
        if not filtered:
            return None

        schema_columns = [
            name if isinstance(name, bytes) else str(name).encode("utf-8")
            for name in filtered[0].column_names
        ]
        values_by_column = {name: [] for name in schema_columns}
        for morsel in filtered:
            current_schema = [
                name if isinstance(name, bytes) else str(name).encode("utf-8")
                for name in morsel.column_names
            ]
            if current_schema != schema_columns:
                raise ValueError("all input morsels must share the same schema")
            for name in schema_columns:
                values_by_column[name].extend(morsel.column(name).to_pylist())

        row_count = len(values_by_column[schema_columns[0]])
        if row_count == 0:
            return None

        sort_columns = [values_by_column[col] for col in self._order_columns]
        row_indexes = list(range(row_count))

        def _cmp(left_idx: int, right_idx: int) -> int:
            for index, _key in enumerate(self.order_by):
                left_value = sort_columns[index][left_idx]
                right_value = sort_columns[index][right_idx]

                left_is_null = left_value is None
                right_is_null = right_value is None
                if left_is_null and right_is_null:
                    continue
                if left_is_null:
                    return 1
                if right_is_null:
                    return -1
                if left_value == right_value:
                    continue

                descending = self._descending[index]
                if descending:
                    return -1 if left_value > right_value else 1
                return -1 if left_value < right_value else 1
            if left_idx == right_idx:
                return 0
            return -1 if left_idx < right_idx else 1

        row_indexes.sort(key=cmp_to_key(_cmp))
        if limit is not None:
            row_indexes = row_indexes[:limit]

        buffers = [[] for _ in schema_columns]
        for row_idx in row_indexes:
            for col_idx, name in enumerate(schema_columns):
                buffers[col_idx].append(values_by_column[name][row_idx])

        return self._build_morsel(schema_columns, buffers)
