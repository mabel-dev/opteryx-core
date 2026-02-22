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

from opteryx.draken.interop.arrow import vector_from_sequence
from opteryx.draken.morsels.morsel import Morsel


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
    def merge_streams(streams: Sequence[Iterable[Morsel] | Morsel]) -> list[Morsel]:
        merged: list[Morsel] = []
        for stream in streams:
            if isinstance(stream, Morsel):
                if stream.num_rows > 0:
                    merged.append(stream)
                continue
            for morsel in stream:
                if morsel is None or morsel.num_rows == 0:
                    continue
                merged.append(morsel)
        return merged


@dataclass
class _SourceState:
    source_id: int
    column_names: list[bytes]
    values_by_column: dict[bytes, list]
    position: int = 0

    @property
    def row_count(self) -> int:
        if not self.column_names:
            return 0
        return len(self.values_by_column[self.column_names[0]])


@dataclass
class _HeapItem:
    state: _SourceState
    sorter: "ShuffleMergeSortOperation"

    def __lt__(self, other: "_HeapItem") -> bool:
        return self.sorter._compare_rows(self.state, other.state) < 0


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
        self._descending = [key.direction.upper().startswith("DESC") for key in self.order_by]

    def _iter_morsels(self, stream: Iterable[Morsel] | Morsel) -> Iterator[Morsel]:
        if isinstance(stream, Morsel):
            yield stream
            return
        for morsel in stream:
            yield morsel

    def _stream_to_source(
        self, stream: Iterable[Morsel] | Morsel, source_id: int
    ) -> _SourceState | None:
        morsels = [m for m in self._iter_morsels(stream) if m is not None and m.num_rows > 0]
        if not morsels:
            return None

        first_names = morsels[0].column_names
        column_names = [
            name if isinstance(name, bytes) else str(name).encode("utf-8") for name in first_names
        ]
        values_by_column = {name: [] for name in column_names}

        for morsel in morsels:
            current_names = [
                name if isinstance(name, bytes) else str(name).encode("utf-8")
                for name in morsel.column_names
            ]
            if current_names != column_names:
                raise ValueError("all input streams must share the same schema")
            for name in column_names:
                values_by_column[name].extend(morsel.column(name).to_pylist())

        return _SourceState(
            source_id=source_id,
            column_names=column_names,
            values_by_column=values_by_column,
            position=0,
        )

    def _compare_rows(self, left: _SourceState, right: _SourceState) -> int:
        for index, key in enumerate(self.order_by):
            column = (
                key.column if isinstance(key.column, bytes) else str(key.column).encode("utf-8")
            )
            left_value = left.values_by_column[column][left.position]
            right_value = right.values_by_column[column][right.position]

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
        if left.position == right.position:
            return 0
        return -1 if left.position < right.position else 1

    def merge_sorted_streams(self, streams: Sequence[Iterable[Morsel] | Morsel]) -> Morsel | None:
        sources = []
        for source_id, stream in enumerate(streams):
            source = self._stream_to_source(stream, source_id=source_id)
            if source is not None:
                sources.append(source)

        if not sources:
            return None

        column_names = sources[0].column_names
        for source in sources[1:]:
            if source.column_names != column_names:
                raise ValueError("all input streams must share the same schema")

        heap: list[_HeapItem] = []
        for source in sources:
            if source.row_count > 0:
                heappush(heap, _HeapItem(state=source, sorter=self))

        merged_values = {name: [] for name in column_names}
        while heap:
            current = heappop(heap).state
            row_idx = current.position
            for name in column_names:
                merged_values[name].append(current.values_by_column[name][row_idx])

            current.position += 1
            if current.position < current.row_count:
                heappush(heap, _HeapItem(state=current, sorter=self))

        names = [name.decode("utf-8") for name in column_names]
        vectors = [vector_from_sequence(merged_values[name]) for name in column_names]
        return Morsel.from_vectors(names, vectors)

    def sort_single_stream(self, morsels: Sequence[Morsel]) -> Morsel | None:
        source = self._stream_to_source(morsels, source_id=0)
        if source is None:
            return None

        row_indexes = list(range(source.row_count))

        def _cmp(left_index: int, right_index: int) -> int:
            left_state = _SourceState(
                source_id=0,
                column_names=source.column_names,
                values_by_column=source.values_by_column,
                position=left_index,
            )
            right_state = _SourceState(
                source_id=0,
                column_names=source.column_names,
                values_by_column=source.values_by_column,
                position=right_index,
            )
            return self._compare_rows(left_state, right_state)

        row_indexes.sort(key=cmp_to_key(_cmp))
        names = [name.decode("utf-8") for name in source.column_names]
        vectors = []
        for name in source.column_names:
            ordered = [source.values_by_column[name][row_idx] for row_idx in row_indexes]
            vectors.append(vector_from_sequence(ordered))
        return Morsel.from_vectors(names, vectors)
