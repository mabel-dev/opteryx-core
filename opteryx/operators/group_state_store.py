# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from __future__ import annotations

import time
from opteryx.draken.interop.arrow import vector_from_sequence
from opteryx.draken.morsels.morsel import Morsel

_SUPPORTED_FUNCTIONS = {
    "count",
    "sum",
    "min",
    "max",
    "mean",
    "avg",
    "count_distinct",
    "distinct",
    "hash_one",
}
_FAST_OUTPUT_FUNCTIONS = {"count", "count_distinct", "mean", "avg"}


def _normalize_column_name(column: str | bytes) -> bytes:
    if isinstance(column, bytes):
        return column
    return str(column).encode("utf-8")


def _default_alias(column: str | bytes | None, function: str) -> str:
    fn = function.lower()
    if column is None:
        return fn
    column_name = column.decode("utf-8") if isinstance(column, bytes) else str(column)
    return f"{column_name}_{fn}"


def _create_backend(group_by_columns, aggregations):
    from opteryx.compiled.aggregations.group_state_store import (
        GroupStateStore as CompiledGroupStateStore,
    )

    return CompiledGroupStateStore(group_by_columns, aggregations)


class ShuffleGroupByOperationV2:
    """
    Group-by operation backed by the compiled GroupStateStore.
    """

    def __init__(
        self,
        group_by_columns: list[str | bytes],
        aggregations: list[object],
    ):
        if not aggregations:
            raise ValueError("at least one aggregation is required")
        self.group_by_columns = [_normalize_column_name(column) for column in group_by_columns]
        self.aggregations = [self._normalize_aggregation(spec) for spec in aggregations]
        self._backend = _create_backend(self.group_by_columns, self.aggregations)
        self.readings = {
            "time_groupby_finalize_backend_ns": 0,
            "time_groupby_finalize_rows_to_vectors_ns": 0,
            "time_groupby_finalize_morsel_build_ns": 0,
            "groupby_finalize_rows_count": 0,
            "groupby_finalize_chunks_emitted": 0,
            "groupby_finalize_fast_path_hits": 0,
            "draken_dict_groupby_fastpath_hits": 0,
            "draken_dict_groupby_fastpath_fallbacks": 0,
        }

    def _is_fast_path_eligible(self) -> bool:
        return (
            len(self.aggregations) == 1
            and len(self.group_by_columns) == 1
            and self.aggregations[0][1] in _FAST_OUTPUT_FUNCTIONS
            and hasattr(self._backend, "finalize_fast_columns")
        )

    @staticmethod
    def _normalize_aggregation(spec: object) -> tuple[str, str, bytes | None]:
        if not all(hasattr(spec, attr) for attr in ("alias", "function", "column")):
            raise TypeError("aggregations must expose alias/function/column")

        function = str(spec.function).lower()
        if function not in _SUPPORTED_FUNCTIONS:
            raise ValueError(f"unsupported aggregation function '{function}'")

        column = spec.column
        if column in ("*", b"*"):
            column = None
        elif column is not None:
            column = _normalize_column_name(column)

        alias = spec.alias or _default_alias(column, function)
        return str(alias), function, column

    def ingest(self, morsel: Morsel) -> None:
        pre_dict_hits = getattr(self._backend, "dict_groupby_fastpath_hits", 0)
        pre_dict_fallbacks = getattr(self._backend, "dict_groupby_fastpath_fallbacks", 0)
        self._backend.ingest(morsel)
        self.readings["draken_dict_groupby_fastpath_hits"] += (
            getattr(self._backend, "dict_groupby_fastpath_hits", 0) - pre_dict_hits
        )
        self.readings["draken_dict_groupby_fastpath_fallbacks"] += (
            getattr(self._backend, "dict_groupby_fastpath_fallbacks", 0) - pre_dict_fallbacks
        )

    def ingest_many(self, morsels) -> None:
        for morsel in morsels:
            self.ingest(morsel)

    def _output_names(self):
        return [alias for alias, _function, _column in self.aggregations] + [
            column.decode("utf-8") for column in self.group_by_columns
        ]

    def _empty_morsel(self) -> Morsel:
        names = self._output_names()
        vectors = [vector_from_sequence([]) for _ in names]
        return Morsel.from_vectors(names, vectors)

    def _rows_to_vectors(self, rows, start: int, stop: int):
        agg_count = len(self.aggregations)
        key_count = len(self.group_by_columns)
        output_values = [[] for _ in range(agg_count)]
        key_outputs = [[] for _ in range(key_count)]

        for row_idx in range(start, stop):
            key, finalized_values = rows[row_idx]
            for agg_idx in range(agg_count):
                output_values[agg_idx].append(finalized_values[agg_idx])
            for key_idx in range(key_count):
                key_outputs[key_idx].append(key[key_idx])

        vectors = [vector_from_sequence(output_values[idx]) for idx in range(agg_count)]
        vectors.extend(vector_from_sequence(key_outputs[idx]) for idx in range(key_count))
        return vectors

    def finalize(self) -> Morsel:
        if self._is_fast_path_eligible():
            backend_st = time.monotonic_ns()
            fast_columns = self._backend.finalize_fast_columns()
            self.readings["time_groupby_finalize_backend_ns"] += time.monotonic_ns() - backend_st
            if fast_columns is not None:
                keys, values = fast_columns
                names = [self.aggregations[0][0], self.group_by_columns[0].decode("utf-8")]
                build_st = time.monotonic_ns()
                vectors = [vector_from_sequence(values), vector_from_sequence(keys)]
                self.readings["time_groupby_finalize_rows_to_vectors_ns"] += (
                    time.monotonic_ns() - build_st
                )
                morsel_st = time.monotonic_ns()
                morsel = Morsel.from_vectors(names, vectors)
                self.readings["time_groupby_finalize_morsel_build_ns"] += (
                    time.monotonic_ns() - morsel_st
                )
                self.readings["groupby_finalize_fast_path_hits"] += 1
                self.readings["groupby_finalize_rows_count"] += len(keys)
                self.readings["groupby_finalize_chunks_emitted"] += 1
                return morsel

        backend_st = time.monotonic_ns()
        rows = self._backend.finalize_rows()
        self.readings["time_groupby_finalize_backend_ns"] += time.monotonic_ns() - backend_st
        self.readings["groupby_finalize_rows_count"] += len(rows)

        if not rows:
            return self._empty_morsel()

        names = self._output_names()
        vector_st = time.monotonic_ns()
        vectors = self._rows_to_vectors(rows, 0, len(rows))
        self.readings["time_groupby_finalize_rows_to_vectors_ns"] += time.monotonic_ns() - vector_st
        morsel_st = time.monotonic_ns()
        morsel = Morsel.from_vectors(names, vectors)
        self.readings["time_groupby_finalize_morsel_build_ns"] += time.monotonic_ns() - morsel_st
        self.readings["groupby_finalize_chunks_emitted"] += 1
        return morsel

    def finalize_morsels(self, chunk_size: int = 65536):
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")

        if self._is_fast_path_eligible():
            names = [self.aggregations[0][0], self.group_by_columns[0].decode("utf-8")]

            if hasattr(self._backend, "finalize_fast_columns_chunked"):
                backend_st = time.monotonic_ns()
                fast_chunks = self._backend.finalize_fast_columns_chunked(chunk_size)
                self.readings["time_groupby_finalize_backend_ns"] += (
                    time.monotonic_ns() - backend_st
                )
                if fast_chunks is not None:
                    self.readings["groupby_finalize_fast_path_hits"] += 1
                    for keys, values in fast_chunks:
                        self.readings["groupby_finalize_rows_count"] += len(keys)
                        vector_st = time.monotonic_ns()
                        vectors = [
                            vector_from_sequence(values),
                            vector_from_sequence(keys),
                        ]
                        self.readings["time_groupby_finalize_rows_to_vectors_ns"] += (
                            time.monotonic_ns() - vector_st
                        )
                        morsel_st = time.monotonic_ns()
                        morsel = Morsel.from_vectors(names, vectors)
                        self.readings["time_groupby_finalize_morsel_build_ns"] += (
                            time.monotonic_ns() - morsel_st
                        )
                        self.readings["groupby_finalize_chunks_emitted"] += 1
                        yield morsel
                    return

            backend_st = time.monotonic_ns()
            fast_columns = self._backend.finalize_fast_columns()
            self.readings["time_groupby_finalize_backend_ns"] += time.monotonic_ns() - backend_st
            if fast_columns is not None:
                keys, values = fast_columns
                self.readings["groupby_finalize_fast_path_hits"] += 1
                self.readings["groupby_finalize_rows_count"] += len(keys)
                total = len(keys)
                for start in range(0, total, chunk_size):
                    stop = min(total, start + chunk_size)
                    vector_st = time.monotonic_ns()
                    vectors = [
                        vector_from_sequence(values[start:stop]),
                        vector_from_sequence(keys[start:stop]),
                    ]
                    self.readings["time_groupby_finalize_rows_to_vectors_ns"] += (
                        time.monotonic_ns() - vector_st
                    )
                    morsel_st = time.monotonic_ns()
                    morsel = Morsel.from_vectors(names, vectors)
                    self.readings["time_groupby_finalize_morsel_build_ns"] += (
                        time.monotonic_ns() - morsel_st
                    )
                    self.readings["groupby_finalize_chunks_emitted"] += 1
                    yield morsel
                return

        backend_st = time.monotonic_ns()
        rows = self._backend.finalize_rows()
        self.readings["time_groupby_finalize_backend_ns"] += time.monotonic_ns() - backend_st
        names = self._output_names()
        self.readings["groupby_finalize_rows_count"] += len(rows)

        if not rows:
            yield self._empty_morsel()
            return

        total = len(rows)
        for start in range(0, total, chunk_size):
            stop = min(total, start + chunk_size)
            vector_st = time.monotonic_ns()
            vectors = self._rows_to_vectors(rows, start, stop)
            self.readings["time_groupby_finalize_rows_to_vectors_ns"] += (
                time.monotonic_ns() - vector_st
            )
            morsel_st = time.monotonic_ns()
            morsel = Morsel.from_vectors(names, vectors)
            self.readings["time_groupby_finalize_morsel_build_ns"] += (
                time.monotonic_ns() - morsel_st
            )
            self.readings["groupby_finalize_chunks_emitted"] += 1
            yield morsel
