# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from __future__ import annotations

from opteryx.draken.interop.arrow import vector_from_sequence
from opteryx.draken.morsels.morsel import Morsel

_SUPPORTED_FUNCTIONS = frozenset(
    {"count", "sum", "min", "max", "mean", "avg", "count_distinct", "distinct", "hash_one"}
)


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

    def __init__(self, group_by_columns: list[str | bytes], aggregations: list[object]):
        if not aggregations:
            raise ValueError("at least one aggregation is required")
        self.group_by_columns = [_normalize_column_name(column) for column in group_by_columns]
        self.aggregations = [self._normalize_aggregation(spec) for spec in aggregations]
        self._backend = _create_backend(self.group_by_columns, self.aggregations)

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
        self._backend.ingest(morsel)

    def ingest_many(self, morsels) -> None:
        for morsel in morsels:
            self.ingest(morsel)

    def finalize(self) -> Morsel:
        if (
            len(self.aggregations) == 1
            and len(self.group_by_columns) == 1
            and self.aggregations[0][1] in ("count", "count_distinct", "mean", "avg")
            and hasattr(self._backend, "finalize_fast_columns")
        ):
            fast_columns = self._backend.finalize_fast_columns()
            if fast_columns is not None:
                keys, values = fast_columns
                names = [self.aggregations[0][0], self.group_by_columns[0].decode("utf-8")]
                vectors = [vector_from_sequence(values), vector_from_sequence(keys)]
                return Morsel.from_vectors(names, vectors)

        rows = self._backend.finalize_rows()

        if not rows and self.group_by_columns:
            names = [alias for alias, _function, _column in self.aggregations] + [
                column.decode("utf-8") for column in self.group_by_columns
            ]
            vectors = [vector_from_sequence([]) for _ in names]
            return Morsel.from_vectors(names, vectors)

        output_values = {alias: [] for alias, _function, _column in self.aggregations}
        key_outputs = {column: [] for column in self.group_by_columns}

        for key, finalized_values in rows:
            for idx, (alias, _function, _column) in enumerate(self.aggregations):
                output_values[alias].append(finalized_values[idx])
            for col_idx, column in enumerate(self.group_by_columns):
                key_outputs[column].append(key[col_idx])

        names = [alias for alias, _function, _column in self.aggregations] + [
            column.decode("utf-8") for column in self.group_by_columns
        ]
        vectors = [
            vector_from_sequence(output_values[alias])
            for alias, _function, _column in self.aggregations
        ]
        vectors.extend(
            vector_from_sequence(key_outputs[column]) for column in self.group_by_columns
        )
        return Morsel.from_vectors(names, vectors)

    def finalize_morsels(self, chunk_size: int = 65536):
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")

        if (
            len(self.aggregations) == 1
            and len(self.group_by_columns) == 1
            and self.aggregations[0][1] in ("count", "count_distinct", "mean", "avg")
            and hasattr(self._backend, "finalize_fast_columns")
        ):
            fast_columns = self._backend.finalize_fast_columns()
            if fast_columns is not None:
                keys, values = fast_columns
                names = [self.aggregations[0][0], self.group_by_columns[0].decode("utf-8")]
                total = len(keys)
                for start in range(0, total, chunk_size):
                    stop = min(total, start + chunk_size)
                    vectors = [
                        vector_from_sequence(values[start:stop]),
                        vector_from_sequence(keys[start:stop]),
                    ]
                    yield Morsel.from_vectors(names, vectors)
                return

        rows = self._backend.finalize_rows()
        names = [alias for alias, _function, _column in self.aggregations] + [
            column.decode("utf-8") for column in self.group_by_columns
        ]

        if not rows and self.group_by_columns:
            vectors = [vector_from_sequence([]) for _ in names]
            yield Morsel.from_vectors(names, vectors)
            return

        if not rows:
            vectors = [vector_from_sequence([]) for _ in names]
            yield Morsel.from_vectors(names, vectors)
            return

        total = len(rows)
        for start in range(0, total, chunk_size):
            stop = min(total, start + chunk_size)
            output_values = {alias: [] for alias, _function, _column in self.aggregations}
            key_outputs = {column: [] for column in self.group_by_columns}

            for key, finalized_values in rows[start:stop]:
                for idx, (alias, _function, _column) in enumerate(self.aggregations):
                    output_values[alias].append(finalized_values[idx])
                for col_idx, column in enumerate(self.group_by_columns):
                    key_outputs[column].append(key[col_idx])

            vectors = [
                vector_from_sequence(output_values[alias])
                for alias, _function, _column in self.aggregations
            ]
            vectors.extend(
                vector_from_sequence(key_outputs[column]) for column in self.group_by_columns
            )
            yield Morsel.from_vectors(names, vectors)
