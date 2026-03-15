# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from __future__ import annotations

from opteryx.draken.interop.arrow import vector_from_sequence
from opteryx.draken.morsels.morsel import Morsel

_DATA_FORMAT = "draken"


_SUPPORTED_FUNCTIONS = {
    "count",
    "sum",
    "min",
    "max",
    "mean",
    "avg",
    "count_distinct",
    "approx_count_distinct",
    "approx_percentile",
    "array_agg",
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


def normalize_group_by_columns(group_by_columns: list[str | bytes]) -> list[bytes]:
    return [_normalize_column_name(column) for column in group_by_columns]


def normalize_aggregations(aggregations: list[object]) -> list[tuple]:
    return [ShuffleGroupByOperationV2._normalize_aggregation(spec) for spec in aggregations]


def create_group_state_engine(group_by_columns, aggregations):
    # Use legacy backend for multi-aggregate queries (carchar multi-agg has segfault)
    if len(aggregations) > 1:
        from opteryx.compiled.aggregations.group_state_store import GroupStateStore

        return GroupStateStore(group_by_columns, aggregations)

    if any(
        agg[1] in ("approx_count_distinct", "approx_percentile", "array_agg")
        for agg in aggregations
    ):
        from opteryx.compiled.aggregations.group_state_store import GroupStateStore

        return GroupStateStore(group_by_columns, aggregations)

    # Use legacy backend for aggregations on complex expressions
    # (carchar can't handle expressions like (event ->> 'key')::TYPE)
    # aggregations are tuples of (alias, function, column[, options])
    if any(agg[2] is None for agg in aggregations):
        from opteryx.compiled.aggregations.group_state_store import GroupStateStore

        return GroupStateStore(group_by_columns, aggregations)

    from opteryx.compiled.aggregations.carchar_group_state_engine import CarcharGroupStateEngine

    return CarcharGroupStateEngine(group_by_columns, aggregations)


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
        self.group_by_columns = normalize_group_by_columns(group_by_columns)
        self.aggregations = normalize_aggregations(aggregations)
        self._engine = create_group_state_engine(self.group_by_columns, self.aggregations)
        self._backend = self._engine.backend
        self.readings = self._engine.readings

    def _record_constant_groupby_vector(self, vec) -> None:
        if vec.__class__.__name__ == "ConstantVector":
            self.readings["draken_constant_groupby_output_vector_hits"] += 1
        else:
            self.readings["draken_constant_groupby_output_vector_fallbacks"] += 1

    def _is_fast_path_eligible(self) -> bool:
        return (
            len(self.aggregations) == 1
            and len(self.group_by_columns) == 1
            and self.aggregations[0][1] in _FAST_OUTPUT_FUNCTIONS
            and hasattr(self._backend, "finalize_fast_columns")
        )

    @staticmethod
    def _normalize_aggregation(spec: object) -> tuple:
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
        options = getattr(spec, "options", None)
        if options is None:
            return str(alias), function, column
        return str(alias), function, column, options

    def ingest(self, morsel: Morsel) -> None:
        self._engine.ingest(morsel)

    def ingest_many(self, morsels) -> None:
        for morsel in morsels:
            self.ingest(morsel)

    def _output_names(self):
        return [aggregation[0] for aggregation in self.aggregations] + [
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
        for idx in range(key_count):
            key_vec = vector_from_sequence(key_outputs[idx])
            self._record_constant_groupby_vector(key_vec)
            vectors.append(key_vec)
        return vectors

    def finalize(self) -> Morsel:
        return self._engine.finalize()

    def finalize_morsels(self, chunk_size: int = 65536):
        yield from self._engine.finalize_morsels(chunk_size)
