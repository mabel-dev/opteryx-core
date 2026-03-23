# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from opteryx.compiled.aggregations.scalar_kernels import ArrayAggState
from opteryx.draken.interop.arrow import vector_from_sequence
from opteryx.draken.morsels.morsel import Morsel

_UNSET = object()

_SUPPORTED_FUNCTIONS = frozenset(
    {
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
        "any_value",
    }
)


@dataclass(frozen=True)
class AggregationSpec:
    alias: str
    function: str
    column: str | bytes | None = None
    options: Any | None = None


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


class ShuffleGroupByOperation:
    """
    Standalone grouped-aggregation operation intended for post-shuffle morsels.

    This object is not wired into the execution engine yet.
    """

    def __init__(self, group_by_columns: list[str | bytes], aggregations: list[AggregationSpec]):
        if not aggregations:
            raise ValueError("at least one aggregation is required")
        self.group_by_columns = [_normalize_column_name(column) for column in group_by_columns]
        self.aggregations = [self._normalize_aggregation(spec) for spec in aggregations]
        self._states: dict[tuple, list[Any]] = {}
        self._rows_seen = 0
        self.timings_ns = {"group": 0, "agg": 0, "finalize": 0}

    @staticmethod
    def _normalize_aggregation(spec: AggregationSpec) -> AggregationSpec:
        if not isinstance(spec, AggregationSpec):
            raise TypeError("aggregations must be AggregationSpec instances")
        function = spec.function.lower()
        if function not in _SUPPORTED_FUNCTIONS:
            raise ValueError(f"unsupported aggregation function '{spec.function}'")

        column = spec.column
        if column in ("*", b"*"):
            column = None
        elif column is not None:
            column = _normalize_column_name(column)

        alias = spec.alias or _default_alias(column, function)
        return AggregationSpec(
            alias=str(alias),
            function=function,
            column=column,
            options=getattr(spec, "options", None),
        )

    @classmethod
    def from_legacy_aggregate_functions(
        cls,
        group_by_columns: list[str | bytes],
        aggregate_functions: list[tuple[str, str, object]],
        aliases: list[str] | None = None,
    ) -> "ShuffleGroupByOperation":
        specs = []
        for index, (field_name, function_name, _count_options) in enumerate(aggregate_functions):
            alias = aliases[index] if aliases and index < len(aliases) else None
            specs.append(
                AggregationSpec(
                    alias=alias or _default_alias(field_name, function_name),
                    function=function_name,
                    column=field_name,
                )
            )
        return cls(group_by_columns=group_by_columns, aggregations=specs)

    def _new_state(self, function: str, options: Any = None):
        if function == "count":
            return 0
        if function in ("sum", "min", "max"):
            return None
        if function in ("mean", "avg"):
            return [0, 0]
        if function in ("count_distinct", "distinct"):
            return set()
        if function == "approx_count_distinct":
            from opteryx.compiled.aggregations.scalar_kernels import ApproximateCountState

            return ApproximateCountState()
        if function == "approx_percentile":
            from opteryx.compiled.aggregations.scalar_kernels import ApproximatePercentileState

            return ApproximatePercentileState(0.5 if options is None else float(options))
        if function == "array_agg":
            return ArrayAggState(options)
        if function == "any_value":
            return _UNSET
        raise ValueError(f"unsupported aggregation function '{function}'")

    def _update_state(self, function: str, state: Any, value: Any):
        if function == "count":
            if value is _UNSET:
                return state + 1
            return state + 1 if value is not None else state
        if function == "sum":
            if value is None:
                return state
            return value if state is None else state + value
        if function == "min":
            if value is None:
                return state
            if state is None:
                return value
            return value if value < state else state
        if function == "max":
            if value is None:
                return state
            if state is None:
                return value
            return value if value > state else state
        if function in ("mean", "avg"):
            if value is None:
                return state
            state[0] += value
            state[1] += 1
            return state
        if function in ("count_distinct", "distinct"):
            if value is None:
                return state
            state.add(value)
            return state
        if function == "approx_count_distinct":
            if value is not None:
                state.add_value(value)
            return state
        if function == "approx_percentile":
            if value is not None:
                state.add_value(value)
            return state
        if function == "array_agg":
            state.add_value(value)
            return state
        if function == "any_value":
            if state is _UNSET and value is not None:
                return value
            return state
        raise ValueError(f"unsupported aggregation function '{function}'")

    def _finalize_state(self, function: str, state: Any):
        if function == "count":
            return state
        if function in ("sum", "min", "max"):
            return state
        if function in ("mean", "avg"):
            return None if state[1] == 0 else state[0] / state[1]
        if function in ("count_distinct", "distinct"):
            return len(state)
        if function == "approx_count_distinct":
            return state.estimate()
        if function == "approx_percentile":
            return state.quantile()
        if function == "array_agg":
            return state.finalize()
        if function == "any_value":
            return None if state is _UNSET else state
        raise ValueError(f"unsupported aggregation function '{function}'")

    def ingest(self, morsel: Morsel) -> None:
        if morsel is None or morsel.num_rows == 0:
            return

        self._rows_seen += int(morsel.num_rows)

        key_values = [morsel.column(column).to_pylist() for column in self.group_by_columns]
        value_columns: dict[bytes, list] = {}
        for aggregation in self.aggregations:
            if aggregation.column is None:
                continue
            if aggregation.column not in value_columns:
                value_columns[aggregation.column] = morsel.column(aggregation.column).to_pylist()

        row_count = morsel.num_rows
        for row_idx in range(row_count):
            group_start = time.perf_counter_ns()
            key = tuple(column[row_idx] for column in key_values) if key_values else ()

            states = self._states.get(key)
            if states is None:
                states = [
                    self._new_state(aggregation.function, aggregation.options)
                    for aggregation in self.aggregations
                ]
                self._states[key] = states
            self.timings_ns["group"] += time.perf_counter_ns() - group_start

            agg_start = time.perf_counter_ns()
            for agg_idx, aggregation in enumerate(self.aggregations):
                if aggregation.column is None:
                    value = _UNSET
                else:
                    value = value_columns[aggregation.column][row_idx]
                states[agg_idx] = self._update_state(aggregation.function, states[agg_idx], value)
            self.timings_ns["agg"] += time.perf_counter_ns() - agg_start

    def ingest_many(self, morsels) -> None:
        for morsel in morsels:
            self.ingest(morsel)

    def finalize(self) -> Morsel:
        finalize_start = time.perf_counter_ns()
        if not self._states and self.group_by_columns:
            names = [aggregation.alias for aggregation in self.aggregations] + [
                column.decode("utf-8") for column in self.group_by_columns
            ]
            vectors = [vector_from_sequence([]) for _ in names]
            result = Morsel.from_vectors(names, vectors)
            self.timings_ns["finalize"] += time.perf_counter_ns() - finalize_start
            return result

        if not self._states:
            empty_key = ()
            self._states[empty_key] = [
                self._new_state(aggregation.function) for aggregation in self.aggregations
            ]

        output_values = {aggregation.alias: [] for aggregation in self.aggregations}
        key_outputs = {column: [] for column in self.group_by_columns}

        for key, states in self._states.items():
            for agg_idx, aggregation in enumerate(self.aggregations):
                output_values[aggregation.alias].append(
                    self._finalize_state(aggregation.function, states[agg_idx])
                )
            for col_idx, column in enumerate(self.group_by_columns):
                key_outputs[column].append(key[col_idx])

        names = [aggregation.alias for aggregation in self.aggregations] + [
            column.decode("utf-8") for column in self.group_by_columns
        ]
        vectors = [
            vector_from_sequence(output_values[aggregation.alias])
            for aggregation in self.aggregations
        ]
        vectors.extend(
            vector_from_sequence(key_outputs[column]) for column in self.group_by_columns
        )
        result = Morsel.from_vectors(names, vectors)
        self.timings_ns["finalize"] += time.perf_counter_ns() - finalize_start
        return result

    def timings_seconds(self) -> dict[str, float]:
        return {name: nanos / 1e9 for name, nanos in self.timings_ns.items()}


def group_by_post_shuffle(
    morsels,
    group_by_columns: list[str | bytes],
    aggregations: list[AggregationSpec],
) -> Morsel:
    operation = ShuffleGroupByOperation(
        group_by_columns=group_by_columns, aggregations=aggregations
    )
    operation.ingest_many(morsels)
    return operation.finalize()
