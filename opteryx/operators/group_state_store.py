# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from __future__ import annotations

from opteryx.draken.interop.arrow import vector_from_sequence
from opteryx.draken.morsels.morsel import Morsel
from opteryx.draken.vectors.string_vector import StringVector
from opteryx.exceptions import UnsupportedSyntaxError

# Values match the C enum in `DrakenEncoding`.
DRAKEN_ENCODING_DENSE = 0
DRAKEN_ENCODING_DICTIONARY = 1
DRAKEN_ENCODING_RLE = 2
DRAKEN_ENCODING_CONSTANT = 3

_DATA_FORMAT = "draken"

# DictionaryVector.dictionary_value_type is exposed at Python level, but the
# Draken enum constants are not. These ids correspond to float32/float64.
_DICT_FLOAT_VALUE_TYPE_IDS = frozenset((20, 21))


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


def _is_dictionary_vector(vec: object) -> bool:
    return getattr(vec, "encoding", None) == DRAKEN_ENCODING_DICTIONARY


def _is_constant_vector(vec: object) -> bool:
    return getattr(vec, "encoding", None) == DRAKEN_ENCODING_CONSTANT


def _is_dictionary_float_vector(vec: object) -> bool:
    return (
        _is_dictionary_vector(vec)
        and getattr(vec, "dictionary_value_type", None) in _DICT_FLOAT_VALUE_TYPE_IDS
    )


def _is_unsupported_count_distinct_vector(vec: object) -> bool:
    return vec.__class__.__name__ == "Float64Vector" or _is_dictionary_float_vector(vec)


def create_group_state_engine(group_by_columns, aggregations):
    if any(
        agg[1] in ("approx_count_distinct", "approx_percentile", "array_agg")
        for agg in aggregations
    ):
        from opteryx.compiled.aggregations.group_state_store import GroupStateStore

        return GroupStateStore(group_by_columns, aggregations)

    # Route only unsupported no-column aggregations to GroupStateStore.
    # COUNT(*) is represented as column=None and is supported natively.
    # Aggregations are tuples of (alias, function, column[, options]).
    if any(agg[2] is None and agg[1] != "count" for agg in aggregations):
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
        self._backend = getattr(self._engine, "backend", self._engine)
        self.readings = self._engine.readings

    def _record_constant_groupby_vector(self, vec) -> None:
        if _is_constant_vector(vec):
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
        reroute_reason = self._group_state_store_reason_for_morsel(morsel)
        if reroute_reason is not None:
            from opteryx.compiled.aggregations.group_state_store import GroupStateStore

            self._engine = GroupStateStore(self.group_by_columns, self.aggregations)
            self._backend = self._engine
            self.readings = self._engine.readings

        try:
            self._engine.ingest(morsel)
        except UnsupportedSyntaxError:
            # Some shapes (e.g., unencoded string keys) are only detected at runtime
            # in the compiled Carchar engine. If it fails, reroute to GroupStateStore
            # and retry.
            from opteryx.compiled.aggregations.group_state_store import GroupStateStore

            self._engine = GroupStateStore(self.group_by_columns, self.aggregations)
            self._backend = self._engine
            self.readings = self._engine.readings
            self._engine.ingest(morsel)

        self.readings = self._engine.readings

    def _group_state_store_reason_for_morsel(self, morsel: Morsel) -> str | None:
        if self._engine.__class__.__name__ != "CarcharGroupStateEngine":
            return None
        if self.readings["feature_groupby_engine_carchar"] != 0:
            return None
        if morsel is None or morsel.num_rows == 0:
            return None

        for group_column in self.group_by_columns:
            group_vec = morsel.column(group_column)
            if _is_constant_vector(group_vec):
                return "constant-key"
            # Carchar only supports native encoded storage for string keys.
            # If a string key arrives as a plain StringVector (non-dictionary),
            # we must reroute to GroupStateStore to avoid runtime errors.
            if isinstance(group_vec, StringVector) and not _is_dictionary_vector(group_vec):
                return "string-key-not-encoded"
            if _is_dictionary_float_vector(group_vec):
                return "dict-float-key"

        for _, function, column, *_ in self.aggregations:
            if function in ("count_distinct", "distinct") and column is not None:
                value_vector = morsel.column(column)
                if value_vector.__class__.__name__ == "Float64Vector":
                    return "count-distinct-dense-float-value"
                if _is_dictionary_float_vector(value_vector):
                    return "count-distinct-dict-float-value"

        return None

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

    def _finalize_rows(self):
        if hasattr(self._engine, "finalize_rows"):
            return self._engine.finalize_rows()
        if hasattr(self._engine, "finalize"):
            finalized = self._engine.finalize()
            if finalized is not None:
                return finalized
        raise AttributeError("group-by engine does not expose a finalize API")

    def finalize(self) -> Morsel:
        if hasattr(self._engine, "finalize"):
            return self._engine.finalize()

        rows = self._finalize_rows()
        if not rows:
            return self._empty_morsel()

        return Morsel.from_vectors(self._output_names(), self._rows_to_vectors(rows, 0, len(rows)))

    def finalize_morsels(self, chunk_size: int = 65536):
        if hasattr(self._engine, "finalize_morsels"):
            yield from self._engine.finalize_morsels(chunk_size)
            return

        rows = self._finalize_rows()
        if not rows:
            yield self._empty_morsel()
            return

        total = len(rows)
        start = 0
        names = self._output_names()
        while start < total:
            stop = min(start + chunk_size, total)
            yield Morsel.from_vectors(names, self._rows_to_vectors(rows, start, stop))
            start = stop
