# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Draken-native global aggregation node.

This operator stays on Draken morsels end-to-end. It does not route through the
grouped carchar backend and it does not delegate execution to the Arrow-based
simple aggregate operator.
"""
from __future__ import annotations

from typing import Generator, Optional

import time

from opteryx.compiled.aggregations.scalar_kernels import ApproximateCountState
from opteryx.compiled.aggregations.scalar_kernels import ApproximatePercentileState
from opteryx.compiled.aggregations.scalar_kernels import approximate_count_draken
from opteryx.compiled.aggregations.scalar_kernels import approximate_percentile_draken
from opteryx.compiled.draken.interop.arrow import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.compiled.nanobind.carchar_native import CarcharSet
from opteryx.exceptions import InvalidFunctionParameterError
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.expression.evaluator import evaluate_and_append_draken
from opteryx.models import QueryProperties
from opteryx.operators.aggregate_helpers import extract_evaluations

from opteryx import EOS

from . import BasePlanNode
from opteryx.operators.catalog import OperatorCategory, ParallelStrategy

_DATA_FORMAT = "draken"
_DRAKEN_ENCODING_CONSTANT = 3


def _column_bytes(identity):
    return identity if isinstance(identity, bytes) else str(identity).encode("utf-8")


def _is_constant_vector_like(vector) -> bool:
    return getattr(vector, "encoding", None) == _DRAKEN_ENCODING_CONSTANT


def _constant_scalar_value(vector):
    if len(vector) == 0:
        return None
    return vector[0]


def _vector_null_count(vector) -> int:
    try:
        return int(vector.null_count)
    except Exception:
        return sum(1 for value in vector.to_pylist() if value is None)


def _vector_valid_values(vector):
    return [value for value in vector.to_pylist() if value is not None]


def _vector_sum(vector):
    valid_count = len(vector) - _vector_null_count(vector)
    if valid_count == 0:
        return None

    if _is_constant_vector_like(vector):
        scalar = _constant_scalar_value(vector)
        if scalar is None:
            return None
        return scalar * valid_count

    if vector.__class__.__name__ in ("Int64Vector", "Float64Vector"):
        return vector.sum()

    values = _vector_valid_values(vector)
    return sum(values) if values else None


def _vector_min(vector):
    if _is_constant_vector_like(vector):
        scalar = _constant_scalar_value(vector)
        return scalar if scalar is not None else None

    values = _vector_valid_values(vector)
    return min(values) if values else None


def _vector_max(vector):
    if _is_constant_vector_like(vector):
        scalar = _constant_scalar_value(vector)
        return scalar if scalar is not None else None

    values = _vector_valid_values(vector)
    return max(values) if values else None


def _ensure_carchar_set(distinct_hashes):
    return distinct_hashes if distinct_hashes is not None else CarcharSet()


def _insert_vector_hashes(distinct_hashes, vector):
    distinct_hashes = _ensure_carchar_set(distinct_hashes)
    distinct_hashes.insert_many(vector.hash())
    return distinct_hashes


def _insert_literal_hash(distinct_hashes, literal):
    distinct_hashes = _ensure_carchar_set(distinct_hashes)
    distinct_hashes.insert_many(vector_from_sequence([literal]).hash())
    return distinct_hashes


class _DrakenAggregateCollector:
    def __init__(self, aggregate):
        self.aggregate = aggregate
        self.aggregate_type = aggregate.value
        self.duplicate_treatment = getattr(aggregate, "duplicate_treatment", None)
        self.output_name = aggregate.schema_column.identity
        self.parameter = aggregate.parameters[0]
        self.percentile = self._extract_percentile(aggregate)

        self._count = 0
        self._sum = None
        self._min = None
        self._max = None
        self._distinct_hashes = None
        self._approx_count = None
        self._approx_percentile = None

    @staticmethod
    def _extract_percentile(aggregate):
        if aggregate.value != "APPROX_PERCENTILE":
            return None
        if len(aggregate.parameters) != 2:
            raise InvalidFunctionParameterError("APPROX_PERCENTILE expects two arguments")
        percentile_node = aggregate.parameters[1]
        if percentile_node.node_type != NodeType.LITERAL:
            raise InvalidFunctionParameterError(
                "APPROX_PERCENTILE percentile argument must be a literal"
            )
        percentile = float(percentile_node.value)
        if percentile < 0.0 or percentile > 1.0:
            raise InvalidFunctionParameterError(
                "APPROX_PERCENTILE percentile must be between 0.0 and 1.0"
            )
        return percentile

    def _update_min(self, value):
        if value is None:
            return
        if self._min is None or value < self._min:
            self._min = value

    def _update_max(self, value):
        if value is None:
            return
        if self._max is None or value > self._max:
            self._max = value

    def _collect_literal(self, literal, row_count):
        if row_count == 0:
            return

        if self.aggregate_type == "COUNT":
            if self.duplicate_treatment == "Distinct":
                self._distinct_hashes = _insert_literal_hash(self._distinct_hashes, literal)
                return

            if literal is not None:
                self._count += row_count
            return

        if literal is None:
            return

        if self.aggregate_type == "SUM":
            contribution = literal * row_count
            self._sum = contribution if self._sum is None else self._sum + contribution
            return

        if self.aggregate_type == "AVG":
            self._sum = (
                (literal * row_count) if self._sum is None else self._sum + (literal * row_count)
            )
            self._count += row_count
            return

        if self.aggregate_type == "MIN":
            self._update_min(literal)
            return

        if self.aggregate_type == "MAX":
            self._update_max(literal)
            return

        if self.aggregate_type == "APPROX_COUNT_DISTINCT":
            if self._approx_count is None:
                self._approx_count = ApproximateCountState()
            self._approx_count.add_repeated_value(literal, row_count)
            return

        if self.aggregate_type == "APPROX_PERCENTILE":
            if self._approx_percentile is None:
                self._approx_percentile = ApproximatePercentileState(self.percentile)
            self._approx_percentile.add_repeated_value(literal, row_count)
            return

    def _collect_vector(self, vector):
        valid_count = len(vector) - _vector_null_count(vector)

        if self.aggregate_type == "COUNT":
            if self.duplicate_treatment == "Distinct":
                self._distinct_hashes = _insert_vector_hashes(self._distinct_hashes, vector)
                return

            self._count += valid_count
            return

        if self.aggregate_type == "COUNT_DISTINCT" or self.aggregate_type == "DISTINCT":
            self._distinct_hashes = _insert_vector_hashes(self._distinct_hashes, vector)
            return

        if valid_count == 0:
            return

        if self.aggregate_type == "SUM":
            chunk_sum = _vector_sum(vector)
            self._sum = chunk_sum if self._sum is None else self._sum + chunk_sum
            return

        if self.aggregate_type == "AVG":
            chunk_sum = _vector_sum(vector)
            self._sum = chunk_sum if self._sum is None else self._sum + chunk_sum
            self._count += valid_count
            return

        if self.aggregate_type == "MIN":
            self._update_min(_vector_min(vector))
            return

        if self.aggregate_type == "MAX":
            self._update_max(_vector_max(vector))
            return

        if self.aggregate_type == "APPROX_COUNT_DISTINCT":
            self._approx_count = approximate_count_draken(vector, self._approx_count)
            return

        if self.aggregate_type == "APPROX_PERCENTILE":
            self._approx_percentile = approximate_percentile_draken(
                vector, self._approx_percentile, self.percentile
            )
            return

    def collect(self, morsel: Morsel):
        if self.parameter.node_type == NodeType.WILDCARD:
            self._count += morsel.num_rows
            return

        if self.parameter.node_type == NodeType.LITERAL:
            self._collect_literal(self.parameter.value, morsel.num_rows)
            return

        vector = morsel.column(_column_bytes(self.parameter.schema_column.identity))
        self._collect_vector(vector)

    def finalize(self):
        if self.aggregate_type == "COUNT":
            if self.duplicate_treatment == "Distinct":
                return 0 if self._distinct_hashes is None else self._distinct_hashes.size()
            return self._count

        if self.aggregate_type == "COUNT_DISTINCT" or self.aggregate_type == "DISTINCT":
            return 0 if self._distinct_hashes is None else self._distinct_hashes.size()

        if self.aggregate_type == "APPROX_COUNT_DISTINCT":
            return 0 if self._approx_count is None else self._approx_count.estimate()

        if self.aggregate_type == "APPROX_PERCENTILE":
            return None if self._approx_percentile is None else self._approx_percentile.quantile()

        if self.aggregate_type == "SUM":
            return self._sum

        if self.aggregate_type == "AVG":
            if self._count == 0 or self._sum is None:
                return None
            return self._sum / self._count

        if self.aggregate_type == "MIN":
            return self._min

        if self.aggregate_type == "MAX":
            return self._max

        raise ValueError(
            f"Unsupported aggregate type for Draken global aggregate: {self.aggregate_type}"
        )


class DrakenAggregateNode(BasePlanNode):
    category = OperatorCategory.AGGREGATE
    parallel_strategy = ParallelStrategy.SINGLE_THREAD
    is_pipeline_breaking = True
    logical_node_type = 'Aggregate'
    def __init__(self, properties: QueryProperties, **parameters):
        super().__init__(properties=properties, **parameters)

        self.aggregates = list(parameters.get("aggregates", []))
        self.evaluatable_nodes = [
            node
            for node in extract_evaluations(self.aggregates)
            if node.node_type != NodeType.LITERAL
        ]

        all_identifiers = [
            node.schema_column.identity
            for node in get_all_nodes_of_type(self.aggregates, select_nodes=(NodeType.IDENTIFIER,))
        ]
        self.all_identifiers = list(dict.fromkeys(all_identifiers))
        self.collectors = [_DrakenAggregateCollector(aggregate) for aggregate in self.aggregates]

    @property
    def config(self):  # pragma: no cover
        from opteryx.expression import format_expression

        return f"AGGREGATE ({', '.join(format_expression(col) for col in self.aggregates)})"

    @property
    def name(self):  # pragma: no cover
        return "Aggregation Draken"

    def _prepare_chunk(self, chunk: Morsel) -> Morsel:
        if self.all_identifiers:
            chunk = chunk.select(self.all_identifiers)

        if self.evaluatable_nodes:
            eval_start = time.monotonic_ns()
            chunk = evaluate_and_append_draken(self.evaluatable_nodes, chunk)
            self.readings["time_aggregate_evaluations"] += time.monotonic_ns() - eval_start

        return chunk

    def _finalize_morsel(self):
        names = []
        vectors = []

        for collector in self.collectors:
            names.append(collector.output_name)
            vectors.append(vector_from_sequence([collector.finalize()]))

        return Morsel.from_vectors(names, vectors)

    def execute(self, morsel):
        draken = self.ensure_draken_morsel(morsel)

        if draken == EOS:
            yield self._finalize_morsel()
            yield EOS
            return

        ingest_start = time.monotonic_ns()

        if isinstance(draken, Morsel):
            if draken.num_rows > 0:
                draken = self._prepare_chunk(draken)
                for collector in self.collectors:
                    collector.collect(draken)
            self.readings["time_aggregate_ingest"] += time.monotonic_ns() - ingest_start
            yield None
            return

        for chunk in draken:
            if chunk is None or chunk is EOS or chunk.num_rows == 0:
                continue
            chunk = self._prepare_chunk(chunk)
            for collector in self.collectors:
                collector.collect(chunk)

        self.readings["time_aggregate_ingest"] += time.monotonic_ns() - ingest_start
        yield None
