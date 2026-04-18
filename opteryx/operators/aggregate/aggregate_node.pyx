# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Draken-native global aggregation node.

This operator stays on Draken morsels end-to-end. The actual accumulation work
is delegated to the lower-level ungrouped aggregate engine; this module only
bridges planner-bound aggregate nodes to that engine and handles a few literal
edge cases.
"""

from __future__ import annotations

import time

from libc.stdint cimport uint8_t

from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.compiled.draken.vectors.vector cimport Vector
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.expression.evaluator import evaluate_and_append_draken
from opteryx.models import QueryProperties
from opteryx.operators.aggregate.helpers import extract_evaluations
from opteryx.operators.aggregate.ungrouped_agg import (
    AnyValueAggregate,
    CountAggregate,
    CountDistinctAggregate,
    CountStarAggregate,
    MaxBytesAggregate,
    MaxFloat64Aggregate,
    MaxInt64Aggregate,
    MinBytesAggregate,
    MinFloat64Aggregate,
    MinInt64Aggregate,
    SumFloat64Aggregate,
    SumInt64Aggregate,
    UngroupedAggregateEngine,
)
from opteryx.types import OrsoTypes

from opteryx import EOS
from opteryx.operators import BasePlanNode

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


def _count_null_bitmap(const uint8_t* bitmap, Py_ssize_t nrows) -> int:
    cdef Py_ssize_t i
    cdef Py_ssize_t count = 0

    if bitmap == NULL:
        return 0

    for i in range(nrows):
        if not ((bitmap[i >> 3] >> (i & 7)) & 1):
            count += 1

    return <int>count


def _vector_null_count(vector) -> int:
    cdef Vector typed_vector
    try:
        return int(vector.null_count)
    except Exception:
        try:
            typed_vector = vector
            return _count_null_bitmap(
                typed_vector.null_bitmap_ptr(),
                len(typed_vector),
            )
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

    # Try to use native sum() method if available (fast path for numeric types)
    if hasattr(vector, 'sum'):
        try:
            return vector.sum()
        except (ValueError, NotImplementedError):
            # NotImplementedError: aggregate not supported for this type
            return None

    # Fallback to Python materialization
    values = _vector_valid_values(vector)
    return sum(values) if values else None


def _vector_min(vector):
    valid_count = len(vector) - _vector_null_count(vector)
    if valid_count == 0:
        return None

    if _is_constant_vector_like(vector):
        scalar = _constant_scalar_value(vector)
        return scalar if scalar is not None else None

    # Try to use native min() method if available
    if hasattr(vector, 'min'):
        try:
            return vector.min()
        except (ValueError, NotImplementedError):
            # ValueError: empty or all-null column
            # NotImplementedError: aggregate not supported for this type
            return None

    # Fallback to Python materialization
    values = _vector_valid_values(vector)
    return min(values) if values else None


def _vector_max(vector):
    valid_count = len(vector) - _vector_null_count(vector)
    if valid_count == 0:
        return None

    if _is_constant_vector_like(vector):
        scalar = _constant_scalar_value(vector)
        return scalar if scalar is not None else None

    # Try to use native max() method if available
    if hasattr(vector, 'max'):
        try:
            return vector.max()
        except (ValueError, NotImplementedError):
            # ValueError: empty or all-null column
            # NotImplementedError: aggregate not supported for this type
            return None

    # Fallback to Python materialization
    values = _vector_valid_values(vector)
    return max(values) if values else None


def _parameter_identity(parameter):
    schema_column = getattr(parameter, "schema_column", None)
    if schema_column is None:
        return None
    identity = getattr(schema_column, "identity", None)
    if identity in (None, "", b""):
        return None
    return _column_bytes(identity)


def _parameter_type(parameter):
    schema_column = getattr(parameter, "schema_column", None)
    if schema_column is None:
        return None
    return getattr(schema_column, "type", None)


def _is_float_type(type_value) -> bool:
    if type_value is None:
        return False
    value = getattr(type_value, "value", type_value)
    return value in ("DOUBLE", "FLOAT", "DECIMAL")


def _is_string_type(type_value) -> bool:
    if type_value is None:
        return False
    value = getattr(type_value, "value", type_value)
    return value in ("VARCHAR", "BLOB")


def _make_literal_spec(aggregate):
    parameter = aggregate.parameters[0] if aggregate.parameters else None
    return {
        "kind": "literal",
        "aggregate_type": aggregate.value,
        "duplicate_treatment": getattr(aggregate, "duplicate_treatment", None),
        "output_name": _column_bytes(aggregate.schema_column.identity),
        "literal": None if parameter is None else parameter.value,
        "count": 0,
        "sum": None,
        "value": None,
        "seen": False,
    }


def _update_literal_spec(spec, row_count: int):
    if row_count == 0:
        return

    aggregate_type = spec["aggregate_type"]
    duplicate_treatment = spec["duplicate_treatment"]
    literal = spec["literal"]

    if aggregate_type == "COUNT":
        if duplicate_treatment == "Distinct":
            if literal is not None:
                spec["seen"] = True
            return
        if literal is not None:
            spec["count"] += row_count
        return

    if aggregate_type in ("COUNT_DISTINCT", "DISTINCT"):
        if literal is not None:
            spec["seen"] = True
        return

    if literal is None:
        return

    if aggregate_type == "SUM":
        contribution = literal * row_count
        spec["sum"] = contribution if spec["sum"] is None else spec["sum"] + contribution
        return

    if aggregate_type == "AVG":
        contribution = literal * row_count
        spec["sum"] = contribution if spec["sum"] is None else spec["sum"] + contribution
        spec["count"] += row_count
        return

    if aggregate_type in ("MIN", "MAX", "ANY_VALUE"):
        spec["value"] = literal
        spec["seen"] = True
        return


def _finalize_literal_spec(spec):
    aggregate_type = spec["aggregate_type"]
    duplicate_treatment = spec["duplicate_treatment"]
    literal = spec["literal"]

    if aggregate_type == "COUNT":
        if duplicate_treatment == "Distinct":
            return 1 if literal is not None else 0
        return spec["count"]

    if aggregate_type in ("COUNT_DISTINCT", "DISTINCT"):
        return 1 if literal is not None else 0

    if aggregate_type == "SUM":
        return spec["sum"]

    if aggregate_type == "AVG":
        if spec["count"] == 0 or spec["sum"] is None:
            return None
        return spec["sum"] / spec["count"]

    if aggregate_type in ("MIN", "MAX", "ANY_VALUE"):
        return spec["value"] if spec["seen"] else None

    raise ValueError(f"Unsupported literal aggregate type: {aggregate_type}")


def _build_engine_aggregate(aggregate):
    parameter = aggregate.parameters[0] if aggregate.parameters else None
    aggregate_type = aggregate.value
    duplicate_treatment = getattr(aggregate, "duplicate_treatment", None)
    output_name = _column_bytes(aggregate.schema_column.identity)
    parameter_name = _parameter_identity(parameter)
    parameter_type = _parameter_type(parameter)

    if aggregate_type == "COUNT":
        if parameter is not None and parameter.node_type == NodeType.WILDCARD:
            return [CountStarAggregate(output_name)], None, None

        if parameter is not None and parameter.node_type == NodeType.LITERAL:
            if parameter.value == "*":
                return [CountStarAggregate(output_name)], None, None
            return [], None, _make_literal_spec(aggregate)

        if duplicate_treatment == "Distinct":
            if parameter_name is None:
                return [], None, _make_literal_spec(aggregate)
            return [CountDistinctAggregate(parameter_name, output_name)], None, None

        if parameter_name is None:
            return [], None, _make_literal_spec(aggregate)

        return [CountAggregate(parameter_name, output_name)], None, None

    if aggregate_type in ("COUNT_DISTINCT", "DISTINCT"):
        if parameter_name is None:
            return [], None, _make_literal_spec(aggregate)
        return [CountDistinctAggregate(parameter_name, output_name)], None, None

    if aggregate_type == "SUM":
        if parameter_name is None:
            return [], None, _make_literal_spec(aggregate)
        if _is_float_type(parameter_type):
            return [SumFloat64Aggregate(parameter_name, output_name)], None, None
        return [SumInt64Aggregate(parameter_name, output_name)], None, None

    if aggregate_type == "AVG":
        if parameter_name is None:
            return [], None, _make_literal_spec(aggregate)
        sum_alias = _column_bytes(f"__avg_sum_{output_name.decode('utf-8', 'ignore')}")
        count_alias = _column_bytes(f"__avg_count_{output_name.decode('utf-8', 'ignore')}")
        if _is_float_type(parameter_type):
            sum_agg = SumFloat64Aggregate(parameter_name, sum_alias)
        else:
            sum_agg = SumInt64Aggregate(parameter_name, sum_alias)
        count_agg = CountAggregate(parameter_name, count_alias)
        return [sum_agg, count_agg], ("avg", sum_alias, count_alias, output_name), None

    if aggregate_type == "MIN":
        if parameter_name is None:
            return [], None, _make_literal_spec(aggregate)
        if _is_string_type(parameter_type):
            return [MinBytesAggregate(parameter_name, output_name)], None, None
        if _is_float_type(parameter_type):
            return [MinFloat64Aggregate(parameter_name, output_name)], None, None
        return [MinInt64Aggregate(parameter_name, output_name)], None, None

    if aggregate_type == "MAX":
        if parameter_name is None:
            return [], None, _make_literal_spec(aggregate)
        if _is_string_type(parameter_type):
            return [MaxBytesAggregate(parameter_name, output_name)], None, None
        if _is_float_type(parameter_type):
            return [MaxFloat64Aggregate(parameter_name, output_name)], None, None
        return [MaxInt64Aggregate(parameter_name, output_name)], None, None

    if aggregate_type == "ANY_VALUE":
        if parameter_name is None:
            return [], None, _make_literal_spec(aggregate)
        return [AnyValueAggregate(parameter_name, output_name)], None, None

    if aggregate_type in ("APPROX_COUNT_DISTINCT", "APPROX_PERCENTILE"):
        raise NotImplementedError(
            f"Approximate aggregate `{aggregate_type}` is no longer supported."
        )

    raise ValueError(f"Unsupported aggregate type for Draken global aggregate: {aggregate_type}")


class UngroupedAggregateNode(BasePlanNode):
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
        self._engine = UngroupedAggregateEngine()
        self._result_specs = []
        self._engine_aggregate_count = 0
        self._finalized = False

        for aggregate in self.aggregates:
            engine_aggs, avg_spec, literal_spec = _build_engine_aggregate(aggregate)
            for engine_agg in engine_aggs:
                self._engine.add_aggregate(engine_agg)
                self._engine_aggregate_count += 1
            if avg_spec is not None:
                self._engine.add_avg_finalizer(avg_spec[1], avg_spec[2], avg_spec[3])
                self._result_specs.append(
                    {
                        "kind": "engine",
                        "output_name": _column_bytes(aggregate.schema_column.identity),
                    }
                )
            elif literal_spec is not None:
                self._result_specs.append(
                    {
                        "kind": "literal",
                        "output_name": literal_spec["output_name"],
                        "state": literal_spec,
                    }
                )
            else:
                self._result_specs.append(
                    {
                        "kind": "engine",
                        "output_name": _column_bytes(aggregate.schema_column.identity),
                    }
                )

    @property
    def config(self):  # pragma: no cover
        from opteryx.expression import format_expression

        return f"AGGREGATE ({', '.join(format_expression(col) for col in self.aggregates)})"

    @property
    def name(self):  # pragma: no cover
        return "Ungrouped Aggregate"

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
        engine_result = None

        if self._engine_aggregate_count:
            engine_result = self._engine.finalize()

        for spec in self._result_specs:
            names.append(spec["output_name"])
            if spec["kind"] == "engine":
                vectors.append(engine_result.column(spec["output_name"]))
            else:
                vectors.append(vector_from_sequence([_finalize_literal_spec(spec["state"])]))

        return Morsel.from_vectors(names, vectors)

    def execute(self, morsel):
        draken = self.ensure_draken_morsel(morsel)

        if draken == EOS:
            if self._finalized:
                return
            self._finalized = True
            yield self._finalize_morsel()
            return

        ingest_start = time.monotonic_ns()

        if isinstance(draken, Morsel):
            if draken.num_rows > 0:
                draken = self._prepare_chunk(draken)
                self._engine.ingest(draken)
                for spec in self._result_specs:
                    if spec["kind"] == "literal":
                        _update_literal_spec(spec["state"], draken.num_rows)
            self.readings["time_aggregate_ingest"] += time.monotonic_ns() - ingest_start
            yield None
            return

        for chunk in draken:
            if chunk is None or chunk is EOS or chunk.num_rows == 0:
                continue
            chunk = self._prepare_chunk(chunk)
            self._engine.ingest(chunk)
            for spec in self._result_specs:
                if spec["kind"] == "literal":
                    _update_literal_spec(spec["state"], chunk.num_rows)

        self.readings["time_aggregate_ingest"] += time.monotonic_ns() - ingest_start
        yield None
