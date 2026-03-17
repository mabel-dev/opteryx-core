# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Draken-native grouped aggregation node.

This node keeps existing planner/expression behavior but executes the grouped
aggregation kernel using the compiled Draken GroupStateStore backend.
"""

from __future__ import annotations

import time

from orso.types import OrsoTypes

from opteryx import EMPTY
from opteryx import EOS
from opteryx.draken.morsels.morsel import Morsel
from opteryx.draken.vectors.constant_vector import from_scalar as constant_from_scalar
from opteryx.exceptions import InvalidFunctionParameterError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.expression.evaluator import evaluate_and_append_draken
from opteryx.models import QueryProperties
from opteryx.operators.aggregate_helpers import extract_evaluations
from opteryx.operators.group_state_store import create_group_state_engine
from opteryx.operators.group_state_store import normalize_aggregations
from opteryx.operators.group_state_store import normalize_group_by_columns
from opteryx.operators.shuffle import AggregationSpec

from . import BasePlanNode

_DATA_FORMAT = "draken"


CHUNK_SIZE = 65536


class DrakenAggregateAndGroupNode(BasePlanNode):
    ENGINE_READING_KEYS = (
        "feature_groupby_engine_carchar",
        "feature_groupby_engine_constant",
        "feature_groupby_engine_group_state_store",
        "feature_groupby_engine_multi_key_fixed",
        "feature_groupby_engine_multi_key_object",
        "draken_dict_groupby_fastpath_hits",
        "draken_dict_groupby_fastpath_fallbacks",
        "draken_constant_groupby_fastpath_hits",
        "draken_constant_groupby_fastpath_fallbacks",
        "draken_constant_groupby_output_vector_hits",
        "draken_constant_groupby_output_vector_fallbacks",
        "groupby_key_store_bytes",
        "groupby_key_store_limit_bytes",
    )
    SUPPORTED_AGGREGATES = frozenset(
        {
            "APPROX_COUNT_DISTINCT",
            "APPROX_PERCENTILE",
            "ARRAY_AGG",
            "COUNT",
            "SUM",
            "MIN",
            "MAX",
            "AVG",
            "COUNT_DISTINCT",
            "ANY_VALUE",
        }
    )
    # MAX was previously omitted: the Draken kernel implemented it but
    # the planner refused to use it in strict mode until we were confident
    # the fast-finalize semantics were deterministic.  With the rewritten
    # expression engine and proper handling of dictionary columns we can
    # safely include it and avoid the Arrow fallback entirely.
    FAST_PATH_AGGREGATES = frozenset(
        {
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "COUNT_DISTINCT",
            "APPROX_COUNT_DISTINCT",
            "APPROX_PERCENTILE",
            "ARRAY_AGG",
            "ANY_VALUE",
        }
    )

    def __init__(self, properties: QueryProperties, **parameters):
        super().__init__(properties=properties, **parameters)
        self.groups = list(parameters["groups"])
        self.aggregates = list(parameters["aggregates"])
        projection = list(parameters["projection"])

        self.groups = [
            (
                group
                if not (group.node_type == NodeType.LITERAL and group.type == OrsoTypes.INTEGER)
                else projection[group.value - 1]
            )
            for group in self.groups
        ]

        all_identifiers = [
            node.schema_column.identity
            for node in get_all_nodes_of_type(
                self.groups + self.aggregates, select_nodes=(NodeType.IDENTIFIER,)
            )
        ]
        self.all_identifiers = list(dict.fromkeys(all_identifiers))
        self.evaluatable_nodes = extract_evaluations(self.aggregates)
        self._needs_arrow_eval = bool(self.evaluatable_nodes) or any(
            group.node_type != NodeType.IDENTIFIER for group in self.groups
        )
        self.group_by_columns = list({node.schema_column.identity for node in self.groups})
        self._aggregation_specs = self._build_aggregation_specs(self.aggregates)

        # Handle GROUP BY without aggregates by adding implicit COUNT(*)
        # This allows the group state engine to work correctly
        if not self._aggregation_specs and self.group_by_columns:
            # Add implicit COUNT(*) aggregate for GROUP BY with no explicit aggregates
            self._aggregation_specs = [
                AggregationSpec(alias="count", function="count", column=None)
            ]
            # Mark that we added an implicit aggregate so we can remove it from output later
            self._implicit_count_added = True
        else:
            self._implicit_count_added = False

        self._normalized_group_by_columns = normalize_group_by_columns(self.group_by_columns)
        self._normalized_aggregations = normalize_aggregations(self._aggregation_specs)
        required_columns = list(self.group_by_columns)
        # Use actual identifiers (base column names) instead of full expressions
        # This handles cases like SUM((event ->> 'bytes_processed')::INTEGER) where
        # we need to select just the 'event' column, not the full expression
        required_columns.extend(
            identifier for identifier in self.all_identifiers if identifier not in required_columns
        )
        # Also include evaluated expression identities so they don't get dropped by select()
        for node in self.evaluatable_nodes:
            identity = node.schema_column.identity
            if identity not in required_columns:
                required_columns.append(identity)
        # Include group expression identities (for complex GROUP BY expressions)
        for node in self.groups:
            if node.node_type != NodeType.IDENTIFIER:
                identity = node.schema_column.identity
                if identity not in required_columns:
                    required_columns.append(identity)
        self._required_columns = list(dict.fromkeys(required_columns))
        self._group_by = create_group_state_engine(
            group_by_columns=self._normalized_group_by_columns,
            aggregations=self._normalized_aggregations,
        )

    @staticmethod
    def supports(aggregates, groups=None) -> bool:
        groups = groups or []
        requires_group_by = False

        for aggregate in aggregates:
            if aggregate.value not in DrakenAggregateAndGroupNode.SUPPORTED_AGGREGATES:
                return False
            if not aggregate.parameters:
                return False
            if aggregate.value == "ARRAY_AGG":
                requires_group_by = True
                if len(aggregate.parameters) != 1:
                    return False
            if aggregate.value == "APPROX_COUNT_DISTINCT" and len(aggregate.parameters) != 1:
                return False
            if aggregate.value == "APPROX_PERCENTILE":
                if len(aggregate.parameters) != 2:
                    return False
                if aggregate.parameters[1].node_type != NodeType.LITERAL:
                    return False

            if aggregate.value not in DrakenAggregateAndGroupNode.FAST_PATH_AGGREGATES:
                # MAX/ONE/ANY_VALUE kernels are not admitted in strict mode
                # until their fast finalize semantics are fully deterministic.
                return False

        if requires_group_by and not groups:
            return False

        if not groups:
            return True

        # Allow expressions in GROUP BY - the execute() method already handles
        # evaluation via evaluate_and_append(self.groups, arrow_table)
        return True

    @property
    def config(self):  # pragma: no cover
        from opteryx.expression import format_expression

        return f"AGGREGATE ({', '.join(format_expression(col) for col in self.aggregates)}) GROUP BY ({', '.join(format_expression(col) for col in self.groups)})"

    @property
    def name(self):  # pragma: no cover
        return "Group By Draken"

    def _build_aggregation_specs(self, aggregates):
        specs = []
        for root in aggregates:
            for aggregator in get_all_nodes_of_type(root, select_nodes=(NodeType.AGGREGATOR,)):
                fn = self._normalize_aggregate_function(aggregator)
                field_node = aggregator.parameters[0]
                options = None
                if fn == "approx_percentile":
                    options = self._extract_percentile_option(aggregator)
                elif fn == "array_agg":
                    options = self._extract_array_agg_options(aggregator)
                # For simple identifiers, use the column name directly
                # For literals (constants), use None to indicate constant aggregation
                # For complex expressions, use the schema identity (which will be evaluated
                # and added as a column to the morsel before aggregation)
                if field_node.node_type == NodeType.WILDCARD:
                    column = "*"
                elif field_node.node_type == NodeType.LITERAL:
                    # Constants like min('a'): the literal will be broadcast to a
                    # ConstantVector column by evaluate_and_append_draken, so use
                    # its schema identity as the column name.
                    column = field_node.schema_column.identity
                elif field_node.node_type == NodeType.IDENTIFIER:
                    column = field_node.schema_column.identity
                else:
                    # Complex expression (e.g., cast, binary op, function call)
                    # The expression will be evaluated and added as a column with this identity
                    column = field_node.schema_column.identity
                specs.append(
                    AggregationSpec(
                        alias=aggregator.schema_column.identity,
                        function=fn,
                        column=column,
                        options=options,
                    )
                )
        return specs

    @staticmethod
    def _extract_percentile_option(aggregator) -> float:
        if len(aggregator.parameters) != 2:
            raise InvalidFunctionParameterError("APPROX_PERCENTILE expects two arguments")
        percentile_node = aggregator.parameters[1]
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

    @staticmethod
    def _normalize_aggregate_function(aggregator) -> str:
        value = aggregator.value
        if value == "COUNT":
            if aggregator.duplicate_treatment == "Distinct":
                return "count_distinct"
            return "count"
        if value == "SUM":
            return "sum"
        if value == "MIN":
            return "min"
        if value == "MAX":
            return "max"
        if value == "AVG":
            return "avg"
        if value == "APPROX_COUNT_DISTINCT":
            return "approx_count_distinct"
        if value == "APPROX_PERCENTILE":
            return "approx_percentile"
        if value == "ARRAY_AGG":
            return "array_agg"
        if value == "ANY_VALUE":
            return "hash_one"
        if value == "COUNT_DISTINCT":
            return "count_distinct"
        raise UnsupportedSyntaxError(f"Unsupported aggregate function for Draken group-by: {value}")

    @staticmethod
    def _extract_array_agg_options(aggregator) -> dict:
        ordered = bool(aggregator.order)
        descending = False
        if aggregator.order:
            if len(aggregator.order) != 1:
                raise InvalidFunctionParameterError(
                    "ARRAY_AGG can only ORDER BY the aggregated column"
                )
            descending = not bool(aggregator.order[0][1])

        limit = None if aggregator.limit is None else int(aggregator.limit)
        if limit is not None and limit < 0:
            raise InvalidFunctionParameterError("ARRAY_AGG LIMIT must be zero or greater")

        return {
            "distinct": aggregator.duplicate_treatment == "Distinct",
            "ordered": ordered,
            "descending": descending,
            "limit": limit,
        }

    def _engine_reading_snapshot(self):
        return {key: self._group_by.readings.get(key, 0) for key in self.ENGINE_READING_KEYS}

    def _accumulate_engine_reading_delta(self, snapshot):
        for key in self.ENGINE_READING_KEYS:
            current = self._group_by.readings.get(key, 0)
            previous = snapshot.get(key, 0)
            delta = current - previous
            if delta:
                self.readings[key] += delta

    def execute(self, morsel, **kwargs):
        _ = kwargs

        draken = self.ensure_draken_morsel(morsel)

        def _decode_dict_value_columns(chunk):
            """Decode dictionary-encoded value columns used in SUM/AVG/MIN/MAX to their
            underlying numeric type so Carchar routes them correctly."""
            import pyarrow as pa

            from opteryx.draken.interop.arrow import vector_from_arrow
            from opteryx.draken.morsels.morsel import Morsel

            needs_rebuild = False
            decoded_columns = {}
            for spec in self._normalized_aggregations:
                fn = spec[1] if len(spec) > 1 else None
                if fn not in ("sum", "avg", "mean", "min", "max"):
                    continue
                col = spec[2] if len(spec) > 2 else None
                if col is None:
                    continue
                col_key = col if isinstance(col, bytes) else col.encode()
                if col_key in decoded_columns:
                    continue
                try:
                    vec = chunk.column(col_key)
                except KeyError:
                    continue
                if not hasattr(vec, "to_arrow"):
                    continue
                arrow_arr = vec.to_arrow()
                if not pa.types.is_dictionary(arrow_arr.type):
                    continue
                decoded_columns[col_key] = vector_from_arrow(arrow_arr.dictionary_decode())
                needs_rebuild = True
            if not needs_rebuild:
                return chunk
            names = list(chunk.column_names)
            vectors = []
            for name in names:
                key = name if isinstance(name, bytes) else name.encode()
                if key in decoded_columns:
                    vectors.append(decoded_columns[key])
                else:
                    vectors.append(chunk.column(key))
            return Morsel.from_vectors(names, vectors)

        def _prepare_draken_chunk(chunk):
            if chunk == EOS:
                return EOS
            if self.all_identifiers:
                chunk = chunk.select(self.all_identifiers)
            if b"*" not in chunk.column_names and "*" not in chunk.column_names:
                star_vector = constant_from_scalar(1, chunk.num_rows, dtype="int8")
                chunk = Morsel.from_vectors(
                    [*chunk.column_names, "*"],
                    [
                        *(
                            chunk.column(name if isinstance(name, bytes) else name.encode())
                            for name in chunk.column_names
                        ),
                        star_vector,
                    ],
                )

            eval_start = time.monotonic_ns()
            try:
                if self.evaluatable_nodes:
                    chunk = evaluate_and_append_draken(self.evaluatable_nodes, chunk)
                chunk = evaluate_and_append_draken(self.groups, chunk)
                self.readings["feature_groupby_draken_eval_native"] += 1
                return chunk
            except (NotImplementedError, TypeError, UnsupportedSyntaxError) as err:
                raise UnsupportedSyntaxError(
                    f"Draken grouped expression evaluation does not support this query shape: {err}"
                ) from err
            finally:
                self.readings["time_group_by_evaluations"] += time.monotonic_ns() - eval_start

        if draken != EOS and self._needs_arrow_eval and isinstance(draken, Morsel):
            draken = _prepare_draken_chunk(draken)

        if draken == EOS:
            pre_engine_snapshot = self._engine_reading_snapshot()
            pre_backend_ns = self._group_by.readings.get("time_groupby_finalize_backend_ns", 0)
            pre_rows_to_vectors_ns = self._group_by.readings.get(
                "time_groupby_finalize_rows_to_vectors_ns", 0
            )
            pre_morsel_build_ns = self._group_by.readings.get(
                "time_groupby_finalize_morsel_build_ns", 0
            )
            pre_rows_count = self._group_by.readings.get("groupby_finalize_rows_count", 0)
            pre_chunks_emitted = self._group_by.readings.get("groupby_finalize_chunks_emitted", 0)
            pre_fast_path_hits = self._group_by.readings.get("groupby_finalize_fast_path_hits", 0)

            st = time.monotonic_ns()
            emitted = 0
            for result in self._group_by.finalize_morsels(chunk_size=CHUNK_SIZE):
                emitted += 1
                # If we added an implicit COUNT(*) for GROUP BY with no aggregates,
                # remove it from the output (it's the first column)
                if self._implicit_count_added:
                    result = result.select(result.column_names[1:])
                yield result
            finalize_total_ns = time.monotonic_ns() - st
            self.readings["time_groupby_finalize"] += finalize_total_ns
            self.readings["groupby_output_morsels"] += emitted

            backend_delta_ns = (
                self._group_by.readings.get("time_groupby_finalize_backend_ns", 0) - pre_backend_ns
            )
            rows_to_vectors_delta_ns = (
                self._group_by.readings.get("time_groupby_finalize_rows_to_vectors_ns", 0)
                - pre_rows_to_vectors_ns
            )
            morsel_build_delta_ns = (
                self._group_by.readings.get("time_groupby_finalize_morsel_build_ns", 0)
                - pre_morsel_build_ns
            )
            self.readings["time_groupby_finalize_backend"] += backend_delta_ns
            self.readings["time_groupby_finalize_rows_to_vectors"] += rows_to_vectors_delta_ns
            self.readings["time_groupby_finalize_morsel_build"] += morsel_build_delta_ns

            accounted_ns = backend_delta_ns + rows_to_vectors_delta_ns + morsel_build_delta_ns
            self.readings["time_groupby_finalize_accounted"] += accounted_ns
            self.readings["time_groupby_finalize_emit_wait"] += max(
                0, finalize_total_ns - accounted_ns
            )

            self.readings["groupby_finalize_rows"] += (
                self._group_by.readings.get("groupby_finalize_rows_count", 0) - pre_rows_count
            )
            self.readings["groupby_finalize_chunks"] += (
                self._group_by.readings.get("groupby_finalize_chunks_emitted", 0)
                - pre_chunks_emitted
            )
            self.readings["groupby_finalize_fast_path_hits"] += (
                self._group_by.readings.get("groupby_finalize_fast_path_hits", 0)
                - pre_fast_path_hits
            )
            self._accumulate_engine_reading_delta(pre_engine_snapshot)

            yield EOS
            return

        ingest_start = time.monotonic_ns()
        if isinstance(draken, Morsel):
            pre_engine_snapshot = self._engine_reading_snapshot()
            if self._needs_arrow_eval:
                draken = _prepare_draken_chunk(draken)
            if self._required_columns:
                draken = draken.select(self._required_columns)
            self._group_by.ingest(draken)
            self._accumulate_engine_reading_delta(pre_engine_snapshot)
            self.readings["time_groupby_ingest"] += time.monotonic_ns() - ingest_start
            yield EMPTY
            return

        pre_engine_snapshot = self._engine_reading_snapshot()
        for chunk in draken:
            if chunk is None or chunk is EOS or chunk.num_rows == 0:
                continue
            if self._needs_arrow_eval:
                chunk = _prepare_draken_chunk(chunk)
            if self._required_columns:
                chunk = chunk.select(self._required_columns)
            self._group_by.ingest(chunk)

        self._accumulate_engine_reading_delta(pre_engine_snapshot)
        self.readings["time_groupby_ingest"] += time.monotonic_ns() - ingest_start

        yield EMPTY
