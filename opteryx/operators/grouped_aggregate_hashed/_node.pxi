# cython: language_level=3

import time

# GroupedAggregateHashedNode — the Python operator boundary.
# Expression pre-evaluation and HAVING logic moved from draken_aggregate_and_group_node.pyx.

from draken.morsels.morsel cimport Morsel
from draken.draken_native import vector_int8_from_constant
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.compiled.expression.compiled_expression import lower, build_bytecode
from opteryx.expression.evaluator import execute_bytecode
from opteryx.models import QueryProperties
from dataclasses import dataclass

from opteryx.operators.aggregate.helpers import extract_evaluations


@dataclass(frozen=True)
class AggregationSpec:
    alias: str
    function: str
    column: str | bytes | None = None
    options: object | None = None

# BasePlanNode in scope via textual include from _operators.pyx (umbrella unit).
# EOS sentinel available as _EOS_SENTINEL in the same scope.


CHUNK_SIZE = 65536


class GroupedAggregateHashedNode(BasePlanNode):

    def __init__(self, properties: QueryProperties, **parameters):
        super().__init__(properties=properties, **parameters)

        self.groups = list(parameters["groups"])
        self.aggregates = list(parameters["aggregates"])
        projection = list(parameters["projection"])

        # Resolve integer position GROUP BY references (e.g. GROUP BY 1)
        self.groups = [
            (
                group
                if not (group.node_type == NodeType.LITERAL and group.type.__class__.__name__ == "INTEGER")
                else projection[group.value - 1]
            )
            for group in self.groups
        ]

        # Collect all base identifiers needed (for morsel.select before eval)
        all_identifiers = [
            node.schema_column.identity
            for node in get_all_nodes_of_type(
                self.groups + self.aggregates, select_nodes=(NodeType.IDENTIFIER,)
            )
        ]
        self.all_identifiers = list(dict.fromkeys(all_identifiers))

        self.evaluatable_nodes = extract_evaluations(self.aggregates)
        self._needs_expression_eval = bool(self.evaluatable_nodes) or any(
            group.node_type != NodeType.IDENTIFIER for group in self.groups
        )

        self.group_by_columns = list({node.schema_column.identity for node in self.groups})

        self._aggregation_specs = self._build_aggregation_specs(self.aggregates)

        # GROUP BY with no explicit aggregates — add implicit COUNT(*) so the engine
        # has at least one aggregate, strip it from output later.
        if not self._aggregation_specs and self.group_by_columns:
            self._aggregation_specs = [
                AggregationSpec(alias="$implicit-count", function="count", column=None)
            ]
            self._implicit_count_added = True
        else:
            self._implicit_count_added = False

        collectors, _key_kinds_placeholder = create_collectors(
            self._aggregation_specs, self.group_by_columns
        )

        variant = parameters.get("group_map_variant", "carchar")
        use_parvi = variant == "parvi"
        self._engine = GroupHashEngine(self.group_by_columns, collectors, True, use_parvi)

        # Required columns for morsel.select() before ingestion
        self._required_columns = self._build_required_columns()

        self._having_condition = parameters.get("having_condition")

    @property
    def config(self):  # pragma: no cover
        from opteryx.expression import format_expression
        return (
            f"AGGREGATE ({', '.join(format_expression(col) for col in self.aggregates)}) "
            f"GROUP BY ({', '.join(format_expression(col) for col in self.groups)})"
        )

    @property
    def name(self):  # pragma: no cover
        return "Grouped Aggregate (Hashed)"

    def _build_aggregation_specs(self, aggregates):
        from opteryx.exceptions import InvalidFunctionParameterError

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

                if field_node.node_type == NodeType.WILDCARD:
                    column = "*"
                else:
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

    def _build_required_columns(self):
        required = list(self.group_by_columns)
        required.extend(
            ident for ident in self.all_identifiers if ident not in required
        )
        for node in self.evaluatable_nodes:
            ident = node.schema_column.identity
            if ident not in required:
                required.append(ident)
        for node in self.groups:
            if node.node_type != NodeType.IDENTIFIER:
                ident = node.schema_column.identity
                if ident not in required:
                    required.append(ident)
        return list(dict.fromkeys(required))

    @staticmethod
    def _normalize_aggregate_function(aggregator) -> str:
        value = aggregator.value
        function = value.lower()
        if function == "count" and aggregator.duplicate_treatment == "Distinct":
            return "count_distinct"
        if function in ("count", "sum", "min", "max", "avg", "median"):
            return function
        if function in ("count_distinct", "approx_count_distinct", "approx_percentile",
                        "array_agg", "any_value"):
            return function
        raise UnsupportedSyntaxError(
            f"Unsupported aggregate function: {value}"
        )

    @staticmethod
    def _extract_percentile_option(aggregator) -> float:
        from opteryx.exceptions import InvalidFunctionParameterError
        if len(aggregator.parameters) != 2:
            raise InvalidFunctionParameterError(
                "APPROX_PERCENTILE requires two arguments: the column and the percentile"
            )
        percentile_node = aggregator.parameters[1]
        if percentile_node.node_type != NodeType.LITERAL:
            raise InvalidFunctionParameterError(
                "APPROX_PERCENTILE percentile argument must be a literal"
            )
        percentile = float(percentile_node.value)
        if not (0.0 <= percentile <= 1.0):
            raise InvalidFunctionParameterError(
                "APPROX_PERCENTILE percentile must be between 0.0 and 1.0"
            )
        return percentile

    @staticmethod
    def _extract_array_agg_options(aggregator) -> dict:
        from opteryx.exceptions import InvalidFunctionParameterError
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

    def _prepare_chunk(self, chunk):
        """Pre-evaluate GROUP BY expressions and add * column."""
        if self.all_identifiers:
            chunk = chunk.select(self.all_identifiers)
        if b"*" not in chunk.column_names and "*" not in chunk.column_names:
            star_vector = vector_int8_from_constant(1, chunk.num_rows)
            chunk.append_vector("*", star_vector)
        eval_start = time.monotonic_ns()
        try:
            if self.evaluatable_nodes:
                chunk = self._evaluate_and_append_bytecode(self.evaluatable_nodes, chunk)
            chunk = self._evaluate_and_append_bytecode(self.groups, chunk)
            self.readings["time_aggregate_evaluations"] += time.monotonic_ns() - eval_start
            return chunk
        except (NotImplementedError, TypeError, UnsupportedSyntaxError) as err:
            raise UnsupportedSyntaxError(
                f"Grouped aggregate expression evaluation does not support this query shape: {err}"
            ) from err

    def _evaluate_and_append_bytecode(self, nodes, morsel):
        """Evaluate nodes via bytecode and append results as columns."""
        from opteryx.expression import should_evaluate

        col_names = list(morsel.column_names)
        col_vecs = []
        for _n in col_names:
            if isinstance(_n, bytes):
                col_vecs.append(morsel.column(_n))
            else:
                col_vecs.append(morsel.column(_n.encode()))

        existing = {
            _n.decode() if isinstance(_n, bytes) else _n for _n in col_names
        }

        for node in nodes:
            if not should_evaluate(node):
                continue
            identity = node.schema_column.identity
            if identity in existing:
                continue

            bc = build_bytecode(lower(node))
            result_vec = execute_bytecode(bc, morsel)
            col_names.append(identity)
            col_vecs.append(result_vec)

        if len(col_vecs) == len(morsel.column_names):
            return morsel

        from draken.morsels.morsel import Morsel as DrakenMorsel
        return DrakenMorsel.from_vectors(col_names, col_vecs)

    def _apply_having_filter(self, morsel):
        bc = build_bytecode(lower(self._having_condition))
        mask = execute_bytecode(bc, morsel)
        return morsel.filter_mask(mask)

    def _push_impl(self, morsel):
        if morsel is _EOS_SENTINEL:
            for chunk in self._finalize():
                self.emit(chunk)
            self.emit(_EOS_SENTINEL)
            return

        if morsel.num_rows == 0:
            return

        if self._needs_expression_eval:
            morsel = self._prepare_chunk(morsel)
        if self._required_columns:
            morsel = morsel.select(self._required_columns)

        ingest_start = time.monotonic_ns()
        self._engine.ingest(morsel)
        self.readings["time_aggregate_ingest"] += time.monotonic_ns() - ingest_start

    def _finalize(self):
        finalize_start = time.monotonic_ns()

        # Pass HAVING filter to engine for early filtering before chunking
        # This avoids reconstructing groups that don't pass the filter.
        filter_fn = self._apply_having_filter if self._having_condition is not None else None

        for chunk in self._engine.finalize_morsels(CHUNK_SIZE, filter_fn=filter_fn):
            if self._implicit_count_added:
                # Drop the first column (the implicit COUNT(*))
                chunk = chunk.select(chunk.column_names[1:])
            yield chunk

        self.readings["time_aggregate_finalize"] += time.monotonic_ns() - finalize_start
        engine_telemetry = self._engine.telemetry()
        self.readings["time_aggregate_resolve"] += engine_telemetry["time_resolve_ns"]
        self.readings["time_aggregate_hash"] += engine_telemetry["time_hash_ns"]
        self.readings["time_aggregate_lookup"] += engine_telemetry["time_lookup_ns"]
        self.readings["time_aggregate_store_keys"] += engine_telemetry["time_store_keys_ns"]
        self.readings["time_aggregate_grow"] += engine_telemetry["time_grow_ns"]
        self.readings["time_aggregate_accumulate"] += engine_telemetry["time_accumulate_ns"]
        self.readings["time_aggregate_reconstruct"] += engine_telemetry["time_reconstruct_ns"]
        self.readings["time_aggregate_reconstruct_single_fixed"] += engine_telemetry["time_reconstruct_single_fixed_ns"]
        self.readings["time_aggregate_reconstruct_single_string"] += engine_telemetry["time_reconstruct_single_string_ns"]
        self.readings["time_aggregate_reconstruct_multi"] += engine_telemetry["time_reconstruct_multi_ns"]
        self.readings["time_aggregate_build_morsel"] += engine_telemetry["time_build_morsel_ns"]
        self.readings["time_aggregate_slice_output"] += engine_telemetry["time_slice_output_ns"]
