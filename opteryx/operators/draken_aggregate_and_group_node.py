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

import numpy
import pyarrow
from orso.types import OrsoTypes

from opteryx import EOS
from opteryx.draken.morsels.morsel import Morsel
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import evaluate_and_append
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import QueryProperties
from opteryx.operators.aggregate_node import extract_evaluations
from opteryx.operators.aggregate_node import project
from opteryx.operators.group_state_store import ShuffleGroupByOperationV2
from opteryx.operators.shuffle import AggregationSpec

from . import BasePlanNode

CHUNK_SIZE = 65536


class DrakenAggregateAndGroupNode(BasePlanNode):
    SUPPORTED_AGGREGATES = frozenset(
        {"COUNT", "SUM", "MIN", "MAX", "AVG", "COUNT_DISTINCT", "DISTINCT", "ONE", "ANY_VALUE"}
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
        required_columns = list(self.group_by_columns)
        required_columns.extend(
            spec.column for spec in self._aggregation_specs if spec.column not in (None, "*")
        )
        self._required_columns = list(dict.fromkeys(required_columns))
        self._group_by = ShuffleGroupByOperationV2(
            group_by_columns=self.group_by_columns,
            aggregations=self._aggregation_specs,
        )

    @staticmethod
    def supports(aggregates, groups=None) -> bool:
        groups = groups or []

        for aggregate in aggregates:
            if aggregate.value not in DrakenAggregateAndGroupNode.SUPPORTED_AGGREGATES:
                return False
            if not aggregate.parameters:
                return False

        return True

    @property
    def config(self):  # pragma: no cover
        from opteryx.managers.expression import format_expression

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
                column = (
                    "*"
                    if field_node.node_type == NodeType.WILDCARD
                    else field_node.schema_column.identity
                )
                specs.append(
                    AggregationSpec(
                        alias=aggregator.schema_column.identity,
                        function=fn,
                        column=column,
                    )
                )
        return specs

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
        if value in ("ONE", "ANY_VALUE"):
            return "hash_one"
        if value in ("DISTINCT", "COUNT_DISTINCT"):
            return "count_distinct"
        raise UnsupportedSyntaxError(f"Unsupported aggregate function for Draken group-by: {value}")

    def execute(self, morsel: pyarrow.Table | Morsel, **kwargs):
        _ = kwargs

        if self._needs_arrow_eval:
            arrow_table = self.ensure_arrow_table(morsel)
            if arrow_table != EOS:
                arrow_table = project(arrow_table, list(self.all_identifiers))
                if "*" not in arrow_table.column_names:
                    arrow_table = arrow_table.append_column(
                        "*", [numpy.ones(shape=arrow_table.num_rows, dtype=numpy.int8)]
                    )
                eval_start = time.monotonic_ns()
                if self.evaluatable_nodes:
                    arrow_table = evaluate_and_append(self.evaluatable_nodes, arrow_table)
                arrow_table = evaluate_and_append(self.groups, arrow_table)
                self.readings["time_group_by_evaluations"] += time.monotonic_ns() - eval_start
            draken = self.ensure_draken_morsel(arrow_table)
        else:
            draken = self.ensure_draken_morsel(morsel)

        if draken == EOS:
            st = time.monotonic_ns()
            emitted = 0
            for result in self._group_by.finalize_morsels(chunk_size=CHUNK_SIZE):
                emitted += 1
                yield result.to_arrow()
            self.readings["time_groupby_finalize"] += time.monotonic_ns() - st
            self.readings["groupby_output_morsels"] += emitted

            yield EOS
            return

        ingest_start = time.monotonic_ns()
        if isinstance(draken, Morsel):
            if self._required_columns:
                draken = draken.select(self._required_columns)
            self._group_by.ingest(draken)
            self.readings["time_groupby_ingest"] += time.monotonic_ns() - ingest_start
            yield None
            return

        for chunk in draken:
            if chunk is None or chunk is EOS or chunk.num_rows == 0:
                continue
            if self._required_columns:
                chunk = chunk.select(self._required_columns)
            self._group_by.ingest(chunk)

        self.readings["time_groupby_ingest"] += time.monotonic_ns() - ingest_start

        yield None
