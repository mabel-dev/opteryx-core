# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Simple Grouping Node

This is a SQL Query Execution Plan Node.

This is the grouping node, this specialized version only performs aggregations that result in,
and are collected as, a single value.
"""

import time

import numpy
import pyarrow
from orso.types import OrsoTypes

from opteryx import EMPTY
from opteryx import EOS
from opteryx.draken import Morsel
from opteryx.expression import NodeType
from opteryx.expression import evaluate_and_append
from opteryx.expression import get_all_nodes_of_type
from opteryx.models import QueryProperties
from opteryx.operators.aggregate_node import AGGREGATORS
from opteryx.operators.aggregate_node import build_aggregations
from opteryx.operators.aggregate_node import extract_evaluations
from opteryx.operators.aggregate_node import project

from . import BasePlanNode

CHUNK_SIZE = 65536


def _append_star_constant_morsel(morsel: Morsel) -> Morsel:
    from opteryx.draken.vectors.constant_vector import from_scalar as constant_from_scalar

    names = morsel.column_names
    vectors = [
        morsel.column(name if isinstance(name, bytes) else name.encode("utf-8")) for name in names
    ]
    names.append("*")
    vectors.append(constant_from_scalar(1, morsel.num_rows, dtype=pyarrow.int8()))
    return Morsel.from_vectors(names, vectors)


def build_finalizer_aggregations(aggregators):
    column_map = {}
    aggs = []

    if not isinstance(aggregators, list):
        aggregators = [aggregators]

    for root in aggregators:
        for aggregator in get_all_nodes_of_type(root, select_nodes=(NodeType.AGGREGATOR,)):
            count_options = None

            field_name = aggregator.schema_column.identity
            if aggregator.value == "COUNT":
                function = AGGREGATORS["SUM"]
            else:
                function = AGGREGATORS[aggregator.value]
            # if the array agg is distinct, base off that function instead
            aggs.append((field_name, function, count_options))
            column_map[aggregator.schema_column.identity] = f"{field_name}_{function}".replace(
                "_hash_", "_"
            )

    return column_map, aggs


class SimpleAggregateAndGroupNode(BasePlanNode):
    SIMPLE_AGGREGATES = {"SUM", "MIN", "MAX", "COUNT", "AVG", "COUNT_DISTINCT"}

    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.groups = list(parameters["groups"])
        self.aggregates = list(parameters["aggregates"])
        projection = list(parameters["projection"])

        # Replace offset based GROUP BYs with their column
        self.groups = [
            (
                group
                if not (group.node_type == NodeType.LITERAL and group.type == OrsoTypes.INTEGER)
                else projection[group.value - 1]
            )
            for group in self.groups
        ]

        # get all the columns anywhere in the groups or aggregates
        all_identifiers = [
            node.schema_column.identity
            for node in get_all_nodes_of_type(
                self.groups + self.aggregates, select_nodes=(NodeType.IDENTIFIER,)
            )
        ]
        self.all_identifiers = list(dict.fromkeys(all_identifiers))

        # Get any functions we need to execute before aggregating
        self.evaluatable_nodes = extract_evaluations(self.aggregates)

        # get the aggregated groupings and functions
        self.group_by_columns = list({node.schema_column.identity for node in self.groups})
        self.column_map, self.aggregate_functions = build_aggregations(self.aggregates)

        self.finalizer_map, self.finalizer_aggregations = build_finalizer_aggregations(
            self.aggregates
        )
        self._is_multi_agg = len(self.aggregate_functions) > 1

        self._use_draken_ops = False
        try:
            from opteryx.config import features

            self._use_draken_ops = bool(features.use_draken_ops_kernels)
        except Exception:
            self._use_draken_ops = False

        self._draken_ops_shape_supported = (
            len(self.group_by_columns) == 1
            and len(self.aggregate_functions) == 1
            and (
                (
                    self.aggregate_functions[0][1] == "count"
                    and self.aggregate_functions[0][0] == "*"
                )
                or (
                    self.aggregate_functions[0][1] in ("mean", "count_distinct")
                    and self.aggregate_functions[0][0] != "*"
                )
            )
        )

        self.buffer = []

    @staticmethod
    def _concat_tables(tables):
        if not tables:
            return pyarrow.Table.from_pydict({})
        try:
            return pyarrow.concat_tables(tables, promote_options="none")
        except Exception:
            return pyarrow.concat_tables(tables, promote_options="permissive")

    @property
    def config(self):  # pragma: no cover
        from opteryx.expression import format_expression

        return f"AGGREGATE ({', '.join(format_expression(col) for col in self.aggregates)}) GROUP BY ({', '.join(format_expression(col) for col in self.groups)})"

    @property
    def name(self):  # pragma: no cover
        return "Group By Simple"

    def execute(self, morsel: pyarrow.Table, **kwargs):
        internal_names = list(self.column_map.values()) + self.group_by_columns
        column_names = list(self.column_map.keys()) + self.group_by_columns

        if morsel == EOS:
            start = time.monotonic_ns()

            self.readings["groupby_buffer_morsels"] += len(self.buffer)

            internal_names = list(self.finalizer_map.values()) + self.group_by_columns
            column_names = list(self.finalizer_map.keys()) + self.group_by_columns

            groups = self._concat_tables(self.buffer)
            self.buffer.clear()
            phase2_rows_in = groups.num_rows
            self.readings["groupby_phase2_rows_in"] += phase2_rows_in
            groups = groups.group_by(self.group_by_columns)
            groups = groups.aggregate(self.finalizer_aggregations)
            groups = groups.select(internal_names)
            groups = groups.rename_columns(column_names)
            phase2_groups_out = groups.num_rows
            self.readings["groupby_phase2_groups_out"] += phase2_groups_out

            if self._is_multi_agg:
                self.readings["groupby_multiagg_phase2_rows_in"] += phase2_rows_in
                self.readings["groupby_multiagg_phase2_groups_out"] += phase2_groups_out

            self.readings["time_groupby_finalize"] += time.monotonic_ns() - start

            num_rows = groups.num_rows
            for start in range(0, num_rows, CHUNK_SIZE):
                yield groups.slice(start, min(CHUNK_SIZE, num_rows - start))

            yield EOS
            return

        if isinstance(morsel, Morsel):
            if self.all_identifiers:
                morsel = morsel.select(self.all_identifiers)
        else:
            morsel = self.ensure_arrow_table(morsel)
            morsel = project(morsel, self.all_identifiers)

        # Allow grouping by functions by evaluating them first
        if self.evaluatable_nodes:
            morsel = evaluate_and_append(self.evaluatable_nodes, morsel)

        morsel = evaluate_and_append(self.groups, morsel)
        self.readings["groupby_phase1_rows_in"] += morsel.num_rows
        self.readings["groupby_phase1_morsels_in"] += 1
        if self._is_multi_agg:
            self.readings["groupby_multiagg_phase1_rows_in"] += morsel.num_rows
            self.readings["groupby_multiagg_phase1_morsels_in"] += 1

        # Add a "*" column, this is an int because when a bool it miscounts
        if "*" not in morsel.column_names:
            if isinstance(morsel, Morsel):
                morsel = _append_star_constant_morsel(morsel)
            else:
                morsel = morsel.append_column(
                    "*", [numpy.full(shape=morsel.num_rows, fill_value=1, dtype=numpy.int8)]
                )

        morsel_arrow = morsel.to_arrow() if isinstance(morsel, Morsel) else morsel

        # use pyarrow to do phase 1 of the group by
        st = time.monotonic_ns()
        # Try using Draken-based grouped aggregation when available and shape-compatible.
        use_draken = self._use_draken_ops and self._draken_ops_shape_supported

        if use_draken:
            from opteryx.compiled.aggregations.group_by_draken import group_by_morsel

            try:
                groups = group_by_morsel(
                    morsel,
                    self.group_by_columns,
                    self.aggregate_functions,
                    internal_names,
                    column_names,
                )
            except Exception:
                groups = None

            if groups is None:
                # Disable Draken ops for the rest of this operator execution to avoid
                # repeated failures on unsupported runtime types.
                self._use_draken_ops = False
                groups = morsel_arrow.group_by(self.group_by_columns)
                groups = groups.aggregate(self.aggregate_functions)
                groups = groups.select(internal_names)
                groups = groups.rename_columns(column_names)
            else:
                # If the group_by_morsel returned grouped values in internal names order,
                # then rename to canonical alias names accordingly to keep downstream logic.
                # If we receive a Draken Morsel, convert to Arrow for further ops
                if isinstance(groups, Morsel):
                    groups = groups.to_arrow()
                # Rename first len(internal_names) columns to alias names
                groups = groups.rename_columns(list(column_names))

        else:
            groups = morsel_arrow.group_by(self.group_by_columns)
            groups = groups.aggregate(self.aggregate_functions)
            groups = groups.select(internal_names)
            groups = groups.rename_columns(column_names)

        self.readings["groupby_phase1_groups_out"] += groups.num_rows
        if self._is_multi_agg:
            self.readings["groupby_multiagg_phase1_groups_out"] += groups.num_rows
        self.readings["time_pregrouping"] += time.monotonic_ns() - st

        self.buffer.append(groups)

        yield EMPTY
