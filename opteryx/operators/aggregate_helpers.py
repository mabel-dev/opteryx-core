# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Shared aggregate metadata and expression helpers used by active operators."""

from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type

AGGREGATORS: dict = {
    "APPROX_COUNT_DISTINCT": "approx_count_distinct",
    "APPROX_PERCENTILE": "approx_percentile",
    "ARRAY_AGG": "hash_list",
    "COUNT": "count",
    "COUNT_DISTINCT": "count_distinct",
    "MAX": "max",
    "AVG": "mean",
    "MIN": "min",
    "ANY_VALUE": "any_value",
    "SUM": "sum",
}


def is_aggregator(name: str) -> bool:
    return name in AGGREGATORS


def aggregator_names() -> list:
    return list(AGGREGATORS.keys())


def extract_evaluations(aggregates):
    """Collect non-aggregate inner expressions that must be pre-evaluated."""

    all_evaluatable_nodes = get_all_nodes_of_type(
        aggregates,
        select_nodes=(
            NodeType.FUNCTION,
            NodeType.BINARY_OPERATOR,
            NodeType.EXTRACTION_OPERATOR,
            NodeType.COMPARISON_OPERATOR,
            NodeType.CAST,
            NodeType.LITERAL,
        ),
    )

    evaluatable_nodes = []
    for node in all_evaluatable_nodes:
        aggregators = get_all_nodes_of_type(node, select_nodes=(NodeType.AGGREGATOR,))
        if len(aggregators) == 0:
            evaluatable_nodes.append(node)

    literal_count = len([n for n in evaluatable_nodes if n.node_type == NodeType.LITERAL])
    if 0 < literal_count < len(evaluatable_nodes):
        evaluatable_nodes = [n for n in evaluatable_nodes if n.node_type != NodeType.LITERAL]

    return evaluatable_nodes
