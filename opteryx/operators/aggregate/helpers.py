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
    "CIDR_AGG": "cidr_agg",
    "CORR": "corr",
    "COUNT": "count",
    "COUNT_DISTINCT": "count_distinct",
    "MAX": "max",
    "AVG": "mean",
    "MEDIAN": "median",
    "MIN": "min",
    "ANY_VALUE": "any_value",
    "SUM": "sum",
    "STDDEV": "stddev",
    "STDDEV_POP": "stddev",   # pure alias — STDDEV is already population stddev
    "STDDEV_SAMP": "stddev_samp",
    "VAR_POP": "var_pop",
    "VAR_SAMP": "var_samp",
}

# Aggregate SPELLINGS that are not aggregates in their own right: the builder
# rewrites each to a base aggregate carrying duplicate_treatment="Distinct"
# (see logical_planner_builders), so `COUNT_DISTINCT(x)` and `COUNT(DISTINCT x)`
# are the same node by the time anything downstream sees them. The map lives here,
# with the registry it rewrites INTO, because two readers need it: the builder that
# performs the rewrite, and reference/window_catalog.py, which has to answer the
# window-support question for the name the USER writes, not the rewritten one.
DISTINCT_SPELLINGS: dict = {
    "COUNT_DISTINCT": "COUNT",
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
            NodeType.CASE,
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

    # A literal that is the DIRECT input of an aggregator (e.g. MIN(1)) must be
    # materialised so its collector has a column to read. A literal that only
    # appears nested inside another evaluatable input (e.g. the 0 in SUM(x + 0))
    # is computed as part of that parent expression, so a separate constant column
    # would be redundant — drop those. Stripping ALL literals would leave a
    # direct-input literal collector with no column (CxxMorsel: column not found)
    # whenever it shares the aggregate with a column-input aggregate.
    direct_input_ids = {
        id(aggregator.parameters[0])
        for aggregator in get_all_nodes_of_type(
            aggregates, select_nodes=(NodeType.AGGREGATOR,)
        )
        if aggregator.parameters
    }
    evaluatable_nodes = [
        node
        for node in evaluatable_nodes
        if node.node_type != NodeType.LITERAL or id(node) in direct_input_ids
    ]

    return evaluatable_nodes
