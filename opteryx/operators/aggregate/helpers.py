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
    "PERCENTILE_CONT": "percentile_cont",
    "MIN": "min",
    "ANY_VALUE": "any_value",
    "SUM": "sum",
    "STDDEV": "stddev",
    "STDDEV_POP": "stddev",   # pure alias — STDDEV is already population stddev
    "STDDEV_SAMP": "stddev_samp",
    "VAR_POP": "var_pop",
    "VAR_SAMP": "var_samp",
}

# One line per aggregate, keyed like AGGREGATORS. Two readers: SHOW FUNCTIONS
# (opteryx/managers/virtual_datasets/functions_data.py), which reports it at
# runtime, and reference/aggregate_catalog.py, which exports it to
# aggregates.json. It lives here, not in reference/, because reference/ is not
# part of the installed package.
AGGREGATE_SUMMARIES: dict = {
    "ANY_VALUE": "Returns one non-null value from the input set.",
    "APPROX_COUNT_DISTINCT": "Estimates the number of distinct input values.",
    "APPROX_PERCENTILE": "Estimates a percentile using sketch-based aggregation.",
    "ARRAY_AGG": "Collects input values into an array.",
    "CIDR_AGG": "Collects IPv4 addresses into the smallest list of CIDR blocks that covers exactly those addresses.",
    "AVG": "Computes the arithmetic mean of the input values.",
    "CORR": "Computes the Pearson correlation coefficient between two numeric columns.",
    "COUNT": "Counts rows or non-null input values.",
    "COUNT_DISTINCT": "Counts distinct non-null input values.",
    "MAX": "Returns the largest non-null input value.",
    "MEDIAN": "Computes the exact median (middle value) of the input values.",
    "MIN": "Returns the smallest non-null input value.",
    "PERCENTILE_CONT": "Computes the exact percentile of the input values, interpolating between neighbours.",
    "SUM": "Sums the input values.",
    "STDDEV": "Computes the population standard deviation of the input values.",
    "STDDEV_POP": "Computes the population standard deviation of the input values.",
    "STDDEV_SAMP": "Computes the sample standard deviation of the input values.",
    "VAR_POP": "Computes the population variance of the input values.",
    "VAR_SAMP": "Computes the sample variance of the input values.",
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

# Ordered-set aggregates: `AGG(fraction) WITHIN GROUP (ORDER BY expr)`, the values
# arriving through WITHIN GROUP rather than the argument list. Two readers, like
# DISTINCT_SPELLINGS: the builder that lowers the syntax (and refuses OVER on it),
# and reference/window_catalog.py, which must not claim a window form for them.
ORDERED_SET_AGGREGATES = frozenset({"PERCENTILE_CONT"})


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
