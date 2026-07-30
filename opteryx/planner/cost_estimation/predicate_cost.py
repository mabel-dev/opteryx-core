# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Predicate evaluation cost estimation.

Pure functions that take a parsed predicate Node and return a relative
per-row evaluation cost. No plan walking, no manifest access -- mirrors
``selectivity.py``'s contract, the other half of the (selectivity, cost)
pair ``cost_estimation.predicate_ordering.PredicateStats`` consumes.

Relocated (2026-07-30) out of ``optimizer/strategies/predicate_ordering.py``,
which was its only consumer, to also be a reusable source for plan-estimate
telemetry -- callers outside that one strategy should read the cost model
from here, not from strategy internals.
"""

from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.types.logical_type import LogicalCategory

# Approximate of the time in seconds (3sf) to compare 1 million records
# These are the core comparisons, Eq, NotEq, Gt, GtEq, Lt, LtEq
BASIC_COMPARISON_COSTS = {
    LogicalCategory.ARRAY: 10.00,  # expensive
    LogicalCategory.VARBINARY: 0.058,  # varies based on length, this is 50 bytes
    LogicalCategory.NVARCHAR: 10.00,  # JSON/complex text (treat as expensive)
    LogicalCategory.BOOLEAN: 0.003,
    LogicalCategory.DATE: 0.008,
    LogicalCategory.DECIMAL: 1.533,
    LogicalCategory.FLOAT: 0.002,
    LogicalCategory.INTEGER: 0.001,
    LogicalCategory.INTERVAL: 10.00,  # expensive
    LogicalCategory.TIMESTAMP: 0.008,
    LogicalCategory.TIME: 10.00,  # expensive
    LogicalCategory.VARCHAR: 0.231,  # varies based on length, this is 50 chars
    LogicalCategory.NULL: 10.00,  # for completeness
    None: 10.00,  # unknown type — treat as expensive
}

# Operation-specific costs (override type-based costs)
# Pattern matching operations are significantly more expensive than simple comparisons
OPERATION_COSTS = {
    "InStr": 2.5,  # substring search (Volnitsky algorithm)
    "IInStr": 2.5,  # case-insensitive substring search
    "NotInStr": 2.5,
    "NotIInStr": 2.5,
    "Like": 2.5,  # pattern matching
    "ILike": 2.5,
    "NotLike": 2.5,
    "NotILike": 2.5,
    "RLike": 3.0,  # regex is even more expensive
    "NotRLike": 3.0,
}

# Fallback µs/million cost for a FUNCTION with no measured catalog cost (legacy
# backfill entries with cost 0.0) -- treated as expensive, not free.
_UNKNOWN_FUNCTION_COST = 100.0


def base_cost(condition) -> float:
    """Relative per-row evaluation cost for a simple (non-function) comparison."""
    op = getattr(condition, "value", None)
    if op in OPERATION_COSTS:
        return OPERATION_COSTS[op]
    col = getattr(condition, "left", None)
    col_type = getattr(col, "schema_column", None)
    if col_type is None:
        return 10.0
    return BASIC_COMPARISON_COSTS.get(col_type.category, 10.0)


def catalog_function_cost(node) -> float:
    """Sum catalog cost estimates for all FUNCTION nodes in the expression subtree.

    Falls back to ``_UNKNOWN_FUNCTION_COST`` for any function with cost 0.0.
    """
    from opteryx.expression.functions import get_catalog

    total = 0.0
    for func_node in get_all_nodes_of_type(node, (NodeType.FUNCTION,)):
        cost = get_catalog().get_cost(func_node.value) or 0.0
        total += cost if cost > 0.0 else _UNKNOWN_FUNCTION_COST
    return total


def predicate_cost(condition) -> float:
    """Relative per-row cost for any predicate: function-containing or simple."""
    if get_all_nodes_of_type(condition, (NodeType.FUNCTION,)):
        return catalog_function_cost(condition)
    return base_cost(condition)
