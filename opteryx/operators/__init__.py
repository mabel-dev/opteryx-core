# isort: skip

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Physical Execution Operators

This module contains all physical execution operators used by Opteryx's query engine.
Operators form a tree structure where each node processes data and passes results to
its parent node, implementing the iterator pattern for memory-efficient processing.

Operator Categories:

Data Sources:
- ReaderNode: Reads data from connectors (files, databases, etc.)
- NullReaderNode: Returns empty table with correct schema (for contradictory predicates)
- FunctionDatasetNode: Generates data from function calls

Joins:
- DrakenInnerJoinNode: Standard inner joins
- OuterJoinNode: Left, right, and full outer joins
- CrossJoinNode: Cartesian product joins
- NestedLoopJoinNode: Nested loop algorithm for joins
- NonEquiJoinNode: Non-equi joins (!=, >, >=, <, <=) using draken
- UnnestJoinNode: Specialized join for array unnesting

Filtering and Selection:
- FilterNode: Applies WHERE clause predicates
- FilterJoinNode: Optimized filter for join conditions
- ProjectionNode: Column selection and expression evaluation
- DistinctNode: Removes duplicate rows

Aggregation:
- DrakenAggregateNode: Global aggregation
- DrakenAggregateAndGroupNode: GROUP BY with aggregation

Sorting and Limiting:
- SortNode: ORDER BY implementation using TimSort
- HeapSortNode: Heap-based sorting for TOP-N queries
- LimitNode: LIMIT/OFFSET clause implementation

Set Operations:
- UnionNode: UNION and UNION ALL operations

Control and Meta:
- ExitNode: Query execution termination
- ExplainNode: Query plan explanation
- ShowColumnsNode: SHOW COLUMNS implementation
- ShowCreateNode: SHOW CREATE VIEW implementation
- ShowValueNode: SHOW variable value display
- SetVariableNode: SET variable operations
- ViewManagementNode: CREATE/ALTER/DROP VIEW operations
- TableManagementNode: ANALYZE TABLE operations

Base Classes:
- BasePlanNode: Base class for all operators
- JoinNode: Base class for join operators

Operator Development:
1. Inherit from BasePlanNode or appropriate base class
2. Implement execute() method that processes data morsels
3. Handle memory management and resource cleanup
4. Support statistics collection for optimization
5. Add comprehensive tests

Example:
    class MyOperatorNode(BasePlanNode):
        def execute(self, morsel):
            # Process the data morsel
            processed_data = self.process_data(morsel)
            yield processed_data

Performance Considerations:
- Use vectorized operations with PyArrow when possible
- Implement memory pooling for large data processing
- Consider async operations for I/O bound operators
- Profile memory usage and optimize accordingly
"""

from .base_plan_node import BasePlanNode, JoinNode  # isort: skip
from .aggregate_helpers import AGGREGATORS
from .catalog import (  # Operator metadata and registry
    OperatorCategory,
    ParallelStrategy,
    get_registry,
)
from .read_node import ReaderNode

__all__ = [
    "OperatorCategory",
    "ParallelStrategy",
    "get_registry",
    "AGGREGATORS",
    "is_aggregator",
    "aggregators",
    "ReaderNode",
]


def is_aggregator(name):
    return name in AGGREGATORS


def aggregators():
    return list(AGGREGATORS.keys())
