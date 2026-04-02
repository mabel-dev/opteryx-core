# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Operator catalog — centralized registry of static metadata for relational operators.

All operator metadata (category, parallelism strategy, dispatch mapping, etc.) lives
here. Operator classes themselves remain clean — they only carry the runtime flags
needed directly by the engine (is_scan, is_join, is_stateless, is_not_explained).

Usage:
    from opteryx.operators.catalog import get_registry, OperatorCategory

    registry = get_registry()
    metadata = registry.get(FilterNode)
    node = registry.create('Filter', query_properties, **config)
"""

from dataclasses import dataclass
from enum import Enum
from threading import RLock
from typing import Dict  # noqa: F401 (Optional used in get() return type)
from typing import Optional
from typing import Type


class OperatorCategory(Enum):
    """Classification of operator types for visualization and scheduling."""

    SCAN = "scan"
    JOIN = "join"
    FILTER = "filter"
    PROJECT = "project"
    AGGREGATE = "aggregate"
    SORT = "sort"
    SET_OP = "set_op"
    LIMIT = "limit"
    DDL = "ddl"
    IO = "io"


class ParallelStrategy(Enum):
    """Intended execution strategy for an operator."""

    SINGLE_THREAD = "single_thread"
    MULTI_THREAD = "multi_thread"
    ASYNC = "async"


@dataclass(frozen=True)
class OperatorMetadata:
    """Static metadata about an operator class."""

    name: str
    operator_class: Type
    category: OperatorCategory
    parallel_strategy: ParallelStrategy = ParallelStrategy.SINGLE_THREAD
    is_pipeline_breaking: bool = False
    is_join: bool = False
    is_scan: bool = False
    is_stateless: bool = False
    is_not_explained: bool = False
    target_queue_depth: int = 0
    batch_size: int = 2048


class OperatorRegistry:
    """Thread-safe registry of operator metadata."""

    def __init__(self):
        self._metadata: Dict[Type, OperatorMetadata] = {}
        self._by_name: Dict[str, OperatorMetadata] = {}
        self._lock = RLock()

    def register(
        self,
        operator_class: Type,
        *,
        name: str,
        category: OperatorCategory,
        parallel_strategy: ParallelStrategy = ParallelStrategy.SINGLE_THREAD,
        is_pipeline_breaking: bool = False,
        is_join: bool = False,
        is_scan: bool = False,
        is_stateless: bool = False,
        is_not_explained: bool = False,
        target_queue_depth: int = 0,
        batch_size: int = 2048,
    ) -> None:
        """Register an operator class with its metadata."""
        with self._lock:
            metadata = OperatorMetadata(
                name=name,
                operator_class=operator_class,
                category=category,
                parallel_strategy=parallel_strategy,
                is_pipeline_breaking=is_pipeline_breaking,
                is_join=is_join,
                is_scan=is_scan,
                is_stateless=is_stateless,
                is_not_explained=is_not_explained,
                target_queue_depth=target_queue_depth,
                batch_size=batch_size,
            )
            self._metadata[operator_class] = metadata
            self._by_name[name] = metadata

    def get(self, operator_class: Type) -> Optional[OperatorMetadata]:
        """Get metadata for an operator class."""
        with self._lock:
            return self._metadata.get(operator_class)

    def get_by_name(self, name: str) -> Optional[OperatorMetadata]:
        """Get metadata by registered name."""
        with self._lock:
            return self._by_name.get(name)

    def create(self, name: str, properties, **kwargs):
        """Instantiate an operator by its registered name."""
        with self._lock:
            meta = self._by_name.get(name)
        if meta is None:
            raise KeyError(f"No operator registered with name '{name}'")
        return meta.operator_class(properties, **kwargs)

    def list(self) -> list:
        """List all registered operator classes."""
        with self._lock:
            return list(self._metadata.keys())


# ---------------------------------------------------------------------------
# Global singleton
# ---------------------------------------------------------------------------

_global_registry: Optional[OperatorRegistry] = None
_registry_lock = RLock()


def _build_registry() -> OperatorRegistry:
    """Explicitly register every operator with its metadata. No magic."""
    # Local imports to avoid circular dependencies at module load time.
    from opteryx.operators.cross_join_node import CrossJoinNode
    from opteryx.operators.distinct_node import DistinctNode
    from opteryx.operators.draken_aggregate_and_group_node import DrakenAggregateAndGroupNode
    from opteryx.operators.draken_aggregate_node import DrakenAggregateNode
    from opteryx.operators.draken_inner_join_node import DrakenInnerJoinNode
    from opteryx.operators.exit_node import ExitNode
    from opteryx.operators.explain_node import ExplainNode
    from opteryx.operators.filter_join_node import FilterJoinNode
    from opteryx.operators.filter_node import FilterNode
    from opteryx.operators.function_dataset_node import FunctionDatasetNode
    from opteryx.operators.heap_sort_node import HeapSortNode
    from opteryx.operators.limit_node import LimitNode
    from opteryx.operators.nested_loop_join_node import NestedLoopJoinNode
    from opteryx.operators.non_equi_join_node import NonEquiJoinNode
    from opteryx.operators.null_reader_node import NullReaderNode
    from opteryx.operators.outer_join_node import OuterJoinNode
    from opteryx.operators.parquet_read_node import ParquetReadNode
    from opteryx.operators.projection_node import ProjectionNode
    from opteryx.operators.read_node import ReaderNode
    from opteryx.operators.set_variable_node import SetVariableNode
    from opteryx.operators.show_columns_node import ShowColumnsNode
    from opteryx.operators.show_create_node import ShowCreateNode
    from opteryx.operators.show_value_node import ShowValueNode
    from opteryx.operators.shuffle_node import ShuffleNode
    from opteryx.operators.sort_node import SortNode
    from opteryx.operators.table_management_node import TableManagementNode
    from opteryx.operators.union_node import UnionNode
    from opteryx.operators.unnest_join_node import UnnestJoinNode
    from opteryx.operators.view_management_node import ViewManagementNode

    r = OperatorRegistry()

    # -- Scan operators -------------------------------------------------------
    r.register(
        ReaderNode,
        name="Reader",
        category=OperatorCategory.SCAN,
        parallel_strategy=ParallelStrategy.MULTI_THREAD,
        is_scan=True,
    )
    r.register(
        ParquetReadNode,
        name="Parquet Reader",
        category=OperatorCategory.SCAN,
        parallel_strategy=ParallelStrategy.MULTI_THREAD,
        is_scan=True,
    )
    r.register(
        NullReaderNode,
        name="Null Reader",
        category=OperatorCategory.SCAN,
        is_scan=True,
    )
    r.register(
        FunctionDatasetNode,
        name="Function Dataset",
        category=OperatorCategory.SCAN,
        is_scan=True,
    )

    # -- Filter / project operators -------------------------------------------
    r.register(
        FilterNode,
        name="Filter",
        category=OperatorCategory.FILTER,
        parallel_strategy=ParallelStrategy.MULTI_THREAD,
        is_stateless=True,
    )
    r.register(
        ProjectionNode,
        name="Projection",
        category=OperatorCategory.PROJECT,
        parallel_strategy=ParallelStrategy.MULTI_THREAD,
        is_stateless=True,
    )
    r.register(
        DistinctNode,
        name="Distinct",
        category=OperatorCategory.SET_OP,
        is_pipeline_breaking=True,
    )

    # -- Aggregate operators --------------------------------------------------
    r.register(
        DrakenAggregateNode,
        name="Aggregate",
        category=OperatorCategory.AGGREGATE,
        is_pipeline_breaking=True,
    )
    r.register(
        DrakenAggregateAndGroupNode,
        name="Aggregate and Group",
        category=OperatorCategory.AGGREGATE,
        is_pipeline_breaking=True,
    )

    # -- Sort / limit operators -----------------------------------------------
    r.register(
        SortNode,
        name="Sort",
        category=OperatorCategory.SORT,
        is_pipeline_breaking=True,
    )
    r.register(
        HeapSortNode,
        name="Heap Sort",
        category=OperatorCategory.SORT,
        is_pipeline_breaking=True,
    )
    r.register(
        LimitNode,
        name="Limit",
        category=OperatorCategory.LIMIT,
    )

    # -- Set operations -------------------------------------------------------
    r.register(
        UnionNode,
        name="Union",
        category=OperatorCategory.SET_OP,
        is_pipeline_breaking=True,
    )
    r.register(
        ShuffleNode,
        name="Shuffle",
        category=OperatorCategory.SET_OP,
        is_pipeline_breaking=True,
    )

    # -- Join operators -------------------------------------------------------
    r.register(
        DrakenInnerJoinNode,
        name="Inner Join",
        category=OperatorCategory.JOIN,
        is_join=True,
        is_pipeline_breaking=True,
    )
    r.register(
        OuterJoinNode,
        name="Outer Join",
        category=OperatorCategory.JOIN,
        is_join=True,
        is_pipeline_breaking=True,
    )
    r.register(
        CrossJoinNode,
        name="Cross Join",
        category=OperatorCategory.JOIN,
        is_join=True,
        is_pipeline_breaking=True,
    )
    r.register(
        NestedLoopJoinNode,
        name="Nested Loop Join",
        category=OperatorCategory.JOIN,
        is_join=True,
        is_pipeline_breaking=True,
    )
    r.register(
        FilterJoinNode,
        name="Filter Join",
        category=OperatorCategory.JOIN,
        is_join=True,
        is_pipeline_breaking=True,
    )
    r.register(
        NonEquiJoinNode,
        name="Non Equi Join",
        category=OperatorCategory.JOIN,
        is_join=True,
        is_pipeline_breaking=True,
    )
    r.register(
        UnnestJoinNode,
        name="Unnest Join",
        category=OperatorCategory.JOIN,
        is_join=True,
    )

    # -- DDL / control operators ----------------------------------------------
    r.register(
        ExitNode,
        name="Exit",
        category=OperatorCategory.IO,
    )
    r.register(
        ExplainNode,
        name="Explain",
        category=OperatorCategory.DDL,
        is_not_explained=True,
    )
    r.register(
        SetVariableNode,
        name="Set Variable",
        category=OperatorCategory.DDL,
        is_not_explained=True,
    )
    r.register(
        ShowColumnsNode,
        name="Show Columns",
        category=OperatorCategory.DDL,
        is_not_explained=True,
    )
    r.register(
        ShowCreateNode,
        name="Show Create",
        category=OperatorCategory.DDL,
        is_not_explained=True,
    )
    r.register(
        ShowValueNode,
        name="Show Value",
        category=OperatorCategory.DDL,
        is_not_explained=True,
    )
    r.register(
        TableManagementNode,
        name="Table Management",
        category=OperatorCategory.DDL,
        is_not_explained=True,
    )
    r.register(
        ViewManagementNode,
        name="View Management",
        category=OperatorCategory.DDL,
        is_not_explained=True,
    )

    return r


def get_registry() -> OperatorRegistry:
    """Get the global operator registry singleton, building it on first call."""
    global _global_registry
    if _global_registry is None:
        with _registry_lock:
            if _global_registry is None:
                _global_registry = _build_registry()
    return _global_registry
