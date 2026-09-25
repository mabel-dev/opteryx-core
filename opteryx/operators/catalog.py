# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Operator catalog — centralized registry of static metadata for relational operators.

All operator metadata (category, parallelism strategy, dispatch mapping, etc.) lives
here, keyed by the operator's KIND — the name the compiler dispatches on and the
native engine reports ("FilterNode", "ParquetReadNode"). Two sorts of operator are
registered:

  * a PhysicalStep kind — an operator with no behaviour of its own, which the
    native compiler lowers straight from its typed logical step. It has no class:
    `create_step` builds the one generic PhysicalStep for it.
  * an operator CLASS — one that does real work (a scan reader, the function
    dataset, a DDL or write sink). Its kind is its class name; `create` builds it.

Usage:
    from opteryx.operators.catalog import get_registry, OperatorCategory

    registry = get_registry()
    metadata = registry.get_by_kind("FilterNode")
    node = registry.create_step("Filter", query_properties, logical_step)
    node = registry.create("Parquet Reader", query_properties, **config)
"""

from dataclasses import dataclass
from enum import Enum
from threading import RLock
from typing import Dict
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


class OperatorParallelism(Enum):
    """How an operator may be parallelised — the thread-safety contract a future
    parallel engine must honour (see docs/EXECUTION_THREAD_SAFETY_CONTRACT.md).

    Orthogonal to ``ParallelStrategy`` (thread vs async dispatch); this captures
    the cloning/merging semantics:

    - STATELESS:           no cross-morsel state; clone or share freely.
    - STATEFUL_MERGEABLE:  clone one instance per worker, combine partials via
                           the operator's ``merge()`` (aggregates, distinct).
    - STATEFUL_SERIAL:     must see all input on a single instance — the safe
                           default and exactly today's behaviour.
    - SINGLETON:           one instance joins N input chains; cannot be cloned
                           (Union, the terminal Exit).

    This is metadata only: the serial engine ignores it. It exists so a parallel
    scheduler can decide per-operator without re-deriving the contract.
    """

    STATELESS = "stateless"
    STATEFUL_MERGEABLE = "stateful_mergeable"
    STATEFUL_SERIAL = "stateful_serial"
    SINGLETON = "singleton"


@dataclass(frozen=True)
class OperatorMetadata:
    """Static metadata about an operator kind."""

    name: str
    # The operator's kind (see the module docstring).
    kind: str
    # The class that implements it, or None for a PhysicalStep kind.
    operator_class: Optional[Type]
    category: OperatorCategory
    parallel_strategy: ParallelStrategy = ParallelStrategy.SINGLE_THREAD
    parallelism: OperatorParallelism = OperatorParallelism.STATEFUL_SERIAL
    # Retired old-engine field (parallel-sink spec); kept as a slot so historical
    # registrations parse, always None now — the native engine owns parallelism.
    parallel_sink: Optional[object] = None
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
        self._by_kind: Dict[str, OperatorMetadata] = {}
        self._by_name: Dict[str, OperatorMetadata] = {}
        self._lock = RLock()

    def register(self, operator_class: Type, **metadata) -> None:
        """Register an operator CLASS; its kind is its class name."""
        self._register(operator_class.__name__, operator_class, **metadata)

    def register_step(self, kind: str, **metadata) -> None:
        """Register a PhysicalStep kind — an operator with no class of its own."""
        self._register(kind, None, **metadata)

    def _register(
        self,
        kind: str,
        operator_class: Optional[Type],
        *,
        name: str,
        category: OperatorCategory,
        parallel_strategy: ParallelStrategy = ParallelStrategy.SINGLE_THREAD,
        parallelism: OperatorParallelism = OperatorParallelism.STATEFUL_SERIAL,
        parallel_sink: Optional[object] = None,
        is_pipeline_breaking: bool = False,
        is_join: bool = False,
        is_scan: bool = False,
        is_stateless: bool = False,
        is_not_explained: bool = False,
        target_queue_depth: int = 0,
        batch_size: int = 2048,
    ) -> None:
        with self._lock:
            if kind in self._by_kind or name in self._by_name:
                raise ValueError(f"Operator {kind!r} / {name!r} is already registered")
            metadata = OperatorMetadata(
                name=name,
                kind=kind,
                operator_class=operator_class,
                category=category,
                parallel_strategy=parallel_strategy,
                parallelism=parallelism,
                parallel_sink=parallel_sink,
                is_pipeline_breaking=is_pipeline_breaking,
                is_join=is_join,
                is_scan=is_scan,
                is_stateless=is_stateless,
                is_not_explained=is_not_explained,
                target_queue_depth=target_queue_depth,
                batch_size=batch_size,
            )
            self._by_kind[kind] = metadata
            self._by_name[name] = metadata

    def get_by_kind(self, kind: str) -> Optional[OperatorMetadata]:
        """Get metadata by operator kind."""
        with self._lock:
            return self._by_kind.get(kind)

    def get_by_name(self, name: str) -> Optional[OperatorMetadata]:
        """Get metadata by registered name."""
        with self._lock:
            return self._by_name.get(name)

    def _metadata_named(self, name: str) -> OperatorMetadata:
        with self._lock:
            meta = self._by_name.get(name)
        if meta is None:
            raise KeyError(f"No operator registered with name '{name}'")
        return meta

    def create(self, name: str, properties, **kwargs):
        """Instantiate an operator CLASS by its registered name."""
        meta = self._metadata_named(name)
        if meta.operator_class is None:
            raise TypeError(f"'{name}' is a PhysicalStep kind; build it with create_step")
        return meta.operator_class(properties, **kwargs)

    def create_step(self, name: str, properties, step, **physical):
        """Build the PhysicalStep for a registered kind from its typed logical
        step, plus the physical planner's own decisions (`physical`)."""
        from opteryx.operators._operators import PhysicalStep

        meta = self._metadata_named(name)
        if meta.operator_class is not None:
            raise TypeError(f"'{name}' is an operator class; build it with create")
        return PhysicalStep(properties, meta.kind, step, **physical)

    def list(self) -> list:
        """Metadata for every registered operator."""
        with self._lock:
            return list(self._by_kind.values())


# ---------------------------------------------------------------------------
# Global singleton
# ---------------------------------------------------------------------------

_global_registry: Optional[OperatorRegistry] = None
_registry_lock = RLock()


def _build_registry() -> OperatorRegistry:
    """Explicitly register every operator with its metadata. No magic."""
    # Local imports to avoid circular dependencies at module load time.
    from opteryx.operators.compaction_commit import CompactionCommitNode
    from opteryx.operators.csv_read import CsvReadNode
    from opteryx.operators.explain import ExplainNode
    from opteryx.operators.function_dataset import FunctionDatasetNode
    from opteryx.operators.jsonl_read import JsonlReadNode
    from opteryx.operators.skene_read import SkeneReadNode
    from opteryx.operators.postgres_read import PostgresReadNode
    from opteryx.operators.null_reader import NullReaderNode
    from opteryx.operators.parquet_read import ParquetReadNode
    from opteryx.operators.read import ReaderNode
    from opteryx.operators.set_variable import SetVariableNode
    from opteryx.operators.show_columns import ShowColumnsNode
    from opteryx.operators.show_grants import ShowGrantsNode
    from opteryx.operators.show_manifest import ShowManifestNode
    from opteryx.operators.show_snapshots import ShowSnapshotsNode
    from opteryx.operators.show_lineage import ShowLineageNode
    from opteryx.operators.show_sources import ShowSourcesNode
    from opteryx.operators.show_create import ShowCreateNode
    from opteryx.operators.show_value import ShowValueNode
    from opteryx.operators.table_management import TableManagementNode
    from opteryx.operators.view_management import ViewManagementNode
    from opteryx.operators.relation_management import RelationManagementNode
    from opteryx.operators.insert import InsertNode
    from opteryx.operators.merge import MergeNode

    r = OperatorRegistry()

    # -- Scan operators -------------------------------------------------------
    r.register(
        ReaderNode,
        name="Reader",
        category=OperatorCategory.SCAN,
        parallelism=OperatorParallelism.STATELESS,
        parallel_strategy=ParallelStrategy.MULTI_THREAD,
        is_scan=True,
    )
    r.register(
        ParquetReadNode,
        name="Parquet Reader",
        category=OperatorCategory.SCAN,
        parallelism=OperatorParallelism.STATELESS,
        parallel_strategy=ParallelStrategy.MULTI_THREAD,
        is_scan=True,
    )
    r.register(
        NullReaderNode,
        name="Null Reader",
        category=OperatorCategory.SCAN,
        parallelism=OperatorParallelism.STATELESS,
        is_scan=True,
    )
    r.register_step(
        "CteRefNode",
        name="CTE Reference",
        category=OperatorCategory.SCAN,
        parallelism=OperatorParallelism.STATELESS,
        is_scan=True,
    )
    r.register(
        JsonlReadNode,
        name="JSONL Reader",
        category=OperatorCategory.SCAN,
        parallelism=OperatorParallelism.STATELESS,
        parallel_strategy=ParallelStrategy.MULTI_THREAD,
        is_scan=True,
    )
    r.register(
        SkeneReadNode,
        name="Skene Reader",
        category=OperatorCategory.SCAN,
        parallelism=OperatorParallelism.STATELESS,
        parallel_strategy=ParallelStrategy.MULTI_THREAD,
        is_scan=True,
    )
    # One server session per scan: the Source serialises get_morsel on its
    # global state, so extra workers add nothing. SINGLE_THREAD says so.
    r.register(
        PostgresReadNode,
        name="Postgres Reader",
        category=OperatorCategory.SCAN,
        parallelism=OperatorParallelism.STATELESS,
        parallel_strategy=ParallelStrategy.SINGLE_THREAD,
        is_scan=True,
    )
    r.register(
        CsvReadNode,
        name="CSV Reader",
        category=OperatorCategory.SCAN,
        parallelism=OperatorParallelism.STATELESS,
        parallel_strategy=ParallelStrategy.MULTI_THREAD,
        is_scan=True,
    )
    r.register(
        FunctionDatasetNode,
        name="Function Dataset",
        category=OperatorCategory.SCAN,
        parallelism=OperatorParallelism.STATELESS,
        is_scan=True,
    )

    # -- Filter / project operators -------------------------------------------
    r.register_step(
        "FilterNode",
        name="Filter",
        category=OperatorCategory.FILTER,
        parallelism=OperatorParallelism.STATELESS,
        parallel_strategy=ParallelStrategy.MULTI_THREAD,
        is_stateless=True,
    )
    r.register_step(
        "ProjectionNode",
        name="Projection",
        category=OperatorCategory.PROJECT,
        parallelism=OperatorParallelism.STATELESS,
        parallel_strategy=ParallelStrategy.MULTI_THREAD,
        is_stateless=True,
    )
    r.register_step(
        "DistinctNode",
        name="Distinct",
        category=OperatorCategory.SET_OP,
        parallelism=OperatorParallelism.STATEFUL_MERGEABLE,
        is_pipeline_breaking=True,
    )

    # -- Aggregate operators --------------------------------------------------
    r.register_step(
        "UngroupedAggregateNode",
        name="Aggregate",
        category=OperatorCategory.AGGREGATE,
        parallelism=OperatorParallelism.STATEFUL_MERGEABLE,
        is_pipeline_breaking=True,
    )
    r.register_step(
        "GroupedAggregateHashedNode",
        name="Aggregate and Group",
        category=OperatorCategory.AGGREGATE,
        parallelism=OperatorParallelism.STATEFUL_MERGEABLE,
        is_pipeline_breaking=True,
    )

    # -- Sort / limit operators -----------------------------------------------
    r.register_step(
        "SortNode",
        name="Sort",
        category=OperatorCategory.SORT,
        is_pipeline_breaking=True,
    )
    r.register_step(
        "HeapSortNode",
        name="Heap Sort",
        category=OperatorCategory.SORT,
        is_pipeline_breaking=True,
    )
    r.register_step(
        "LimitNode",
        name="Limit",
        category=OperatorCategory.LIMIT,
    )
    # Runtime cardinality guard for an uncorrelated scalar subquery the planner
    # could not statically prove single-row — buffers the subquery result and
    # enforces SQL's "one row or NULL" at the materialization boundary
    # (native_scalar_guard.hpp).
    r.register_step(
        "ScalarGuardNode",
        name="Scalar Guard",
        category=OperatorCategory.LIMIT,
        is_pipeline_breaking=True,
    )

    # -- Window operators -----------------------------------------------------
    # ROW_NUMBER() OVER (PARTITION BY ...) — streaming per-partition counter.
    r.register_step(
        "WindowNode",
        name="Window",
        category=OperatorCategory.PROJECT,
    )
    # SUM/COUNT/AVG/MIN/MAX OVER (... ROWS/RANGE BETWEEN ...) — sliding-window
    # aggregate. A separate node/sink from Window — see native_window_frame.hpp.
    r.register_step(
        "FramedWindowNode",
        name="Framed Window",
        category=OperatorCategory.PROJECT,
    )

    # -- Set operations -------------------------------------------------------
    r.register_step(
        "UnionNode",
        name="Union",
        category=OperatorCategory.SET_OP,
        parallelism=OperatorParallelism.SINGLETON,
        is_pipeline_breaking=True,
    )

    # -- Join operators -------------------------------------------------------
    r.register_step(
        "AsofJoinNode",
        name="ASOF Join",
        category=OperatorCategory.JOIN,
        is_join=True,
        is_pipeline_breaking=True,
    )
    r.register_step(
        "BandJoinNode",
        name="Band Join",
        category=OperatorCategory.JOIN,
        is_join=True,
        is_pipeline_breaking=True,
    )
    r.register_step(
        "DrakenInnerJoinNode",
        name="Inner Join",
        category=OperatorCategory.JOIN,
        is_join=True,
        is_pipeline_breaking=True,
    )
    r.register_step(
        "OuterJoinNode",
        name="Outer Join",
        category=OperatorCategory.JOIN,
        is_join=True,
        is_pipeline_breaking=True,
    )
    r.register_step(
        "CrossJoinNode",
        name="Cross Join",
        category=OperatorCategory.JOIN,
        is_join=True,
        is_pipeline_breaking=True,
    )
    r.register_step(
        "NestedLoopJoinNode",
        name="Nested Loop Join",
        category=OperatorCategory.JOIN,
        is_join=True,
        is_pipeline_breaking=True,
    )
    r.register_step(
        "FilterJoinNode",
        name="Filter Join",
        category=OperatorCategory.JOIN,
        is_join=True,
        is_pipeline_breaking=True,
    )
    r.register_step(
        "ExistenceJoinNode",
        name="Existence Join",
        category=OperatorCategory.JOIN,
        is_join=True,
        is_pipeline_breaking=True,
    )
    r.register_step(
        "UnnestJoinNode",
        name="Unnest Join",
        category=OperatorCategory.JOIN,
        is_join=True,
    )

    # -- DDL / control operators ----------------------------------------------
    r.register_step(
        "ExitNode",
        name="Exit",
        category=OperatorCategory.IO,
        parallelism=OperatorParallelism.SINGLETON,
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
        ShowGrantsNode,
        name="Show Grants",
        category=OperatorCategory.DDL,
        is_not_explained=True,
    )
    r.register(
        ShowManifestNode,
        name="Show Manifest",
        category=OperatorCategory.DDL,
        is_not_explained=True,
    )
    r.register(
        ShowSnapshotsNode,
        name="Show Snapshots",
        category=OperatorCategory.DDL,
        is_not_explained=True,
    )
    r.register(
        ShowLineageNode,
        name="Show Lineage",
        category=OperatorCategory.DDL,
        is_not_explained=True,
    )
    r.register(
        ShowSourcesNode,
        name="Show Sources",
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
    r.register(
        RelationManagementNode,
        name="Relation Management",
        category=OperatorCategory.DDL,
        is_not_explained=True,
    )
    r.register(
        InsertNode,
        name="Insert",
        category=OperatorCategory.IO,
        is_not_explained=True,
    )
    r.register(
        MergeNode,
        name="Merge",
        category=OperatorCategory.IO,
        is_not_explained=True,
    )
    r.register(
        CompactionCommitNode,
        name="Compaction Commit",
        category=OperatorCategory.IO,
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
