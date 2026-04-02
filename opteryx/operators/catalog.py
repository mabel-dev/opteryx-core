# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Operator catalog — centralized registry of static metadata for relational operators.

This module provides a single source of truth for operator properties (category,
parallelism strategy, etc.) needed by the executor to make scheduling decisions.

Usage:
    from opteryx.operators.catalog import get_registry, OperatorCategory

    registry = get_registry()
    metadata = registry.get(FilterNode)
    all_operators = registry.list()
"""

from dataclasses import dataclass
from enum import Enum
from threading import RLock
from typing import Dict, Optional, Type


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
        self._dispatch_table: Dict = {}  # LogicalPlanStepType -> operator class
        self._lock = RLock()

    def register(self, operator_class: Type, **metadata_kwargs) -> None:
        """
        Register an operator class with its metadata.

        Args:
            operator_class: The operator class (subclass of BasePlanNode)
            **metadata_kwargs: Metadata fields (category, parallel_strategy, etc.)

        Raises:
            ValueError: If category is missing or invalid.
        """
        with self._lock:
            # Validate required fields
            if "category" not in metadata_kwargs:
                raise ValueError(f"{operator_class.__name__} must define `category`")

            category = metadata_kwargs["category"]
            if not isinstance(category, OperatorCategory):
                raise ValueError(
                    f"{operator_class.__name__}.category must be OperatorCategory, "
                    f"got {type(category)}"
                )

            # Extract optional fields with defaults
            parallel_strategy = metadata_kwargs.get(
                "parallel_strategy", ParallelStrategy.SINGLE_THREAD
            )
            logical_node_type = metadata_kwargs.get("logical_node_type", None)

            # Create metadata instance
            metadata = OperatorMetadata(
                operator_class=operator_class,
                category=category,
                parallel_strategy=parallel_strategy,
                is_pipeline_breaking=metadata_kwargs.get("is_pipeline_breaking", False),
                is_join=metadata_kwargs.get("is_join", False),
                is_scan=metadata_kwargs.get("is_scan", False),
                is_stateless=metadata_kwargs.get("is_stateless", False),
                is_not_explained=metadata_kwargs.get("is_not_explained", False),
                target_queue_depth=metadata_kwargs.get("target_queue_depth", 0),
                batch_size=metadata_kwargs.get("batch_size", 2048),
            )

            # Store metadata
            self._metadata[operator_class] = metadata

            # Register in dispatch table if logical_node_type provided
            if logical_node_type is not None:
                if logical_node_type in self._dispatch_table:
                    existing = self._dispatch_table[logical_node_type]
                    raise ValueError(
                        f"Multiple operators for {logical_node_type}: "
                        f"{existing.__name__} and {operator_class.__name__}"
                    )
                self._dispatch_table[logical_node_type] = operator_class

    def get(self, operator_class: Type) -> Optional[OperatorMetadata]:
        """
        Get metadata for an operator class.

        Args:
            operator_class: The operator class

        Returns:
            OperatorMetadata if registered, None otherwise.
        """
        with self._lock:
            return self._metadata.get(operator_class)

    def list(self) -> list:
        """List all registered operator classes."""
        with self._lock:
            return list(self._metadata.keys())

    def dispatch(self, logical_node_type, query_properties, **node_config):
        """
        Factory method to instantiate an operator from a logical plan node type.

        Args:
            logical_node_type: LogicalPlanStepType enum value
            query_properties: QueryProperties object
            **node_config: Configuration dict from logical plan node

        Returns:
            Instantiated operator (BasePlanNode subclass)

        Raises:
            ValueError: If logical_node_type has no registered operator
        """
        # Convert enum to string name if needed
        node_type_key = logical_node_type.name if hasattr(logical_node_type, 'name') else str(logical_node_type)

        with self._lock:
            operator_class = self._dispatch_table.get(node_type_key)

        if operator_class is None:
            raise ValueError(
                f"No operator registered for logical node type {logical_node_type}"
            )

        return operator_class(properties=query_properties, **node_config)


# Global singleton registry
_global_registry: Optional[OperatorRegistry] = None
_registry_lock = RLock()


def get_registry() -> OperatorRegistry:
    """Get the global operator registry singleton."""
    global _global_registry
    if _global_registry is None:
        with _registry_lock:
            if _global_registry is None:
                _global_registry = OperatorRegistry()
    return _global_registry
