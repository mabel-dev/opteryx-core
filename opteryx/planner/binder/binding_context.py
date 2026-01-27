# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from typing import Dict

from opteryx.models import ExecutionContext
from opteryx.models import QueryTelemetry
from opteryx.virtual_datasets import derived


@dataclass
class BindingContext:
    """
    Holds the context needed for the binding phase of the query engine.

    Attributes:
        schemas: Dict[str, Any]
            Data schemas available during the binding phase.
        query_id: str
            Query ID.
        connection: ExecutionContext
            Query execution context.
        relations: Set
            Relations involved in the current query.
    """

    schemas: Dict[str, Any]
    query_id: str
    execution_context: ExecutionContext
    relations: Dict[str, str]
    telemetry: QueryTelemetry

    @classmethod
    def initialize(cls, query_id: str, execution_context=None) -> "BindingContext":
        """
        Initialize a new BindingContext with the given query ID and connection.

        Parameters:
            query_id: str
                Query ID.
            execution_context: Any, optional
                Database connection, defaults to None.

        Returns:
            A new BindingContext instance.
        """
        return cls(
            schemas={"$derived": derived.schema()},  # Replace with the actual schema
            query_id=query_id,
            execution_context=execution_context,
            relations={},
            telemetry=QueryTelemetry(query_id),
        )

    def copy(self) -> "BindingContext":
        """
        Create a deep copy of this BindingContext.

        Returns:
            A new BindingContext instance with copied attributes.
        """
        return BindingContext(
            schemas=deepcopy(self.schemas),
            query_id=self.query_id,
            execution_context=self.execution_context,
            relations={k: v for k, v in self.relations.items()},
            telemetry=self.telemetry,
        )
