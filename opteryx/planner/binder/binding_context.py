# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict

from opteryx.managers.virtual_datasets import derived
from opteryx.models import ExecutionContext
from opteryx.models import QueryTelemetry


@dataclass
class BindingContext:
    """
    Holds the context needed for the binding phase of the query engine.

    Attributes:
        schemas: Dict[str, Any]
            Data schemas available during the binding phase.
        manifests: Dict[str, Any]
            Bound Manifest objects, keyed by relation alias — populated by
            visit_scan alongside `schemas` and consumed by visit_show_manifest
            (SHOW MANIFEST FOR). Not deep-copied like `schemas`: a Manifest
            holds native draken vector handles that read-only consumers share
            rather than clone.
        query_id: str
            Query ID.
        connection: ExecutionContext
            Query execution context.
        relations: Set
            Relations involved in the current query.
        outer_schemas: Dict[str, Any]
            Schemas of the ENCLOSING query scope, for resolving correlated
            references from inside a subquery. Empty for a top-level query.

            SQL resolves an unqualified name against the innermost scope that
            provides it, falling outwards. `schemas` is the innermost scope;
            `outer_schemas` is consulted ONLY when a name is not found there,
            so this is strictly additive — it can never change how an
            already-resolvable name binds, only make a previously
            unresolvable one bind (and be tagged as correlated).
    """

    schemas: Dict[str, Any]
    query_id: str
    execution_context: ExecutionContext
    relations: Dict[str, str]
    telemetry: QueryTelemetry
    outer_schemas: Dict[str, Any] = field(default_factory=dict)
    manifests: Dict[str, Any] = field(default_factory=dict)

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
            # NOT deep-copied: the outer scope is read-only from in here, and
            # copying it would detach resolved columns from the outer query's
            # own schema objects (identity comparisons downstream rely on
            # those being the same objects).
            outer_schemas=self.outer_schemas,
            manifests={k: v for k, v in self.manifests.items()},
        )

    def open_correlated_scope(self) -> "BindingContext":
        """
        Return a context for binding a nested subquery.

        The subquery starts with an empty local scope (its own FROM clause
        populates it) and sees THIS context's schemas as the enclosing scope,
        so a correlated reference resolves outwards and is tagged.

        Enclosing scopes nest: an inner subquery sees its parent's locals
        layered over whatever its parent could already see.
        """
        return BindingContext(
            schemas={"$derived": derived.schema()},
            query_id=self.query_id,
            execution_context=self.execution_context,
            relations={},
            telemetry=self.telemetry,
            outer_schemas={**self.outer_schemas, **self.schemas},
        )
