# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import Set

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
        snapshots: Dict[str, Any]
            Bound commit histories, keyed by relation alias — populated by
            visit_scan ONLY for a Scan the planner marked `for_snapshots_only`
            (SHOW SNAPSHOTS FOR) and consumed by visit_show_snapshots. Unlike
            `manifests`, an ordinary Scan leaves nothing here: a relation's
            history is a second catalog round trip that no other statement
            reads, so it is fetched where it is the answer and nowhere else.
        query_id: str
            Query ID.
        connection: ExecutionContext
            Query execution context.
        relations: Set
            Relations involved in the current query.
        schema_only: bool
            Bind names and types, and read NOTHING a name cannot be resolved
            without. A scan then takes the connector's schema alone and leaves
            `manifests` empty, rather than building the Manifest - the file list
            and per-column statistics for the whole relation.

            Set only by the edit-time check path, which stops at the end of
            binding: everything that consumes a Manifest (statistics, manifest
            pruning, the physical planner's reader choice) runs after that point,
            so on that path the Manifest is fetched and then discarded. It is the
            larger of the two cloud reads binding performs.

            A statement that cannot be bound without a Manifest - SHOW MANIFEST
            FOR is the only one - fails loud here rather than binding against a
            Manifest that isn't there.

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
    snapshots: Dict[str, Any] = field(default_factory=dict)
    schema_only: bool = False
    # Column identities that must survive the bind-time schema narrowing even though
    # no SELECT / GROUP BY / aggregate operand names them.
    #
    # The narrowing in aggregate.py and project.py keeps only the columns the
    # projection or aggregation references, which is right for every column whose
    # only role is to be SELECTed. It is wrong for a column an OPERATOR reads
    # structurally: `CROSS JOIN UNNEST(arr)` determines the ROW COUNT from `arr`, so
    # `SELECT COUNT(*) FROM t CROSS JOIN UNNEST(arr) AS v` names no column at all and
    # the narrowing dropped `arr` — leaving the compiler with an unnest whose source
    # is not in the stream, which it correctly refused.
    #
    # Populated bottom-up (the binder visits the unnest before the aggregate above
    # it), so by the time a narrowing site runs, every unnest beneath it is recorded.
    retained_columns: Set[Any] = field(default_factory=set)

    @classmethod
    def initialize(
        cls, query_id: str, execution_context=None, schema_only: bool = False
    ) -> "BindingContext":
        """
        Initialize a new BindingContext with the given query ID and connection.

        Parameters:
            query_id: str
                Query ID.
            execution_context: Any, optional
                Database connection, defaults to None.
            schema_only: bool, optional
                Skip the per-relation Manifest read; see the class docstring.

        Returns:
            A new BindingContext instance.
        """
        return cls(
            schemas={"$derived": derived.schema()},  # Replace with the actual schema
            query_id=query_id,
            execution_context=execution_context,
            relations={},
            telemetry=QueryTelemetry(query_id),
            schema_only=schema_only,
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
            snapshots={k: v for k, v in self.snapshots.items()},
            schema_only=self.schema_only,
            retained_columns=set(self.retained_columns),
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
            schema_only=self.schema_only,
        )
