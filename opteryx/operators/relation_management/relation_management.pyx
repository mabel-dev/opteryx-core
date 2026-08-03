# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Relation Management Node

Handles CREATE / DROP / TRUNCATE TABLE operations at execution time.
Operates on relations managed by Writable connectors.
"""

from typing import Optional

from opteryx.constants import QueryStatus
from opteryx.exceptions import DatasetNotFoundError
from opteryx.models import NonTabularResult
from opteryx.models import QueryProperties

# BasePlanNode/JoinNode in scope via _operators.pyx include.


class RelationManagementNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.action: str = parameters.get("action")

        # CREATE
        self.relation_name: Optional[str] = parameters.get("relation_name")
        self.schema = parameters.get("schema")
        self.if_not_exists: bool = parameters.get("if_not_exists", False)

        # DROP
        self.relation_names = parameters.get("relation_names")
        self.connectors = parameters.get("connectors")
        self.if_exists: bool = parameters.get("if_exists", False)

        # CREATE COLLECTION
        self.collection_name: Optional[str] = parameters.get("collection_name")

        # DROP COLLECTION
        self.collection_names = parameters.get("collection_names")

        # ALTER ... CLUSTER BY
        self.cluster_columns = parameters.get("cluster_columns")

        # ALTER ... RENAME TO
        self.new_relation_name: Optional[str] = parameters.get("new_relation_name")

        # ALTER WORKSPACE ... SET
        self.workspace_name: Optional[str] = parameters.get("workspace_name")
        self.property_name: Optional[str] = parameters.get("property_name")
        self.property_value = parameters.get("property_value")

        # CREATE / TRUNCATE / ALTER
        self.connector = parameters.get("connector")

    @property
    def name(self):
        return "Relation Management"

    @property
    def config(self):
        if self.action == "drop_relation":
            return f"drop {', '.join(self.relation_names or [])}"
        if self.action == "create_collection":
            return f"create collection {self.collection_name}"
        if self.action == "drop_collection":
            return f"drop collection {', '.join(self.collection_names or [])}"
        if self.action == "cluster_by":
            return f"cluster {self.relation_name} by ({', '.join(self.cluster_columns or [])})"
        if self.action == "rename_relation":
            return f"rename {self.relation_name} to {self.new_relation_name}"
        if self.action == "alter_workspace":
            return f"alter workspace {self.workspace_name} set {self.property_name} = {self.property_value}"
        return f"{self.action} {self.relation_name}"

    @property
    def _author(self):
        """The session user this DDL is attributed to, or None when unauthenticated.

        None is passed through rather than substituted, so a store that requires
        attribution rejects the write instead of recording an invented identity.
        """
        from opteryx.variables import resolve

        return resolve("external_user", self.properties.variables, None) or None

    def __call__(self, morsel=None, **kwargs) -> NonTabularResult:
        if self.action == "create_relation":
            if self.connector.relation_exists(self.relation_name):
                if self.if_not_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise ValueError(f"relation already exists: {self.relation_name}")
            self.connector.create_relation(self.relation_name, self.schema, author=self._author)
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_relation":
            dropped = 0
            for relation_name in self.relation_names:
                connector = self.connectors[relation_name]
                if not connector.relation_exists(relation_name):
                    if self.if_exists:
                        continue
                    raise DatasetNotFoundError(connector=connector, dataset=relation_name)
                connector.drop_relation(
                    relation_name, if_exists=self.if_exists, author=self._author
                )
                dropped += 1
            return NonTabularResult(record_count=dropped, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "create_collection":
            # No exists-check here, unlike create_relation: the connector settles
            # existence in a single atomic call, so there is no window between
            # checking and creating. The cost is that IF NOT EXISTS cannot report
            # 0-vs-1 for "already there" - the count is 1 for "the collection now
            # exists", not "a collection was created this instant".
            self.connector.create_collection(
                self.collection_name, if_not_exists=self.if_not_exists, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_collection":
            dropped = 0
            for collection_name in self.collection_names:
                connector = self.connectors[collection_name]
                if not connector.collection_exists(collection_name):
                    if self.if_exists:
                        continue
                    raise DatasetNotFoundError(connector=connector, dataset=collection_name)
                connector.drop_collection(
                    collection_name, if_exists=self.if_exists, author=self._author
                )
                dropped += 1
            return NonTabularResult(record_count=dropped, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "truncate_relation":
            if not self.connector.relation_exists(self.relation_name):
                raise DatasetNotFoundError(connector=self.connector, dataset=self.relation_name)
            self.connector.truncate_relation(self.relation_name, author=self._author)
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "cluster_by":
            if not self.connector.relation_exists(self.relation_name):
                if self.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.connector, dataset=self.relation_name)
            # Declared on the Writable mixin, and visit_alter_relation has
            # already rejected a non-Writable connector.
            self.connector.set_cluster_by(
                self.relation_name, self.cluster_columns, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "rename_relation":
            if not self.connector.relation_exists(self.relation_name):
                if self.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.connector, dataset=self.relation_name)
            # A rename must never absorb an existing relation - that would
            # destroy the target's data and history with no DROP in the SQL.
            if self.connector.relation_exists(self.new_relation_name):
                raise ValueError(f"relation already exists: {self.new_relation_name}")
            self.connector.rename_relation(
                self.relation_name, self.new_relation_name, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_workspace":
            self.connector.set_workspace_property(
                self.workspace_name,
                self.property_name,
                self.property_value,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        else:
            raise NotImplementedError(f"Unsupported relation action: {self.action}")
