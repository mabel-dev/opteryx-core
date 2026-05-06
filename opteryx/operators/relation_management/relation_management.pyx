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

from . import BasePlanNode


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

        # CREATE / TRUNCATE
        self.connector = parameters.get("connector")

    @property
    def name(self):
        return "Relation Management"

    @property
    def config(self):
        if self.action == "drop_relation":
            return f"drop {', '.join(self.relation_names or [])}"
        return f"{self.action} {self.relation_name}"

    def __call__(self, morsel=None, **kwargs) -> NonTabularResult:
        if self.action == "create_relation":
            if self.connector.relation_exists(self.relation_name):
                if self.if_not_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise ValueError(f"relation already exists: {self.relation_name}")
            self.connector.create_relation(self.relation_name, self.schema)
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_relation":
            dropped = 0
            for relation_name in self.relation_names:
                connector = self.connectors[relation_name]
                if not connector.relation_exists(relation_name):
                    if self.if_exists:
                        continue
                    raise DatasetNotFoundError(connector=connector, dataset=relation_name)
                connector.drop_relation(relation_name, if_exists=self.if_exists)
                dropped += 1
            return NonTabularResult(record_count=dropped, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "truncate_relation":
            if not self.connector.relation_exists(self.relation_name):
                raise DatasetNotFoundError(connector=self.connector, dataset=self.relation_name)
            self.connector.truncate_relation(self.relation_name)
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        else:
            raise NotImplementedError(f"Unsupported relation action: {self.action}")
