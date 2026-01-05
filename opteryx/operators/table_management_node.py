# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0

"""
View Management Node

Handles CREATE/ALTER/DROP VIEW operations at execution time.
"""

from opteryx.connectors import TableType
from opteryx.constants import QueryStatus
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import NonTabularResult
from opteryx.models import QueryProperties

from . import BasePlanNode


class TableManagementNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        # Action should be one of: 'create_view', 'alter_view', 'drop_view'
        self.action: str = parameters.get("action")

        # CREATE / ALTER
        self.table_name: str = parameters.get("table_name")

    @property
    def name(self):  # pragma: no cover - simple string
        return "Table Management"

    @property
    def config(self):  # pragma: no cover - simple string
        return f"{self.action} {self.table_name}"

    def __call__(self, morsel=None, **kwargs) -> NonTabularResult:
        # Perform the action and return a NonTabularResult object

        if self.action == "analyze_table":
            from opteryx.connectors import connector_factory

            connector = connector_factory(self.table_name, telemetry=self.telemetry)
            entity_type, entity = connector.locate_object(self.table_name)
            if entity is None:
                raise Exception(f"Table '{self.table_name}' does not exist.")
            if entity_type != TableType.Table:
                raise UnsupportedSyntaxError(
                    f"ANALYZE TABLE can only be performed on tables, not {entity_type.value}."
                )

            entity.refresh_manifest("system")

            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        else:
            raise NotImplementedError(f"Unsupported view action: {self.action}")
