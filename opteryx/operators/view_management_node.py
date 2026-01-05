# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0

"""
View Management Node

Handles CREATE/ALTER/DROP VIEW operations at execution time.
"""

from typing import Optional

from opteryx.connectors import TableType
from opteryx.constants import QueryStatus
from opteryx.exceptions import DatasetNotFoundError
from opteryx.models import NonTabularResult
from opteryx.models import QueryProperties

from . import BasePlanNode


class ViewManagementNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        # Action should be one of: 'create_view', 'alter_view', 'drop_view', 'comment'
        self.action: str = parameters.get("action")

        # CREATE / ALTER
        self.view_name: Optional[str] = parameters.get("view_name")
        self.query = parameters.get("query")
        self.or_replace = parameters.get("or_replace", False)
        self.materialized = parameters.get("materialized", False)

        # DROP
        self.view_names = parameters.get("view_names")
        # Binder supplies a mapping of view_name -> connector for drops
        self.connectors = parameters.get("connectors")

        # COMMENT
        self.object_name: Optional[str] = parameters.get("object_name")
        self.comment: Optional[str] = parameters.get("comment")
        self.if_exists: bool = parameters.get("if_exists", False)

        # Single connector (create/alter/comment)
        self.connector = parameters.get("connector")

    @property
    def name(self):  # pragma: no cover - simple string
        return "View Management"

    @property
    def config(self):  # pragma: no cover - simple string
        if self.action == "drop_view":
            return f"drop {', '.join(self.view_names or [])}"
        elif self.action == "comment":
            return f"comment on {self.object_name}"
        return f"{self.action} {self.view_name}"

    def __call__(self, morsel=None, **kwargs) -> NonTabularResult:
        # Perform the action and return a NonTabularResult object

        if self.action in ("create_view", "alter_view"):
            from opteryx.third_party import sqloxide

            if self.query is None:
                # Nothing to store
                raise ValueError("No view query supplied")

            # The rust module expects and returns lists of statements
            view_sql = sqloxide.ast_to_sql([{"Query": self.query}])[0]

            if not self.connector:
                # Defensive: if connector is missing, derive via connector_factory lazily
                from opteryx.connectors import connector_factory

                self.connector = connector_factory(self.view_name, telemetry=self.telemetry)

            self.connector.create_view(
                self.view_name, view_sql, update_if_exists=self.or_replace, owner="opteryx"
            )

            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_view":
            if not self.view_names:
                raise ValueError("No view names supplied for DROP VIEW")

            dropped = 0
            for vn in self.view_names:
                # Prefer connector instances prepared in binder
                connector = None
                if self.connectors and vn in self.connectors:
                    connector = self.connectors[vn]
                else:
                    from opteryx.connectors import connector_factory

                    connector = connector_factory(vn, telemetry=self.telemetry)

                if connector.locate_object(vn)[0] != TableType.View:
                    raise DatasetNotFoundError(connector=connector, dataset=vn)

                connector.drop_view(vn)
                dropped += 1

            return NonTabularResult(record_count=dropped, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "comment":
            # COMMENT ON VIEW/TABLE/EXTENSION
            if not self.object_name:
                raise ValueError("No object name supplied for COMMENT")

            if not self.connector:
                # Defensive: if connector is missing, derive via connector_factory lazily
                from opteryx.connectors import connector_factory

                self.connector = connector_factory(self.object_name, telemetry=self.telemetry)

            # Try to locate the object to verify it exists (unless IF EXISTS is specified)
            object_type, _ = self.connector.locate_object(self.object_name)
            if object_type not in (TableType.View, TableType.Table):
                raise DatasetNotFoundError(connector=self.connector, dataset=self.object_name)

            # Store the comment via the connector's generic comment API
            # Ensure the connector implements the API before calling.
            if not hasattr(self.connector, "set_comment"):
                raise NotImplementedError("Connector does not support updating comments")

            self.connector.set_comment(self.object_name, self.comment, describer="system")

            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        else:
            raise NotImplementedError(f"Unsupported view action: {self.action}")
