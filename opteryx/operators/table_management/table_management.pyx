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

"""
View Management Node

Handles CREATE/ALTER/DROP VIEW operations at execution time.
"""

from typing import Generator, Optional
from opteryx.connectors import TableType
from opteryx.constants import QueryStatus
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.models import NonTabularResult
from opteryx.models import QueryProperties

# BasePlanNode/JoinNode in scope via _operators.pyx include.

class TableManagementNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        # Action should be one of: 'create_view', 'alter_view', 'drop_view',
        # 'analyze_table', 'drop_statistics'
        self.action: str = parameters.get("action")

        # CREATE / ALTER
        self.table_name: str = parameters.get("table_name")

        # ANALYZE / DROP STATISTICS — FOR COLUMNS scope ([] = whole table)
        self.columns: list = parameters.get("analyze_columns") or []

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
            from opteryx.operators.table_management._analyze import analyze_table

            connector = connector_factory(self.table_name, telemetry=self.telemetry)
            table_engine = connector.table_engine(self.table_name, telemetry=self.telemetry)
            written = analyze_table(table_engine, self.columns)
            return NonTabularResult(record_count=written, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_statistics":
            from opteryx.connectors import connector_factory
            from opteryx.operators.table_management._analyze import drop_statistics

            connector = connector_factory(self.table_name, telemetry=self.telemetry)
            table_engine = connector.table_engine(self.table_name, telemetry=self.telemetry)
            removed = drop_statistics(table_engine, self.columns)
            return NonTabularResult(record_count=removed, status=QueryStatus.SQL_SUCCESS)

        else:
            raise NotImplementedError(f"Unsupported table action: {self.action}")
