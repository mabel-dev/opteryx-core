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
from opteryx.models import object_message
from opteryx.models import QueryProperties

# BasePlanNode/JoinNode in scope via _operators.pyx include.

class TableManagementNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, step):
        # ANALYZE / DROP STATISTICS — the FOR COLUMNS scope is this node's
        # `columns` ([] = whole table).
        BasePlanNode.__init__(self, properties, step, step.analyze_columns, step.pre_update_columns)

        # 'analyze_table' or 'drop_statistics'
        self.action: str = step.action
        self.table_name: str = step.table_name

    @property
    def name(self):  # pragma: no cover - simple string
        return "Table Management"

    @property
    def config(self):  # pragma: no cover - simple string
        return f"{self.action} {self.table_name}"

    @property
    def _author(self):
        """The session user this operation is attributed to, or None when
        unauthenticated. None is passed through rather than substituted, so a
        store that requires attribution rejects the write instead of recording
        an invented identity (same contract as Insert/RelationManagement)."""
        from opteryx.variables import resolve

        return resolve("external_user", self.properties.variables, None) or None

    def __call__(self, morsel=None, **kwargs) -> NonTabularResult:
        # Perform the action and return a NonTabularResult object

        if self.action == "analyze_table":
            from opteryx.connectors import connector_factory
            from opteryx.operators.table_management._analyze import analyze_table

            connector = connector_factory(self.table_name, telemetry=self.telemetry)
            table_engine = connector.table_engine(self.table_name, telemetry=self.telemetry)
            written = analyze_table(table_engine, self.columns, author=self._author)
            return NonTabularResult(
                record_count=written,
                status=QueryStatus.SQL_SUCCESS,
                message=object_message(
                    "analyzed", "table", self.table_name, f"({written:,} column(s) profiled)"
                ),
            )

        elif self.action == "drop_statistics":
            from opteryx.connectors import connector_factory
            from opteryx.operators.table_management._analyze import drop_statistics

            connector = connector_factory(self.table_name, telemetry=self.telemetry)
            table_engine = connector.table_engine(self.table_name, telemetry=self.telemetry)
            removed = drop_statistics(table_engine, self.columns)
            return NonTabularResult(
                record_count=removed,
                status=QueryStatus.SQL_SUCCESS,
                message=object_message(
                    "dropped statistics for", "table", self.table_name,
                    f"({removed:,} column(s))",
                ),
            )

        else:
            raise NotImplementedError(f"Unsupported table action: {self.action}")
