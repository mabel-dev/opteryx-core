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
Show Create Node

This is a SQL Query Execution Plan Node.
"""

from typing import Generator, Optional
from opteryx.exceptions import DatasetNotFoundError, UnsupportedSyntaxError
from opteryx.models import QueryProperties

# BasePlanNode/JoinNode in scope via _operators.pyx include.


class ShowCreateNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        self.object_type = parameters.get("object_type")
        self.object_name = parameters.get("object_name")
        # Bound by visit_show, which authorizes the read first. Never derived
        # here - deriving it locally is what let this run unauthorized.
        self.connector = parameters.get("connector")

    @property
    def name(self):  # pragma: no cover
        return "Show"

    @property
    def config(self):  # pragma: no cover
        return ""

    def _view_statement(self):
        from opteryx.connectors import TableType
        from opteryx.models.create_statement import render_create_view

        object_type, _ = self.connector.locate_object(self.object_name)
        if object_type != TableType.View:
            raise DatasetNotFoundError(dataset=self.object_name, connector="VIEW")

        view_definition = self.connector.get_view(self.object_name)
        return render_create_view(self.object_name, view_definition.statement)

    def _table_statement(self):
        """Reconstruct a table's DDL - see opteryx.models.create_statement.

        A materialized view's backing store is a relation in every other
        respect, so `SHOW CREATE TABLE` would happily describe one as a table -
        producing a CREATE that recreates the storage and loses the view. It is
        turned away by name, pointing at the statement that answers.
        """
        from opteryx.models.create_statement import render_create_table

        if not self.connector.relation_exists(self.object_name):
            raise DatasetNotFoundError(dataset=self.object_name, connector="TABLE")
        if self.connector.is_materialized_view(self.object_name):
            raise UnsupportedSyntaxError(
                f"'{self.object_name}' is a materialized view, not a table; use "
                "'**SHOW CREATE MATERIALIZED VIEW**'."
            )

        return render_create_table(
            self.object_name,
            self.connector.relation_schema(self.object_name),
            relationships=self.connector.list_relationships(self.object_name),
            cluster_columns=self.connector.cluster_by_columns(self.object_name),
        )

    def _materialized_view_statement(self):
        from opteryx.models.create_statement import render_create_materialized_view

        if not self.connector.is_materialized_view(self.object_name):
            raise DatasetNotFoundError(dataset=self.object_name, connector="MATERIALIZED VIEW")
        return render_create_materialized_view(
            self.object_name, self.connector.materialized_view_definition(self.object_name)
        )

    def _task_statement(self):
        from opteryx.models.create_statement import render_create_task

        if not self.connector.is_task(self.object_name):
            raise DatasetNotFoundError(dataset=self.object_name, connector="TASK")
        return render_create_task(
            self.object_name, self.connector.task_definition(self.object_name)
        )

    def execute(self, morsel):
        # Static dispatch on an object type the planner has already reduced to
        # one of four spellings - an unknown one cannot reach here.
        _STATEMENT_BUILDERS = {
            "TABLE": self._table_statement,
            "VIEW": self._view_statement,
            "MATERIALIZED VIEW": self._materialized_view_statement,
            "TASK": self._task_statement,
        }
        create_statement = _STATEMENT_BUILDERS[self.object_type]()

        vectors = [
            vector_from_sequence([self.object_name], dtype=_draken_native.DrakenType.VARCHAR),
            vector_from_sequence([create_statement], dtype=_draken_native.DrakenType.VARCHAR),
        ]
        morsel = Morsel.from_vectors([self.object_name, "create_statement"], vectors)
        yield morsel
