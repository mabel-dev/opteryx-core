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
Property Graph Management Node

Handles CREATE / DROP PROPERTY GRAPH at execution time. The binder has already
validated the definition and rendered the canonical text (binder/property_graph.py);
this stores or removes it, in the view store the binder bound.
"""

from opteryx.connectors import TableType
from opteryx.constants import QueryStatus
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import NonTabularResult
from opteryx.models import QueryProperties
from opteryx.models import object_message

# BasePlanNode in scope via _operators.pyx include.


class PropertyGraphManagementNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, step, str action):
        """`action` is 'create' or 'drop'."""
        BasePlanNode.__init__(self, properties, step, step.columns, step.pre_update_columns)
        self.action: str = action

    @property
    def name(self):  # pragma: no cover - simple string
        return "Property Graph Management"

    @property
    def config(self):  # pragma: no cover - simple string
        return f"{self.action} {self.step.graph_name}"

    @property
    def _author(self):
        """The session user this DDL is attributed to, or None when unauthenticated
        (see ViewManagementNode._author)."""
        from opteryx.variables import resolve

        return resolve("external_user", self.properties.variables, None) or None

    def __call__(self, morsel=None, **kwargs) -> NonTabularResult:
        connector = self.step.connector
        if not connector:
            # The binder resolves the VIEW STORE and nothing else may - see
            # ViewManagementNode.
            raise InvalidInternalStateError(
                f"property graph {self.action} reached execution without a bound view store"
            )
        graph_name = self.step.graph_name
        existing = connector.locate_object(graph_name)[0]

        if self.action == "create":
            if self.step.if_not_exists and existing == TableType.PropertyGraph:
                return NonTabularResult(
                    record_count=0,
                    status=QueryStatus.SQL_SUCCESS,
                    message=object_message(
                        "property graph", "", graph_name, "already exists, nothing created"
                    ),
                )
            update_if_exists = bool(self.step.or_replace)
            connector.create_property_graph(
                graph_name,
                self.step.statement,
                update_if_exists=update_if_exists,
                owner=self._author,
                vertex_tables=self.step.vertex_tables,
                edge_tables=self.step.edge_tables,
            )
            # what the store DID: OR REPLACE over an existing graph replaced it
            verb = "replaced" if update_if_exists and existing == TableType.PropertyGraph else "created"
            return NonTabularResult(
                record_count=1,
                status=QueryStatus.SQL_SUCCESS,
                message=object_message(verb, "property graph", graph_name),
            )

        if self.action == "drop":
            if existing != TableType.PropertyGraph:
                if self.step.if_exists:
                    return NonTabularResult(
                        record_count=0,
                        status=QueryStatus.SQL_SUCCESS,
                        message=object_message(
                            "property graph", "", graph_name, "does not exist, nothing dropped"
                        ),
                    )
                raise DatasetNotFoundError(connector=connector, dataset=graph_name)
            connector.drop_property_graph(graph_name, author=self._author)
            return NonTabularResult(
                record_count=1,
                status=QueryStatus.SQL_SUCCESS,
                message=object_message("dropped", "property graph", graph_name),
            )

        raise InvalidInternalStateError(f"unknown property graph action {self.action!r}")
