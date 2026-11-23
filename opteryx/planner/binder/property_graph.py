# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Binding CREATE / DROP PROPERTY GRAPH (SQL/PGQ; docs/PROPERTY_GRAPH_DESIGN.md).

A property graph is catalog text, like a view: it is stored in the view store,
gated at WRITE, and shares the relation namespace. CREATE validates the
definition against the element tables (every table and column must exist, and
the creator must be able to read them) and renders the CANONICAL text that is
stored - every key, endpoint, label and property written out - so a reader of
it never needs a schema.
"""

from typing import Dict
from typing import List
from typing import Tuple

from opteryx.compiled.structures.plan_steps import PlanStep
from opteryx.exceptions import SqlError
from opteryx.exceptions import md_table
from opteryx.planner.binder.binding_context import BindingContext


def _name_parts(object_name) -> List[str]:
    return [part["Identifier"]["value"] for part in object_name]


def _graph_error(message: str):
    from opteryx.exceptions import compose

    return SqlError(compose(message))


def _assert_name_free_in_source(graph_name: str, context: BindingContext) -> None:
    """Refuse a graph name that a TABLE or VIEW in the workspace's DATA binding holds.

    The store checks its own names when it writes; this covers the data binding,
    which can be a different catalog sharing the namespace (see the view
    equivalent in binder/view.py).
    """
    from opteryx.connectors import TableType
    from opteryx.connectors import connector_factory

    existing, _ = connector_factory(graph_name, telemetry=context.telemetry).locate_object(graph_name)
    if existing in (TableType.Table, TableType.View):
        raise _graph_error(
            f"{md_table(graph_name)} already names a {existing.value.lower()}. Tables, views "
            "and property graphs share one namespace, so a name identifies exactly one of them."
        )


def _relation_columns(parts: List[str], context: BindingContext) -> Dict[str, str]:
    """The relation's columns, by lower-cased name -> the name as the schema spells it.

    Bound, schema-only, as the creator: the relation must resolve and be readable,
    which is the same check CREATE VIEW makes of its sources.
    """
    from opteryx.models import current_name_of
    from opteryx.models.create_statement import quote_identifier
    from opteryx.planner.binder.view import bind_schema_only

    relation = ".".join(quote_identifier(part) for part in parts)
    head = bind_schema_only(f"SELECT * FROM {relation}", context)
    names = {}
    for column in head.columns:
        name = current_name_of(column)
        # recorded as a list when one expression was named more than once; the first is its name
        if type(name) in (list, tuple):
            name = name[0]
        names.setdefault(str(name).lower(), str(name))
    return names


class _Element:
    __slots__ = ("parts", "relation", "name", "columns", "key", "label", "properties", "endpoints")

    def __init__(self, parsed: Dict, context: BindingContext):
        self.parts = _name_parts(parsed["relation"])
        self.relation = ".".join(self.parts)
        self.name = parsed["alias"]["value"] if parsed["alias"] else self.parts[-1]
        self.columns = _relation_columns(self.parts, context)
        self.key = [self.column(i["value"], "KEY") for i in parsed["key"]]
        self.label = parsed["label"]["value"] if parsed["label"] else self.name

        properties = parsed["properties"]
        if properties in ("Unspecified", "AllColumns"):
            # without a clause SQL/PGQ means every column; expanded now so the
            # stored text never depends on the table's schema later
            self.properties = [(column, column) for column in self.columns.values()]
        elif properties == "NoProperties":
            self.properties = []
        else:
            self.properties = []
            for listed in properties["Listed"]:
                column = self.column(listed["column"]["value"], "PROPERTIES")
                name = listed["alias"]["value"] if listed["alias"] else column
                self.properties.append((column, name))
        named = [name for _, name in self.properties]
        duplicate = next((n for n in named if named.count(n) > 1), None)
        if duplicate is not None:
            raise _graph_error(f"`{self.name}` declares the property `{duplicate}` twice.")
        self.endpoints = {}

    def column(self, written: str, clause: str) -> str:
        found = self.columns.get(written.lower())
        if found is None:
            raise _graph_error(
                f"{md_table(self.relation)} has no column `{written}` (in `{self.name}`'s {clause})."
            )
        return found

    def rendered(self) -> Dict:
        return {
            "relation": self.parts,
            "name": self.name,
            "key": self.key,
            "label": self.label,
            "properties": self.properties,
            **self.endpoints,
        }


def _foreign_key(edge: _Element, vertex: _Element, end: str, context: BindingContext):
    """`SOURCE <vertex>` with no KEY ... REFERENCES: the edge table's one informational
    FOREIGN KEY to the vertex table supplies the columns."""
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable

    connector = connector_factory(edge.relation, telemetry=context.telemetry)
    declared = []
    # Writable is where relationships are declared and listed (capabilities/writable.py)
    if isinstance(connector, Writable):
        declared = [
            r
            for r in connector.list_relationships(edge.relation)
            if [p.lower() for p in r["references_relation_parts"]] == [p.lower() for p in vertex.parts]
        ]
    if len(declared) != 1:
        found = "no" if not declared else f"{len(declared)}"
        raise _graph_error(
            f"`{edge.name}` {end} names `{vertex.name}` without KEY ... REFERENCES, and "
            f"{md_table(edge.relation)} has {found} FOREIGN KEY to {md_table(vertex.relation)}. "
            f"Write {end} KEY (<column>) REFERENCES {vertex.name} (<column>)."
        )
    relationship = declared[0]
    return (
        [edge.column(relationship["column_name"], end)],
        [vertex.column(relationship["references_column_name"], f"{end} REFERENCES")],
    )


def _endpoint(edge: _Element, parsed: Dict, end: str, vertices: Dict, context: BindingContext):
    vertex_name = parsed["vertex"]["value"]
    vertex = vertices.get(vertex_name)
    if vertex is None:
        raise _graph_error(
            f"`{edge.name}` {end} references `{vertex_name}`, which is not a vertex table of "
            f"this graph (its vertex tables: {', '.join(f'`{v}`' for v in vertices)})."
        )
    if parsed["key"] is None:
        key, references = _foreign_key(edge, vertex, end, context)
    else:
        key = [edge.column(i["value"], f"{end} KEY") for i in parsed["key"]]
        references = [vertex.column(i["value"], f"{end} REFERENCES") for i in parsed["references"]]
        if len(key) != len(references):
            raise _graph_error(
                f"`{edge.name}` {end} KEY has {len(key)} column(s) but REFERENCES has {len(references)}."
            )
    return key, vertex.name, references


def visit_create_property_graph(self, node: PlanStep, context: BindingContext) -> Tuple[PlanStep, BindingContext]:
    from opteryx.managers.permissions import can_perform_action
    from opteryx.models.create_statement import render_create_property_graph
    from opteryx.planner.binder.view import _view_store

    node.connector = _view_store(node.graph_name, context)
    if not can_perform_action(context.execution_context, node.graph_name, action="WRITE"):
        raise PermissionError(f"User does not have permission to create property graph {node.graph_name}")
    _assert_name_free_in_source(node.graph_name, context)

    definition = node.definition
    vertices = [_Element(parsed, context) for parsed in definition["vertex_tables"]]
    edges = [_Element(parsed, context) for parsed in definition["edge_tables"]]

    names, labels = set(), set()
    for element in (*vertices, *edges):
        if element.name in names:
            raise _graph_error(
                f"two element tables are both named `{element.name}`; give one a different **AS** name."
            )
        names.add(element.name)
        if element.label in labels:
            raise _graph_error(
                f"the label `{element.label}` is on more than one element table, which is not supported."
            )
        labels.add(element.label)

    by_name = {vertex.name: vertex for vertex in vertices}
    for edge, parsed in zip(edges, definition["edge_tables"]):
        edge.endpoints = {
            "source": _endpoint(edge, parsed["source"], "SOURCE", by_name, context),
            "destination": _endpoint(edge, parsed["destination"], "DESTINATION", by_name, context),
        }

    # The same bound elements twice: rendered into the canonical text, and kept
    # as they are for the catalog to store beside it, so a reader that draws the
    # graph (Studio, OData) never has to parse the text back.
    node.vertex_tables = [vertex.rendered() for vertex in vertices]
    node.edge_tables = [edge.rendered() for edge in edges]
    node.statement = render_create_property_graph(node.graph_name, node.vertex_tables, node.edge_tables)
    node.columns = []
    return node, context


def visit_drop_property_graph(self, node: PlanStep, context: BindingContext) -> Tuple[PlanStep, BindingContext]:
    from opteryx.managers.permissions import can_perform_action
    from opteryx.planner.binder.view import _view_store

    node.connector = _view_store(node.graph_name, context)
    # WRITE, as DROP VIEW: a graph is text that CREATE can always put back.
    if not can_perform_action(context.execution_context, node.graph_name, action="WRITE"):
        raise PermissionError(
            f"User does not have permission to drop property graph {node.graph_name} (write required)"
        )
    node.columns = []
    return node, context
