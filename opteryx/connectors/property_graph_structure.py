"""
A property graph, read as a relation: one row per element table.

A property graph holds no rows of its own - its vertices and edges are the rows
of the tables it names, and GQL is how those are queried. But its name sits in
the `workspace.collection.<object>` namespace beside tables and views, and a
name in that namespace answers to a read. What it answers with is its
STRUCTURE: one row per vertex table and per edge table, enough to draw the
graph without reading any of its data. Never the contents of those tables.

The rows come from the element tables the catalog stores beside the graph's
canonical statement (`vertex-tables` / `edge-tables`, written from the same
bind). A graph created before those were stored has only the statement, and
then the statement is parsed instead: it is the canonical text, with every key,
endpoint, label and property written out, so the parse loses nothing.

Lists (keys, properties) are comma-separated VARCHAR, as
`information_schema.tasks.writes` is: a column name cannot contain a comma.
A property whose name differs from its column reads `column AS name`, the way
the statement spells it.
"""

from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel

from opteryx.connectors.base.base_connector import BaseTable
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.types import logical_type as _lt
from opteryx.types.schema import ColumnDescriptor
from opteryx.types.schema import RelationDescriptor


class PropertyGraphRelation(UnsupportedSyntaxError):
    """Raised by `OpteryxTable` when the name it was asked to load is a property
    graph. `OpteryxConnector.table_engine` catches it and serves the graph's
    structure instead; anything else that constructs an `OpteryxTable`
    directly still gets the refusal it always got."""

    def __init__(self, dataset: str):
        self.dataset = dataset
        super().__init__(
            f"'{dataset}' is a property graph, not a table. Reading it through a "
            "workspace returns its structure, one row per element table; query its "
            "contents with GQL (the gqlparser package lowers GQL to SQL over the "
            "graph's tables), or read its tables directly."
        )


STRUCTURE_COLUMNS = (
    "element_kind",  # 'vertex' | 'edge'
    "element_name",  # the AS name edges refer to their vertices by
    "label",
    "table_name",  # fully qualified
    "key_columns",
    "properties",
    "source_vertex",  # edges only, from here down
    "source_key",
    "source_references",
    "destination_vertex",
    "destination_key",
    "destination_references",
)


def _joined(values) -> Optional[str]:
    return None if values is None else ", ".join(values)


def _property_list(properties) -> Optional[str]:
    if properties is None:
        return None
    return ", ".join(
        p["column"] if p["column"] == p["name"] else f"{p['column']} AS {p['name']}"
        for p in properties
    )


def structure_rows(vertex_tables: List[Dict], edge_tables: List[Dict]) -> List[tuple]:
    """Rows in STRUCTURE_COLUMNS order, vertices first, each in definition order."""
    rows = []
    for kind, elements in (("vertex", vertex_tables), ("edge", edge_tables)):
        for element in elements:
            source = element.get("source") or {}
            destination = element.get("destination") or {}
            rows.append(
                (
                    kind,
                    element["name"],
                    element["label"],
                    ".".join(element["relation"]),
                    _joined(element["key"]),
                    _property_list(element.get("properties")),
                    source.get("vertex"),
                    _joined(source.get("key")),
                    _joined(source.get("references")),
                    destination.get("vertex"),
                    _joined(destination.get("key")),
                    _joined(destination.get("references")),
                )
            )
    return rows


def _values(identifiers) -> Optional[List[str]]:
    return None if identifiers is None else [i["value"] for i in identifiers]


def _parsed_element(parsed: Dict) -> Dict:
    """One element table from the parser's dict shape, in the catalog's stored shape."""
    relation = [part["Identifier"]["value"] for part in parsed["relation"]]
    name = parsed["alias"]["value"] if parsed["alias"] else relation[-1]
    properties = parsed["properties"]
    if properties == "NoProperties":
        listed = []
    elif isinstance(properties, dict):
        listed = [
            {
                "column": p["column"]["value"],
                "name": p["alias"]["value"] if p["alias"] else p["column"]["value"],
            }
            for p in properties["Listed"]
        ]
    else:
        # ALL COLUMNS / no clause: the canonical text never carries these (the
        # binder expands them), so a statement that does is not canonical, and
        # the property list is honestly unknown rather than empty.
        listed = None
    element = {
        "name": name,
        "label": parsed["label"]["value"] if parsed["label"] else name,
        "relation": relation,
        "key": _values(parsed["key"]),
        "properties": listed,
    }
    for end in ("source", "destination"):
        endpoint = parsed.get(end)
        if endpoint:
            element[end] = {
                "vertex": endpoint["vertex"]["value"],
                "key": _values(endpoint["key"]),
                "references": _values(endpoint["references"]),
            }
    return element


def elements_from_statement(statement: str):
    """(vertex_tables, edge_tables) parsed back out of a canonical statement."""
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    (parsed,) = sqloxide.parse_sql(do_sql_rewrite(statement), _dialect="opteryx")
    body = parsed["CreatePropertyGraph"]
    return (
        [_parsed_element(v) for v in body["vertex_tables"]],
        [_parsed_element(e) for e in body["edge_tables"]],
    )


def graph_elements(record: Dict):
    """(vertex_tables, edge_tables) for a stored graph record: the stored element
    tables when the record has them, else parsed from its statement."""
    if record.get("vertex-tables") is not None:
        return record["vertex-tables"], record.get("edge-tables") or []
    return elements_from_statement(record["statement"])


class PropertyGraphStructureTable(BaseTable):
    """Reads a property graph's name as its structure (see the module docstring).

    The READ gate is the ordinary one on the graph's own name - this does not
    govern its own permissions. It names the element tables but reads none of
    them: a name is a citation, not access, and reading what it names still
    needs a grant on that table.
    """

    __mode__ = "Internal"
    interal_only = True  # routes through the generic "Reader" physical node

    def __init__(self, *, dataset, catalog, workspace, telemetry=None, **kwargs):
        BaseTable.__init__(self, dataset=dataset, telemetry=telemetry)
        self.catalog = catalog
        self.workspace = workspace

    def get_dataset_schema(self) -> RelationDescriptor:
        self.schema = RelationDescriptor(
            name=self.dataset,
            columns=[ColumnDescriptor(name=name, column_type=_lt.VARCHAR) for name in STRUCTURE_COLUMNS],
        )
        return self.schema

    def read_dataset(self, columns=None, predicates=None, **kwargs) -> Iterable[Morsel]:
        collection, _, name = self.dataset.rpartition(".")
        record = self.catalog.load_property_graph((collection, name))
        rows = structure_rows(*graph_elements(record))
        vectors = [
            vector_from_sequence([row[i] for row in rows], dtype=DrakenType.VARCHAR)
            for i in range(len(STRUCTURE_COLUMNS))
        ]
        yield Morsel.from_vectors(list(STRUCTURE_COLUMNS), vectors)
