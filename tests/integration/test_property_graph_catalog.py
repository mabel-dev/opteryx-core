"""Property graph DDL through `OpteryxConnector` and the real catalog code.

The catalog's property graph methods are `opteryx_catalog`'s own, borrowed onto
the in-memory test catalog (tests/tools/local_catalog.py), so this exercises the
production write path - connector, catalog, author attribution, audit - with
memory in place of Firestore.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import SqlError
from opteryx.exceptions import UnsupportedSyntaxError
from tests.tools.local_catalog import local_catalog_class

GRAPH = """CREATE PROPERTY GRAPH pgws.sky.orbits
  VERTEX TABLES (
    pgws.sky.planets KEY (id) LABEL Planet PROPERTIES (id, name),
    pgws.sky.moons KEY (id) LABEL Moon PROPERTIES (id, name)
  )
  EDGE TABLES (
    pgws.sky.moons AS orbit KEY (id)
      SOURCE KEY (id) REFERENCES moons (id)
      DESTINATION KEY (planet_id) REFERENCES planets (id)
      LABEL ORBITS NO PROPERTIES
  )"""


def _rows(sql):
    out = []
    for morsel in opteryx.session(user="tester").execute_to_morsels(sql):
        out.extend(zip(*[morsel.column(n).to_pylist() for n in morsel.column_names]))
    return out


@pytest.fixture
def catalog(tmp_path):
    import opteryx.connectors as connectors

    catalog = local_catalog_class(str(tmp_path))
    saved_default = connectors._default_connector
    saved_prefixes = dict(connectors._storage_prefixes)
    saved_cache = dict(connectors._connector_cache)
    connectors._storage_prefixes.pop("pgws", None)
    connectors._connector_cache.clear()
    opteryx.set_default_connector(OpteryxConnector, catalog=catalog)
    try:
        _rows("CREATE TABLE pgws.sky.planets AS SELECT id, name FROM $planets")
        _rows("CREATE TABLE pgws.sky.moons AS SELECT id, name, id % 3 + 1 AS planet_id FROM $planets")
        yield catalog
    finally:
        connectors._default_connector = saved_default
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved_prefixes)
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved_cache)


def test_create_show_and_drop_through_the_catalog(catalog, capsys):
    _rows(GRAPH)
    ((name, statement),) = _rows("SHOW CREATE PROPERTY GRAPH pgws.sky.orbits")
    assert name == "pgws.sky.orbits"
    assert statement.startswith("CREATE PROPERTY GRAPH pgws.sky.orbits\n  VERTEX TABLES (")
    assert "DESTINATION KEY (planet_id) REFERENCES planets (id)" in statement

    _rows("DROP PROPERTY GRAPH pgws.sky.orbits")
    with pytest.raises(DatasetNotFoundError):
        _rows("SHOW CREATE PROPERTY GRAPH pgws.sky.orbits")

    audit = capsys.readouterr().out
    assert '"action":"create_property_graph"' in audit and '"author":"tester"' in audit
    assert '"action":"drop_property_graph"' in audit


def test_the_catalog_stores_the_canonical_text_with_its_author(catalog):
    _rows(GRAPH)
    instance = catalog(workspace="pgws")
    record = instance.load_property_graph(("sky", "orbits"))
    assert record["author"] == "tester"
    assert record["statement"] == _rows("SHOW CREATE PROPERTY GRAPH pgws.sky.orbits")[0][1]


def test_or_replace_and_if_not_exists(catalog):
    _rows(GRAPH)
    with pytest.raises(Exception, match="already exists"):
        _rows(GRAPH)
    _rows(GRAPH.replace("PROPERTY GRAPH", "PROPERTY GRAPH IF NOT EXISTS", 1))
    _rows(
        "CREATE OR REPLACE PROPERTY GRAPH pgws.sky.orbits VERTEX TABLES "
        "(pgws.sky.planets KEY (id) LABEL P NO PROPERTIES)"
    )
    assert "LABEL P NO PROPERTIES" in _rows("SHOW CREATE PROPERTY GRAPH pgws.sky.orbits")[0][1]


STRUCTURE = [
    ("vertex", "planets", "Planet", "pgws.sky.planets", "id", "id, name", None, None, None, None, None, None),
    ("vertex", "moons", "Moon", "pgws.sky.moons", "id", "id, name", None, None, None, None, None, None),
    ("edge", "orbit", "ORBITS", "pgws.sky.moons", "id", "", "moons", "id", "id", "planets", "planet_id", "id"),
]


def test_reading_a_graph_returns_its_structure_not_its_data(catalog):
    """One row per element table - enough to draw the graph, and none of the rows
    of the tables it names."""
    _rows(GRAPH)
    assert _rows("SELECT * FROM pgws.sky.orbits") == STRUCTURE
    assert _rows(
        "SELECT element_name, source_vertex, destination_vertex FROM pgws.sky.orbits "
        "WHERE element_kind = 'edge'"
    ) == [("orbit", "moons", "planets")]


def test_a_renamed_property_reads_as_the_statement_spells_it(catalog):
    _rows(
        "CREATE PROPERTY GRAPH pgws.sky.named VERTEX TABLES "
        "(pgws.sky.planets KEY (id) LABEL P PROPERTIES (id, name AS title))"
    )
    assert _rows("SELECT properties FROM pgws.sky.named") == [("id, name AS title",)]


def test_a_graph_stored_without_its_element_tables_is_read_from_its_statement(catalog):
    """A graph created before the catalog stored element tables has only the
    canonical text; the structure is parsed back out of it, and matches."""
    _rows(GRAPH)
    instance = catalog(workspace="pgws")
    record = instance.load_property_graph(("sky", "orbits"))
    instance.create_property_graph(
        ("sky", "orbits"), record["statement"], author="tester", update_if_exists=True
    )
    assert "vertex-tables" not in instance.load_property_graph(("sky", "orbits"))
    assert _rows("SELECT * FROM pgws.sky.orbits") == STRUCTURE


def test_a_graph_cannot_take_a_table_name(catalog):
    with pytest.raises(SqlError, match="already names a table"):
        _rows("CREATE PROPERTY GRAPH pgws.sky.planets VERTEX TABLES (pgws.sky.moons KEY (id))")


def test_drop_if_exists_on_a_missing_graph(catalog):
    _rows("DROP PROPERTY GRAPH IF EXISTS pgws.sky.nothing")
    with pytest.raises(DatasetNotFoundError):
        _rows("DROP PROPERTY GRAPH pgws.sky.nothing")


def test_the_catalog_stores_the_bound_element_tables(catalog):
    """The binder's elements reach the catalog beside the text, so a reader that
    draws the graph never parses SQL to get them back."""
    _rows(GRAPH)
    record = catalog(workspace="pgws").load_property_graph(("sky", "orbits"))

    assert [(v["name"], v["label"], v["relation"]) for v in record["vertex-tables"]] == [
        ("planets", "Planet", ["pgws", "sky", "planets"]),
        ("moons", "Moon", ["pgws", "sky", "moons"]),
    ]
    assert record["vertex-tables"][0]["properties"] == [
        {"column": "id", "name": "id"},
        {"column": "name", "name": "name"},
    ]
    ((edge),) = record["edge-tables"]
    assert (edge["name"], edge["label"], edge["properties"]) == ("orbit", "ORBITS", [])
    assert edge["source"] == {"vertex": "moons", "key": ["id"], "references": ["id"]}
    assert edge["destination"] == {"vertex": "planets", "key": ["planet_id"], "references": ["id"]}


def test_a_catalog_without_element_tables_still_gets_the_text():
    """Engine and catalog deploy separately: a catalog whose create_property_graph
    predates the element tables is called with the text alone, not refused."""
    calls = []

    class _OldCatalog:
        def create_property_graph(self, identifier, statement, author=None, update_if_exists=False):
            calls.append((identifier, statement, author, update_if_exists))

    connector = object.__new__(OpteryxConnector)
    connector._parse_identifier = lambda name: ("pgws", name.split(".", 1)[1])
    connector._get_catalog = lambda workspace: _OldCatalog()
    connector.create_property_graph(
        "pgws.sky.orbits", "CREATE ...", owner="tester", vertex_tables=[{}], edge_tables=[]
    )
    assert calls == [(("sky", "orbits"), "CREATE ...", "tester", False)]
