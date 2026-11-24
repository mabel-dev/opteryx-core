"""GRAPH_TABLE (SQL/PGQ): a graph pattern in the FROM clause.

`GRAPH_TABLE (<graph> MATCH <pattern> COLUMNS (<columns>))` is a table factor,
so it composes with the rest of SQL. sqlparser has no grammar for it; the
rewriter turns the span into a table-function call the parser can read, and the
logical planner replaces that call with the subquery the pattern lowers to
(opteryx/planner/graph_table.py).

Every result here is compared against the same question written as plain SQL
joins, through the real catalog code over an in-memory store.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

pytest.importorskip("gqlparser")

import opteryx
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import SqlError
from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.planner.graph_table import rewrite_graph_tables
from opteryx.planner.sql_rewriter import do_sql_rewrite
from tests.tools.local_catalog import local_catalog_class

GRAPH = """CREATE PROPERTY GRAPH gt.org.g
  VERTEX TABLES (
    gt.org.people AS people KEY (id) LABEL Person PROPERTIES (id, name),
    gt.org.teams AS teams KEY (id) LABEL Team PROPERTIES (id, name)
  )
  EDGE TABLES (
    gt.org.people AS manages KEY (id)
      SOURCE KEY (manager_id) REFERENCES people (id)
      DESTINATION KEY (id) REFERENCES people (id)
      LABEL MANAGES NO PROPERTIES,
    gt.org.memberships AS member_of KEY (person_id, team_id)
      SOURCE KEY (person_id) REFERENCES people (id)
      DESTINATION KEY (team_id) REFERENCES teams (id)
      LABEL MEMBER_OF NO PROPERTIES
  )"""

MANAGES = "GRAPH_TABLE (gt.org.g MATCH (m:Person)-[:MANAGES]->(r:Person) COLUMNS (m.name AS manager, r.name AS report))"


def _rows(sql, user="tester"):
    out = []
    for morsel in opteryx.session(user=user).execute_to_morsels(sql):
        out.extend(zip(*[morsel.column(n).to_pylist() for n in morsel.column_names]))
    return out


@pytest.fixture
def catalog(tmp_path):
    import opteryx.connectors as connectors

    catalog = local_catalog_class(str(tmp_path))
    saved_default = connectors._default_connector
    saved_prefixes = dict(connectors._storage_prefixes)
    saved_cache = dict(connectors._connector_cache)
    connectors._storage_prefixes.pop("gt", None)
    connectors._connector_cache.clear()
    opteryx.set_default_connector(OpteryxConnector, catalog=catalog)
    try:
        # nine people, each managed by id // 2; three teams; everyone in one team
        _rows("CREATE TABLE gt.org.people AS SELECT id, name, CAST(id / 2 AS INTEGER) AS manager_id FROM $planets")
        _rows("CREATE TABLE gt.org.teams AS SELECT id, name FROM $planets WHERE id <= 3")
        _rows("CREATE TABLE gt.org.memberships AS SELECT id AS person_id, id % 3 + 1 AS team_id FROM $planets")
        _rows(GRAPH)
        yield catalog
    finally:
        connectors._default_connector = saved_default
        connectors._storage_prefixes.clear()
        connectors._storage_prefixes.update(saved_prefixes)
        connectors._connector_cache.clear()
        connectors._connector_cache.update(saved_cache)


# --- it returns what the joins return ------------------------------------------------

CASES = [
    (
        "one hop",
        f"SELECT * FROM {MANAGES} ORDER BY manager, report",
        "SELECT m.name AS manager, r.name AS report FROM gt.org.people m "
        "JOIN gt.org.people r ON r.manager_id = m.id ORDER BY manager, report",
    ),
    (
        "aggregated, under an alias",
        f"SELECT g.manager, COUNT(*) AS reports FROM {MANAGES} AS g GROUP BY g.manager ORDER BY reports DESC, g.manager",
        "SELECT m.name AS manager, COUNT(*) AS reports FROM gt.org.people m "
        "JOIN gt.org.people r ON r.manager_id = m.id GROUP BY m.name ORDER BY reports DESC, manager",
    ),
    (
        "joined to an ordinary table",
        "SELECT g.person, t.name AS team FROM GRAPH_TABLE (gt.org.g MATCH (p:Person)-[:MEMBER_OF]->(t:Team) "
        "COLUMNS (p.name AS person, t.id AS team_id)) AS g JOIN gt.org.teams t ON t.id = g.team_id ORDER BY person",
        "SELECT p.name AS person, t.name AS team FROM gt.org.people p JOIN gt.org.memberships m "
        "ON m.person_id = p.id JOIN gt.org.teams t ON t.id = m.team_id ORDER BY person",
    ),
    (
        "as the right side of a join",
        "SELECT t.name, COUNT(*) AS n FROM gt.org.teams t JOIN GRAPH_TABLE (gt.org.g MATCH "
        "(p:Person)-[:MEMBER_OF]->(x:Team) COLUMNS (x.id AS team_id)) AS g ON g.team_id = t.id "
        "GROUP BY t.name ORDER BY t.name",
        "SELECT t.name, COUNT(*) AS n FROM gt.org.teams t JOIN gt.org.memberships m ON m.team_id = t.id "
        "GROUP BY t.name ORDER BY t.name",
    ),
    (
        "two hops through the junction table, WHERE inside the pattern",
        "SELECT * FROM GRAPH_TABLE (gt.org.g MATCH (m:Person)-[:MANAGES]->(r:Person)-[:MEMBER_OF]->(t:Team) "
        "WHERE t.name = 'Venus' COLUMNS (m.name AS manager, r.name AS report)) ORDER BY manager, report",
        "SELECT m.name AS manager, r.name AS report FROM gt.org.people m JOIN gt.org.people r "
        "ON r.manager_id = m.id JOIN gt.org.memberships x ON x.person_id = r.id JOIN gt.org.teams t "
        "ON t.id = x.team_id WHERE t.name = 'Venus' ORDER BY manager, report",
    ),
    (
        "inside a CTE",
        f"WITH pairs AS (SELECT * FROM {MANAGES}) SELECT COUNT(*) FROM pairs",
        "SELECT COUNT(*) FROM gt.org.people m JOIN gt.org.people r ON r.manager_id = m.id",
    ),
    (
        "twice in one statement",
        "SELECT a.n, b.n FROM GRAPH_TABLE (gt.org.g MATCH (p:Person) COLUMNS (COUNT(*) AS n)) AS a "
        "CROSS JOIN GRAPH_TABLE (gt.org.g MATCH (t:Team) COLUMNS (COUNT(*) AS n)) AS b",
        "SELECT a.n, b.n FROM (SELECT COUNT(*) AS n FROM gt.org.people) a "
        "CROSS JOIN (SELECT COUNT(*) AS n FROM gt.org.teams) b",
    ),
    (
        "a literal that looks like the clause",
        "SELECT * FROM GRAPH_TABLE (gt.org.g MATCH (p:Person) WHERE p.name <> 'COLUMNS (x) it''s' "
        "COLUMNS (COUNT(*) AS n))",
        "SELECT COUNT(*) AS n FROM gt.org.people p WHERE p.name <> 'COLUMNS (x) it''s'",
    ),
    (
        "written across lines, lower case",
        "SELECT *\nFROM graph_table (\n  gt.org.g\n  MATCH (m:Person)-[:MANAGES]->(r:Person)\n"
        "  COLUMNS (m.name AS manager, r.name AS report)\n)\nORDER BY manager, report",
        "SELECT m.name AS manager, r.name AS report FROM gt.org.people m "
        "JOIN gt.org.people r ON r.manager_id = m.id ORDER BY manager, report",
    ),
]


@pytest.mark.parametrize("graph_table, by_hand", [c[1:] for c in CASES], ids=[c[0] for c in CASES])
def test_graph_table_returns_what_the_joins_return(catalog, graph_table, by_hand):
    expected = _rows(by_hand)
    assert expected, "the hand-written query must return rows, or the comparison proves nothing"
    assert _rows(graph_table) == expected


def test_a_view_over_a_graph_table(tmp_path):
    """The reason this is SQL and not a second language: it composes. Over the
    local store, which keeps views and graphs side by side."""
    from opteryx.connectors import register_workspace
    from opteryx.connectors.local_store_connector import LocalStoreConnector

    register_workspace("gtv", LocalStoreConnector, store_root=str(tmp_path))
    _rows("CREATE TABLE gtv.people AS SELECT id, name, CAST(id / 2 AS INTEGER) AS manager_id FROM $planets")
    _rows(
        "CREATE PROPERTY GRAPH gtv.g VERTEX TABLES (gtv.people KEY (id) LABEL Person PROPERTIES (id, name)) "
        "EDGE TABLES (gtv.people AS manages KEY (id) SOURCE KEY (manager_id) REFERENCES people (id) "
        "DESTINATION KEY (id) REFERENCES people (id) LABEL MANAGES NO PROPERTIES)"
    )
    _rows(
        "CREATE VIEW gtv.reporting AS SELECT manager, COUNT(*) AS reports FROM GRAPH_TABLE (gtv.g MATCH "
        "(m:Person)-[:MANAGES]->(r:Person) COLUMNS (m.name AS manager, r.id AS report_id)) GROUP BY manager"
    )
    expected = _rows(
        "SELECT m.name AS manager, COUNT(*) AS reports FROM gtv.people m "
        "JOIN gtv.people r ON r.manager_id = m.id GROUP BY m.name ORDER BY reports DESC, manager"
    )
    assert expected
    assert _rows("SELECT * FROM gtv.reporting ORDER BY reports DESC, manager") == expected


# --- the rewrite ---------------------------------------------------------------------


def test_the_rewriter_leaves_everything_else_alone():
    for sql in (
        "SELECT 1",
        "SELECT * FROM t WHERE note = 'GRAPH_TABLE (g MATCH (a) COLUMNS (a.x))'",
        "SELECT graph_table FROM t",
        "SELECT * FROM my_graph_table (1)",
        "-- GRAPH_TABLE (g MATCH (a) COLUMNS (a.x))\nSELECT 1",
        "SELECT * FROM GRAPH_TABLE('g', 'MATCH (a) COLUMNS (a.x)')",
    ):
        assert rewrite_graph_tables(sql) == (sql, [])


def test_the_rewrite_records_its_edit_so_positions_map_back():
    sql = f"SELECT * FROM {MANAGES} WHERE nope = 1"
    rewritten = do_sql_rewrite(sql)
    assert "GRAPH_TABLE('gt.org.g', 'MATCH" in rewritten
    # a position after the GRAPH_TABLE lands on the same text in what was written
    at = rewritten.index("nope")
    assert sql[rewritten.to_source_offset(at) :].startswith("nope")
    # one inside the replacement maps to where the GRAPH_TABLE starts
    inside = rewritten.index("MATCH")
    assert sql[rewritten.to_source_offset(inside) :].startswith("GRAPH_TABLE")


def test_an_error_after_a_graph_table_points_at_the_right_place(catalog):
    with pytest.raises(SqlError) as error:
        _rows(f"SELECT nope FROM {MANAGES}")
    assert "nope" in str(error.value)


# --- analysis and permissions --------------------------------------------------------


def test_analyze_query_names_the_graph():
    info = opteryx.analyze_query(f"SELECT * FROM {MANAGES} AS g JOIN gt.org.teams t ON t.name = g.manager")
    assert info["query_type"] == "Query" and info["is_read"] is True
    assert sorted(info["tables"]) == ["gt.org.g", "gt.org.teams"]


def _deny(monkeypatch, *denied):
    import opteryx.planner.binder.dataset as dataset_binder
    import opteryx.planner.graph_table as graph_table

    def allowed(_context, relation, action="READ"):
        return relation not in denied

    monkeypatch.setattr("opteryx.managers.permissions.can_perform_action", allowed)
    monkeypatch.setattr(dataset_binder, "can_perform_action", allowed, raising=False)
    return graph_table


def test_reading_the_graph_needs_read_on_the_graph(catalog, monkeypatch):
    """Checked before the pattern is looked at: a caller with no access must not
    learn which labels the graph has from the error a wrong one produces."""
    _deny(monkeypatch, "gt.org.g")
    for pattern in ("(p:Person)", "(p:NoSuchLabel)"):
        with pytest.raises(PermissionError, match="permission to read gt.org.g"):
            _rows(f"SELECT * FROM GRAPH_TABLE (gt.org.g MATCH {pattern} COLUMNS (p.name AS n))")


def test_a_graph_lends_no_access_to_its_tables(catalog, monkeypatch):
    _deny(monkeypatch, "gt.org.memberships")
    with pytest.raises(PermissionError, match="gt.org.memberships"):
        _rows(
            "SELECT * FROM GRAPH_TABLE (gt.org.g MATCH (p:Person)-[:MEMBER_OF]->(t:Team) "
            "COLUMNS (p.name AS person, t.name AS team))"
        )
    # a pattern that does not touch the denied table is unaffected
    assert _rows(f"SELECT COUNT(*) FROM {MANAGES}")


# --- refusals ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "inner, error, message",
    [
        ("gt.org.g MATCH (p:Robot) COLUMNS (p.name AS n)", SqlError, "no label `Robot`"),
        ("gt.org.g MATCH (p:Person)", UnsupportedSyntaxError, "needs a \\*\\*COLUMNS\\*\\* clause"),
        ("gt.org.g MATCH (p:Person) COLUMNS (p.name AS n) LIMIT 5", UnsupportedSyntaxError, "must be the last clause"),
        ("gt.org.g MATCH (p:Person) COLUMNS (p)", UnsupportedSyntaxError, "use one of its properties"),
        ("gt.org.nope MATCH (p:Person) COLUMNS (p.name AS n)", DatasetNotFoundError, "nope"),
        ("gt.org.people MATCH (p:Person) COLUMNS (p.name AS n)", DatasetNotFoundError, "people"),
    ],
)
def test_what_cannot_be_planned_says_why(catalog, inner, error, message):
    with pytest.raises(error, match=message):
        _rows(f"SELECT * FROM GRAPH_TABLE ({inner})")


def test_sqlparser_format_matches_the_parser_this_engine_is_built_with():
    """gqlparser's statement dicts are spliced into this engine's AST, so the two
    must be shaped for the same sqlparser release."""
    import re

    from opteryx.planner.graph_table import SQLPARSER_FORMAT

    with open(os.path.join(os.path.dirname(__file__), "../../Cargo.toml")) as file:
        cargo = file.read()
    version = re.search(r'\[dependencies\.sqlparser\]\s*version\s*=\s*"(\d+\.\d+)', cargo).group(1)
    assert version == SQLPARSER_FORMAT
