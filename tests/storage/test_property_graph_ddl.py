"""CREATE / DROP / SHOW CREATE PROPERTY GRAPH (SQL/PGQ) against a local store.

docs/PROPERTY_GRAPH_DESIGN.md: a property graph is catalog text, like a view. CREATE
validates the definition against the element tables and stores CANONICAL text -
every element name, key, endpoint, label and property written out - which is what
SHOW CREATE returns and what gqlparser reads.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import SqlError
from opteryx.exceptions import UnsupportedSyntaxError

PEOPLE_GRAPH = """CREATE PROPERTY GRAPH ws.social
  VERTEX TABLES (
    ws.people KEY (id) LABEL Person PROPERTIES (id, name),
    ws.companies KEY (ID) LABEL Company PROPERTIES (id, legal_name AS name)
  )
  EDGE TABLES (
    ws.employment KEY (person_id, company_id)
      SOURCE KEY (person_id) REFERENCES people (id)
      DESTINATION KEY (company_id) REFERENCES companies (id)
      LABEL WORKS_AT PROPERTIES (since)
  )"""

CANONICAL = """CREATE PROPERTY GRAPH ws.social
  VERTEX TABLES (
    ws.people AS people KEY (id)
      LABEL Person PROPERTIES (id, name),
    ws.companies AS companies KEY (id)
      LABEL Company PROPERTIES (id, legal_name AS name)
  )
  EDGE TABLES (
    ws.employment AS employment KEY (person_id, company_id)
      SOURCE KEY (person_id) REFERENCES people (id)
      DESTINATION KEY (company_id) REFERENCES companies (id)
      LABEL WORKS_AT PROPERTIES (since)
  );"""


def run(session, sql):
    return list(session.execute_to_morsels(sql))


def rows(session, sql):
    out = []
    for morsel in session.execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        out.extend(zip(*columns))
    return out


def show_create(session, name):
    ((_, statement),) = rows(session, f"SHOW CREATE PROPERTY GRAPH {name}")
    return statement


@pytest.fixture
def session(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))
    s = opteryx.session()
    run(s, "CREATE TABLE ws.people (id BIGINT, name VARCHAR, age BIGINT)")
    run(s, "CREATE TABLE ws.companies (id BIGINT, legal_name VARCHAR)")
    run(s, "CREATE TABLE ws.employment (person_id BIGINT, company_id BIGINT, since BIGINT)")
    return opteryx.session()


# --- create, show, drop ------------------------------------------------------------


def test_create_stores_the_canonical_text(session, tmp_path):
    run(session, PEOPLE_GRAPH)
    assert (tmp_path / "ws" / "social" / "property_graph.json").exists()
    # KEY (ID) is spelled as the schema spells it; element names are written out
    assert show_create(session, "ws.social") == CANONICAL


def test_canonical_text_recreates_itself(session):
    run(session, PEOPLE_GRAPH)
    canonical = show_create(session, "ws.social")
    run(session, "DROP PROPERTY GRAPH ws.social")
    run(session, canonical)
    assert show_create(session, "ws.social") == canonical


def test_all_columns_and_no_clause_expand_to_the_schema(session):
    run(
        session,
        "CREATE PROPERTY GRAPH ws.g VERTEX TABLES ("
        "ws.people KEY (id) PROPERTIES ALL COLUMNS, ws.companies AS c KEY (id) LABEL Co)",
    )
    statement = show_create(session, "ws.g")
    assert "LABEL people PROPERTIES (id, name, age)" in statement  # label defaults to element name
    assert "ws.companies AS c KEY (id)\n      LABEL Co PROPERTIES (id, legal_name)" in statement


def test_no_properties(session):
    run(session, "CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people KEY (id) LABEL P NO PROPERTIES)")
    assert "LABEL P NO PROPERTIES" in show_create(session, "ws.g")


def test_endpoints_default_from_a_foreign_key(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))
    session = opteryx.session()
    run(session, "CREATE TABLE ws.people (id BIGINT, name VARCHAR)")
    run(
        session,
        "CREATE TABLE ws.knows (a BIGINT, b BIGINT, "
        "CONSTRAINT fk_a FOREIGN KEY (a) REFERENCES ws.people (id) NOT ENFORCED)",
    )
    run(
        session,
        "CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people KEY (id) LABEL Person PROPERTIES (id)) "
        "EDGE TABLES (ws.knows KEY (a, b) SOURCE people DESTINATION KEY (b) REFERENCES people (id) "
        "LABEL KNOWS NO PROPERTIES)",
    )
    assert "SOURCE KEY (a) REFERENCES people (id)" in show_create(session, "ws.g")


def test_endpoint_without_a_foreign_key_is_refused(session):
    with pytest.raises(SqlError, match="has no FOREIGN KEY to"):
        run(
            session,
            "CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people KEY (id), ws.companies KEY (id)) "
            "EDGE TABLES (ws.employment KEY (person_id) SOURCE people "
            "DESTINATION KEY (company_id) REFERENCES companies (id))",
        )


def test_drop(session, tmp_path):
    run(session, PEOPLE_GRAPH)
    run(session, "DROP PROPERTY GRAPH ws.social")
    assert not (tmp_path / "ws" / "social").exists()
    with pytest.raises(DatasetNotFoundError):
        run(session, "DROP PROPERTY GRAPH ws.social")
    run(session, "DROP PROPERTY GRAPH IF EXISTS ws.social")


def test_or_replace_and_if_not_exists(session):
    run(session, PEOPLE_GRAPH)
    with pytest.raises(ValueError, match="property graph already exists"):
        run(session, PEOPLE_GRAPH)
    run(session, PEOPLE_GRAPH.replace("CREATE PROPERTY GRAPH", "CREATE PROPERTY GRAPH IF NOT EXISTS"))
    run(
        session,
        "CREATE OR REPLACE PROPERTY GRAPH ws.social VERTEX TABLES (ws.people KEY (id) LABEL P NO PROPERTIES)",
    )
    assert "LABEL P NO PROPERTIES" in show_create(session, "ws.social")


def test_or_replace_and_if_not_exists_together_are_refused(session):
    with pytest.raises(UnsupportedSyntaxError, match="cannot combine"):
        run(session, PEOPLE_GRAPH.replace("CREATE PROPERTY GRAPH", "CREATE OR REPLACE PROPERTY GRAPH IF NOT EXISTS"))


def test_show_create_of_a_missing_graph(session):
    with pytest.raises(DatasetNotFoundError):
        show_create(session, "ws.nope")


# --- validation at CREATE ---------------------------------------------------------


@pytest.mark.parametrize(
    "statement, message",
    [
        ("CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.missing KEY (id))", "missing"),
        ("CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people KEY (nope))", "has no column `nope`"),
        ("CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people KEY (id) PROPERTIES (id, nope))",
         "has no column `nope`"),
        ("CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people KEY (id) PROPERTIES (id, name AS id))",
         "declares the property `id` twice"),
        ("CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people KEY (id), ws.people KEY (id))",
         "both named `people`"),
        ("CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people KEY (id) LABEL X, ws.companies KEY (id) LABEL X)",
         "the label `X` is on more than one element table"),
        ("CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people KEY (id)) EDGE TABLES (ws.employment KEY (person_id) "
         "SOURCE KEY (person_id) REFERENCES nope (id) DESTINATION KEY (company_id) REFERENCES people (id))",
         "references `nope`, which is not a vertex table"),
        ("CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people KEY (id)) EDGE TABLES (ws.employment KEY (person_id) "
         "SOURCE KEY (person_id, company_id) REFERENCES people (id) DESTINATION KEY (company_id) REFERENCES people (id))",
         "SOURCE KEY has 2 column"),
        ("CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people KEY (id)) EDGE TABLES (ws.employment KEY (person_id) "
         "SOURCE KEY (person_id) REFERENCES people (nope) DESTINATION KEY (company_id) REFERENCES people (id))",
         "has no column `nope`"),
    ],
)  # fmt: skip
def test_definitions_that_do_not_fit_their_tables_are_refused(session, statement, message):
    with pytest.raises((SqlError, DatasetNotFoundError)) as err:
        run(session, statement)
    assert message in str(err.value)


@pytest.mark.parametrize(
    "statement, message",
    [
        ("CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people LABEL P)", "needs **KEY**"),
        ("CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people KEY (id) DEFAULT LABEL)", "**DEFAULT LABEL**"),
        ("CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people KEY (id) LABEL A LABEL B)", "one **LABEL**"),
        ("CREATE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people KEY (id) PROPERTIES (age * 2 AS x))",
         "Expressions as properties are not supported"),
        ("CREATE PROPERTY GRAPH ws.g EDGE TABLES (ws.people KEY (id))", "Expected: **CREATE**"),
        ("DROP PROPERTY GRAPH ws.g CASCADE", "nothing for CASCADE"),
    ],
)  # fmt: skip
def test_grammar_refusals(session, statement, message):
    with pytest.raises(UnsupportedSyntaxError) as err:
        run(session, statement)
    assert message in str(err.value)
    assert "OPTERYX-SYNTAX" not in str(err.value)


# --- one namespace ------------------------------------------------------------------


def test_graph_cannot_take_a_table_name(session):
    with pytest.raises(SqlError, match="already names a table"):
        run(session, "CREATE PROPERTY GRAPH ws.people VERTEX TABLES (ws.companies KEY (id))")


def test_graph_cannot_take_a_view_name(session):
    run(session, "CREATE VIEW ws.v AS SELECT id FROM ws.people")
    with pytest.raises((SqlError, ValueError), match="already"):
        run(session, "CREATE PROPERTY GRAPH ws.v VERTEX TABLES (ws.companies KEY (id))")


def test_table_and_view_cannot_take_a_graph_name(session):
    run(session, PEOPLE_GRAPH)
    with pytest.raises(Exception, match="already exists as a property graph"):
        run(session, "CREATE TABLE ws.social (id BIGINT)")
    with pytest.raises(Exception, match="already exists as a property graph"):
        run(session, "CREATE VIEW ws.social AS SELECT id FROM ws.people")


def test_selecting_from_a_graph_says_what_it_is(session):
    run(session, PEOPLE_GRAPH)
    with pytest.raises(UnsupportedSyntaxError, match="is a property graph, not a table"):
        run(session, "SELECT * FROM ws.social")


# --- statement classification -----------------------------------------------------


def test_analyze_query_classifies_graph_ddl_like_view_ddl():
    for statement in (PEOPLE_GRAPH, "DROP PROPERTY GRAPH ws.social"):
        info = opteryx.analyze_query(statement)
        view_like = opteryx.analyze_query("CREATE VIEW ws.social AS SELECT 1")
        assert info["tables"] == ["ws.social"]
        assert info["permission_required"] == view_like["permission_required"]
        assert info["is_ddl"] is True


def test_analyze_query_names_the_graph_for_show_create():
    assert opteryx.analyze_query("SHOW CREATE PROPERTY GRAPH ws.social")["tables"] == ["ws.social"]
