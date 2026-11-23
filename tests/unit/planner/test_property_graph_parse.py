"""The aside parser's property graph productions (src/aside/graph.rs, admin.rs).

What the planner reads (the dict shape), and that the gate claims exactly
`CREATE [OR REPLACE] PROPERTY GRAPH` / `DROP PROPERTY GRAPH` / `SHOW CREATE
PROPERTY GRAPH` and nothing else.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.planner import parse_statement
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party import sqloxide


def parse(statement: str):
    return sqloxide.parse_sql(do_sql_rewrite(statement), _dialect="opteryx")


def names(object_name):
    return [part["Identifier"]["value"] for part in object_name]


def test_create_shape():
    (statement,) = parse(
        'CREATE OR REPLACE PROPERTY GRAPH ws.g VERTEX TABLES (ws.people AS p KEY (id) LABEL "Person" '
        "PROPERTIES (id, full_name AS name)) EDGE TABLES (ws.knows KEY (a, b) SOURCE KEY (a) REFERENCES p (id) "
        "DESTINATION p NO PROPERTIES)"
    )
    body = statement["CreatePropertyGraph"]
    assert names(body["name"]) == ["ws", "g"]
    assert (body["or_replace"], body["if_not_exists"]) == (True, False)
    (vertex,) = body["vertex_tables"]
    assert names(vertex["relation"]) == ["ws", "people"]
    assert vertex["alias"]["value"] == "p"
    assert [k["value"] for k in vertex["key"]] == ["id"]
    assert (vertex["label"]["value"], vertex["label"]["quote_style"]) == ("Person", '"')
    listed = vertex["properties"]["Listed"]
    assert [(p["column"]["value"], p["alias"] and p["alias"]["value"]) for p in listed] == [
        ("id", None),
        ("full_name", "name"),
    ]
    assert vertex["source"] is None and vertex["destination"] is None
    (edge,) = body["edge_tables"]
    assert [k["value"] for k in edge["source"]["key"]] == ["a"]
    assert edge["source"]["vertex"]["value"] == "p"
    assert edge["destination"] == {"key": None, "vertex": edge["destination"]["vertex"], "references": None}
    assert edge["label"] is None and edge["properties"] == "NoProperties"


@pytest.mark.parametrize(
    "clause, expected",
    [("", "Unspecified"), ("PROPERTIES ALL COLUMNS", "AllColumns"), ("NO PROPERTIES", "NoProperties")],
)
def test_properties_forms(clause, expected):
    (statement,) = parse(f"CREATE PROPERTY GRAPH g VERTEX TABLES (t KEY (k) {clause})")
    assert statement["CreatePropertyGraph"]["vertex_tables"][0]["properties"] == expected


def test_drop_and_show_create_shapes():
    (drop,) = parse("DROP PROPERTY GRAPH IF EXISTS ws.g")
    assert names(drop["DropPropertyGraph"]["name"]) == ["ws", "g"]
    assert drop["DropPropertyGraph"]["if_exists"] is True
    (show,) = parse("SHOW CREATE PROPERTY GRAPH ws.g")
    assert show["ShowCreate"]["obj_type"] == "PropertyGraph"
    assert names(show["ShowCreate"]["obj_name"]) == ["ws", "g"]


@pytest.mark.parametrize(
    "statement, root",
    [
        ("CREATE OR REPLACE VIEW v AS SELECT 1", "CreateView"),
        ("CREATE TABLE property (graph INT)", "CreateTable"),
        ("DROP TABLE graph", "Drop"),
        ("DROP VIEW property", "Drop"),
        ("SHOW CREATE VIEW v", "ShowCreate"),
    ],
)
def test_other_statements_pass_through(statement, root):
    (parsed,) = parse(statement)
    assert next(iter(parsed)) == root


@pytest.mark.parametrize(
    "statement, message",
    [
        ("CREATE PROPERTY GRAPH g VERTEX TABLES (t KEY (k)) EDGE TABLES (e KEY (k) LABEL E)",
         "Expected: **CREATE**"),
        ("CREATE PROPERTY GRAPH g VERTEX TABLES (t KEY (k) PROPERTIES (a, ))", "<column> [**AS** <name>]"),
        ("CREATE PROPERTY GRAPH g VERTEX TABLES ()", "Expected: **CREATE**"),
        ("DROP PROPERTY GRAPH a, b", "**DROP PROPERTY GRAPH**"),
        ("SHOW CREATE PROPERTY GRAPH g extra", "**SHOW CREATE PROPERTY GRAPH**"),
    ],
)  # fmt: skip
def test_refusals_quote_the_grammar(statement, message):
    with pytest.raises(UnsupportedSyntaxError) as err:
        parse_statement(statement)
    assert message in str(err.value)
