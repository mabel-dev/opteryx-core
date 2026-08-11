"""
Test Session.check - edit-time checking that stops at the end of binding.

What a checker owes an editor: an error WITH A POSITION, the result shape before
there is a result, and the columns in scope for completion. What it must never do is
say a statement is fine when it is not, so the shared-path property (`bind_statement`
is the same function `query_planner` calls) is pinned here too.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx
from opteryx.catalog_cache import CatalogCache
from opteryx.exceptions import ColumnNotFoundError
from opteryx.exceptions import ParameterError
from opteryx.exceptions import QueryParseError


def _session():
    return opteryx.session()


# ============================ what it reports ============================


def test_clean_statement_reports_its_result_shape():
    """The names and types the statement will produce, before it produces any."""
    result = _session().check("SELECT name, gravity * 2 AS g2 FROM $planets")

    assert result.ok
    assert result.error is None
    assert [column.name for column in result.columns] == ["name", "g2"]
    assert result.columns[0].type == "VARCHAR"
    # the expression's type is resolved, not guessed
    assert result.columns[1].type.startswith("DECIMAL")


def test_relations_are_reported_for_completion():
    """Under the name a column has to be qualified by, with every column it has."""
    result = _session().check("SELECT name FROM $planets AS p")

    assert result.ok
    assert [relation.alias for relation in result.relations] == ["p"]
    assert result.relations[0].relation == "$planets"
    names = [column.name for column in result.relations[0].columns]
    assert "name" in names
    # completion needs the columns NOT typed yet
    assert "gravity" in names


def test_relations_keep_full_width_under_an_aggregate():
    """Regression: the binder narrows a scan's schema IN PLACE to the columns the
    statement referenced, so reading the schema back left `SELECT COUNT(*) ... GROUP
    BY name` reporting `$planets` as a one-column relation - and completion would
    then offer only the column already typed."""
    result = _session().check("SELECT COUNT(*) FROM $planets GROUP BY name")

    assert result.ok
    assert len(result.relations) == 1
    assert len(result.relations[0].columns) > 1


def test_statement_with_no_result_set_reports_no_columns():
    result = _session().check("SET x = 1")

    assert result.ok
    assert result.columns == ()


# ============================ what it finds wrong ============================


def test_unknown_column_is_reported_with_a_range_over_it():
    result = _session().check("SELECT nam FROM $planets")

    assert not result.ok
    assert isinstance(result.error, ColumnNotFoundError)
    position = result.position
    assert position is not None
    # the range covers the name that was written, so an editor underlines `nam`
    assert "SELECT nam FROM $planets"[position.start_offset : position.end_offset] == "nam"


def test_a_statement_that_does_not_parse_reports_where():
    result = _session().check("SELECT name, FROM $planets")

    assert not result.ok
    assert isinstance(result.error, QueryParseError)
    assert result.position is not None
    # nothing parsed, so there is nothing to complete from - and it says so by
    # reporting nothing rather than by guessing
    assert result.columns == ()
    assert result.relations == ()


def test_unknown_relation_is_reported_not_raised():
    result = _session().check("SELECT * FROM does_not_exist")

    assert not result.ok
    assert result.error is not None


def test_unknown_function_is_reported_with_a_range_over_it():
    statement = "SELECT UPPERCASE(name) FROM $planets"
    result = _session().check(statement)

    assert not result.ok
    position = result.position
    assert position is not None
    assert statement[position.start_offset : position.end_offset].startswith("UPPERCASE")


def test_incompatible_types_are_reported():
    result = _session().check("SELECT 1 + 'a' FROM $planets")

    assert not result.ok
    assert result.error is not None


def test_missing_parameter_is_reported_not_raised():
    """An unsupplied `:name` is a thing the reader can fix, so it is a diagnostic and
    not an exception - even though ParameterError is not a SqlError."""
    result = _session().check("SELECT name FROM $planets WHERE gravity > :threshold")

    assert not result.ok
    assert isinstance(result.error, ParameterError)


def test_supplied_parameters_bind():
    result = _session().check(
        "SELECT name FROM $planets WHERE gravity > :threshold", params={"threshold": 1}
    )

    assert result.ok


def test_a_write_to_a_read_only_relation_is_reported():
    result = _session().check("INSERT INTO $planets (id) VALUES (1)")

    assert not result.ok
    assert result.error is not None


def test_an_empty_statement_raises_rather_than_reporting():
    """Nothing to check is the caller getting it wrong, not the reader."""
    from opteryx.exceptions import MissingSqlStatement

    with pytest.raises(MissingSqlStatement):
        _session().check("")


# ============================ identities, for completion ============================


def _by_identity(result):
    return {(i.type, i.identity): i for i in result.identities}


def test_same_named_columns_from_two_relations_both_survive():
    """`ON p.id = q.id` references a column called `id` from each leg. Keying the
    dedupe on the name alone kept one and gave it that one's source — hiding a
    referenced column and asserting a relation for it the reader could not rely on."""
    result = _session().check(
        "SELECT p.name FROM $planets AS p INNER JOIN $planets AS q ON p.id = q.id"
    )

    assert result.ok
    sources = sorted(i.source for i in result.identities if i.identity == "id")
    assert sources == ["p", "q"]


def test_identities_name_relations_columns_and_computed_columns():
    """The shape the autocomplete suggestor consumes: every name the statement makes
    available and what it stands for."""
    result = _session().check(
        "SELECT COUNT(*) AS c, ROUND(gravity) FROM $planets AS t "
        "WHERE name = 'Earth' GROUP BY ROUND(gravity)"
    )

    assert result.ok
    found = _by_identity(result)

    # the relation, under its alias, defined by what it actually is
    assert found[("relation", "t")].definition == "$planets"

    # a column the statement DEFINES - named by its alias, defined by its expression
    assert found[("column", "c")].definition == "COUNT(*)"
    assert found[("column", "c")].data_type == "INT64"

    # ... and unaliased, named by the rendering the engine calls it by
    assert found[("column", "ROUND(gravity)")].definition == "ROUND(gravity)"

    # a column the statement REFERENCES, qualified by where it comes from
    assert found[("column", "name")].definition == "name"
    assert found[("column", "name")].source == "t"
    assert found[("column", "name")].data_type == "VARCHAR"


def test_an_alias_is_defined_by_the_column_it_renames():
    """An aliased identifier RENDERS as its alias, so rendering it to get a definition
    made `s.name AS moon` mean `moon` - true and useless. It stands for the column
    underneath."""
    result = _session().check("SELECT p.name AS n FROM $planets AS p")

    identity = _by_identity(result)[("column", "n")]
    assert identity.definition == "name"
    assert identity.source == "p"


def test_one_column_seen_at_two_plan_levels_is_reported_once():
    """The head's output columns carry no relation, so an aliased column was collected
    both qualified and bare and a suggestor would offer the same name twice."""
    result = _session().check(
        "SELECT p.name, s.name AS moon FROM $planets AS p "
        "INNER JOIN testdata.satellites AS s ON p.id = s.planetId"
    )

    assert result.ok
    moons = [i for i in result.identities if i.identity == "moon"]
    assert len(moons) == 1
    assert moons[0].definition == "name"
    assert moons[0].source == "s"


def test_identities_are_serializable():
    result = _session().check("SELECT COUNT(*) AS c FROM $planets AS t").as_dict()

    assert {"identity": "c", "type": "column", "definition": "COUNT(*)",
            "source": None, "data_type": "INT64"} in result["identities"]
    assert {"identity": "t", "type": "relation", "definition": "$planets",
            "source": None, "data_type": None} in result["identities"]


def test_an_unresolved_name_is_not_offered_as_an_identity():
    """`nam` is the typo that broke the statement. Reporting it would have a suggestor
    propose the reader's own mistake back to them as a column of the table."""
    result = _session().check("SELECT nam FROM $planets AS t WHERE gravity > 1")

    assert not result.ok
    assert ("column", "nam") not in _by_identity(result)
    # what DID resolve is still reported
    assert ("column", "gravity") in _by_identity(result)


def test_minted_aliases_are_never_offered():
    """The resolver mints `$view-a1B2` / `$union-c3D4` to keep spliced copies apart.
    They resolve to nothing if typed, so a suggestor must never see them."""
    for statement in (
        "WITH c AS (SELECT id, name FROM $planets WHERE mass > 1) SELECT name FROM c",
        "SELECT name FROM $planets UNION SELECT name FROM $planets",
    ):
        result = _session().check(statement)
        assert result.ok, statement
        offered = (
            [i.identity for i in result.identities]
            + [i.source for i in result.identities if i.source]
            + [r.alias for r in result.relations]
        )
        assert not [name for name in offered if name.startswith(("$view-", "$union-"))], statement


def test_a_column_inside_a_cte_body_is_qualified_by_the_relation_not_the_splice():
    """`mass` is referenced inside the CTE body, where the reader wrote `$planets`.
    Reporting it as coming `from $view-bn46` tells them nothing they can use."""
    result = _session().check(
        "WITH c AS (SELECT id, name FROM $planets WHERE mass > 1) SELECT name FROM c"
    )

    assert result.ok
    assert _by_identity(result)[("column", "mass")].source == "$planets"


def test_a_cte_is_addressable_by_the_name_it_was_given():
    """Both names are in scope and both are reported: `c` for the outer query, and the
    relation underneath for someone typing inside the body."""
    result = _session().check(
        "WITH c AS (SELECT id, name FROM $planets) SELECT name FROM c"
    )

    by_alias = {relation.alias: relation for relation in result.relations}
    assert set(by_alias) == {"c", "$planets"}
    # the CTE exposes only what it selects; the table underneath exposes everything
    assert [column.name for column in by_alias["c"].columns] == ["id", "name"]
    assert len(by_alias["$planets"].columns) > 2
    assert ("relation", "c") in _by_identity(result)


# ================= columns in scope, including on a broken statement =================


def test_relations_survive_a_failed_bind():
    """The point of this for an editor: the statement being typed is wrong most of the
    time, and completion has to work anyway. Binding is bottom-up, so a query broken in
    its SELECT list has already resolved its FROM - this reports what the real binder
    resolved, not a guess."""
    result = _session().check("SELECT nam FROM $planets AS t WHERE gravity > 1")

    assert not result.ok
    assert [relation.alias for relation in result.relations] == ["t"]
    names = [column.name for column in result.relations[0].columns]
    assert "name" in names and "gravity" in names
    # ... at full width, so the suggestor can offer the column they meant
    assert len(names) > 2


def test_relations_are_empty_when_the_from_clause_itself_is_wrong():
    """Nothing resolved, so nothing is offered - rather than something invented."""
    result = _session().check("SELECT * FROM no_such_table")

    assert not result.ok
    assert result.relations == ()


# ================= what the statement IS (superset of analyze_query) =================

_ANALYZE_FIELDS = (
    "query_type",
    "tables",
    "parameters",
    "is_read",
    "is_mutation",
    "is_ddl",
    "permission_required",
)


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT * FROM t WHERE d = :dept",
        "SELECT u.name FROM users AS u JOIN orders AS o ON u.id = o.user_id",
        "WITH c AS (SELECT * FROM users) SELECT * FROM c",
        "INSERT INTO t (a) VALUES (1)",
        "DROP TABLE t",
        "CREATE VIEW v AS SELECT 1",
        "REFRESH MATERIALIZED VIEW opteryx.public.daily",
        "SHOW COLUMNS FROM users",
    ],
)
def test_check_agrees_with_analyze_query(statement):
    """One parse, one answer. `check` reports everything `analyze_query` does, from
    the same AST — two derivations of 'what is this statement' would be two chances
    to disagree."""
    analyzed = opteryx.analyze_query(statement)
    checked = _session().check(statement)

    for field in _ANALYZE_FIELDS:
        expected = analyzed[field]
        actual = getattr(checked, field)
        if isinstance(expected, list):
            actual = list(actual)
        assert actual == expected, f"{field}: {actual!r} != {expected!r} for {statement}"


def test_what_the_statement_is_survives_a_failed_bind():
    """The reason this is worth having: a statement that will not bind for want of a
    parameter still says WHICH parameter, which is the one thing needed to fix it."""
    result = _session().check("SELECT name FROM $planets WHERE gravity > :threshold")

    assert not result.ok
    assert isinstance(result.error, ParameterError)
    assert result.parameters == ("threshold",)
    assert result.query_type == "Query"
    assert result.permission_required == "reader"


def test_supplying_parameters_does_not_empty_the_parameter_list():
    """Read from the PRE-rewrite AST. The AST rewriter substitutes placeholders, so a
    statement rewritten with its parameters supplied no longer records that a `:name`
    was ever written."""
    result = _session().check(
        "SELECT name FROM $planets WHERE gravity > :threshold", params={"threshold": 1}
    )

    assert result.ok
    assert result.parameters == ("threshold",)


def test_a_statement_that_does_not_parse_reports_nothing_about_itself():
    """No AST, so nothing to read off it — reported as absent, not guessed."""
    result = _session().check("SELECT name, FROM $planets")

    assert not result.ok
    assert result.query_type is None
    assert result.tables == ()
    assert result.permission_required is None


def test_ddl_reports_the_tier_it_needs():
    result = _session().check("DROP TABLE $planets")

    assert result.query_type == "Drop"
    assert result.is_ddl
    assert result.permission_required == "owner"


# ============================ the wire form ============================


def test_as_dict_is_plain_data():
    result = _session().check("SELECT nam FROM $planets").as_dict()

    assert result["ok"] is False
    assert result["error"]["type"] == "ColumnNotFoundError"
    assert isinstance(result["error"]["message"], str)
    position = result["error"]["position"]
    assert position["start_line"] == 1
    assert position["end_offset"] > position["start_offset"]


def test_as_dict_of_a_clean_statement_carries_no_error():
    result = _session().check("SELECT name FROM $planets").as_dict()

    assert result["ok"] is True
    assert result["error"] is None
    assert result["columns"] == [{"name": "name", "type": "VARCHAR", "nullable": True}]


def test_as_dict_carries_every_analyze_field_as_plain_data():
    result = _session().check("SELECT * FROM t WHERE d = :dept").as_dict()

    for field in _ANALYZE_FIELDS:
        assert field in result
    assert result["tables"] == ["t"]
    assert result["parameters"] == ["dept"]
    assert result["query_type"] == "Query"


# ============================ it does not run anything ============================


def test_checking_does_not_execute():
    """A check binds and stops. If it executed, this would return rows and the row
    count below would not be the one a fresh session reports."""
    session = _session()
    result = session.check("SELECT name FROM $planets")

    assert result.ok
    with pytest.raises(Exception):
        # nothing was executed, so there is no row count to ask for
        session.rowcount


def test_the_check_path_and_the_planner_path_are_the_same_front_half():
    """`query_planner` calls `bind_statement`; a second parse-and-bind would drift,
    and a checker that drifts is a checker that lies."""
    import inspect

    from opteryx.planner import query_planner

    assert "bind_statement(" in inspect.getsource(query_planner)


def test_a_schema_only_bind_reads_no_manifest():
    from opteryx.models import ExecutionContext
    from opteryx.models import QueryTelemetry
    from opteryx.planner import bind_statement
    from opteryx.planner.logical_planner import LogicalPlanStepType

    plan, _, _ = bind_statement(
        operation="SELECT name FROM $planets",
        parameters=[],
        visibility_filters=None,
        execution_context=ExecutionContext(memberships=["public"]),
        query_id="test",
        telemetry=QueryTelemetry("test"),
        schema_only=True,
    )

    scans = [node for _, node in plan.nodes(True) if node.node_type == LogicalPlanStepType.Scan]
    assert scans
    for scan in scans:
        assert scan.manifest is None


# ============================ the catalog cache ============================


def test_cache_returns_what_was_put_in_it():
    cache = CatalogCache(ttl=60)
    cache.put("a.b", ("dataset", "handle"))

    assert cache.get("a.b") == ("dataset", "handle")
    assert cache.get("a.c") is None


def test_cache_round_trips_a_negative_answer():
    """`(None, None)` is 'not in this catalog' and has to survive the round trip -
    otherwise every keystroke re-asks the catalog about a name it has already said no
    to, which is the common case while a name is being typed."""
    cache = CatalogCache(ttl=60)
    cache.put("nope", (None, None))

    assert cache.get("nope") == (None, None)


def test_cache_expires():
    cache = CatalogCache(ttl=0.01)
    cache.put("a.b", ("dataset", "handle"))

    import time

    time.sleep(0.05)
    assert cache.get("a.b") is None


def test_cache_evicts_the_oldest_when_full():
    cache = CatalogCache(ttl=60, maxsize=2)
    cache.put("one", 1)
    cache.put("two", 2)
    cache.put("three", 3)

    assert len(cache) == 2
    assert cache.get("one") is None
    assert cache.get("three") == 3


def test_cache_can_be_invalidated():
    cache = CatalogCache(ttl=60)
    cache.put("a.b", ("dataset", "handle"))
    cache.invalidate("a.b")

    assert cache.get("a.b") is None


def test_cache_rejects_a_ttl_that_never_expires():
    with pytest.raises(ValueError):
        CatalogCache(ttl=0)


def test_resolve_relation_asks_the_catalog_once_per_ttl(monkeypatch):
    """The whole point: a burst of keystrokes costs one round trip per relation."""
    from opteryx.managers import views

    calls = []

    class _Connector:
        eidetic = True

        def get_relation(self, relation):
            calls.append(relation)
            return ("dataset", f"handle-for-{relation}")

    monkeypatch.setattr(views, "connector_factory", lambda relation, telemetry: _Connector())

    cache = CatalogCache(ttl=60)
    for _ in range(5):
        kind, obj = views.resolve_relation("space.planets", None, cache)
        assert kind == "dataset"
        assert obj == "handle-for-space.planets"

    assert calls == ["space.planets"]


def test_resolve_relation_without_a_cache_asks_every_time():
    """The execute path passes no cache, and must not quietly get a stale one: the
    dataset document is the version pointer."""
    from opteryx.managers import views

    calls = []

    class _Connector:
        eidetic = True

        def get_relation(self, relation):
            calls.append(relation)
            return ("dataset", "handle")

    original = views.connector_factory
    views.connector_factory = lambda relation, telemetry: _Connector()
    try:
        for _ in range(3):
            views.resolve_relation("space.planets", None)
    finally:
        views.connector_factory = original

    assert len(calls) == 3


def test_query_planner_takes_no_catalog_cache():
    """Structural: an execute-path caller cannot pass one even by accident."""
    import inspect

    from opteryx.planner import query_planner

    assert "catalog_cache" not in inspect.signature(query_planner).parameters


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
