"""Unit tests for ALTER TABLE ... CREATE TAG / DROP TAG.

sqlparser has no grammar for tag DDL at all, so the OpteryxDialect parses these
statements itself (`parse_tag_ddl` in src/opteryx_dialect.rs), the way the
Snowflake dialect parses its own. What it cannot do is invent an AST node -
`AlterTableOperation` has no `CreateTag` - so the parsed result reaches the
planner inside `SetTblProperties` under reserved `__opteryx.tag.*` keys.

That transport is the interesting part to test: it must be readable by the
planner, and NOT writable by a reader, or tag DDL would have a documented
spelling and a second, discoverable one.

Execution needs a live catalog and belongs to the integration suite.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.getcwd()))

import pytest

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.planner.logical_planner import do_logical_planning_phase
from opteryx.planner.logical_planner.logical_planner import LogicalPlanStepType
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party import sqloxide


def _plan_node(sql: str):
    """The single logical node a tag statement plans to."""
    parsed = sqloxide.parse_sql(str(do_sql_rewrite(sql)), _dialect="opteryx")[0]
    plan = do_logical_planning_phase(parsed)[0]
    node_ids = list(plan.nodes())
    assert len(node_ids) == 1, "tag DDL is one node"
    return plan[node_ids[0]]


# --- CREATE TAG ----------------------------------------------------------


def test_create_tag_defaults_to_the_current_version():
    """Omitting the clause is the same as writing CURRENT - the common case is
    tagging what you just committed."""
    node = _plan_node("ALTER TABLE reports CREATE TAG report_202602")
    assert node.node_type == LogicalPlanStepType.CreateTag
    assert node.relation_name == "reports"
    assert node.tag_name == "report_202602"
    assert node.version_spec == "current"


def test_create_tag_as_of_a_snapshot_id():
    node = _plan_node("ALTER TABLE reports CREATE TAG r1 AS OF VERSION 12345")
    assert node.version_spec == "12345"


def test_create_tag_as_of_previous():
    """PREVIOUS survives to the connector as a word, not as a number.

    The planner does not resolve it: CURRENT and PREVIOUS name a snapshot only
    the catalog can identify, and resolving here would read the catalog to build
    a plan that then reads it again.
    """
    node = _plan_node("ALTER TABLE reports CREATE TAG r1 AS OF VERSION PREVIOUS")
    assert node.version_spec == "previous"


def test_the_tag_name_may_be_quoted_or_bare():
    quoted = _plan_node("ALTER TABLE reports CREATE TAG 'report_202602'")
    bare = _plan_node("ALTER TABLE reports CREATE TAG report_202602")
    assert quoted.tag_name == bare.tag_name == "report_202602"


def test_the_case_the_reader_typed_reaches_the_catalog():
    """Tag names fold to lowercase, but the catalog owns that rule - a second
    copy of it in the parser is a second place for it to drift."""
    node = _plan_node("ALTER TABLE reports CREATE TAG Report_202602")
    assert node.tag_name == "Report_202602"


def test_a_version_that_is_neither_a_number_nor_a_keyword_is_refused():
    with pytest.raises(Exception) as err:
        _plan_node("ALTER TABLE reports CREATE TAG r1 AS OF VERSION nonsense")
    assert "CURRENT" in str(err.value)


# --- DROP TAG ------------------------------------------------------------


def test_drop_tag():
    node = _plan_node("ALTER TABLE reports DROP TAG report_202602")
    assert node.node_type == LogicalPlanStepType.DropTag
    assert node.tag_name == "report_202602"


def test_drop_tag_honours_if_exists_on_the_table():
    node = _plan_node("ALTER TABLE IF EXISTS reports DROP TAG r1")
    assert node.if_exists is True


def test_drop_tag_takes_no_version():
    """A tag names one snapshot and is dropped by name; there is nothing to
    qualify."""
    with pytest.raises(Exception):
        _plan_node("ALTER TABLE reports DROP TAG r1 AS OF VERSION 12")


# --- the transport is not a second spelling ------------------------------


def test_a_hand_written_reserved_property_is_refused():
    """The reserved keys are an internal transport. A reader who discovers them
    and types them by hand is told no, so tag DDL has exactly one spelling.

    The two are told apart by the SHAPE of the key rather than by trusting the
    prefix: the dialect emits an unquoted identifier containing dots, which
    reader text cannot produce - a bare key cannot contain a dot, and a quoted
    key arrives carrying its quote style.
    """
    with pytest.raises(UnsupportedSyntaxError) as err:
        _plan_node(
            "ALTER TABLE reports SET TBLPROPERTIES "
            "('__opteryx.tag.action'='create', '__opteryx.tag.name'='sneaky')"
        )
    assert "reserved" in str(err.value)
    assert "CREATE TAG" in str(err.value)


def test_ordinary_set_tblproperties_is_still_unsupported():
    """Adding the transport must not accidentally introduce property support."""
    with pytest.raises(UnsupportedSyntaxError) as err:
        _plan_node("ALTER TABLE reports SET TBLPROPERTIES ('a'='b')")
    assert "TBLPROPERTIES" in str(err.value)


# --- the rest of ALTER TABLE is untouched --------------------------------


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("ALTER TABLE reports DROP COLUMN c", LogicalPlanStepType.DropColumn),
        ("ALTER TABLE reports ADD COLUMN c INTEGER", LogicalPlanStepType.AddColumn),
        (
            "ALTER TABLE reports ADD COLUMN IF NOT EXISTS c INTEGER",
            LogicalPlanStepType.AddColumn,
        ),
        # Qualified on both sides: an unqualified rename reads as a
        # cross-workspace move, which is refused for its own reasons.
        ("ALTER TABLE ws.reports RENAME TO ws.other", LogicalPlanStepType.RenameRelation),
        ("ALTER TABLE reports CLUSTER BY (a)", LogicalPlanStepType.AlterRelation),
        ("ALTER TABLE reports RENAME COLUMN a TO b", LogicalPlanStepType.RenameColumn),
    ],
)
def test_other_alter_table_statements_still_plan(sql, expected):
    """`DROP TAG` and `DROP COLUMN` share a prefix and only the word after DROP
    separates them, so the tag probe has to rewind cleanly on a miss."""
    assert _plan_node(sql).node_type == expected
