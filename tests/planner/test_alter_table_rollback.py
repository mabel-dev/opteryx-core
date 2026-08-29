"""Unit tests for ALTER TABLE ... ROLLBACK TO VERSION.

Like tag DDL, sqlparser has no grammar for this, so the OpteryxDialect parses it
itself (`parse_rollback_ddl` in src/opteryx_dialect.rs) and hands the result to
the planner inside `SetTblProperties` under a reserved `__opteryx.rollback.*`
key - `AlterTableOperation` has no variant for it and inventing one means
forking sqlparser.

The version is carried as TEXT and resolved by the connector, where the catalog
is. `current`, `previous`, a tag name and a snapshot id are four different kinds
of answer, and a planner that resolved them would read the catalog to build a
plan that then reads it again.

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
    """The single logical node a rollback statement plans to."""
    parsed = sqloxide.parse_sql(str(do_sql_rewrite(sql)), _dialect="opteryx")[0]
    plan = do_logical_planning_phase(parsed)[0]
    node_ids = list(plan.nodes())
    assert len(node_ids) == 1, "rollback DDL is one node"
    return plan[node_ids[0]]


# --- the statement -------------------------------------------------------


def test_rollback_to_a_snapshot_id():
    node = _plan_node("ALTER TABLE reports ROLLBACK TO VERSION 12345")

    assert node.node_type == LogicalPlanStepType.RollbackRelation
    assert node.relation_name == "reports"
    assert node.version_spec == "12345"


def test_rollback_to_previous():
    """PREVIOUS survives as a word. It means the previous VERSION OF THE DATA,
    which only the catalog can identify - it steps over the compaction commits
    that changed no rows."""
    node = _plan_node("ALTER TABLE reports ROLLBACK TO VERSION PREVIOUS")

    assert node.version_spec == "previous"


def test_rollback_to_a_tag():
    """The most useful spelling: the tag somebody made before the migration."""
    node = _plan_node("ALTER TABLE reports ROLLBACK TO VERSION report_202602")

    assert node.version_spec == "report_202602"


def test_a_quoted_tag_name_means_the_same_as_a_bare_one():
    quoted = _plan_node("ALTER TABLE reports ROLLBACK TO VERSION 'report_202602'")
    bare = _plan_node("ALTER TABLE reports ROLLBACK TO VERSION report_202602")

    assert quoted.version_spec == bare.version_spec == "report_202602"


def test_rollback_to_current_is_accepted_and_left_for_the_catalog():
    """A no-op rather than an error: a rollback that has to be retried should be
    safe to retry, and the catalog reports whether the head actually moved."""
    node = _plan_node("ALTER TABLE reports ROLLBACK TO VERSION CURRENT")

    assert node.version_spec == "current"


def test_the_old_spelling_latest_names_the_word_that_replaced_it():
    with pytest.raises(Exception) as err:
        _plan_node("ALTER TABLE reports ROLLBACK TO VERSION LATEST")

    assert "CURRENT" in str(err.value)


def test_if_exists_is_carried():
    node = _plan_node("ALTER TABLE IF EXISTS reports ROLLBACK TO VERSION 12345")

    assert node.if_exists is True
    assert node.relation_name == "reports"


def test_a_qualified_relation_name_survives():
    node = _plan_node("ALTER TABLE space.coll.reports ROLLBACK TO VERSION 12345")

    assert node.relation_name == "space.coll.reports"


# --- the transport is not a second spelling ------------------------------


def test_the_reserved_key_cannot_be_written_by_hand():
    """Otherwise rollback would have a documented spelling and a discoverable
    one. A key a reader typed arrives quoted; the dialect's does not."""
    with pytest.raises(UnsupportedSyntaxError):
        _plan_node(
            "ALTER TABLE reports SET TBLPROPERTIES "
            "('__opteryx.rollback.version'='12345')"
        )


# --- the version is required ---------------------------------------------


def test_a_version_is_required():
    with pytest.raises(Exception):
        _plan_node("ALTER TABLE reports ROLLBACK TO VERSION")


def test_rollback_without_to_version_is_not_this_statement():
    with pytest.raises(Exception):
        _plan_node("ALTER TABLE reports ROLLBACK 12345")
