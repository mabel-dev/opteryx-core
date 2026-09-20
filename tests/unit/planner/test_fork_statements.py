"""The fork statements, from text to logical plan (FORKS_DESIGN.md S8).

Three statements, two routes into the planner. `CREATE TABLE ... CLONE` needs no
intercept at all - the vendored parser already produces a `CreateTable` carrying
a `clone` field, which is why that spelling was chosen over a bare `CLONE ... TO`
that would have needed a regex, a classification, an autocomplete entry and a
`SHOW CREATE` form of its own. `RESYNC` and `DETACH` do come through `pre_parse`,
because sqlparser's ALTER TABLE grammar has no such clause.

What is held here is the front half: that each statement reaches the right node
with the right fields, that the combinations which cannot mean anything are
refused with a reason, and that the classifier sees BOTH ends of a clone - the
upstream is read, and a caller pre-flighting permissions that never hears about
it would check the wrong half of the statement.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import do_logical_planning_phase
from opteryx.planner.pre_parse import pre_parse
from opteryx.third_party import sqloxide
from opteryx.utils.query_parser import parse_query_info

UPSTREAM = "samples.tpch_sf1.lineitem"
FORK = "personal.justin.lineitem"


def _node(sql):
    """The single logical node `sql` plans to."""
    ast = pre_parse(sql) or sqloxide.parse_sql(sql, _dialect="opteryx")
    plan, _ast, _ctes = do_logical_planning_phase(ast[0])
    return plan[list(plan.nodes())[0]]


# --------------------------------------------------------------------------
# 1. CREATE TABLE ... CLONE
# --------------------------------------------------------------------------


def test_clone_plans_to_a_clone_node_naming_both_ends():
    node = _node(f"CREATE TABLE {FORK} CLONE {UPSTREAM}")

    assert node.node_type == LogicalPlanStepType.CloneRelation
    assert node.relation_name == FORK
    assert node.source_relation == UPSTREAM


def test_clone_needs_no_pre_parse_intercept():
    # The reason this spelling was chosen: the parser already knows it.
    assert pre_parse(f"CREATE TABLE {FORK} CLONE {UPSTREAM}") is None


@pytest.mark.parametrize("option", ["OR REPLACE", "TEMPORARY", "TRANSIENT"])
def test_clone_refuses_options_that_describe_a_shape_it_does_not_define(option):
    # A clone adopts the upstream's schema whole, so accepting any of these
    # would mean silently ignoring what was written.
    with pytest.raises(UnsupportedSyntaxError):
        _node(f"CREATE {option} TABLE {FORK} CLONE {UPSTREAM}")


def test_clone_and_as_select_together_are_refused():
    with pytest.raises(UnsupportedSyntaxError, match="not both"):
        _node(f"CREATE TABLE {FORK} CLONE {UPSTREAM} AS SELECT 1")


def test_clone_refuses_a_bare_name_at_either_end():
    with pytest.raises(UnsupportedSyntaxError, match="names datasets"):
        _node(f"CREATE TABLE {FORK} CLONE justacollection")


def test_a_plain_create_table_is_untouched():
    node = _node("CREATE TABLE personal.justin.t (a INTEGER)")
    assert node.node_type == LogicalPlanStepType.CreateRelation


# --------------------------------------------------------------------------
# 2. ALTER TABLE ... RESYNC | DETACH
# --------------------------------------------------------------------------


def test_resync_plans_to_a_resync_node():
    node = _node(f"ALTER TABLE {FORK} RESYNC")

    assert node.node_type == LogicalPlanStepType.ResyncRelation
    assert node.relation_name == FORK
    assert node.force is False


def test_resync_force_sets_the_flag():
    # FORCE is the difference between a refresh and one that supersedes the
    # caller's own commits, so it must not be lost between the two layers.
    assert _node(f"ALTER TABLE {FORK} RESYNC FORCE").force is True


def test_detach_plans_to_a_detach_node():
    node = _node(f"ALTER TABLE {FORK} DETACH")

    assert node.node_type == LogicalPlanStepType.DetachRelation
    assert node.relation_name == FORK


def test_the_clauses_are_case_insensitive():
    assert _node(f"alter table {FORK} resync force").force is True
    assert _node(f"Alter Table {FORK} Detach").node_type == LogicalPlanStepType.DetachRelation


@pytest.mark.parametrize(
    "sql",
    [
        f"ALTER TABLE {FORK} ADD COLUMN x INTEGER",
        f"ALTER TABLE {FORK} RENAME TO personal.justin.other",
        f"ALTER TABLE {FORK} DROP COLUMN x",
    ],
)
def test_other_alter_table_forms_are_left_to_the_parser(sql):
    # The intercept must rule itself out cheaply rather than claim the verb:
    # almost every ALTER TABLE belongs to the parser.
    assert pre_parse(sql) is None


def test_a_misspelt_fork_clause_says_what_was_expected():
    with pytest.raises(UnsupportedSyntaxError, match="RESYNC"):
        _node(f"ALTER TABLE {FORK} RESYNC EXTRA")


# --------------------------------------------------------------------------
# 3. Classification - what a pre-flight sees
# --------------------------------------------------------------------------


def test_a_clone_reports_both_ends_as_tables():
    # Without the upstream, a caller checking permissions would be told the
    # statement touches only a dataset that does not exist yet, and would
    # never check the one it copies.
    info = parse_query_info(f"CREATE TABLE {FORK} CLONE {UPSTREAM}")

    assert set(info["tables"]) == {FORK, UPSTREAM}
    assert info["is_ddl"] is True
    assert info["permission_required"] == "owner"


def test_resync_is_a_mutation_and_detach_is_ddl():
    # RESYNC replaces contents; DETACH changes what the dataset IS. Both are
    # owner-tier - RESYNC FORCE can supersede the caller's own commits, and
    # DETACH starts billing them for storage they were borrowing.
    resync = parse_query_info(f"ALTER TABLE {FORK} RESYNC")
    detach = parse_query_info(f"ALTER TABLE {FORK} DETACH")

    assert (resync["is_mutation"], resync["is_ddl"]) == (True, False)
    assert (detach["is_mutation"], detach["is_ddl"]) == (False, True)
    assert resync["permission_required"] == detach["permission_required"] == "owner"


def test_the_fork_is_the_reported_target_of_both():
    assert parse_query_info(f"ALTER TABLE {FORK} RESYNC")["tables"] == [FORK]
    assert parse_query_info(f"ALTER TABLE {FORK} DETACH")["tables"] == [FORK]
