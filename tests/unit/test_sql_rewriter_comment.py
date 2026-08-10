"""
COMMENT ON TABLE / VIEW reaches the planner as itself.

These used to assert that the SQL rewriter turned both into COMMENT ON EXTENSION,
because sqlparser's `parse_comment` had no TABLE or VIEW branch. It has both now
(CommentObject::Table, CommentObject::View), so the rewrite was downgrading a correct
parse and has been deleted. What matters is no longer the intermediate text - it is that
the statement is not rewritten at all, and that the object type survives to the plan.
"""

import pytest

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.planner.logical_planner import do_logical_planning_phase
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party import sqloxide


def _plan(sql: str):
    rewritten = do_sql_rewrite(sql)
    # The rewriter has no business touching a COMMENT statement at all.
    assert str(rewritten) == sql
    parsed = sqloxide.parse_sql(str(rewritten), _dialect="opteryx")
    return do_logical_planning_phase(parsed[0])[0]


def _comment_node(plan):
    return [plan[nid] for nid in plan.nodes()][0]


def test_comment_on_table_plans_as_table():
    node = _comment_node(_plan("COMMENT ON TABLE workspace.collection.table IS 'test comment'"))
    assert node.object_type == "Table"
    assert node.object_name == "workspace.collection.table"
    assert node.comment == "test comment"


def test_comment_on_view_plans_as_view():
    node = _comment_node(_plan("COMMENT ON VIEW workspace.collection.view IS 'test comment'"))
    assert node.object_type == "View"
    assert node.object_name == "workspace.collection.view"


def test_comment_if_exists_is_carried():
    node = _comment_node(_plan("COMMENT IF EXISTS ON TABLE test.table IS 'comment'"))
    assert node.object_type == "Table"
    assert node.if_exists is True


def test_comment_is_case_insensitive():
    node = _comment_node(_plan("comment on table test.table is 'comment'"))
    assert node.object_type == "Table"


@pytest.mark.parametrize("object_type", ["COLUMN", "EXTENSION", "SCHEMA"])
def test_comment_on_other_object_types_is_refused(object_type):
    """Everything sqlparser can parse but Opteryx has no comment store for.

    EXTENSION is in this list on purpose: it was only ever the rewriter's internal
    target, never syntax anyone was meant to write, and it now fails by name rather
    than reaching the operator as a comment on a table that does not exist.
    """
    with pytest.raises(UnsupportedSyntaxError):
        _plan(f"COMMENT ON {object_type} test.thing IS 'comment'")


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
