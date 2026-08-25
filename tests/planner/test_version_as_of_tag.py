"""Unit tests for VERSION AS OF <tag>.

Drives the SQL rewriter and the planner's extraction helpers directly, the same
way ``test_version_as_of.py`` drives the snapshot-id form. Catalog-level
resolution (``OpteryxTable._resolve_snapshot`` calling ``catalog.resolve_tag``)
needs a live catalog and belongs to the integration suite, not here.

The thing under test is a re-spelling. sqlparser reads a NUMBER after
``VERSION AS OF`` (``parse_number_value``, not ``parse_expr``), so a tag name
cannot travel under the reader's own spelling and the rewriter carries it as
``AT(TAG => '<tag>')`` - a slot in the VERSION space, not the timestamp space,
because a tag names a snapshot and never a point in time.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.getcwd()))

import pytest

from opteryx.exceptions import UnsupportedSyntaxError
from opteryx.planner.logical_planner.logical_planner_builders import (
    extract_timetravel_tag,
    is_tag_clause,
    is_version_as_of_clause,
)
from opteryx.planner.sql_rewriter import do_sql_rewrite
from opteryx.third_party import sqloxide


def _rewritten(sql: str) -> str:
    return str(do_sql_rewrite(sql))


def _parse_version(sql: str):
    parsed = sqloxide.parse_sql(_rewritten(sql), _dialect="opteryx")[0]
    return parsed["Query"]["body"]["Select"]["from"][0]["relation"]["Table"]["version"]


# --- the rewrite ---------------------------------------------------------


def test_a_quoted_tag_is_carried_as_an_at_clause():
    assert (
        _rewritten("SELECT * FROM reports VERSION AS OF 'report_202602'")
        == "SELECT * FROM reports AT(TAG => 'report_202602')"
    )


def test_an_unquoted_tag_means_the_same_thing():
    """A tag name IS an identifier, so someone who created it unquoted will read
    it back unquoted."""
    assert _rewritten("SELECT * FROM reports VERSION AS OF report_202602") == _rewritten(
        "SELECT * FROM reports VERSION AS OF 'report_202602'"
    )


def test_a_snapshot_id_is_left_alone():
    """The number form still reaches the parser unrewritten - the tag pass must
    not capture it."""
    sql = "SELECT * FROM reports VERSION AS OF 42"
    assert _rewritten(sql) == sql


def test_previous_is_still_the_sentinel_and_not_a_tag():
    """PREVIOUS is a bare word and would match the tag pattern, so ordering
    between the two passes is load-bearing: by the time the tag pass runs it is
    already the digit 0."""
    assert _rewritten("SELECT * FROM reports VERSION AS OF PREVIOUS") == (
        "SELECT * FROM reports VERSION AS OF 0"
    )


def test_the_phrase_inside_a_string_literal_is_untouched():
    """Every rewrite carries a quoted-span guard; this one is no exception."""
    sql = "SELECT 'VERSION AS OF report_202602' FROM reports"
    assert _rewritten(sql) == sql


def test_current_is_refused_on_a_read():
    """`CURRENT` is a CREATE TAG spelling. Accepting it here would silently look
    up a tag named `current` and report it missing, which explains nothing."""
    with pytest.raises(UnsupportedSyntaxError) as err:
        _rewritten("SELECT * FROM reports VERSION AS OF CURRENT")
    assert "CURRENT" in str(err.value)


def test_positions_still_point_at_what_the_reader_typed():
    """The rewrite is an edit, not a reformat: text either side of it must not
    move, or every parser position downstream becomes unusable."""
    sql = "SELECT name FROM reports VERSION AS OF 'r1' WHERE id > 3"
    rewritten = _rewritten(sql)
    assert rewritten.startswith("SELECT name FROM reports ")
    assert rewritten.endswith(" WHERE id > 3")


def test_the_reversed_word_order_says_which_order_to_use():
    """`AS OF VERSION` on a read is refused by NAME, not by token.

    Both orders are real in this dialect - reads take `VERSION AS OF`, tag DDL
    takes Iceberg's `CREATE TAG x AS OF VERSION y` - so someone who has just
    written the DDL reaches for the same order on the read. The parser's own
    complaint there is "Expected: end of statement, found: VERSION", which
    blames the wrong token and names neither order.
    """
    with pytest.raises(UnsupportedSyntaxError) as err:
        _rewritten("SELECT * FROM reports AS OF VERSION test_tag")
    message = str(err.value)
    # The suggestion echoes the reader's OWN operand, in a code span the display
    # surface makes copyable - a corrected statement, not a shape to translate.
    assert "`VERSION AS OF test_tag`" in message
    assert "CREATE TAG" in message


@pytest.mark.parametrize(
    "operand", ["test_tag", "'quoted_tag'", "42"]
)
def test_the_suggestion_carries_whatever_was_written(operand):
    with pytest.raises(UnsupportedSyntaxError) as err:
        _rewritten(f"SELECT * FROM reports AS OF VERSION {operand}")
    assert f"`VERSION AS OF {operand}`" in str(err.value)


def test_a_missing_operand_still_names_the_form():
    """Nothing to echo is not nothing to say."""
    with pytest.raises(UnsupportedSyntaxError) as err:
        _rewritten("SELECT * FROM reports AS OF VERSION")
    assert "snapshot id" in str(err.value)


def test_the_ddl_keeps_its_own_word_order():
    """The rule that helps the reader must not refuse the statement they were
    copying from."""
    sql = "ALTER TABLE reports CREATE TAG test_tag AS OF VERSION CURRENT"
    assert _rewritten(sql) == sql


def test_the_reversed_order_inside_a_literal_is_not_a_statement():
    sql = "SELECT 'AS OF VERSION x' FROM reports"
    assert _rewritten(sql) == sql


# --- reading it back out -------------------------------------------------


def test_a_tag_clause_is_recognised_as_one():
    version = _parse_version("SELECT * FROM reports VERSION AS OF 'report_202602'")
    assert is_tag_clause(version)
    assert extract_timetravel_tag(version) == "report_202602"


def test_a_tag_is_not_a_snapshot_id_clause():
    """The two arms are mutually exclusive - a tag resolves through the catalog
    by name, an id does not resolve at all."""
    version = _parse_version("SELECT * FROM reports VERSION AS OF 'report_202602'")
    assert not is_version_as_of_clause(version)


def test_a_snapshot_id_is_not_a_tag_clause():
    version = _parse_version("SELECT * FROM reports VERSION AS OF 42")
    assert is_tag_clause(version) is False
    assert is_version_as_of_clause(version)


def test_a_timestamp_is_not_a_tag_clause():
    version = _parse_version("SELECT * FROM reports TIMESTAMP AS OF '2024-12-15'")
    assert is_tag_clause(version) is False


def test_no_version_clause_is_not_a_tag_clause():
    assert is_tag_clause(None) is False


def test_the_case_the_reader_typed_survives_the_planner():
    """Tag names fold to lowercase, but the catalog owns that rule. Folding here
    too would put a second, silent copy of it in the planner."""
    version = _parse_version("SELECT * FROM reports VERSION AS OF Report_202602")
    assert extract_timetravel_tag(version) == "Report_202602"


def test_an_at_clause_that_is_not_a_tag_is_refused_by_name():
    """`AT(...)` is not a spelling this dialect offers - the rewriter is its only
    source - so anything else arriving there is named rather than supported.

    This is also where `AT(TIMESTAMP => ...)` used to be advertised as legacy
    syntax by a docstring while every code path raised. The claim is withdrawn:
    there is one spelling for a point-in-time read.
    """
    from opteryx.planner.logical_planner.logical_planner_builders import (
        extract_timetravel_timestamp,
    )

    version = _parse_version("SELECT * FROM reports AT(TIMESTAMP => '2024-12-15')")
    assert is_tag_clause(version) is False
    with pytest.raises(UnsupportedSyntaxError) as err:
        extract_timetravel_timestamp(version)
    assert "TIMESTAMP AS OF" in str(err.value)
