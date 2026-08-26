"""The WINDOW clause: `OVER w`, and `OVER (w ...)`, against what they must ANSWER.

WHAT WENT WRONG

`WINDOW w AS (...)` parsed, and then nothing read it. The parser hands a named
reference over as `{"NamedWindow": ...}` and an inherit-and-extend reference as a
`WindowSpec` carrying a `window_name`; the builder accepted only a plain inline
`WindowSpec`, so both were dropped. The two halves failed differently and both
silently:

  * `SUM(mass) OVER w` lost its `over` entirely, stopped looking like a window, and
    was planned as an ORDINARY AGGREGATE — nine rows collapsed to one global total.
  * `SUM(mass) OVER (w)` kept its `over` and lost only the base window's PARTITION
    BY, so it stayed a window, kept all nine rows, and changed only the VALUES. That
    is the worse of the two: nothing about the result looks wrong.

WHY THE ASSERTIONS ARE ON VALUES

Both wrong forms RAN. A test that asserted only that a named window did not raise
would have passed for as long as the defect existed, which is why every case below
compares against the inline spelling of the same specification, row for row, or
asserts the refusal. `$planets` is the fixture because its `gravity` has one
repeated value (Mercury and Mars, 3.7), so a dropped PARTITION BY cannot answer the
same thing as an honoured one.

THE FIX, AND WHY THERE ARE NO SEPARATE RULES HERE

Names are resolved into the specifications they stand for on the AST, before the
projection is built (`_resolve_named_windows`), so by the time the planner reaches
a window there is no such thing as a named one. That is deliberate: it means the
ORDER BY requirement, the frame refusals and the framed-aggregate restriction are
not re-implemented for a second spelling and cannot drift from the first. Those
rules are held against BOTH spellings in
tests/sql/test_window_catalog_matches_engine.py.
"""

import os
import sys

import pytest

from opteryx.exceptions import SqlError
from opteryx.exceptions import UnsupportedSyntaxError
from tests.helpers import execute_and_get_arrow

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def _rows(sql: str) -> dict:
    return execute_and_get_arrow(sql).to_pydict()


#: The inline spelling of every specification named below, keyed by the SQL the
#: named spelling has to match. Each pair is (named SQL, inline SQL).
_EQUIVALENT = {
    # `OVER w` — the form that used to collapse nine rows to one.
    "whole": (
        "SELECT id, SUM(mass) OVER w AS r FROM $planets "
        "WINDOW w AS (PARTITION BY gravity) ORDER BY id",
        "SELECT id, SUM(mass) OVER (PARTITION BY gravity) AS r FROM $planets ORDER BY id",
    ),
    # `OVER (w)` — the form that kept the row count and changed only the values.
    "extended_by_nothing": (
        "SELECT id, SUM(mass) OVER (w) AS r FROM $planets "
        "WINDOW w AS (PARTITION BY gravity) ORDER BY id",
        "SELECT id, SUM(mass) OVER (PARTITION BY gravity) AS r FROM $planets ORDER BY id",
    ),
    # Inherit a PARTITION BY and ADD an ORDER BY: the running form of it.
    "extended_by_an_order_by": (
        "SELECT id, SUM(mass) OVER (w ORDER BY id) AS r FROM $planets "
        "WINDOW w AS (PARTITION BY gravity) ORDER BY id",
        "SELECT id, SUM(mass) OVER (PARTITION BY gravity ORDER BY id) AS r "
        "FROM $planets ORDER BY id",
    ),
    # A definition extending an EARLIER definition, inside the clause itself.
    "chained_definitions": (
        "SELECT id, SUM(mass) OVER v AS r FROM $planets "
        "WINDOW w AS (PARTITION BY gravity), v AS (w ORDER BY id) ORDER BY id",
        "SELECT id, SUM(mass) OVER (PARTITION BY gravity ORDER BY id) AS r "
        "FROM $planets ORDER BY id",
    ),
    # A ranking function over a named window.
    "ranking": (
        "SELECT id, ROW_NUMBER() OVER w AS r FROM $planets "
        "WINDOW w AS (PARTITION BY gravity ORDER BY id) ORDER BY id",
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY gravity ORDER BY id) AS r "
        "FROM $planets ORDER BY id",
    ),
    # A frame on the named window, used WHOLE — `OVER w` may reference a framed one.
    "framed": (
        "SELECT id, SUM(mass) OVER w AS r FROM $planets WINDOW w AS "
        "(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) ORDER BY id",
        "SELECT id, SUM(mass) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) "
        "AS r FROM $planets ORDER BY id",
    ),
}


@pytest.mark.parametrize("case", sorted(_EQUIVALENT))
def test_named_spelling_answers_what_the_inline_spelling_answers(case):
    named_sql, inline_sql = _EQUIVALENT[case]
    inline = _rows(inline_sql)
    named = _rows(named_sql)

    assert len(named["id"]) == 9, (
        f"{case}: {len(named['id'])} rows, not 9 — the window was planned as an aggregate"
    )
    assert named == inline, f"{case}: named {named} != inline {inline}"


def test_the_partitioned_answer_is_not_the_global_one():
    """The guard on the comparisons above: the two answers must be distinguishable.

    If a global SUM happened to equal the per-partition SUMs, every equivalence
    assertion here would hold for a dropped PARTITION BY. It does not, and this
    fails loudly if the fixture ever changes so that it does.
    """
    partitioned = _rows(
        "SELECT SUM(mass) OVER (PARTITION BY gravity) AS r FROM $planets"
    )["r"]
    assert len(set(partitioned)) > 1, "$planets no longer distinguishes the two answers"
    assert 2666.6266 not in partitioned


def test_window_names_are_matched_case_insensitively():
    """As identifiers are everywhere else in the engine."""
    assert _rows(
        "SELECT id, SUM(mass) OVER W AS r FROM $planets "
        "WINDOW w AS (PARTITION BY gravity) ORDER BY id"
    ) == _rows("SELECT id, SUM(mass) OVER (PARTITION BY gravity) AS r FROM $planets ORDER BY id")


def test_a_named_window_is_scoped_to_its_query_block():
    """A subquery's own WINDOW clause resolves against its own definitions."""
    assert _rows(
        "SELECT r FROM (SELECT id, SUM(mass) OVER w AS r FROM $planets "
        "WINDOW w AS (PARTITION BY gravity)) AS s WHERE s.id = 3"
    ) == {"r": [5.97]}


def test_a_named_window_reaches_qualify():
    """QUALIFY borrows its windows into the projection; they are resolved by then."""
    assert _rows(
        "SELECT id FROM $planets WINDOW w AS (ORDER BY id) "
        "QUALIFY ROW_NUMBER() OVER w < 3"
    ) == {"id": [1, 2]}


def test_a_named_window_reaches_the_statement_order_by():
    """A window used only as a sort key is hoisted from ORDER BY and resolved there."""
    assert _rows(
        "SELECT name FROM $planets WINDOW w AS (ORDER BY id DESC) "
        "ORDER BY ROW_NUMBER() OVER w"
    ) == _rows("SELECT name FROM $planets ORDER BY id DESC")


def test_named_and_inline_spellings_of_one_spec_are_computed_once():
    """Resolving before the hoist is what lets the two spellings dedup onto one column."""
    table = execute_and_get_arrow(
        "SELECT SUM(mass) OVER w AS a, SUM(mass) OVER (PARTITION BY gravity) AS b "
        "FROM $planets WINDOW w AS (PARTITION BY gravity)"
    )
    assert table.num_rows == 9
    assert table.column("a").to_pylist() == table.column("b").to_pylist()


@pytest.mark.parametrize(
    "case, sql, fragment",
    [
        (
            "no definition anywhere",
            "SELECT SUM(mass) OVER w AS r FROM $planets",
            "is not defined",
        ),
        (
            "definition is in another query block",
            "SELECT * FROM (SELECT SUM(mass) OVER w AS r FROM $planets) AS s "
            "WINDOW w AS (PARTITION BY gravity)",
            "is not defined",
        ),
        (
            "a typo gets the suggestion",
            "SELECT SUM(mass) OVER x AS r FROM $planets WINDOW w AS (PARTITION BY gravity)",
            "Did you mean",
        ),
        (
            "defined twice",
            "SELECT SUM(mass) OVER w AS r FROM $planets "
            "WINDOW w AS (PARTITION BY gravity), w AS (ORDER BY id)",
            "defined more than once",
        ),
        (
            "a definition may not name a LATER definition",
            "SELECT SUM(mass) OVER v AS r FROM $planets "
            "WINDOW v AS (w ORDER BY id), w AS (PARTITION BY gravity)",
            "is not defined",
        ),
        (
            "a definition may not name itself",
            "SELECT SUM(mass) OVER w AS r FROM $planets WINDOW w AS (w ORDER BY id)",
            "is not defined",
        ),
        (
            "extending may not override PARTITION BY",
            "SELECT SUM(mass) OVER (w PARTITION BY id) AS r FROM $planets "
            "WINDOW w AS (PARTITION BY gravity)",
            "PARTITION BY",
        ),
        (
            "extending may not override ORDER BY",
            "SELECT SUM(mass) OVER (w ORDER BY id) AS r FROM $planets "
            "WINDOW w AS (ORDER BY name)",
            "ORDER BY",
        ),
        (
            "a framed window has nothing left to extend",
            "SELECT SUM(mass) OVER (w ORDER BY id) AS r FROM $planets WINDOW w AS "
            "(PARTITION BY gravity ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
            "has a frame",
        ),
    ],
)
def test_named_window_refusals(case, sql, fragment):
    """A clean plan-time refusal, not a quietly wrong answer."""
    with pytest.raises(SqlError) as raised:
        execute_and_get_arrow(sql)
    assert fragment in str(raised.value), case


@pytest.mark.parametrize(
    "case, sql, fragment",
    [
        (
            "ranking function needs an ORDER BY, named or not",
            "SELECT ROW_NUMBER() OVER w AS r FROM $planets WINDOW w AS (PARTITION BY gravity)",
            "ORDER BY",
        ),
        (
            "ranking function refuses a frame, named or not",
            "SELECT ROW_NUMBER() OVER w AS r FROM $planets WINDOW w AS "
            "(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
            "not supported",
        ),
        (
            "an unframed aggregate refuses a running window, named or not",
            "SELECT MEDIAN(mass) OVER w AS r FROM $planets WINDOW w AS (ORDER BY id)",
            "running/framed window",
        ),
        (
            "a window in HAVING is refused, named or not",
            "SELECT COUNT(*) AS c FROM $planets HAVING COUNT(*) OVER w > 100 "
            "WINDOW w AS (PARTITION BY gravity)",
            "HAVING",
        ),
        (
            "a window inside an aggregate is refused, named or not",
            "SELECT SUM(SUM(mass) OVER w) AS r FROM $planets WINDOW w AS (PARTITION BY gravity)",
            "cannot appear inside",
        ),
    ],
)
def test_the_inline_spec_rules_apply_to_the_named_spelling(case, sql, fragment):
    """Resolution happens before validation, so these are the SAME refusals.

    Not a second set of rules for a second spelling — the whole point of resolving on
    the AST is that there is only one set. A refusal missing here would mean the named
    spelling had found a way past a rule the inline one obeys.
    """
    with pytest.raises(UnsupportedSyntaxError) as raised:
        execute_and_get_arrow(sql)
    assert fragment in str(raised.value), case


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
