"""
SELECT-list EXISTS / IN — the existence JOIN.

A WHERE-clause `EXISTS` becomes a SEMI join: the verdict decides whether the row
survives. In the SELECT list the verdict is a VALUE — every outer row survives
carrying its own answer — which is the same probe emitting the boolean instead of
filtering on it ("left existence" / "left existence anti", native_join2.hpp's
SemiAntiProbeOperator::emit_existence).

Every case here is asserted against an equivalent JOIN rewrite computed by the
engine itself, not against a hand-written expected list: the rewrite is the
oracle, so a shared bug in both would have to be a bug in ordinary joins.

Three-valued cases (`IN` / `NOT IN` over a NULL-bearing subquery) are asserted
against literal expectations instead, because the LEFT JOIN rewrite CANNOT
express them — that difference is the whole point of the null-aware flag.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError

_SESSION = opteryx.session()


def _rows(sql):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(morsel[i])
    return out


def _same(subquery_sql, join_sql):
    """The subquery form and its join rewrite must agree, row for row."""
    got = _rows(subquery_sql)
    expected = _rows(join_sql)
    assert got == expected, f"\n  subquery: {got}\n  join    : {expected}"
    return got


def test_uncorrelated_in_matches_join_rewrite():
    got = _same(
        "SELECT p.id, p.id IN (SELECT q.id FROM $planets q WHERE q.id < 4) AS f "
        "FROM $planets p ORDER BY p.id",
        "SELECT p.id, (m.id IS NOT NULL) AS f FROM $planets p "
        "LEFT JOIN (SELECT DISTINCT q.id FROM $planets q WHERE q.id < 4) m "
        "ON m.id = p.id ORDER BY p.id",
    )
    # Pinned, so a rewrite that broke BOTH sides identically still fails here.
    assert [row[1] for row in got] == [True, True, True, False, False, False, False, False, False]


def test_uncorrelated_not_in_matches_join_rewrite():
    got = _same(
        "SELECT p.id, p.id NOT IN (SELECT q.id FROM $planets q WHERE q.id < 4) AS f "
        "FROM $planets p ORDER BY p.id",
        "SELECT p.id, (m.id IS NULL) AS f FROM $planets p "
        "LEFT JOIN (SELECT DISTINCT q.id FROM $planets q WHERE q.id < 4) m "
        "ON m.id = p.id ORDER BY p.id",
    )
    assert [row[1] for row in got] == [False, False, False, True, True, True, True, True, True]


def test_uncorrelated_exists_is_one_answer_for_every_row():
    # No key to probe on, so this is not the existence join at all: it lowers to
    # `COUNT(*) > 0` cross joined on. The count is an UNGROUPED aggregate, which
    # emits exactly one row structurally — no cardinality guard involved.
    assert [row[0] for row in _rows(
        "SELECT EXISTS (SELECT 1 FROM $planets q WHERE q.id < 4) AS f FROM $planets"
    )] == [True] * 9
    assert [row[0] for row in _rows(
        "SELECT EXISTS (SELECT 1 FROM $planets q WHERE q.id > 100) AS f FROM $planets"
    )] == [False] * 9
    assert [row[0] for row in _rows(
        "SELECT NOT EXISTS (SELECT 1 FROM $planets q WHERE q.id > 100) AS f FROM $planets"
    )] == [True] * 9


def test_correlated_exists_matches_join_rewrite():
    got = _same(
        "SELECT p.id, EXISTS (SELECT 1 FROM $planets q WHERE q.id = p.id AND q.id < 4) AS f "
        "FROM $planets p ORDER BY p.id",
        "SELECT p.id, (m.id IS NOT NULL) AS f FROM $planets p "
        "LEFT JOIN (SELECT DISTINCT q.id FROM $planets q WHERE q.id < 4) m "
        "ON m.id = p.id ORDER BY p.id",
    )
    assert [row[1] for row in got] == [True, True, True, False, False, False, False, False, False]


def test_correlated_not_exists_matches_join_rewrite():
    got = _same(
        "SELECT p.id, NOT EXISTS (SELECT 1 FROM $planets q WHERE q.id = p.id AND q.id < 4) AS f "
        "FROM $planets p ORDER BY p.id",
        "SELECT p.id, (m.id IS NULL) AS f FROM $planets p "
        "LEFT JOIN (SELECT DISTINCT q.id FROM $planets q WHERE q.id < 4) m "
        "ON m.id = p.id ORDER BY p.id",
    )
    assert [row[1] for row in got] == [False, False, False, True, True, True, True, True, True]


def test_exists_does_not_multiply_the_outer_row():
    # The reason a SELECT-list existence test cannot be an ordinary INNER or LEFT
    # join to the raw subquery: several matching inner rows would duplicate the
    # outer row. The probe collapses to a verdict, so the row count is the OUTER
    # relation's, always.
    rows = _rows(
        "SELECT p.id, EXISTS (SELECT 1 FROM $planets q WHERE q.gravity = p.gravity) AS f "
        "FROM $planets p"
    )
    assert len(rows) == 9
    # gravity 3.7 is shared by two planets — a naive join would emit both.
    assert all(row[1] for row in rows)


def test_exists_inside_case_inside_an_aggregate():
    # The motivating shape: the existence test is owned by the AGGREGATE, not the
    # Project, so the join has to be built BELOW the aggregate.
    _same(
        "SELECT SUM(CASE WHEN EXISTS "
        "(SELECT 1 FROM $planets q WHERE q.id = p.id AND q.id < 4) THEN 1 ELSE 0 END) AS c "
        "FROM $planets p",
        "SELECT SUM(CASE WHEN m.id IS NOT NULL THEN 1 ELSE 0 END) AS c FROM $planets p "
        "LEFT JOIN (SELECT DISTINCT q.id FROM $planets q WHERE q.id < 4) m ON m.id = p.id",
    )


def test_exists_inside_case_inside_a_grouped_aggregate():
    # As above, with a GROUP BY. The outer correlation key (`p.id`) is named by
    # nothing in the SELECT list or GROUP BY, so the bind-time schema narrowing
    # used to drop it and leave the join with an unresolvable probe key — see
    # BindingContext.retained_columns.
    _same(
        "SELECT p.gravity > 5 AS g, SUM(CASE WHEN EXISTS "
        "(SELECT 1 FROM $planets q WHERE q.id = p.id AND q.id < 4) THEN 1 ELSE 0 END) AS c "
        "FROM $planets p GROUP BY p.gravity > 5 ORDER BY 1",
        "SELECT p.gravity > 5 AS g, SUM(CASE WHEN m.id IS NOT NULL THEN 1 ELSE 0 END) AS c "
        "FROM $planets p LEFT JOIN (SELECT DISTINCT q.id FROM $planets q WHERE q.id < 4) m "
        "ON m.id = p.id GROUP BY p.gravity > 5 ORDER BY 1",
    )


def test_correlated_non_equality_rides_the_probe():
    # A correlated NON-equality cannot be a join key and cannot be a post-join
    # filter either — the probe has already collapsed the row to a verdict. It
    # rides on the join as `residual`, evaluated per candidate pair inside the
    # existence test, exactly as it does for a WHERE-clause EXISTS.
    #
    # Self-correlating on id and then demanding a DIFFERENT gravity: q is p, so
    # the non-equality is false for every row. Drop the equality and the answer
    # flips for the planets whose gravity is not unique.
    assert [row[1] for row in _rows(
        "SELECT p.id, EXISTS (SELECT 1 FROM $planets q "
        "WHERE q.id = p.id AND q.gravity <> p.gravity) AS f FROM $planets p ORDER BY p.id"
    )] == [False] * 9
    _same(
        "SELECT p.id, EXISTS (SELECT 1 FROM $planets q "
        "WHERE q.gravity = p.gravity AND q.id <> p.id) AS f FROM $planets p ORDER BY p.id",
        "SELECT p.id, (m.id IS NOT NULL) AS f FROM $planets p LEFT JOIN "
        "(SELECT DISTINCT a.id AS id FROM $planets a INNER JOIN $planets b "
        " ON a.gravity = b.gravity AND a.id <> b.id) m ON m.id = p.id ORDER BY p.id",
    )


def test_equality_plus_band_correlation_inside_a_grouped_aggregate():
    """
    Everything at once: an equality correlation AND a range correlation, inside a
    CASE, inside SUM, under a GROUP BY. The equality becomes the join key, the
    range rides as the residual, and the aggregate owns the whole expression so
    the join is built beneath it.
    """
    _same(
        "SELECT p.gravity, COUNT(*) AS n, SUM(CASE WHEN EXISTS ("
        "  SELECT 1 FROM $planets q WHERE q.gravity = p.gravity "
        "  AND q.id BETWEEN p.id AND p.id + 2) THEN 1 ELSE 0 END) AS c "
        "FROM $planets p GROUP BY p.gravity ORDER BY 1",
        "SELECT p.gravity, COUNT(*) AS n, SUM(CASE WHEN m.k IS NOT NULL THEN 1 ELSE 0 END) AS c "
        "FROM $planets p LEFT JOIN (SELECT DISTINCT a.id AS k FROM $planets a "
        "  INNER JOIN $planets b ON a.gravity = b.gravity AND b.id BETWEEN a.id AND a.id + 2) m "
        "ON m.k = p.id GROUP BY p.gravity ORDER BY 1",
    )


def test_in_is_three_valued_over_a_null_bearing_subquery():
    # `x IN (SELECT y)` is UNKNOWN — a NULL, not False — when x matched nothing
    # and some y was NULL. The LEFT JOIN rewrite cannot express this: it answers
    # False. This is what `existence_three_valued` carries.
    #
    # surface_pressure is NULL for planets 5..8, and 92.0 for planet 2.
    got = [row[1] for row in _rows(
        "SELECT p.id, p.surface_pressure IN "
        "(SELECT q.surface_pressure FROM $planets q WHERE q.id IN (2, 5)) AS f "
        "FROM $planets p ORDER BY p.id"
    )]
    # planet 2 matches (92.0); everything else either has no match against a
    # NULL-bearing list (UNKNOWN) or is itself NULL (UNKNOWN).
    assert got[1] is True, got
    assert all(value is None for index, value in enumerate(got) if index != 1), got


def test_not_in_is_three_valued_over_a_null_bearing_subquery():
    got = [row[1] for row in _rows(
        "SELECT p.id, p.surface_pressure NOT IN "
        "(SELECT q.surface_pressure FROM $planets q WHERE q.id IN (2, 5)) AS f "
        "FROM $planets p ORDER BY p.id"
    )]
    assert got[1] is False, got
    assert all(value is None for index, value in enumerate(got) if index != 1), got


def test_in_over_an_empty_subquery_is_false_never_unknown():
    # `x IN ()` is FALSE and `x NOT IN ()` is TRUE even for a NULL x — an empty
    # build side has nothing to be unknown about.
    assert [row[0] for row in _rows(
        "SELECT surface_pressure IN (SELECT q.surface_pressure FROM $planets q WHERE q.id > 100) "
        "FROM $planets ORDER BY id"
    )] == [False] * 9
    assert [row[0] for row in _rows(
        "SELECT surface_pressure NOT IN "
        "(SELECT q.surface_pressure FROM $planets q WHERE q.id > 100) "
        "FROM $planets ORDER BY id"
    )] == [True] * 9


def test_correlated_in_is_refused():
    # The three-valued flag's interaction with correlation keys — which are an
    # existence test, not a membership test — is not worked out, so it is
    # refused rather than guessed at.
    with pytest.raises(UnsupportedSyntaxError, match="SELECT"):
        _rows(
            "SELECT p.id, p.gravity IN "
            "(SELECT q.gravity FROM $planets q WHERE q.id = p.id) AS f FROM $planets p"
        )


def test_alias_survives_the_rewrite():
    # The flag replaces the whole SELECT-list entry, so it has to carry the entry's
    # output name with it.
    for sql in (
        "SELECT EXISTS (SELECT 1 FROM $planets q WHERE q.id = p.id) AS flagged FROM $planets p",
        "SELECT p.id IN (SELECT q.id FROM $planets q) AS flagged FROM $planets p",
    ):
        for morsel in _SESSION.execute_to_morsels(sql):
            assert b"flagged" in morsel.column_names, (sql, morsel.column_names)
            break


if __name__ == "__main__":  # pragma: no cover
    import traceback

    failures = 0
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"✅ {name}")
            except Exception:  # noqa: BLE001
                failures += 1
                print(f"❌ {name}")
                traceback.print_exc()
    print("FAILURES" if failures else "ALL PASSED")
