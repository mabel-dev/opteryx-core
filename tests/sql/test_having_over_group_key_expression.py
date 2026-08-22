"""`GROUP BY <expr> HAVING <the same expr>` is standard SQL, and must run.

HAVING may name a grouping EXPRESSION, not only an alias for one — DuckDB, Postgres
and the standard all agree. Opteryx refused it, for every computed key:

    SELECT UPPER(name) u, COUNT(*) n FROM t GROUP BY UPPER(name) HAVING UPPER(name) > 'A'
    SELECT id+gravity g, COUNT(*) n FROM t GROUP BY id+gravity  HAVING id+gravity > 1
      -> NotSupportedError: projecting a column the engine could not resolve here

The message named nothing and offered nothing, and — the reason this was worth fixing
rather than re-wording — the ILLEGAL neighbour got the SAME message:

    ... GROUP BY UPPER(name) HAVING name > 'A'        -- `name` survives neither
                                                     -- grouping nor aggregation

so a legal query and an invalid one were indistinguishable. Two engine behaviours also
pointed the same way: `ORDER BY UPPER(name)` over the same GROUP BY already resolved
against the computed key, and `HAVING u > 'A'` (the alias) worked — and the alias form
is the LESS standard spelling, since standard HAVING does not see SELECT aliases at all.
We accepted the extension and refused the standard.

The cause was the HAVING pass-through walk. HAVING expressions absent from the SELECT
list ride through the Project so the Filter above it can read them, and that walk
collected the bare identifiers it found. Under a computed key it found the LEAF — `name`
— and asked the Project ABOVE the aggregate to carry a column the aggregate does not
emit. An identifier inside a grouping expression is consumed by the GROUPING, exactly as
one inside an aggregate is consumed by the AGGREGATE; the existing `_aggregate_operands`
skip was the precedent.

Three things changed, and each is load-bearing:

  1. The HAVING collection moved BELOW the GROUP BY resolution, so it matches against
     the final key list (positions and aliases resolved), and a matched sub-expression
     is SUBSTITUTED by the key's own node rather than left as a private copy nothing
     binds. Where the SELECT list carries the same expression the projection's node
     wins — which of the two the stream carries depends on whether the Filter is pushed
     below the aggregate or left above the Project.
  2. The compiler loads a sub-expression the stream ALREADY CARRIES instead of
     recomputing it (`_bind_precomputed_subexpressions`). Without this, a predicate left
     above the Project — a CASE, which pushdown does not move — tried to rebuild
     `UPPER(name)` from a `name` the aggregate had dropped.
  3. Predicate pushdown's HAVING-fold gate stopped reporting a group key by its LEAF.
     `HAVING UPPER(name) > 'B' OR COUNT(*) > 1` reads a key the aggregate emits, but the
     gate saw `name`, declined the fold, and let the predicate — aggregator and all —
     flow BELOW the aggregate, referencing a COUNT that had not been computed yet. A
     plain-column key hid this completely: there the key identity and the leaf identity
     are the same thing.

Every expectation below is DuckDB's answer for the same data, checked when these tests
were written. The illegal form now raises, naming the column.

NOT COVERED, and deliberately: `GROUP BY ROLLUP(...)` with a filter on a group key.
That returns the rollup SUBTOTAL rows the filter should have removed — a silent wrong
answer that predates this work, reproduces on plain columns, and reproduces on the
subquery-wrapper spelling too. See the session notes.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError

# Two dotted names sharing a first segment, one with no delimiter, one NULL.
SRC = """(SELECT * FROM (VALUES
    ('alpha.beta.gamma'),
    ('alpha.two.x'),
    ('beta.one.y'),
    ('solo'),
    (NULL)
) AS v(name))"""


def rows(sql):
    """Every output row as a tuple, sorted — no spelling here promises an order."""
    session = opteryx.session()
    out: dict = {}
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        for key, values in morsel.to_arrow().to_pydict().items():
            out.setdefault(key, []).extend(values)
    return sorted(zip(*out.values()), key=repr) if out else []


# ------------------------------------------------ the legal form, against DuckDB

@pytest.mark.parametrize(
    "sql,expected",
    [
        # The key repeated, and selected.
        (f"SELECT UPPER(name) u, COUNT(*) n FROM {SRC} GROUP BY UPPER(name) "
         "HAVING UPPER(name) > 'B'",
         [("BETA.ONE.Y", 1), ("SOLO", 1)]),
        # The key repeated, and NOT selected — the case the pass-through exists for.
        (f"SELECT COUNT(*) n FROM {SRC} GROUP BY UPPER(name) HAVING UPPER(name) > 'B'",
         [(1,), (1,)]),
        # An arithmetic key, and a concatenation key: this was never about UPPER.
        (f"SELECT name || '!' k, COUNT(*) n FROM {SRC} GROUP BY name || '!' "
         "HAVING name || '!' > 'b'",
         [("beta.one.y!", 1), ("solo!", 1)]),
        # Two keys, filtering on the second.
        (f"SELECT UPPER(name) a, LENGTH(name) b, COUNT(*) n FROM {SRC} "
         "GROUP BY UPPER(name), LENGTH(name) HAVING LENGTH(name) > 4",
         [("ALPHA.BETA.GAMMA", 16, 1), ("ALPHA.TWO.X", 11, 1), ("BETA.ONE.Y", 10, 1)]),
        # The key under a unary operator, and negated.
        (f"SELECT UPPER(name) u, COUNT(*) n FROM {SRC} GROUP BY UPPER(name) "
         "HAVING UPPER(name) IS NOT NULL",
         [("ALPHA.BETA.GAMMA", 1), ("ALPHA.TWO.X", 1), ("BETA.ONE.Y", 1), ("SOLO", 1)]),
        (f"SELECT UPPER(name) u, COUNT(*) n FROM {SRC} GROUP BY UPPER(name) "
         "HAVING NOT (UPPER(name) > 'B')",
         [("ALPHA.BETA.GAMMA", 1), ("ALPHA.TWO.X", 1)]),
        # GROUP BY ALL stands for the projection's non-aggregate expressions, and
        # HAVING may name one of those the same way.
        (f"SELECT UPPER(name) u, COUNT(*) n FROM {SRC} GROUP BY ALL "
         "HAVING UPPER(name) > 'B'",
         [("BETA.ONE.Y", 1), ("SOLO", 1)]),
        # A subscript key — the expression this whole investigation started from.
        (f"SELECT SPLIT(name,'.')[1] o, COUNT(*) n FROM {SRC} "
         "GROUP BY SPLIT(name,'.')[1] HAVING SPLIT(name,'.')[1] IS NOT NULL",
         [("beta", 1), ("one", 1), ("two", 1)]),
    ],
)
def test_having_may_repeat_the_group_key_expression(sql, expected):
    assert rows(sql) == expected


def test_key_combined_with_an_aggregate_under_and_and_under_or():
    """AND and OR are different plans, not two spellings of one. AND lets each conjunct
    be considered for the fold separately; OR presents ONE predicate carrying both a key
    and an aggregate, which is the shape that exposed the pushdown gate reading the
    key's leaf."""
    assert rows(
        f"SELECT UPPER(name) u, COUNT(*) n FROM {SRC} GROUP BY UPPER(name) "
        "HAVING COUNT(*) > 0 AND UPPER(name) > 'B'"
    ) == [("BETA.ONE.Y", 1), ("SOLO", 1)]
    assert rows(
        f"SELECT UPPER(name) u, COUNT(*) n FROM {SRC} GROUP BY UPPER(name) "
        "HAVING UPPER(name) > 'B' OR COUNT(*) > 1"
    ) == [("BETA.ONE.Y", 1), ("SOLO", 1)]


def test_key_inside_a_case_is_left_above_the_project_and_still_resolves():
    """Pushdown moves a plain compare below the aggregate, where the key's operands are
    still available. It does NOT move a CASE — so this predicate stays above the Project
    and must LOAD the key column rather than rebuild it from a dropped `name`."""
    assert rows(
        f"SELECT UPPER(name) u, COUNT(*) n FROM {SRC} GROUP BY UPPER(name) "
        "HAVING CASE WHEN UPPER(name) > 'B' THEN TRUE ELSE FALSE END"
    ) == [("BETA.ONE.Y", 1), ("SOLO", 1)]


# --------------------------------------------------- the illegal neighbour, named

@pytest.mark.parametrize(
    "predicate",
    [
        "name > 'B'",                                    # the key's own leaf
        "CASE WHEN name > 'B' THEN TRUE ELSE FALSE END",  # the same, inside a CASE
    ],
)
def test_an_ungrouped_column_in_having_is_refused_by_name(predicate):
    """`name` survives neither the grouping nor an aggregate, so a grouped row has no
    single value for it — DuckDB raises a BinderException here. It used to get the same
    unresolvable-column message the LEGAL form got; now it names the column and says
    what to do."""
    with pytest.raises(UnsupportedSyntaxError) as exc:
        rows(
            f"SELECT UPPER(name) u, COUNT(*) n FROM {SRC} GROUP BY UPPER(name) "
            f"HAVING {predicate}"
        )
    message = str(exc.value)
    assert "name" in message, message
    assert "not grouped" in message, message


# ------------------------------------------------------------ nothing else moved

@pytest.mark.parametrize(
    "sql,expected",
    [
        # The alias spelling — the extension that worked all along.
        (f"SELECT UPPER(name) u, COUNT(*) n FROM {SRC} GROUP BY UPPER(name) HAVING u > 'B'",
         [("BETA.ONE.Y", 1), ("SOLO", 1)]),
        # A plain-column key, selected and unselected. Here the key identity and the
        # leaf identity are the same thing, which is exactly why this path always
        # worked and hid the defect.
        (f"SELECT name, COUNT(*) n FROM {SRC} GROUP BY name HAVING name > 'b'",
         [("beta.one.y", 1), ("solo", 1)]),
        (f"SELECT COUNT(*) n FROM {SRC} GROUP BY name HAVING name > 'b'",
         [(1,), (1,)]),
        # Aggregate-only HAVING, including one the SELECT list does not carry.
        # The NULL group is kept: COUNT(*) counts ROWS, and the NULL-key group has one.
        (f"SELECT UPPER(name) u, COUNT(*) n FROM {SRC} GROUP BY UPPER(name) HAVING COUNT(*) > 0",
         [("ALPHA.BETA.GAMMA", 1), ("ALPHA.TWO.X", 1), ("BETA.ONE.Y", 1), ("SOLO", 1), (None, 1)]),
        (f"SELECT name FROM {SRC} GROUP BY name HAVING COUNT(*) > 0",
         [("alpha.beta.gamma",), ("alpha.two.x",), ("beta.one.y",), ("solo",), (None,)]),
        # HAVING with no GROUP BY at all — one group, and no key list to match against.
        (f"SELECT COUNT(*) n FROM {SRC} HAVING COUNT(*) > 1", [(5,)]),
    ],
)
def test_the_spellings_that_already_worked_still_do(sql, expected):
    assert rows(sql) == expected


def test_a_window_over_the_grouped_result_still_sees_the_surviving_groups():
    """HAVING is planned INSIDE the window boundary, so the window runs over the groups
    that SURVIVED it. The key-resolution rewrite must not disturb that ordering."""
    assert rows(
        f"SELECT UPPER(name) u, COUNT(*) n, SUM(COUNT(*)) OVER () t FROM {SRC} "
        "GROUP BY UPPER(name) HAVING UPPER(name) > 'B'"
    ) == [("BETA.ONE.Y", 1, 2), ("SOLO", 1, 2)]


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
