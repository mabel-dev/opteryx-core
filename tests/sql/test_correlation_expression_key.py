"""Value-level regression tests for a correlated EXISTS/IN whose correlation
predicate compares an EXPRESSION of the subquery's own columns to an outer
column.

    SELECT p.id FROM $planets p
    WHERE EXISTS (SELECT 1 FROM $planets q WHERE CAST(q.id AS VARCHAR) = p.name)

`extract_join_fields` attributes a join key to a leg by its relation name, and
an expression names none — so every join decorrelation built for this shape came
out with key lists shorter than its pair list and tripped the internal-state
guard ("decorrelation built a join key naming a relation that is on neither
leg"). It fired on all four decorrelating lowerings: the SEMI/ANTI filter join,
the SELECT-list existence join, the COUNT-boolean materialization used under
OR/NOT, and the scalar-subquery join.

The fix projects the expression as a real column on the leg that computes it —
the same rewrite `JoinKeyMaterializationStrategy` performs for an ON-clause
operand, using that module's own primitive — in `_lift_correlations`, the ONE
producer of key pairs, so every consumer sees an ordinary column key.

Only the INNER side is materialised. An expression on the OUTER side
(`q.id = p.id + 1`) is not recognised as a correlation by `_split_correlations`
at all and keeps its existing clean refusal — out of scope by design.

Each supported case is asserted against BOTH a hand-derived row set and the
derived-table rewrite a query author would otherwise have to write by hand
(`FROM (SELECT TRIM(name) AS k ...)`), which is the same question asked without
the expression key.

$planets, by id: 1 Mercury, 2 Venus, 3 Earth, 4 Mars, 5 Jupiter, 6 Saturn,
7 Uranus, 8 Neptune, 9 Pluto; gravity 3.7, 8.9, 9.8, 3.7, 23.1, 9.0, 8.7, 11.0,
0.7 — verified directly with `SELECT id, name, gravity FROM $planets ORDER BY id`.
`TRIM(name)` is the identity on every one of them, so a correlation on
`TRIM(q.name) = p.name` matches exactly the row it came from; no planet name is
a numeral, so `CAST(q.id AS VARCHAR) = p.name` matches nothing.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError


def _first_col(sql):
    """Every row's first column, sorted — identifies which planets survived."""
    session = opteryx.session()
    values = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None or morsel.num_rows == 0:
            continue
        values.extend(list(morsel.to_arrow().to_pydict().values())[0])
    return sorted(values)


def _columns(sql):
    """Every row as a tuple of its columns, sorted — for the flag-valued cases."""
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None or morsel.num_rows == 0:
            continue
        table = morsel.to_arrow().to_pydict()
        rows.extend(zip(*table.values()))
    return sorted(rows)


# (label, expression-key query, hand-written derived-table equivalent, expected ids)
SUPPORTED = [
    (
        "exists",
        "SELECT p.id FROM $planets p WHERE EXISTS "
        "(SELECT 1 FROM $planets q WHERE TRIM(q.name) = p.name)",
        "SELECT p.id FROM $planets p WHERE EXISTS "
        "(SELECT 1 FROM (SELECT TRIM(name) AS k FROM $planets) q WHERE q.k = p.name)",
        [1, 2, 3, 4, 5, 6, 7, 8, 9],
    ),
    (
        "not exists",
        "SELECT p.id FROM $planets p WHERE NOT EXISTS "
        "(SELECT 1 FROM $planets q WHERE TRIM(q.name) = p.name)",
        "SELECT p.id FROM $planets p WHERE NOT EXISTS "
        "(SELECT 1 FROM (SELECT TRIM(name) AS k FROM $planets) q WHERE q.k = p.name)",
        [],
    ),
    (
        # No planet is named for a number, so the key matches nothing — the
        # motivating CAST shape, which is also the one that has to survive a
        # type difference between the two sides of the correlation.
        "exists over a cast key",
        "SELECT p.id FROM $planets p WHERE EXISTS "
        "(SELECT 1 FROM $planets q WHERE CAST(q.id AS VARCHAR) = p.name)",
        "SELECT p.id FROM $planets p WHERE EXISTS "
        "(SELECT 1 FROM (SELECT CAST(id AS VARCHAR) AS k FROM $planets) q WHERE q.k = p.name)",
        [],
    ),
    (
        # The expression key rides alongside a purely local filter, which
        # predicate pushdown must still be able to reach past the projected
        # column and push onto the scan. gravity > 9 is {3, 5, 8}.
        "exists with a local filter",
        "SELECT p.id FROM $planets p WHERE EXISTS "
        "(SELECT 1 FROM $planets q WHERE TRIM(q.name) = p.name AND q.gravity > 9)",
        "SELECT p.id FROM $planets p WHERE EXISTS "
        "(SELECT 1 FROM (SELECT TRIM(name) AS k, gravity FROM $planets) q "
        "WHERE q.k = p.name AND q.gravity > 9)",
        [3, 5, 8],
    ),
    (
        # The correlated non-equality becomes the join's residual, evaluated per
        # candidate pair inside the existence probe — the half of the motivating
        # netflow query that already worked, now beside an expression key.
        "exists with a correlated non-equality residual",
        "SELECT p.id FROM $planets p WHERE EXISTS "
        "(SELECT 1 FROM $planets q WHERE TRIM(q.name) = p.name AND q.id BETWEEN 3 AND 6)",
        "SELECT p.id FROM $planets p WHERE EXISTS "
        "(SELECT 1 FROM (SELECT TRIM(name) AS k, id FROM $planets) q "
        "WHERE q.k = p.name AND q.id BETWEEN 3 AND 6)",
        [3, 4, 5, 6],
    ),
    (
        "exists with a second, ordinary column key",
        "SELECT p.id FROM $planets p WHERE EXISTS "
        "(SELECT 1 FROM $planets q WHERE TRIM(q.name) = p.name AND q.id = p.id)",
        "SELECT p.id FROM $planets p WHERE EXISTS "
        "(SELECT 1 FROM (SELECT TRIM(name) AS k, id FROM $planets) q "
        "WHERE q.k = p.name AND q.id = p.id)",
        [1, 2, 3, 4, 5, 6, 7, 8, 9],
    ),
    (
        # Under OR the existence test cannot BE the join; it lowers to the
        # COUNT-boolean instead, which keys on the same pairs. id > 6 is
        # {7, 8, 9}; the EXISTS arm adds {1, 2}.
        "exists under OR takes the boolean-value lowering",
        "SELECT p.id FROM $planets p WHERE p.id > 6 OR EXISTS "
        "(SELECT 1 FROM $planets q WHERE TRIM(q.name) = p.name AND q.id < 3)",
        "SELECT p.id FROM $planets p WHERE p.id > 6 OR EXISTS "
        "(SELECT 1 FROM (SELECT TRIM(name) AS k, id FROM $planets) q "
        "WHERE q.k = p.name AND q.id < 3)",
        [1, 2, 7, 8, 9],
    ),
    (
        # One column read TWICE by the same expression: the supplying-node search
        # must not mistake the repeat for an unbound identifier. Splitting a name
        # and re-joining it reconstructs it, so every planet matches its own row.
        "expression key reading one column twice",
        "SELECT p.id FROM $planets p WHERE EXISTS "
        "(SELECT 1 FROM $planets q "
        "WHERE SUBSTRING(q.name, 1, 1) || SUBSTRING(q.name, 2, 20) = p.name)",
        "SELECT p.id FROM $planets p WHERE EXISTS "
        "(SELECT 1 FROM (SELECT SUBSTRING(name, 1, 1) || SUBSTRING(name, 2, 20) AS k "
        "FROM $planets) q WHERE q.k = p.name)",
        [1, 2, 3, 4, 5, 6, 7, 8, 9],
    ),
    (
        "correlated IN",
        "SELECT p.id FROM $planets p WHERE p.id IN "
        "(SELECT q.id FROM $planets q WHERE TRIM(q.name) = p.name)",
        "SELECT p.id FROM $planets p WHERE p.id IN "
        "(SELECT q.id FROM (SELECT TRIM(name) AS k, id FROM $planets) q WHERE q.k = p.name)",
        [1, 2, 3, 4, 5, 6, 7, 8, 9],
    ),
    (
        "correlated NOT IN",
        "SELECT p.id FROM $planets p WHERE p.id NOT IN "
        "(SELECT q.id FROM $planets q WHERE TRIM(q.name) = p.name)",
        "SELECT p.id FROM $planets p WHERE p.id NOT IN "
        "(SELECT q.id FROM (SELECT TRIM(name) AS k, id FROM $planets) q WHERE q.k = p.name)",
        [],
    ),
    (
        "correlated scalar subquery in WHERE",
        "SELECT p.id FROM $planets p WHERE p.id = "
        "(SELECT MAX(q.id) FROM $planets q WHERE TRIM(q.name) = p.name)",
        "SELECT p.id FROM $planets p WHERE p.id = "
        "(SELECT MAX(q.id) FROM (SELECT TRIM(name) AS k, id FROM $planets) q WHERE q.k = p.name)",
        [1, 2, 3, 4, 5, 6, 7, 8, 9],
    ),
    (
        # A Subquery is a NAMING boundary: the materialising Project has to go
        # ABOVE it, not inside it, or the key it emits is not one of the columns
        # the subquery exports ("projecting a column the engine could not
        # resolve here"). id > 5 is {6, 7, 8, 9}.
        "expression key over a derived-table relation",
        "SELECT p.id FROM $planets p WHERE EXISTS "
        "(SELECT 1 FROM (SELECT id, name FROM $planets) q "
        "WHERE TRIM(q.name) = p.name AND q.id > 5)",
        "SELECT p.id FROM $planets p WHERE EXISTS "
        "(SELECT 1 FROM (SELECT id, TRIM(name) AS k FROM $planets) q "
        "WHERE q.k = p.name AND q.id > 5)",
        [6, 7, 8, 9],
    ),
    (
        "expression key over a CTE relation",
        "WITH c AS (SELECT id, name FROM $planets) SELECT p.id FROM $planets p "
        "WHERE EXISTS (SELECT 1 FROM c q WHERE TRIM(q.name) = p.name AND q.id > 5)",
        "WITH c AS (SELECT id, TRIM(name) AS k FROM $planets) SELECT p.id FROM $planets p "
        "WHERE EXISTS (SELECT 1 FROM c q WHERE q.k = p.name AND q.id > 5)",
        [6, 7, 8, 9],
    ),
]


@pytest.mark.parametrize(
    "label,expression_key_sql,derived_table_sql,expected", SUPPORTED
)
def test_expression_correlation_key(label, expression_key_sql, derived_table_sql, expected):
    assert _first_col(expression_key_sql) == expected, label
    assert _first_col(derived_table_sql) == expected, f"{label} (derived-table equivalent)"


def test_select_list_existence_flag_values():
    """The SELECT-list form keeps every outer row and carries its own verdict, so
    the row COUNT proves nothing — the flag values are what has to match.
    `q.id < 4` restricts the match to Mercury/Venus/Earth."""
    expression_key = (
        "SELECT p.id, EXISTS (SELECT 1 FROM $planets q "
        "WHERE TRIM(q.name) = p.name AND q.id < 4) AS f FROM $planets p"
    )
    derived_table = (
        "SELECT p.id, EXISTS (SELECT 1 FROM (SELECT TRIM(name) AS k, id FROM $planets) q "
        "WHERE q.k = p.name AND q.id < 4) AS f FROM $planets p"
    )
    expected = [(i, i < 4) for i in range(1, 10)]
    assert _columns(expression_key) == sorted(expected)
    assert _columns(derived_table) == sorted(expected)


def test_outer_side_expression_is_still_refused():
    """An expression on the OUTER side is not a correlation `_split_correlations`
    recognises, so it is refused as an uncorrelated EXISTS. Deliberately out of
    scope: only the inner side is materialised."""
    with pytest.raises(UnsupportedSyntaxError) as raised:
        _first_col(
            "SELECT p.id FROM $planets p WHERE EXISTS "
            "(SELECT 1 FROM $planets q WHERE q.id = p.id + 1)"
        )
    assert "correlated equality predicate" in str(raised.value)


def test_expression_reading_no_subquery_column_is_refused():
    """A correlation whose inner side reads nothing from the subquery keys
    nothing, whatever we project — refused by name, not by internal-state guard."""
    with pytest.raises(UnsupportedSyntaxError) as raised:
        _first_col(
            "SELECT p.id FROM $planets p WHERE EXISTS "
            "(SELECT 1 FROM $planets q WHERE CAST(1 AS VARCHAR) = p.name)"
        )
    assert "reads no column of the subquery" in str(raised.value)


def test_expression_straddling_both_scopes_is_refused():
    """An expression over columns of BOTH scopes is a theta condition, not a key
    — projecting it on either leg is impossible, and the message says which half
    to move."""
    with pytest.raises(UnsupportedSyntaxError) as raised:
        _first_col(
            "SELECT p.id FROM $planets p WHERE EXISTS "
            "(SELECT 1 FROM $planets q WHERE CAST(q.id + p.id AS VARCHAR) = p.name)"
        )
    assert "BOTH the subquery and the outer query" in str(raised.value)


def test_volatile_expression_is_refused():
    """A volatile expression must never be relocated into a projection: that
    changes it from once-per-pair to once-per-row. `hoistable_operand_leg` is the
    shared decision that says no."""
    with pytest.raises(UnsupportedSyntaxError):
        _first_col(
            "SELECT p.id FROM $planets p WHERE EXISTS "
            "(SELECT 1 FROM $planets q WHERE CAST(RANDOM() AS VARCHAR) = p.name)"
        )


if __name__ == "__main__":  # pragma: no cover
    for case in SUPPORTED:
        test_expression_correlation_key(*case)
        print(f"✅ {case[0]}")
    for check in (
        test_select_list_existence_flag_values,
        test_outer_side_expression_is_still_refused,
        test_expression_reading_no_subquery_column_is_refused,
        test_expression_straddling_both_scopes_is_refused,
        test_volatile_expression_is_refused,
    ):
        check()
        print(f"✅ {check.__name__}")
    print("✅ okay")
