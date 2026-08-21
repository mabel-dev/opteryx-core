"""A CAST mints its own column identity; two CASTs that RENDER alike are not one column.

`inner_binder`'s CAST arm (planner/binder/binder.py) built a `FunctionColumn` -
which mints a unique `$derived_<rand>` identity in `__post_init__`, because identity
is the engine's per-column handle and handles must be unique - and then STAMPED THE
RENDERED EXPRESSION TEXT OVER IT:

    schema_column.identity = column_name.encode("utf-8")

So every CAST whose rendering matched became the SAME column. Two derived tables each
computing `CAST(n AS INTEGER)` over their own `n` both got identity `n::INTEGER`, and a
join of them emitted ONE leg's values for BOTH columns. It was the only identity
override in the binder; every other computed-column arm lets the minted id stand.

The failure is SILENT and, unlike the query that led here, DETERMINISTIC - the row
count, the column names and the types are all correct, only the values are another
column's. Which leg won varied with plan shape (the inner join below returned the LEFT
leg's values twice, the cross join the RIGHT leg's), so a wrong answer here does not
even look consistent between two spellings of the same idea.

WHY THE OVERRIDE WAS NOT NEEDED: expression reuse - the thing that makes
`SELECT UPPER(a), UPPER(a)` one computation - is matched by NAME, via the
`schema.find_column(column_name)` lookup earlier in `inner_binder`, never by identity.
`FunctionColumn.__post_init__` says as much. Two CASTs in ONE scope still collapse
through that lookup; what the override added was collapsing across scopes that cannot
see each other, which is precisely the wrong answer.

These assert VALUES. A count assertion catches none of this: every shape below
returned the right NUMBER of rows while it was wrong.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

# Two literal relations with a same-named column `n` holding DISTINGUISHABLE values.
# Same-named is the whole point: `n` renders as `n` regardless of which relation it
# came from, so `CAST(n AS INTEGER)` renders identically on both sides.
LEFT = "(VALUES (1,'10'),(2,'20')) AS v(k, n)"
RIGHT = "(VALUES (1,'77'),(2,'88')) AS v(k, n)"


def rows(sql):
    """Every row of a result, as tuples in column order."""
    session = opteryx.session()
    collected = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        table = morsel.to_arrow().to_pydict()
        collected.extend(zip(*table.values()))
    return sorted(collected)


def test_inner_join_of_two_derived_tables_casting_a_same_named_column():
    """The reported shape. Returned the LEFT leg's values in both columns."""
    answer = rows(
        f"SELECT p.a AS a, q.b AS b "
        f"FROM (SELECT k, CAST(n AS INTEGER) AS a FROM {LEFT}) p "
        f"INNER JOIN (SELECT k, CAST(n AS INTEGER) AS b FROM {RIGHT}) q ON p.k = q.k"
    )
    assert answer == [(10, 77), (20, 88)], answer


def test_cross_join_of_two_derived_tables_casting_a_same_named_column():
    """The same collision with no join key. Returned the RIGHT leg's values in both.

    Worth asserting separately from the inner join: the two spellings lost DIFFERENT
    columns, so a fix verified on only one of them proves nothing about the other.
    """
    answer = rows(
        f"SELECT p.a AS a, q.b AS b "
        f"FROM (SELECT CAST(n AS INTEGER) AS a FROM {LEFT} WHERE k = 1) p, "
        f"(SELECT CAST(n AS INTEGER) AS b FROM {RIGHT} WHERE k = 1) q"
    )
    assert answer == [(10, 77)], answer


def test_three_derived_tables_casting_a_same_named_column():
    """Three legs collapsed onto one column, not merely two onto each other."""
    answer = rows(
        f"SELECT p.a AS a, q.b AS b, r.c AS c "
        f"FROM (SELECT CAST(n AS INTEGER) AS a FROM {LEFT} WHERE k = 1) p, "
        f"(SELECT CAST(n AS INTEGER) AS b FROM {RIGHT} WHERE k = 1) q, "
        f"(SELECT CAST(n AS INTEGER) AS c FROM {RIGHT} WHERE k = 2) r"
    )
    assert answer == [(10, 77, 88)], answer


def test_the_collision_survives_differing_target_types_only_by_rendering():
    """Two CASTs of a same-named column to the SAME type over DIFFERENT relations.

    Distinct target types render differently and so never collided; this pins that the
    trigger is the rendering matching, not the CAST itself.
    """
    answer = rows(
        f"SELECT p.a AS a, q.b AS b "
        f"FROM (SELECT CAST(n AS VARCHAR) AS a FROM {LEFT} WHERE k = 2) p, "
        f"(SELECT CAST(n AS VARCHAR) AS b FROM {RIGHT} WHERE k = 2) q"
    )
    assert answer == [("20", "88")], answer


def test_union_all_legs_casting_a_same_named_column():
    """Never broken - the legs' columns are stacked, not held side by side - and asserted
    so a future identity change cannot quietly make them share one."""
    answer = rows(
        f"SELECT CAST(n AS INTEGER) AS v FROM {LEFT} "
        f"UNION ALL SELECT CAST(n AS INTEGER) AS v FROM {RIGHT}"
    )
    assert answer == [(10,), (20,), (77,), (88,)], answer


def test_qualified_casts_over_a_shared_cte_self_join():
    """The TPC-DS Q75 shape: two references to one materialize-once CTE, each CAST.

    `FROM c x, c y` forces the CASTs to be written qualified, so they render apart and
    never collided. Asserted because this is the shape the defect was reported against
    and it must stay correct on both sides of the fix.
    """
    answer = rows(
        f"WITH c AS (SELECT k, n FROM {LEFT}) "
        f"SELECT CAST(x.n AS INTEGER) AS xv, CAST(y.n AS INTEGER) AS yv "
        f"FROM c x, c y WHERE x.k = 1 AND y.k = 2"
    )
    assert answer == [(10, 20)], answer


def test_one_scope_still_collapses_two_identical_casts():
    """The guard against over-fixing.

    Minting per CAST must not stop `CAST(n AS INTEGER)` written twice in ONE scope from
    being ONE computation - that collapse comes from the by-NAME lookup in
    `inner_binder`, not from the identity, and this asserts the name path still carries
    it. Both spellings must answer, and answer the same.
    """
    answer = rows(
        f"SELECT CAST(n AS INTEGER) AS a, CAST(n AS INTEGER) AS b FROM {LEFT} WHERE k = 1"
    )
    assert answer == [(10, 10)], answer


def test_nested_cast_of_a_same_named_column_over_a_derived_tables_cast():
    """An outer CAST re-casting the column the derived table already CAST."""
    answer = rows(
        f"SELECT CAST(t.n AS INTEGER) AS outer_v, t.inner_v AS inner_v "
        f"FROM (SELECT n, CAST(n AS INTEGER) AS inner_v FROM {LEFT} WHERE k = 1) t"
    )
    assert answer == [(10, 10)], answer


@pytest.mark.parametrize(
    "target, left_value, right_value",
    [
        ("INTEGER", 10, 77),
        ("VARCHAR", "10", "77"),
        ("FLOAT64", 10.0, 77.0),
    ],
)
def test_the_collision_across_cast_target_types(target, left_value, right_value):
    """The override was on the arm shared by EVERY `CAST(... AS <type>)`, so the defect
    was never specific to one target type."""
    answer = rows(
        f"SELECT p.a AS a, q.b AS b "
        f"FROM (SELECT CAST(n AS {target}) AS a FROM {LEFT} WHERE k = 1) p, "
        f"(SELECT CAST(n AS {target}) AS b FROM {RIGHT} WHERE k = 1) q"
    )
    assert answer == [(left_value, right_value)], answer


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
