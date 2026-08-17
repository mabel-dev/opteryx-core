"""A derived table's relation aliases are private to it.

    SELECT y.a, x.a
    FROM (SELECT p.id AS a FROM t p, t d WHERE d.id = p.id) y,
         (SELECT p.id AS a FROM t p, t d WHERE d.id = p.id) x

raised `AmbiguousDatasetError: Dataset 'd' is referenced more than once`. Nothing
is ambiguous: each `d` is declared inside a different subquery and neither is
addressable from anywhere the other can be seen. Renaming one to `e` ran and
answered correctly, which is what identified alias reuse across SIBLING derived
tables — not a real collision — as the trigger. Seven TPC-DS queries (Q02, Q28,
Q59, Q61, Q65, Q88, Q90) were failing on it.

TWO FAULTS, ONE SHAPE:

  * `visit_subquery` (planner/binder/subquery.py) sacked the relations it knew by
    walking `context.schemas` and popping the matching names. That is not the same
    set: the Project below the boundary narrows schemas to the columns it emits, so
    a relation contributing NO projected column - the `d` above, which only appears
    in the WHERE - had already lost its schema and survived the pop.
  * `traverse` (planner/binder/traversal.py) merges each child's relations into the
    shared context INSIDE the loop over children, so whatever the first sibling
    leaked was in the second sibling's starting scope. That merge is not a bug: it
    is the only thing that makes `FROM t, t` ambiguous, since peers are otherwise
    bound against independent copies.

The fix names the scope instead of patching the leak: a Subquery node is a naming
boundary, so `traverse` empties `relations` on the way in and restores the
enclosing names on the way out, and the boundary exports exactly one name - its
alias. Emptying on the way IN is what makes an enclosing alias invisible to the
subquery, so an inner alias shadows an outer one rather than colliding with it.

WHAT MUST STILL BE REFUSED is half of this file. A fix that stopped raising is
trivially available and wrong: one FROM scope naming one relation twice is
genuinely ambiguous and every spelling of that is asserted below.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.exceptions import AmbiguousDatasetError

# The nine planet ids. Each derived table below self-joins `$planets` on `id`, so it
# holds exactly these - a fixture that is worth stating because a boundary that lost
# rows and a boundary that lost columns both show up as a changed multiset here.
IDS = list(range(1, 10))


def rows(sql):
    """Every row of a result, as tuples in column order."""
    session = opteryx.session()
    collected = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        table = morsel.to_arrow().to_pydict()
        collected.extend(zip(*table.values()))
    return collected


def test_sibling_derived_tables_may_reuse_one_internal_alias():
    """The reported repro, asserted on VALUES.

    Both derived tables hold the nine ids, so the cross join is every ordered pair
    of them. A test that only asserted "it no longer raises" would pass on a
    boundary that dropped rows or bound `x.a` to `y`'s column.
    """
    answer = rows(
        "SELECT y.a AS ya, x.a AS xa FROM "
        "(SELECT p.id AS a FROM $planets p, $planets d WHERE d.id = p.id) y, "
        "(SELECT p.id AS a FROM $planets p, $planets d WHERE d.id = p.id) x"
    )
    assert sorted(answer) == sorted((y, x) for y in IDS for x in IDS), answer


def test_the_reused_alias_may_be_the_one_that_is_projected():
    """`d` carrying the projected column, rather than only the WHERE, is the same case."""
    answer = rows(
        "SELECT y.a AS ya, x.a AS xa FROM "
        "(SELECT d.id AS a FROM $planets d) y, (SELECT d.id AS a FROM $planets d) x"
    )
    assert sorted(answer) == sorted((y, x) for y in IDS for x in IDS), answer


def test_three_siblings_reuse_one_alias():
    """Two siblings only prove the first leak; the merge is cumulative across all of them."""
    answer = rows(
        "SELECT a1.a AS c1, a2.a AS c2, a3.a AS c3 FROM "
        "(SELECT d.id AS a FROM $planets d) a1, "
        "(SELECT d.id AS a FROM $planets d) a2, "
        "(SELECT d.id AS a FROM $planets d) a3"
    )
    assert len(answer) == len(IDS) ** 3, len(answer)
    assert sorted(answer) == sorted((i, j, k) for i in IDS for j in IDS for k in IDS)


def test_siblings_joined_on_a_predicate_that_reads_both():
    """An explicit join over the two derived tables, so the ON binds across them."""
    answer = rows(
        "SELECT y.a AS ya, x.a AS xa FROM (SELECT d.id AS a FROM $planets d) y "
        "INNER JOIN (SELECT d.id AS a FROM $planets d) x ON y.a = x.a"
    )
    assert sorted(answer) == [(i, i) for i in IDS], answer


def test_nested_siblings_reuse_one_alias():
    """The siblings are inside a third derived table - scopes nest."""
    answer = rows(
        "SELECT o.a AS a FROM (SELECT i.a FROM "
        "(SELECT d.id AS a FROM $planets d) i, (SELECT d.id AS a FROM $planets d) j "
        "WHERE i.a = j.a) o"
    )
    assert sorted(v for (v,) in answer) == IDS, answer


def test_a_derived_table_alias_may_shadow_an_enclosing_relation():
    """An enclosing alias is not visible inside a derived table, so `p` shadows `p`.

    The values say WHICH `p` the inner projection read: the inner one, over the
    whole relation, joined back to the outer `p` on equal ids.
    """
    answer = rows(
        "SELECT p.name AS name, q.i AS i FROM $planets p, "
        "(SELECT p.id AS i FROM $planets p) q WHERE p.id = q.i"
    )
    assert len(answer) == 9, answer
    assert sorted(i for _, i in answer) == IDS, answer


def test_a_derived_table_may_carry_its_own_alias_inside_itself():
    answer = rows("SELECT y.id AS id FROM (SELECT y.id FROM $planets y) y")
    assert sorted(v for (v,) in answer) == IDS, answer


def test_union_legs_may_reuse_aliases():
    """Each leg is its own scope; the planner gives each leg's scans a `$union-` alias."""
    plain = rows(
        "SELECT a.id AS id FROM $planets a INNER JOIN $planets b ON a.id = b.id "
        "UNION ALL "
        "SELECT a.id AS id FROM $planets a INNER JOIN $planets b ON a.id = b.id"
    )
    assert sorted(v for (v,) in plain) == sorted(IDS + IDS), plain

    derived = rows(
        "SELECT y.a AS a FROM (SELECT p.id AS a FROM $planets p, $planets d WHERE d.id = p.id) y "
        "UNION ALL "
        "SELECT y.a AS a FROM (SELECT p.id AS a FROM $planets p, $planets d WHERE d.id = p.id) y"
    )
    assert sorted(v for (v,) in derived) == sorted(IDS + IDS), derived


def test_a_subquery_in_where_may_reuse_the_outer_alias():
    """The IN-subquery's `p` is its own; the outer `p` is still the one projected."""
    answer = rows(
        "SELECT p.name AS name FROM $planets p "
        "WHERE p.id IN (SELECT p.id FROM $planets p WHERE p.id < 4)"
    )
    assert sorted(v for (v,) in answer) == ["Earth", "Mercury", "Venus"], answer


def test_a_correlated_subquery_may_reuse_an_alias_from_the_enclosing_scope():
    """`d` is an outer relation AND the EXISTS body's alias; the correlation still binds.

    The correlated `p.id` resolves outwards to the enclosing scope while `d.id`
    resolves to the body's own `d` - if the scope boundary had also hidden the outer
    SCHEMAS, this would fail to bind rather than answer.
    """
    answer = rows(
        "SELECT p.name AS name FROM $planets p, $planets d "
        "WHERE d.id = p.id AND EXISTS (SELECT d.id FROM $planets d WHERE d.id = p.id)"
    )
    assert len(answer) == 9, answer

    narrowed = rows(
        "SELECT p.name AS name FROM $planets p "
        "WHERE EXISTS (SELECT d.id FROM $planets d WHERE d.id = p.id AND d.id < 4)"
    )
    assert sorted(v for (v,) in narrowed) == ["Earth", "Mercury", "Venus"], narrowed


def test_sibling_relations_in_one_scope_still_see_each_other():
    """The guard against over-isolating: a predicate reading both legs must bind.

    Isolating peers from each other completely would leave this unbindable, which is
    the failure mode a fix that simply deleted the relations merge would produce.
    """
    on_clause = rows(
        "SELECT p.name AS pname, d.name AS dname FROM $planets p "
        "INNER JOIN $planets d ON d.id = p.id"
    )
    assert len(on_clause) == 9, on_clause
    assert all(pname == dname for pname, dname in on_clause), on_clause

    where_clause = rows(
        "SELECT p.name AS pname, d.name AS dname FROM $planets p, $planets d "
        "WHERE d.id = p.id"
    )
    assert sorted(where_clause) == sorted(on_clause), where_clause

    using_clause = rows("SELECT a.name AS name FROM $planets a INNER JOIN $planets b USING (id)")
    assert len(using_clause) == 9, using_clause


@pytest.mark.parametrize(
    "statement",
    [
        # One scope, one name, two relations - with and without an alias.
        "SELECT * FROM $planets, $planets",
        "SELECT * FROM $planets AS a, $planets AS a",
        "SELECT * FROM $planets JOIN $planets ON id = 1",
        # Two derived tables exporting the same name into one scope. This is the case
        # the leak used to catch by accident (it raised on the leaked INNER alias
        # instead, so it named the wrong relation), and it is caught at the boundary
        # now: nothing in the outer scope can say which `y` it means.
        "SELECT y.a FROM (SELECT id AS a FROM $planets) y, (SELECT id AS a FROM $planets) y",
        # A derived table whose alias collides with a sibling SCAN's alias. This one
        # was previously ACCEPTED - `p` addressed the scan and the derived relation was
        # unreachable - and is refused now for the same reason as the line above.
        "SELECT p.name FROM $planets p, (SELECT id AS a FROM $planets) p",
        # Two references to one CTE, neither aliased. Both splice in as a Subquery
        # named `c`, which is the same collision.
        "WITH c AS (SELECT id FROM $planets) SELECT * FROM c, c",
    ],
)
def test_one_scope_naming_one_relation_twice_is_still_ambiguous(statement):
    with pytest.raises(AmbiguousDatasetError):
        rows(statement)


def test_the_ambiguity_names_the_relation_the_reader_wrote():
    """The message must name the duplicated name, not something internal to a subquery.

    Before the fix this class of error reported the leaked private alias, so the
    reader was pointed at a relation that was not the problem.
    """
    with pytest.raises(AmbiguousDatasetError) as caught:
        rows("SELECT y.a FROM (SELECT id AS a FROM $planets) y, (SELECT id AS a FROM $planets) y")
    assert "`y`" in str(caught.value), str(caught.value)


def test_distinct_aliases_over_one_cte_are_not_ambiguous():
    """The counterpart: the same CTE twice, each reference named, must bind."""
    answer = rows(
        "WITH c AS (SELECT p.id AS i FROM $planets p, $planets d WHERE d.id = p.id) "
        "SELECT x.i AS xi, y.i AS yi FROM c x, c y WHERE x.i = y.i"
    )
    assert sorted(answer) == [(i, i) for i in IDS], answer


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
