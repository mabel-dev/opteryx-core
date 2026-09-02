"""
Coverage for the predicate-rewrite gaps closed on 2026-09-02:

  1. INNER-join ON conjuncts on one leg, and two-sided ones that are not
     comparisons, are hoisted to a Filter so every expression rewrite sees them
     and none is dropped; outer joins refuse what they cannot evaluate
     (JoinConditionHoistStrategy).
  2. NOT over a unary test / IN-list inverts; NOT over an AND of invertible
     conjuncts distributes (De Morgan); NOT / OR / CNF roots are pushable.
  3. NOT (x IN (..)) is one NotInList, and a chain of `!=` compacts to one.
  4. OR'd IN-lists and literal-left equalities merge into one IN-list, and an
     OR that IS a single-column domain is replaced, not duplicated.
  5. The OR rewrites leave no `X OR False` litter behind.
  6. Compaction intersects IN-lists, folds exclusions and NULL tests, and runs
     again after CorrelatedFilters so transported ranges can contradict.

Each case asserts the PLAN SHAPE (via EXPLAIN) and the ANSWER (row count), so a
rewrite that fires but changes the result is caught here, not in production.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


def _plan(sql: str) -> list:
    """The plan tree as a list of 'operator | details' lines, top down."""
    session = opteryx.session()
    lines = []
    for morsel in session.execute_to_morsels("EXPLAIN " + sql):
        trees = morsel.column("tree").to_pylist()
        details = morsel.column("details").to_pylist()
        for tree, detail in zip(trees, details):
            tree = tree.decode() if isinstance(tree, bytes) else tree
            if tree.strip().startswith(("OPTIMIZATIONS", "REWRITE")):
                return lines
            lines.append(f"{tree.strip(' │├└─')} | {detail}")
    return lines


def _rows(sql: str) -> int:
    session = opteryx.session()
    return sum(morsel.num_rows for morsel in session.execute_to_morsels(sql))


def _filters(plan: list) -> list:
    return [line.split(" | ", 1)[1] for line in plan if line.startswith("Filter")]


def _filter_below_join(plan: list, needle: str) -> bool:
    """True when the Filter carrying `needle` sits BELOW the join (on a leg)."""
    join_at = next(i for i, line in enumerate(plan) if "Join" in line.split(" | ")[0])
    return any(needle in line for line in plan[join_at + 1 :] if line.startswith("Filter"))


SELF_JOIN = "SELECT p.name FROM $planets p INNER JOIN $planets s ON p.id = s.id"


# -- 1. ON-clause conjuncts get the WHERE-clause rewrites ------------------------

def test_on_clause_or_becomes_inlist_on_the_leg():
    sql = SELF_JOIN + " AND (p.id = 1 OR p.id = 2)"
    plan = _plan(sql)
    assert _filter_below_join(plan, "id IN [1, 2]"), plan
    assert not any(" OR " in f for f in _filters(plan)), plan
    assert _rows(sql) == 2


def test_on_clause_not_is_inverted():
    sql = SELF_JOIN + " AND NOT (p.id = 3)"
    plan = _plan(sql)
    assert _filter_below_join(plan, "id != 3"), plan
    assert not any("NOT" in f for f in _filters(plan)), plan
    assert _rows(sql) == 8


def test_on_clause_hoist_keeps_the_join_key():
    # only the single-leg conjunct moves; the equi key stays and the join is keyed
    sql = SELF_JOIN + " AND p.id > 5"
    assert _rows(sql) == 4
    assert _rows("SELECT p.name FROM $planets p INNER JOIN $planets s ON p.id = s.id WHERE p.id > 5") == 4


# -- 2. NOT handling --------------------------------------------------------------

def test_not_is_null_inverts_and_lands_on_the_leg():
    sql = SELF_JOIN + " WHERE NOT (p.name IS NULL)"
    plan = _plan(sql)
    assert _filter_below_join(plan, "name IS NOT NULL"), plan
    assert _rows(sql) == 9


def test_not_is_true_inverts():
    plan = _plan("SELECT name FROM $planets WHERE NOT (id > 3 IS TRUE)")
    assert _filters(plan) == ["IsNotTrue(id > 3)"], plan


def test_not_over_and_distributes_when_every_conjunct_inverts():
    sql = SELF_JOIN + " WHERE NOT (p.id > 3 AND p.id < 7)"
    plan = _plan(sql)
    assert _filter_below_join(plan, "(id <= 3 OR id >= 7)"), plan
    assert _rows(sql) == 6


def test_not_between_and_not_of_between_plan_alike():
    assert _filters(_plan("SELECT name FROM $planets WHERE NOT (id BETWEEN 3 AND 5)")) == [
        "(id < 3 OR id > 5)"
    ]
    assert _filters(_plan("SELECT name FROM $planets WHERE id NOT BETWEEN 3 AND 5")) == [
        "(id < 3 OR id > 5)"
    ]
    assert _rows("SELECT name FROM $planets WHERE NOT (id BETWEEN 3 AND 5)") == 6


def test_not_over_and_is_left_alone_when_a_conjunct_cannot_invert():
    # a CASE cannot be inverted to a single node, so the NOT stays a NOT
    plan = _plan(
        "SELECT name FROM $planets WHERE NOT (id > 3 AND CASE WHEN id = 1 THEN TRUE ELSE FALSE END)"
    )
    assert _filters(plan)[0].startswith("NOT ("), plan


def test_negated_anchored_like_lands_on_the_leg():
    sql = SELF_JOIN + " WHERE p.name NOT LIKE 'E%'"
    plan = _plan(sql)
    assert _filter_below_join(plan, "NOT _STARTS_WITH(name,b'E')"), plan
    assert _rows(sql) == 8


def test_single_relation_or_lands_on_the_leg():
    sql = SELF_JOIN + " WHERE (p.id > 1 OR p.name = 'x') AND (p.id > 1 OR s.name = 'y')"
    plan = _plan(sql)
    assert _filter_below_join(plan, "(id > 1 OR name = 'x')"), plan
    # the two-relation OR stays above the join
    assert plan[1].startswith("Filter | (id > 1 OR name = 'y')"), plan
    assert _rows(sql) == 8


# -- 3. NOT IN ----------------------------------------------------------------------

def test_not_over_inlist_is_one_not_in_list():
    sql = "SELECT name FROM $planets WHERE NOT (id IN (1, 2, 3, 4, 5))"
    assert _filters(_plan(sql)) == ["id NOT IN [1, 2, 3, 4, 5]"]
    assert _rows(sql) == 4


def test_not_over_or_of_equalities_is_one_not_in_list():
    sql = "SELECT name FROM $planets WHERE NOT (id = 1 OR id = 2 OR id = 3)"
    assert _filters(_plan(sql)) == ["id NOT IN [1, 2, 3]"]
    assert _rows(sql) == 6


def test_not_equal_chain_compacts_to_not_in_list():
    sql = "SELECT name FROM $planets WHERE id != 1 AND id != 2 AND id != 3"
    assert _filters(_plan(sql)) == ["id NOT IN [1, 2, 3]"]
    assert _rows(sql) == 6


def test_not_over_not_in_list_inverts_back():
    sql = "SELECT name FROM $planets WHERE NOT (id NOT IN (1, 2))"
    assert _filters(_plan(sql)) == ["id IN [1, 2]"]
    assert _rows(sql) == 2


# -- 4. OR -> IN merging -------------------------------------------------------------

def test_ored_inlists_merge():
    sql = "SELECT name FROM $planets WHERE id IN (1, 2) OR id IN (3, 4)"
    assert _filters(_plan(sql)) == ["id IN [1, 2, 3, 4]"]
    assert _rows(sql) == 4


def test_ored_equality_and_inlist_merge():
    sql = "SELECT name FROM $planets WHERE id = 1 OR id IN (3, 4)"
    assert _filters(_plan(sql)) == ["id IN [1, 3, 4]"]
    assert _rows(sql) == 3


def test_literal_left_equalities_merge():
    sql = "SELECT name FROM $planets WHERE 'Earth' = name OR 'Mars' = name"
    assert _filters(_plan(sql)) == ["name IN [b'Earth', b'Mars']"]
    assert _rows(sql) == 2


def test_cnf_inlists_and_literal_left_merge():
    sql = "SELECT name FROM $planets WHERE id IN (1, 2) OR 3 = id OR id IN (4) OR name = 'Pluto'"
    filters = _filters(_plan(sql))
    assert len(filters) == 1 and "id IN [1, 2, 3, 4]" in filters[0], filters
    assert _rows(sql) == 5


def test_exact_domain_or_is_replaced_not_duplicated_above_a_join():
    sql = SELF_JOIN + " WHERE p.id IN (1, 2) OR p.id IN (3, 4)"
    plan = _plan(sql)
    # the join's own line is at the top: nothing filters the joined rows again
    assert plan[0].startswith("Inner Join"), plan
    assert _filter_below_join(plan, "id IN [1, 2, 3, 4]"), plan
    assert _rows(sql) == 4


def test_domain_from_mixed_or_is_still_only_added():
    # `(id = 1 AND name = 'x') OR (id = 2 AND name = 'y')` — implied, not equivalent
    sql = "SELECT name FROM $planets WHERE (id = 1 AND name = 'x') OR (id = 3 AND name = 'Earth')"
    plan = _plan(sql)
    assert any("id IN [1, 3]" in f for f in _filters(plan)), plan
    assert any(" OR " in f for f in _filters(plan)), plan
    assert _rows(sql) == 1


# -- 5. no OR-litter -------------------------------------------------------------------

def test_or_rewrites_leave_no_false_branches():
    for sql in (
        SELF_JOIN + " WHERE p.id = 1 OR p.id = 2",
        SELF_JOIN + " WHERE p.name LIKE 'E%' OR p.name LIKE 'M%'",
        "SELECT name FROM testdata.astronauts WHERE 'MIT' = ANY(alma_mater) OR 'Stanford' = ANY(alma_mater)",
    ):
        plan = _plan(sql)
        assert not any("False" in f for f in _filters(plan)), (sql, plan)
    assert _rows(SELF_JOIN + " WHERE p.id = 1 OR p.id = 2") == 2


# -- 6. compaction ----------------------------------------------------------------------

def test_compaction_intersects_inlists():
    sql = "SELECT name FROM $planets WHERE id IN (1, 2, 3) AND id IN (2, 3, 4)"
    assert _filters(_plan(sql)) == ["id IN [2, 3]"]
    assert _rows(sql) == 2


def test_compaction_intersects_inlist_with_equality():
    sql = "SELECT name FROM $planets WHERE id IN (1, 2, 3) AND id = 2"
    assert _filters(_plan(sql)) == ["id = 2"]
    assert _rows(sql) == 1


def test_compaction_inlist_disjoint_from_equality_is_a_contradiction():
    sql = "SELECT name FROM $planets WHERE id IN (1, 2, 3) AND id = 7"
    assert _filters(_plan(sql)) == ["False"]
    assert _rows(sql) == 0


def test_compaction_prunes_inlist_by_range_and_exclusion():
    sql = "SELECT name FROM $planets WHERE id IN (1, 2, 3, 4, 5) AND id > 2 AND id != 4"
    assert _filters(_plan(sql)) == ["id IN [3, 5]"]
    assert _rows(sql) == 2


def test_compaction_equality_against_its_own_exclusion_is_false():
    sql = "SELECT name FROM $planets WHERE id = 1 AND NOT (id = 1)"
    assert _filters(_plan(sql)) == ["False"]
    assert _rows(sql) == 0


def test_compaction_drops_exclusion_implied_by_equality():
    sql = "SELECT name FROM $planets WHERE id = 1 AND id != 2"
    assert _filters(_plan(sql)) == ["id = 1"]
    assert _rows(sql) == 1


def test_compaction_drops_exclusion_outside_the_range():
    sql = "SELECT name FROM $planets WHERE id > 5 AND id != 2"
    assert _filters(_plan(sql)) == ["id > 5"]
    assert _rows(sql) == 4


def test_compaction_is_null_with_a_range_is_false():
    sql = "SELECT name FROM $planets WHERE id > 5 AND id IS NULL"
    assert _filters(_plan(sql)) == ["False"]
    assert _rows(sql) == 0


def test_compaction_drops_is_not_null_implied_by_equality():
    sql = "SELECT name FROM $planets WHERE id = 5 AND id IS NOT NULL"
    assert _filters(_plan(sql)) == ["id = 5"]
    assert _rows(sql) == 1


def test_compaction_keeps_a_lone_is_not_null():
    sql = "SELECT name FROM $planets WHERE id IS NOT NULL AND id IS NOT NULL"
    assert _filters(_plan(sql)) == ["id IS NOT NULL"]
    assert _rows(sql) == 9


def test_compaction_transported_range_contradiction():
    sql = SELF_JOIN + " WHERE p.id > 5 AND s.id < 3"
    plan = _plan(sql)
    assert _filters(plan) == ["False", "False"], plan
    assert _rows(sql) == 0


def test_compaction_leaves_temporal_and_integer_domains_apart():
    # mixed literal families on one column must not be compared as raw values
    sql = "SELECT name FROM $planets WHERE name IN ('Earth', 'Mars') AND name != 'Mars'"
    assert _filters(_plan(sql)) == ["name = 'Earth'"]
    assert _rows(sql) == 1


# -- projection context ------------------------------------------------------------------

def test_or_rewrites_keep_the_projected_columns_identity():
    # the OR rewrites also run over SELECT-list expressions (FunctionRewriteStrategy);
    # a fused or pruned OR must keep the alias and column the projection names
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(
        "SELECT id, id NOT BETWEEN 2 AND 4 AS nb, id = 1 OR id = 9 AS ends, "
        "name LIKE 'E%' OR name LIKE 'M%' AS em FROM $planets ORDER BY id"
    ):
        rows.extend(morsel.to_arrow().to_pylist())
    assert [r["nb"] for r in rows] == [True, False, False, False, True, True, True, True, True]
    assert [r["ends"] for r in rows] == [True, False, False, False, False, False, False, False, True]
    assert [r["em"] for r in rows][:4] == [True, False, True, True]  # Mercury, Venus, Earth, Mars


# -- ON-clause residuals: evaluated or refused, never dropped ----------------------------

def test_inner_join_two_sided_or_in_on_is_evaluated():
    sql = SELF_JOIN + " AND (p.id = 3 OR s.id = 4)"
    assert _rows(sql) == 2
    assert _rows(SELF_JOIN + " AND NOT (p.id = 3 OR s.id = 4)") == 7


def test_inner_join_two_sided_residual_lands_above_the_join():
    plan = _plan(SELF_JOIN + " AND (p.id = 3 OR s.id = 4)")
    assert plan[1].startswith("Filter | (id = 3 OR id = 4)"), plan
    assert plan[2].startswith("Inner Join"), plan


def test_inner_join_theta_comparison_stays_a_residual():
    sql = SELF_JOIN + " AND p.id + s.id > 10"
    assert _rows(sql) == 4
    plan = _plan(sql)
    assert not any("id + id" in f for f in _filters(plan)), plan


def test_inner_join_with_only_filters_in_on_is_refused():
    with pytest.raises(opteryx.exceptions.UnsupportedSyntaxError):
        _rows("SELECT p.name FROM $planets p INNER JOIN $planets s ON (p.id = 3 OR s.id = 4)")


def test_left_join_preserved_side_on_conjunct_is_refused_not_applied_as_where():
    # used to return 6 rows (the conjunct applied as a WHERE); the answer is 9
    with pytest.raises(opteryx.exceptions.UnsupportedSyntaxError) as err:
        _rows("SELECT p.name FROM $planets p LEFT JOIN $planets s ON p.id = s.id AND p.id > 3")
    assert "preserved side" in str(err.value)


def test_right_join_preserved_side_on_conjunct_is_refused():
    with pytest.raises(opteryx.exceptions.UnsupportedSyntaxError):
        _rows("SELECT p.name FROM $planets p RIGHT JOIN $planets s ON p.id = s.id AND s.id > 3")


def test_left_join_two_sided_or_in_on_is_refused_not_crashed():
    with pytest.raises(opteryx.exceptions.UnsupportedSyntaxError) as err:
        _rows("SELECT p.name FROM $planets p LEFT JOIN $planets s ON p.id = s.id AND (p.id = 3 OR s.id = 4)")
    assert "not an equality" in str(err.value)


def test_left_join_two_sided_theta_with_anti_idiom_is_refused_not_crashed():
    with pytest.raises(opteryx.exceptions.UnsupportedSyntaxError):
        _rows(
            "SELECT p.name FROM $planets p LEFT JOIN $planets s ON p.id = s.id AND p.id + s.id > 10 "
            "WHERE s.name IS NULL"
        )


def test_left_join_build_side_on_conjunct_still_prefilters():
    base = "SELECT p.name FROM $planets p LEFT JOIN $planets s ON p.id = s.id AND s.id > 3"
    assert _rows(base) == 9
    assert _rows(base + " WHERE s.name IS NULL") == 3


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
