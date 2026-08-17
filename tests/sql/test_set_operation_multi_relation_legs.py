# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""A set-operation leg is not one relation, and its columns are matched by POSITION.

    SELECT c_last_name, c_first_name, d_date FROM store_sales, date_dim, customer
    WHERE ... EXCEPT SELECT ... FROM catalog_sales, date_dim, customer WHERE ...

failed to bind — `UnexpectedDatasetReferenceError: Dataset $union-XXXX is not
available after being used on the right side of a ANTI or SEMI JOIN` — for any
INTERSECT or EXCEPT where either leg touched more than ONE relation. TPC-DS Q87 is
exactly that shape. A leg over one relation worked, which is why it survived.

INTERSECT/EXCEPT become semi/anti joins, and the ON condition used to be built as
the CROSS PRODUCT of `left relation names x right relation names x projected column
names`. With one relation per leg that is one equality and correct by accident. With
two it is four, and the ones naming the relation the leg does NOT project reference
columns the leg's own projection has already narrowed away.

The rewrite now runs at BIND time (`binder/set_ops._rewrite_setop_to_join`) and pairs
the legs POSITIONALLY — column i to column i, which is what SQL specifies — from each
leg's bound output columns. Three pre-bind/bind-time builders became one.

WHAT THESE TESTS PIN, beyond "it runs":

  * VALUES. A join built on the wrong columns returns a plausible row count. Every
    case here is asserted against the answer computed another way.
  * EVERY output column participates. A leg pair that agrees on the first column and
    disagrees on the second must return nothing — the failure mode of a wrong ON is
    silently comparing a subset. `SELECT *` is asserted structurally too (the ON
    carries one equality per column of the relation), because a wildcard over one
    table cannot disagree with itself on any column and so cannot catch it by value.
    An earlier draft of this fix compared `SELECT *` legs on the FILTER's column
    alone and every value-based test still passed.
  * Position, not name. Legs whose columns are named differently still match.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.planner.logical_planner import LogicalPlanStepType

# Each leg below self-joins `$planets` on `id`, so it holds exactly the nine planets.
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
    return sorted(collected, key=lambda row: tuple(str(value) for value in row))


def join_on_width(sql):
    """How many equalities the set operation's join condition carries.

    One per output column is the contract; fewer is a set operation that calls two
    rows equal without having compared them.
    """
    from opteryx.models import QueryTelemetry
    from opteryx.planner import bind_logical_plan, build_logical_plan, parse_statement

    telemetry = QueryTelemetry("test")
    session = opteryx.session()
    clean_sql, statements = parse_statement(sql)
    plan, _ = build_logical_plan(statements, clean_sql, None, telemetry)
    plan = bind_logical_plan(
        plan, clean_sql, None, session.context, "test", telemetry
    )

    widths = [
        len(get_all_nodes_of_type(node.on, (NodeType.COMPARISON_OPERATOR,)))
        for _, node in plan.nodes(True)
        if node.node_type == LogicalPlanStepType.Join
        and node.on is not None
        and ("semi" in str(node.type) or "anti" in str(node.type))
    ]
    assert widths, "no semi/anti join in the plan — the set operation was not rewritten"
    return max(widths)


TWO_RELATION_LEG = "SELECT p.id AS i FROM $planets p, $planets d WHERE d.id = p.id"
THREE_RELATION_LEG = (
    "SELECT p.id AS i FROM $planets p, $planets d, $planets e "
    "WHERE d.id = p.id AND e.id = p.id"
)


def test_intersect_over_two_relation_legs():
    answer = rows(f"{TWO_RELATION_LEG} INTERSECT {TWO_RELATION_LEG}")
    assert [value for (value,) in answer] == IDS, answer


def test_except_over_two_relation_legs():
    assert rows(f"{TWO_RELATION_LEG} EXCEPT {TWO_RELATION_LEG}") == [], "X EXCEPT X is empty"
    narrowed = rows(f"{TWO_RELATION_LEG} EXCEPT {TWO_RELATION_LEG} AND p.id > 4")
    assert [value for (value,) in narrowed] == [1, 2, 3, 4], narrowed


def test_three_relation_legs():
    """TPC-DS Q87's shape: three relations per leg, and an EXCEPT."""
    assert rows(f"{THREE_RELATION_LEG} INTERSECT {THREE_RELATION_LEG}") == [(i,) for i in IDS]
    narrowed = rows(f"{THREE_RELATION_LEG} EXCEPT {THREE_RELATION_LEG} AND p.id > 4")
    assert [value for (value,) in narrowed] == [1, 2, 3, 4], narrowed


@pytest.mark.parametrize(
    "statement,expected",
    [
        # One leg joins, the other does not — the sides need not agree on shape.
        (f"{TWO_RELATION_LEG} INTERSECT SELECT id FROM $planets", IDS),
        (f"SELECT id FROM $planets INTERSECT {TWO_RELATION_LEG}", IDS),
        (f"{TWO_RELATION_LEG} EXCEPT SELECT id FROM $planets", []),
        # A derived table collapses its leg to one relation; mixing the two spellings
        # must answer the same.
        (
            f"SELECT t.i FROM ({TWO_RELATION_LEG}) t INTERSECT SELECT id FROM $planets",
            IDS,
        ),
    ],
)
def test_asymmetric_legs(statement, expected):
    assert [value for (value,) in rows(statement)] == expected, statement


def test_chained_set_operations_over_multi_relation_legs():
    """A nested set operation is a leg too — and by then it is already a join.

    Its output is its LEFT leg's columns; reading the join's own `.columns` instead
    would count both legs' keys and report a two-column leg for a one-column set
    operation.
    """
    chained = (
        f"{TWO_RELATION_LEG} INTERSECT {TWO_RELATION_LEG} AND p.id > 2 "
        f"EXCEPT {TWO_RELATION_LEG} AND p.id > 6"
    )
    assert [value for (value,) in rows(chained)] == [3, 4, 5, 6], chained


def test_every_projected_column_is_compared():
    """Legs that agree on the first column and differ on the second match NOTHING.

    This is the assertion a wrong ON condition fails: comparing a subset of the
    output columns returns rows that are not in the intersection at all.
    """
    offset_leg = "SELECT p.id AS i, d.name AS n FROM $planets p, $planets d WHERE d.id = p.id + 1"
    aligned_leg = "SELECT p.id AS i, d.name AS n FROM $planets p, $planets d WHERE d.id = p.id"

    # Both legs produce the same ids; only the second column disagrees.
    assert [i for i, _ in rows(offset_leg)] == [1, 2, 3, 4, 5, 6, 7, 8]
    assert [i for i, _ in rows(aligned_leg)] == IDS

    assert rows(f"{offset_leg} INTERSECT {aligned_leg}") == [], "the name column was not compared"
    # ... and the same pair under EXCEPT keeps every row, for the same reason.
    assert len(rows(f"{offset_leg} EXCEPT {aligned_leg}")) == 8


def test_legs_are_matched_by_position_not_by_name():
    """`SELECT id AS a ... INTERSECT SELECT id AS b ...` pairs a with b.

    Name matching was the pre-bind rewrite's documented limitation; positional
    matching is what SQL specifies, and it is what makes the multi-relation case
    resolvable at all.
    """
    answer = rows(
        "SELECT id AS a FROM $planets WHERE id < 5 "
        "INTERSECT SELECT id AS b FROM $planets WHERE id > 2"
    )
    assert [value for (value,) in answer] == [3, 4], answer


@pytest.mark.parametrize("operator", ["INTERSECT", "EXCEPT"])
def test_a_wildcard_leg_compares_every_column(operator):
    """`SELECT *` must compare the WHOLE row.

    Structural, deliberately: both legs read one relation, so no value they could
    take makes a subset comparison disagree with the full one. The count is read off
    the relation rather than hard-coded so a corpus that gains a column does not
    quietly weaken this.
    """
    width = len(rows("SELECT * FROM $planets LIMIT 1")[0])
    statement = f"SELECT * FROM $planets WHERE id < 5 {operator} SELECT * FROM $planets WHERE id > 2"
    assert join_on_width(statement) == width

    # The unfiltered spelling reaches the leg's columns by a different route (the
    # planner resolves the wildcard at the scan, so the leg has no Project at all).
    unfiltered = f"SELECT * FROM $planets {operator} SELECT * FROM $planets WHERE id > 6"
    assert join_on_width(unfiltered) == width


def test_a_wildcard_leg_answers_as_the_named_projection_does():
    named = rows("SELECT id, name FROM $planets WHERE id < 5 INTERSECT SELECT id, name FROM $planets WHERE id > 2")
    star = rows("SELECT id, name FROM (SELECT * FROM $planets WHERE id < 5 INTERSECT SELECT * FROM $planets WHERE id > 2) AS t")
    assert star == named, (star, named)


def test_a_column_count_mismatch_is_still_refused():
    """The rewrite declines an unequal pairing rather than zipping it short, so the
    count check still reports it."""
    with pytest.raises(ValueError, match="column count mismatch"):
        rows("SELECT id, name FROM $planets INTERSECT SELECT id FROM $planets")


def test_null_is_its_own_key_across_multi_relation_legs():
    """The not-distinct rule (see test_set_operation_null_semantics) survives the move.

    `X EXCEPT X` is empty for any X, INCLUDING the NULL rows — which is only true if
    the join treats NULL as equal to NULL.
    """
    leg = (
        "SELECT p.surface_pressure AS s FROM $planets p, $planets d WHERE d.id = p.id"
    )
    assert rows(f"{leg} EXCEPT {leg}") == []
    intersected = rows(f"{leg} INTERSECT {leg}")
    distinct = rows("SELECT DISTINCT surface_pressure FROM $planets")
    assert len(intersected) == len(distinct), (intersected, distinct)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
