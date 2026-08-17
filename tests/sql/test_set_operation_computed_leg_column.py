# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
A set-operation leg whose own top projection is a COMPUTED expression — not a bare
identifier — failed to lower INTERSECT/EXCEPT to a join, TPC-DS Q08's shape:

    SELECT ca_zip FROM (
        SELECT SUBSTRING(ca_zip, 1, 5) ca_zip FROM customer_address WHERE ...
    ) A1
    INTERSECT
    SELECT ca_zip FROM (
        SELECT SUBSTRING(ca_zip, 1, 5) ca_zip, count(*) cnt
        FROM customer_address, customer WHERE ... GROUP BY ca_zip HAVING count(*) > 10
    ) A1

`binder/set_ops._rewrite_setop_to_join` needs to know which relation each leg's
output column came from to build the join's ON condition. For a bare identifier
that is `.source`; for an aggregate output it falls back to the schema column's
`.origin`. A computed projection (`SUBSTRING(...)`, arithmetic, ...) has neither —
`.source` is unset (the value isn't ANY one column) and the ExpressionColumn /
FunctionColumn it binds to carries no `.origin` (that field is populated for
aggregate outputs, not plain projected expressions). `_setop_leg_columns` gave up
and returned None, the rewrite declined, and the Intersect/Except node survived
bind time unconverted — the physical planner has no builder for it and raised
`InvalidInternalStateError: Unexpected logical node encountered during physical
planning: Intersect`.

The fix reads `.relations` instead — a Node attribute `inner_binder` already sets on
every bound expression (binder.py: `node.relations = set(sources)`) to the full set
of relations any identifier inside the expression resolves to. Trusted only when it
names exactly ONE relation on the leg's side: a literal-only projection (`SELECT 1`)
leaves it empty and a genuinely cross-relation expression leaves it ambiguous, and
both must still decline rather than guess — see the sibling coverage in
test_set_operation_multi_relation_legs.py for that boundary.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def rows(sql):
    """Every row of a result, as tuples in column order."""
    session = opteryx.session()
    collected = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None or not morsel.num_rows:
            continue
        names = morsel.column_names
        for i in range(morsel.num_rows):
            collected.append(tuple(morsel.column(name)[i] for name in names))
    return sorted(collected)


# A bare, uncomputed leg's own top projection is a computed expression with no
# wrapping derived table — the minimal shape that exposes the gap. The other leg is
# Q08's own shape: the computed expression sits inside a GROUP BY / HAVING
# aggregate, wrapped in a derived table that projects a single column back out.
COMPUTED_LEG = "SELECT SUBSTRING(name, 1, 3) AS n FROM $planets WHERE id <= 4"
AGGREGATE_LEG = (
    "SELECT n FROM ("
    "SELECT SUBSTRING(name, 1, 3) AS n, COUNT(*) AS c FROM $planets "
    "WHERE id BETWEEN 3 AND 9 GROUP BY SUBSTRING(name, 1, 3) HAVING COUNT(*) >= 1"
    ") A1"
)

# id<=4: Mercury, Venus, Earth, Mars -> Mer, Ven, Ear, Mar
# 3<=id<=9: Earth, Mars, Jupiter, Saturn, Uranus, Neptune, Pluto -> Ear, Mar, Jup, Sat, Ura, Nep, Plu
COMPUTED_LEG_VALUES = {"Mer", "Ven", "Ear", "Mar"}
AGGREGATE_LEG_VALUES = {"Ear", "Mar", "Jup", "Sat", "Ura", "Nep", "Plu"}


def test_intersect_with_computed_leg_column():
    answer = rows(f"{COMPUTED_LEG} INTERSECT {AGGREGATE_LEG}")
    expected = sorted((v,) for v in COMPUTED_LEG_VALUES & AGGREGATE_LEG_VALUES)
    assert answer == expected, answer
    assert answer == [("Ear",), ("Mar",)], answer


def test_except_with_computed_leg_column():
    answer = rows(f"{COMPUTED_LEG} EXCEPT {AGGREGATE_LEG}")
    expected = sorted((v,) for v in COMPUTED_LEG_VALUES - AGGREGATE_LEG_VALUES)
    assert answer == expected, answer
    assert answer == [("Mer",), ("Ven",)], answer


def test_intersect_with_computed_leg_column_on_the_right():
    """Same gap, mirrored: the computed leg is the RIGHT side of the set op.

    `_rewrite_setop_to_join` resolves both sides independently, so the fix must not
    be an accident of always landing on the left.
    """
    answer = rows(f"{AGGREGATE_LEG} INTERSECT {COMPUTED_LEG}")
    expected = sorted((v,) for v in AGGREGATE_LEG_VALUES & COMPUTED_LEG_VALUES)
    assert answer == expected, answer
    assert answer == [("Ear",), ("Mar",)], answer


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
