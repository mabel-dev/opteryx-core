# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
PredicateCompactionStrategy: OR-range union.

    col < 4 OR (col >= 4 AND col < 7) OR (col >= 7 AND col < 9)  =>  col < 9

Each test pairs a correctness check (rowcount, so a wrong merge shows up as a
wrong answer) with a plan-shape check via EXPLAIN (so an accidentally-correct
non-optimization doesn't read as a pass). EXPLAIN runs against `$planets`
rather than `testdata.planets` because predicate pushdown folds a compacted
predicate straight into the Parquet scan, which would hide the FILTER's
condition text from the `details` column entirely.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from tests.helpers import execute_and_get_rowcount


def _explain_details(sql):
    """Return the `details` column values for an EXPLAIN of `sql`."""
    morsel = list(opteryx.session().execute_to_morsels("EXPLAIN " + sql))[0]
    names = [c.decode() if isinstance(c, bytes) else c for c in morsel.column_names]
    data = {n: morsel.column(morsel.column_names[i]).to_pylist() for i, n in enumerate(names)}
    return data["details"]


def test_or_range_union_collapses_three_touching_ranges():
    # Mirrors the reported CVSS bucket filter: three contiguous ranges on one
    # column, joined by OR, are exactly equivalent to a single upper bound.
    sql = (
        "mass < 4.0 OR (mass >= 4.0 AND mass < 7.0) OR (mass >= 7.0 AND mass < 9.0)"
    )
    count = execute_and_get_rowcount(f"SELECT id FROM testdata.planets WHERE {sql}")
    assert count == execute_and_get_rowcount("SELECT id FROM testdata.planets WHERE mass < 9.0")
    assert count == 5

    details = "\n".join(_explain_details(f"SELECT id FROM $planets WHERE {sql}"))
    assert "mass < 9.0" in details
    assert "OR" not in details


def test_or_range_union_collapses_to_between():
    sql = "(id >= 2 AND id < 4) OR (id >= 4 AND id < 6)"
    count = execute_and_get_rowcount(f"SELECT id FROM testdata.planets WHERE {sql}")
    assert count == execute_and_get_rowcount(
        "SELECT id FROM testdata.planets WHERE id >= 2 AND id < 6"
    )
    assert count == 4  # ids 2, 3, 4, 5

    details = "\n".join(_explain_details(f"SELECT id FROM $planets WHERE {sql}"))
    assert "id >= 2" in details and "id < 6" in details
    assert "OR" not in details


def test_or_range_union_drops_contradictory_branch():
    # The first branch can never be true for any value (id > 100 AND id < 50);
    # it should be dropped, leaving just the second branch.
    sql = "(id > 100 AND id < 50) OR id < 3"
    count = execute_and_get_rowcount(f"SELECT id FROM testdata.planets WHERE {sql}")
    assert count == execute_and_get_rowcount("SELECT id FROM testdata.planets WHERE id < 3")
    assert count == 2  # ids 1, 2

    details = "\n".join(_explain_details(f"SELECT id FROM $planets WHERE {sql}"))
    assert details.strip().startswith("id < 3")
    assert "OR" not in details


def test_or_range_union_leaves_disjoint_ranges_unmerged():
    # No overlap and no adjacency between the two ranges: nothing to merge, and
    # the result must NOT collapse to a single range.
    sql = "mass < 1.0 OR mass >= 100.0"
    count = execute_and_get_rowcount(f"SELECT id FROM testdata.planets WHERE {sql}")
    assert count == 6  # ids 1, 4, 9 (mass<1.0) + 5, 6, 8 (mass>=100.0)

    details = "\n".join(_explain_details(f"SELECT id FROM $planets WHERE {sql}"))
    assert "OR" in details


def test_or_range_union_bails_on_mixed_columns():
    # Branches touch different columns -- must not be reasoned about as ranges
    # on a single column, and must be left semantically untouched.
    sql = "(id < 3) OR (mass < 1.0)"
    count = execute_and_get_rowcount(f"SELECT id FROM testdata.planets WHERE {sql}")
    assert count == 4  # ids 1, 2 (id<3) + 4, 9 (mass<1.0)

    details = "\n".join(_explain_details(f"SELECT id FROM $planets WHERE {sql}"))
    assert "OR" in details


def test_or_range_union_bails_on_non_range_operator():
    # NotEq isn't a range bound -- the whole OR must be left alone rather than
    # partially or incorrectly rewritten.
    sql = "id != 5 OR id < 3"
    count = execute_and_get_rowcount(f"SELECT id FROM testdata.planets WHERE {sql}")
    assert count == 8  # every id except 5

    details = "\n".join(_explain_details(f"SELECT id FROM $planets WHERE {sql}"))
    assert "OR" in details


def test_or_range_union_preserves_null_semantics_on_full_coverage():
    # surface_pressure is NULL for ids 5-8 (the gas giants). The two branches
    # here cover the entire non-NULL domain, but collapsing to unconditional
    # TRUE would be wrong: the original OR still evaluates NULL (not TRUE) for
    # a NULL surface_pressure, so those 4 rows must stay excluded.
    sql = "surface_pressure < 50 OR surface_pressure >= 50"
    count = execute_and_get_rowcount(f"SELECT id FROM testdata.planets WHERE {sql}")
    assert count == execute_and_get_rowcount(
        "SELECT id FROM testdata.planets WHERE surface_pressure IS NOT NULL"
    )
    assert count == 5  # NOT 9 -- ids 5, 6, 7, 8 (NULL) must stay excluded

    details = "\n".join(_explain_details(f"SELECT id FROM $planets WHERE {sql}"))
    assert "OR" in details  # must not have collapsed to unconditional TRUE


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
