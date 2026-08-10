# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
INTERSECT and EXCEPT compare rows with IS NOT DISTINCT FROM, not with `=`.

SQL:2016 §7.13: the set operators match rows that are "not distinct from" each
other, and two NULLs are not distinct. So NULL is an ordinary key VALUE here — it
equals itself — which is the opposite of what `=` does everywhere else in SQL.

Until 2026-08-10 every set-operation path built its join with `Eq`, and the native
filter join then excluded NULL-keyed rows from the hash table entirely. The result
was that `X EXCEPT X` — empty by definition for any X — returned the NULL rows, and
`X INTERSECT X` silently dropped them. Both are SILENT wrong answers, and both were
invisible on a non-nullable column, which is why the existing INTERSECT/EXCEPT
coverage (id/name on $planets) never saw it. `SELECT *` made it look like a
wildcard bug because a 20-column projection almost always contains a nullable one.

The fix is a THIRD key rule on the filter join — `left semi/anti not-distinct`,
JoinMode 6/7 — alongside the two that already existed. All three disagree only on
NULL, which is exactly why substituting one for another is a wrong answer rather
than an error:

    left semi / left anti        EXISTS / NOT EXISTS   NULL matches nothing
    left anti null-aware         NOT IN                any NULL empties the result
    left semi/anti not-distinct  INTERSECT / EXCEPT    NULL equals NULL

The tests below are written as INVARIANTS over a relation rather than as expected
row counts against a fixed corpus, so they keep their meaning if $planets changes:
a relation minus itself is empty, and a relation intersected with itself is its own
distinct projection. The `ALL` forms carry the multiset version of the same claim.
The regression only appears when the projected column CONTAINS NULL, so each case
is asserted against both a nullable and a non-nullable column — a fix that made
these pass by disabling the rule for non-nullable columns would still fail here.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

# `surface_pressure` is NULL for the four gas giants; `id` and `name` have no NULLs.
# The NULL-free columns are the control: they passed before the fix and must still.
NULLABLE = "surface_pressure"
NON_NULLABLE = "id"


def _row_count(sql: str) -> int:
    return sum(morsel.num_rows for morsel in opteryx.session().execute_to_morsels(sql))


def _column_names(relation: str) -> list:
    """The relation's columns, in schema order — read off a morsel rather than
    hard-coded, so the wildcard equivalence below stays true if the corpus gains a
    column (which is exactly the change that would make a stale list silently
    weaken the test)."""
    for morsel in opteryx.session().execute_to_morsels(f"SELECT * FROM {relation} LIMIT 1"):
        return [
            name.decode() if isinstance(name, bytes) else name for name in morsel.column_names
        ]
    raise AssertionError(f"{relation} produced no morsel to read a schema from")


@pytest.mark.parametrize("column", [NULLABLE, NON_NULLABLE])
def test_except_self_is_empty(column):
    """X EXCEPT X is empty for every X. The NULL rows used to survive it."""
    count = _row_count(
        f"SELECT {column} FROM testdata.planets "
        f"EXCEPT SELECT {column} FROM testdata.planets"
    )
    assert count == 0, f"{column}: a relation minus itself returned {count} rows"


@pytest.mark.parametrize("column", [NULLABLE, NON_NULLABLE])
def test_intersect_self_is_the_distinct_projection(column):
    """X INTERSECT X is DISTINCT X — NULL included, as one group."""
    distinct = _row_count(f"SELECT DISTINCT {column} FROM testdata.planets")
    intersected = _row_count(
        f"SELECT {column} FROM testdata.planets "
        f"INTERSECT SELECT {column} FROM testdata.planets"
    )
    assert intersected == distinct, (
        f"{column}: INTERSECT with itself returned {intersected}, "
        f"but DISTINCT over the same column returns {distinct}"
    )


@pytest.mark.parametrize("column", [NULLABLE, NON_NULLABLE])
def test_except_all_self_is_empty(column):
    """Multiset difference of a relation with itself is empty."""
    count = _row_count(
        f"SELECT {column} FROM testdata.planets "
        f"EXCEPT ALL SELECT {column} FROM testdata.planets"
    )
    assert count == 0, f"{column}: EXCEPT ALL with itself returned {count} rows"


@pytest.mark.parametrize("column", [NULLABLE, NON_NULLABLE])
def test_intersect_all_self_keeps_every_row(column):
    """Multiset intersection with itself keeps every row, NULLs included."""
    total = _row_count("SELECT * FROM testdata.planets")
    count = _row_count(
        f"SELECT {column} FROM testdata.planets "
        f"INTERSECT ALL SELECT {column} FROM testdata.planets"
    )
    assert count == total, (
        f"{column}: INTERSECT ALL with itself kept {count} of {total} rows"
    )


def test_multi_column_key_with_a_partial_null_matches_itself():
    """(value, NULL) must equal (value, NULL).

    A composite key is the case a NULL sentinel can never handle, and the one the
    old hash-sentinel approach got wrong even when it got single columns right:
    draken hashes NULL PER COLUMN before combining, so the composite hash already
    agreed on both sides — the join was discarding those rows on purpose.
    """
    assert _row_count(
        f"SELECT {NON_NULLABLE}, {NULLABLE} FROM testdata.planets "
        f"EXCEPT SELECT {NON_NULLABLE}, {NULLABLE} FROM testdata.planets"
    ) == 0
    total = _row_count("SELECT * FROM testdata.planets")
    assert _row_count(
        f"SELECT {NON_NULLABLE}, {NULLABLE} FROM testdata.planets "
        f"INTERSECT SELECT {NON_NULLABLE}, {NULLABLE} FROM testdata.planets"
    ) == total


@pytest.mark.parametrize("operator", ["EXCEPT", "INTERSECT"])
def test_wildcard_set_operation_matches_the_named_projection(operator):
    """`SELECT *` must answer exactly as the named-column form does.

    The wildcard path is a SEPARATE rewrite (the binder's, not the plan
    rewriter's), so it needs its own assertion or it can regress alone. Stated as
    an equivalence rather than a row count: whatever the operator returns for the
    full column list, spelling that list with `*` must return the same.
    """
    columns = ", ".join(_column_names("testdata.planets"))
    star = _row_count(
        f"SELECT * FROM testdata.planets {operator} "
        f"SELECT * FROM testdata.planets WHERE id > 6"
    )
    named = _row_count(
        f"SELECT {columns} FROM testdata.planets {operator} "
        f"SELECT {columns} FROM testdata.planets WHERE id > 6"
    )
    assert star == named, f"`SELECT *` {operator} gave {star}, named columns gave {named}"

    # The equivalence ALONE is not enough: before the fix both spellings were wrong
    # in the same way, so it held while both answers were incorrect. Anchor the
    # wildcard path to an absolute invariant as well — `SELECT *` against itself.
    # A 20-column projection is where this bites hardest, since it only takes ONE
    # nullable column among them to poison the whole row comparison.
    star_self = _row_count(
        f"SELECT * FROM testdata.planets {operator} SELECT * FROM testdata.planets"
    )
    expected = 0 if operator == "EXCEPT" else _row_count("SELECT * FROM testdata.planets")
    assert star_self == expected, (
        f"`SELECT * {operator} SELECT *` over the same relation returned "
        f"{star_self}, expected {expected}"
    )


def test_not_in_keeps_its_own_null_rule():
    """The set-op rule must NOT have leaked into NOT IN.

    `x NOT IN (… NULL …)` is UNKNOWN for every x, so the result is empty. This is
    the null-aware ANTI mode, and it is the mode most easily confused with the new
    one — sharing the ANTI operator but disagreeing with it on exactly this. If
    the not-distinct rule leaked here, this returns rows.
    """
    assert _row_count(
        f"SELECT name FROM testdata.planets WHERE {NULLABLE} NOT IN "
        f"(SELECT {NULLABLE} FROM testdata.planets WHERE id > 6)"
    ) == 0


def test_ordinary_join_still_refuses_null_keys():
    """An equi-join is NOT a set operation: NULL must remain unmatchable.

    Scoping check for the new rule. The four NULL rows must not self-match, so the
    inner join returns only the rows with a non-NULL, non-duplicated value.
    """
    non_null_rows = _row_count(
        f"SELECT {NULLABLE} FROM testdata.planets WHERE {NULLABLE} IS NOT NULL"
    )
    matched = _row_count(
        f"SELECT a.name FROM testdata.planets AS a "
        f"INNER JOIN testdata.planets AS b ON a.{NULLABLE} = b.{NULLABLE}"
    )
    assert matched == non_null_rows, (
        f"inner join on a nullable column returned {matched}; NULL keys appear to "
        f"be matching each other, which only set operations may do"
    )


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
