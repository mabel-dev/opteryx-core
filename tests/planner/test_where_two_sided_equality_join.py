"""Regression tests for a two-sided equality in WHERE over a join.

A WHERE-clause equality referencing exactly the two legs of a join
(`... JOIN ... ON a.k = b.k WHERE a.c = b.c`) used to be routed into
`Join.condition` -- a field the execution compiler never reads (it reads
`on`/`residual`). The predicate was removed from the collected set, so
`complete()` never restored it as a Filter either, and the query silently
returned the answer to the *unfiltered* join.

The WHERE form must be row-for-row identical to the ON form, and to a
subquery-filter oracle that the optimizer cannot fold into the join.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], "..", ".."))

import opteryx


def _rows(sql):
    """Return the result as a sorted list of value-tuples (column names excluded).

    Column *names* differ between the equivalent formulations (`f.Mission` vs
    `x.Mission`), so identity is asserted on values and row multiplicity.
    """
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None or morsel.num_rows == 0:
            continue
        table = morsel.to_arrow()
        for record in table.to_pylist():
            rows.append(tuple(record.values()))
    return sorted(rows, key=repr)


def _count(sql):
    session = opteryx.session()
    total = 0
    for morsel in session.execute_to_morsels(sql):
        if morsel is not None:
            total += morsel.num_rows
    return total


COLUMNS = "f.Mission, f.Company, l.Company, f.Location"
INNER = "testdata.missions f INNER JOIN testdata.missions l ON f.Mission = l.Mission"

# The oracle: the equality is applied above a subquery, so the two-sided
# predicate never reaches the join node and cannot be folded away.
ORACLE = (
    "SELECT * FROM (SELECT f.Mission, f.Company AS fc, l.Company AS lc, f.Location "
    "FROM testdata.missions f INNER JOIN testdata.missions l ON f.Mission = l.Mission) x "
    "WHERE x.fc = x.lc"
)


# --- the P0: WHERE form must equal ON form, row for row ---

def test_where_equality_matches_on_equality_row_for_row():
    where_form = _rows(f"SELECT {COLUMNS} FROM {INNER} WHERE f.Company = l.Company")
    on_form = _rows(
        f"SELECT {COLUMNS} FROM testdata.missions f INNER JOIN testdata.missions l "
        "ON f.Mission = l.Mission AND f.Company = l.Company"
    )
    assert where_form == on_form, (
        f"WHERE form returned {len(where_form)} rows, ON form {len(on_form)}"
    )


def test_where_equality_matches_subquery_oracle_row_for_row():
    where_form = _rows(f"SELECT {COLUMNS} FROM {INNER} WHERE f.Company = l.Company")
    oracle = _rows(ORACLE)
    assert where_form == oracle, (
        f"WHERE form returned {len(where_form)} rows, oracle {len(oracle)}"
    )


def test_where_equality_actually_filters():
    """The join is not a no-op: the predicate must remove rows.

    Guards against a 'fix' that merely makes two identically-broken forms agree.
    """
    filtered = _count(f"SELECT 1 FROM {INNER} WHERE f.Company = l.Company")
    unfiltered = _count(f"SELECT 1 FROM {INNER}")
    assert filtered < unfiltered, (
        f"predicate removed no rows: {filtered} == {unfiltered}"
    )


def test_equality_and_inequality_partition_the_join():
    """`=` and `<>` over a non-null column must sum to the unfiltered join."""
    equal = _count(f"SELECT 1 FROM {INNER} WHERE f.Company = l.Company")
    not_equal = _count(f"SELECT 1 FROM {INNER} WHERE f.Company <> l.Company")
    unfiltered = _count(f"SELECT 1 FROM {INNER}")
    assert equal + not_equal == unfiltered, (
        f"{equal} + {not_equal} != {unfiltered}"
    )


# --- the fold must decline when the equality is not representable as join fields ---

def test_non_representable_equality_is_kept_as_a_filter():
    """`a.c = b.c || ''` cannot become a join key; it must survive as a Filter."""
    where_form = _rows(f"SELECT f.Mission FROM {INNER} WHERE f.Company = l.Company || ''")
    oracle = _rows(
        "SELECT x.Mission FROM (SELECT f.Mission, f.Company AS fc, l.Company AS lc "
        "FROM testdata.missions f INNER JOIN testdata.missions l ON f.Mission = l.Mission) x "
        "WHERE x.fc = x.lc || ''"
    )
    assert where_form == oracle, (
        f"declined form returned {len(where_form)} rows, oracle {len(oracle)}"
    )
    assert len(where_form) > 0


# --- outer joins: the predicate is materialised above the join, not folded ---

def _outer_case(join_type):
    base = f"testdata.missions f {join_type} testdata.missions l ON f.Mission = l.Mission"
    where_form = _rows(f"SELECT {COLUMNS} FROM {base} WHERE f.Company = l.Company")
    oracle = _rows(
        "SELECT * FROM (SELECT f.Mission, f.Company AS fc, l.Company AS lc, f.Location "
        f"FROM testdata.missions f {join_type} testdata.missions l ON f.Mission = l.Mission) x "
        "WHERE x.fc = x.lc"
    )
    assert where_form == oracle, (
        f"{join_type}: WHERE form {len(where_form)} rows, oracle {len(oracle)}"
    )


def test_left_join_where_equality():
    _outer_case("LEFT JOIN")


def test_right_join_where_equality():
    _outer_case("RIGHT JOIN")


def test_full_outer_join_where_equality():
    _outer_case("FULL OUTER JOIN")


# --- a two-sided equality must not be folded into a CROSS JOIN incorrectly ---

def test_cross_join_where_equality():
    where_form = _rows(
        "SELECT f.Mission, f.Company, l.Company FROM testdata.missions f "
        "CROSS JOIN testdata.missions l WHERE f.Mission = l.Mission AND f.Company = l.Company"
    )
    on_form = _rows(
        "SELECT f.Mission, f.Company, l.Company FROM testdata.missions f "
        "INNER JOIN testdata.missions l ON f.Mission = l.Mission AND f.Company = l.Company"
    )
    assert where_form == on_form, (
        f"CROSS form {len(where_form)} rows, INNER form {len(on_form)}"
    )


if __name__ == "__main__":
    test_where_equality_matches_on_equality_row_for_row()
    test_where_equality_matches_subquery_oracle_row_for_row()
    test_where_equality_actually_filters()
    test_equality_and_inequality_partition_the_join()
    test_non_representable_equality_is_kept_as_a_filter()
    test_left_join_where_equality()
    test_right_join_where_equality()
    test_full_outer_join_where_equality()
    test_cross_join_where_equality()
    print("OK")
