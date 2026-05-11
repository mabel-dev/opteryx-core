"""Regression tests for the FilterImpliedGroupKeyReductionStrategy optimizer rule.

The strategy strips GROUP BY keys that are provably single-valued because a
WHERE clause below the aggregate constrains them with col = literal. Results
must be identical to the unoptimized form.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

import opteryx


def _rows(sql):
    session = opteryx.session()
    morsels = [m for m in session.execute_to_morsels(sql) if m is not None and m.num_rows > 0]
    rows = []
    for m in morsels:
        for i in range(m.num_rows):
            rows.append({
            (col.decode() if isinstance(col, bytes) else col): m.column(
                col.encode() if isinstance(col, str) else col,
                col.encode() if isinstance(col, str) else col,
            )[i]
            for col in m.column_names
        })
    return rows


def _count(sql):
    session = opteryx.session()
    total = 0
    for m in session.execute_to_morsels(sql):
        if m is not None:
            total += m.num_rows
    return total


# --- correctness: single-column equality + GROUP BY that column ---

def test_single_implied_key_count_is_correct():
    rows = _rows("SELECT name, COUNT(*) AS n FROM $planets WHERE name = 'Earth' GROUP BY name")
    assert len(rows) == 1, f"expected 1 row, got {len(rows)}"
    assert rows[0]["name"] == b"Earth"
    assert rows[0]["n"] == 1


def test_single_implied_key_string_value_preserved():
    rows = _rows("SELECT name, COUNT(*) AS n FROM $planets WHERE name = 'Mars' GROUP BY name")
    assert len(rows) == 1
    assert rows[0]["name"] == b"Mars"


def test_single_implied_key_integer():
    rows = _rows("SELECT id, COUNT(*) AS n FROM $planets WHERE id = 3 GROUP BY id")
    assert len(rows) == 1
    assert rows[0]["id"] == 3
    assert rows[0]["n"] == 1


# --- multi-column GROUP BY where one key is constant-implied ---

def test_multi_key_partial_reduction_is_correct():
    # name is constant-implied; id is not — id should still partition
    rows = _rows(
        "SELECT name, id, COUNT(*) AS n FROM $planets WHERE name = 'Earth' GROUP BY name, id"
    )
    assert len(rows) == 1
    assert rows[0]["name"] == b"Earth"


def test_multi_key_surviving_key_partitions():
    # Multiple planets exist; id partitions, name is constant — expect one row per id
    rows = _rows(
        "SELECT name, id, COUNT(*) AS n FROM $planets WHERE name != 'Pluto' GROUP BY name, id"
    )
    # name is NOT constant-implied here; optimization must not fire
    assert len(rows) > 1


# --- equality inside AND chain ---

def test_conjunction_implied_key():
    rows = _rows(
        "SELECT name, id, COUNT(*) AS n FROM $planets "
        "WHERE name = 'Earth' AND id = 3 GROUP BY name, id"
    )
    assert len(rows) == 1
    assert rows[0]["name"] == b"Earth"
    assert rows[0]["id"] == 3


# --- OR branch must NOT trigger the optimization ---

def test_inequality_predicate_does_not_reduce():
    # name is not constant-implied by an inequality; all planets still partition
    rows = _rows(
        "SELECT name, COUNT(*) AS n FROM $planets WHERE id > 0 GROUP BY name"
    )
    assert len(rows) == 9


# --- equality on column NOT in GROUP BY is a no-op ---

def test_filter_on_non_group_column_is_noop():
    rows = _rows(
        "SELECT name, COUNT(*) AS n FROM $planets WHERE id = 3 GROUP BY name"
    )
    # id is not a group key; name still partitions normally — one row (Earth, id=3)
    assert len(rows) == 1
    assert rows[0]["name"] == b"Earth"


# --- single group key that is constant-implied: keep at least one key ---

def test_single_group_key_implied_not_collapsed():
    # With only one group key (name = 'Earth') the optimization must keep it
    # to preserve AggregateAndGroup semantics (zero rows on empty input).
    # The result should still be correct.
    rows = _rows("SELECT name, COUNT(*) AS n FROM $planets WHERE name = 'Earth' GROUP BY name")
    assert len(rows) == 1
    assert rows[0]["name"] == b"Earth"


# --- empty input: AggregateAndGroup must return zero rows, not one ---

def test_aggregate_and_group_empty_input_returns_zero_rows():
    # Predicate matches no rows; GROUP BY must return 0 rows (not 1)
    n = _count(
        "SELECT name, COUNT(*) AS n FROM $planets WHERE name = 'NotAPlanet' GROUP BY name"
    )
    assert n == 0, f"expected 0 rows on empty input, got {n}"


if __name__ == "__main__":
    test_single_implied_key_count_is_correct()
    test_single_implied_key_string_value_preserved()
    test_single_implied_key_integer()
    test_multi_key_partial_reduction_is_correct()
    test_multi_key_surviving_key_partitions()
    test_conjunction_implied_key()
    test_inequality_predicate_does_not_reduce()
    test_filter_on_non_group_column_is_noop()
    test_single_group_key_implied_not_collapsed()
    test_aggregate_and_group_empty_input_returns_zero_rows()
    print("OK")
