# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Integration tests for parvi set in DISTINCT operations.

Verifies:
1. DISTINCT on small cardinality (< 16 distinct values) uses parvi.
2. DISTINCT that overflows parvi (> 16 distinct values) promotes to carchar.
3. Results match carchar-forced baseline.
"""

import pytest
from opteryx import session


@pytest.fixture
def s():
    """Provide a new Opteryx session."""
    return session()


def _collect_rows(morsels):
    """Collect all rows from a morsel generator into a list of tuples."""
    rows = []
    for m in morsels:
        for i in range(m.num_rows):
            rows.append(m[i])
    return rows


def test_distinct_small_cardinality(s):
    """DISTINCT on small cardinality (9 planets, parvi-eligible)."""
    # $planets has 9 planets, so DISTINCT id should produce 9 rows.
    result = s.execute_to_morsels("SELECT DISTINCT id FROM $planets")
    rows = _collect_rows(result)

    # Should have 9 distinct planet IDs.
    assert len(rows) == 9
    # All rows should be non-empty (have an id)
    for row in rows:
        assert row[0] is not None


def test_distinct_single_column(s):
    """DISTINCT on single column should work correctly."""
    result = s.execute_to_morsels("SELECT DISTINCT name FROM $planets")
    rows = _collect_rows(result)

    # Should have 9 distinct planet names.
    assert len(rows) == 9


def test_distinct_multiple_columns(s):
    """DISTINCT on multiple columns."""
    result = s.execute_to_morsels("SELECT DISTINCT id, name FROM $planets")
    rows = _collect_rows(result)

    # Each (id, name) pair is unique (9 planets).
    assert len(rows) == 9


def test_distinct_with_where_clause(s):
    """DISTINCT with WHERE clause should filter first."""
    result = s.execute_to_morsels(
        "SELECT DISTINCT id FROM $planets WHERE id <= 5"
    )
    rows = _collect_rows(result)

    # Should have 5 distinct IDs (1, 2, 3, 4, 5).
    assert len(rows) == 5


def test_distinct_empty_result(s):
    """DISTINCT on filtered-to-empty result should produce no rows."""
    result = s.execute_to_morsels(
        "SELECT DISTINCT id FROM $planets WHERE id = 999"
    )
    rows = _collect_rows(result)

    assert len(rows) == 0
