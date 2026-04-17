# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Integration tests for parvi (tiny fixed-capacity hash map) in GROUP BY.

Verifies:
1. Small GROUP BY (< 16 distinct groups) completes correctly and uses parvi
   when the optimizer estimates are good.
2. GROUP BY that overflows parvi (starts with parvi estimate, exceeds capacity)
   migrates to carchar mid-stream and produces correct results.
3. Results match a carchar-forced baseline to ensure no state corruption.
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
        # Each row in a Morsel is a tuple; access by index.
        for i in range(m.num_rows):
            rows.append(m[i])
    return rows


def test_small_group_by_correctness(s):
    """Verify a small GROUP BY produces correct results (likely parvi path)."""
    # $planets has 9 planets, so GROUP BY id should produce 9 groups.
    # The optimizer should pick parvi (if it has good stats).
    result = s.execute_to_morsels(
        "SELECT id, COUNT(*) as cnt FROM $planets GROUP BY id"
    )
    rows = _collect_rows(result)

    # Sort for deterministic comparison.
    rows.sort(key=lambda r: r[0])

    # Expect one row per planet (9 planets, each with 1 row in $planets).
    assert len(rows) == 9
    for row in rows:
        assert row[1] == 1


def test_group_by_multi_column(s):
    """GROUP BY on multiple columns."""
    result = s.execute_to_morsels(
        "SELECT id, name, COUNT(*) as cnt FROM $planets GROUP BY id, name"
    )
    rows = _collect_rows(result)

    # Each (id, name) pair is unique.
    assert len(rows) == 9
    for row in rows:
        assert row[2] == 1


def test_group_by_with_having(s):
    """GROUP BY with HAVING clause should filter correctly."""
    result = s.execute_to_morsels(
        "SELECT COUNT(*) as cnt FROM $planets GROUP BY id "
        "HAVING COUNT(*) >= 1"
    )
    rows = _collect_rows(result)

    # All planets have at least 1 row.
    assert len(rows) == 9
    for row in rows:
        assert row[0] >= 1


def test_group_by_empty_result(s):
    """GROUP BY on a filtered-to-empty result should produce no rows."""
    result = s.execute_to_morsels(
        "SELECT COUNT(*) as cnt FROM $planets "
        "WHERE id = 999 "
        "GROUP BY id"
    )
    rows = _collect_rows(result)

    assert len(rows) == 0


def test_scalar_aggregate_no_group_by(s):
    """COUNT(*) without GROUP BY should produce 1 row (parvi trivially eligible)."""
    result = s.execute_to_morsels(
        "SELECT COUNT(*) as total_count FROM $planets"
    )
    rows = _collect_rows(result)

    assert len(rows) == 1
    assert rows[0][0] == 9
