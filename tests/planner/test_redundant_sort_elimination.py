"""
Tests for RedundantSortEliminationStrategy — dropping a sort whose ordering a
later sort overwrites.

The shape is a view (here, its inline equivalent: a subquery carrying its own
ORDER BY) read by a query that orders again. The lower sort's work is thrown
away by the upper one, so it is removed; the upper one is always kept.

Two obligations are asserted:

  - the sort really goes (counted off EXPLAIN's rendered plan), and goes
    whether or not the two sorts share keys — a re-sorted sort is wasted work
    regardless of what it sorted by;

  - the rows are unchanged. Every case is run against the same query with the
    strategy switched off, and the two must agree EXACTLY — including row
    order, which is the property this rewrite is closest to breaking.

The blocking cases matter as much as the firing ones: a LIMIT, an aggregate or
a DISTINCT between the two sorts makes the lower sort load-bearing, and the
rewrite must decline.
"""

import os
import re
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx import config


def run(sql):
    sess = opteryx.session()
    rows = []
    for morsel in sess.execute_to_morsels(sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        rows.extend(zip(*columns))
    sess.close()
    return rows


def run_without_strategy(sql):
    """The same query with the rewrite switched off — the correctness oracle."""
    config.features.disable_redundant_sort_elimination = True
    try:
        return run(sql)
    finally:
        config.features.disable_redundant_sort_elimination = False


def explain_text(sql):
    parts = []
    for row in run("EXPLAIN " + sql):
        parts.append(
            " ".join(v.decode() if isinstance(v, (bytes, bytearray)) else str(v) for v in row)
        )
    return "\n".join(parts)


def sort_count(sql):
    """Sort steps in the plan. `Heap Sort` is a different operator (a fused
    Order+Limit) and is deliberately not counted by this."""
    return len(re.findall(r"(?:^|─ )Sort\b", explain_text(sql), flags=re.MULTILINE))


def heap_sort_count(sql):
    return len(re.findall(r"Heap Sort\b", explain_text(sql)))


_MATCHING_KEYS = """
    SELECT name, id FROM (SELECT name, id FROM $planets ORDER BY id DESC) AS v
     ORDER BY id DESC
"""

_DIFFERENT_KEYS = """
    SELECT name, id FROM (SELECT name, id FROM $planets ORDER BY name) AS v
     ORDER BY id DESC
"""

_THROUGH_A_FILTER = """
    SELECT name FROM (SELECT name, id FROM $planets ORDER BY id DESC) AS v
     WHERE id > 2 ORDER BY name
"""

_THREE_DEEP = """
    SELECT name FROM (
        SELECT name, id FROM (
            SELECT name, id FROM $planets ORDER BY name
        ) AS inner_v ORDER BY id
    ) AS v ORDER BY name DESC
"""

_BLOCKED_BY_LIMIT = """
    SELECT name, id FROM (SELECT name, id FROM $planets ORDER BY id DESC LIMIT 3) AS v
     ORDER BY id DESC
"""

_BLOCKED_BY_AGGREGATE = """
    SELECT COUNT(*) AS c, gravity
      FROM (SELECT gravity, id FROM $planets ORDER BY id DESC) AS v
     GROUP BY gravity ORDER BY gravity
"""

_BLOCKED_BY_DISTINCT = """
    SELECT DISTINCT name FROM (SELECT name FROM $planets ORDER BY name DESC) AS v
     ORDER BY name
"""


def _assert_rows_unchanged(sql):
    assert run(sql) == run_without_strategy(sql), f"rows or row order changed for: {sql}"


def test_sort_under_a_sort_is_removed():
    assert sort_count(_MATCHING_KEYS) == 1
    _assert_rows_unchanged(_MATCHING_KEYS)


def test_removal_does_not_require_matching_keys():
    # The lower sort orders by name, the upper by id — the lower one's work is
    # discarded just the same.
    assert sort_count(_DIFFERENT_KEYS) == 1
    _assert_rows_unchanged(_DIFFERENT_KEYS)


def test_removal_reaches_through_a_filter():
    assert sort_count(_THROUGH_A_FILTER) == 1
    _assert_rows_unchanged(_THROUGH_A_FILTER)


def test_chain_of_three_sorts_collapses_to_one():
    assert sort_count(_THREE_DEEP) == 1
    _assert_rows_unchanged(_THREE_DEEP)


def test_limit_between_the_sorts_blocks_removal():
    # The inner ORDER BY + LIMIT fuses into a Heap Sort, which picks WHICH rows
    # survive. Removing it would change the answer, not just the ordering.
    assert heap_sort_count(_BLOCKED_BY_LIMIT) == 1
    assert sort_count(_BLOCKED_BY_LIMIT) == 1
    _assert_rows_unchanged(_BLOCKED_BY_LIMIT)


def test_aggregate_between_the_sorts_blocks_removal():
    assert sort_count(_BLOCKED_BY_AGGREGATE) == 2
    _assert_rows_unchanged(_BLOCKED_BY_AGGREGATE)


def test_distinct_between_the_sorts_blocks_removal():
    assert sort_count(_BLOCKED_BY_DISTINCT) == 2
    _assert_rows_unchanged(_BLOCKED_BY_DISTINCT)


def test_explain_reports_the_removal():
    assert "remove redundant sort" in explain_text(_MATCHING_KEYS)


if __name__ == "__main__":
    import traceback

    tests = [
        test_sort_under_a_sort_is_removed,
        test_removal_does_not_require_matching_keys,
        test_removal_reaches_through_a_filter,
        test_chain_of_three_sorts_collapses_to_one,
        test_limit_between_the_sorts_blocks_removal,
        test_aggregate_between_the_sorts_blocks_removal,
        test_distinct_between_the_sorts_blocks_removal,
        test_explain_reports_the_removal,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  ✅ {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  ❌ {t.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
