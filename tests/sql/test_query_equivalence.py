"""
Pairs of queries that must return the same rows.

These assertions were previously carried by `tests/fuzzing/fuzz_constant_folding.py`,
which presented itself as an implementation of CODDTest ("Constant Optimization
Driven Database System Testing", Zhang & Rigger, SIGMOD 2025). It was not one:
it performed no expression evaluation and no constant propagation, and its
`--iterations 500` re-ran a `random.choice` over these same hardcoded pairs. It
was also never collected by pytest, so nothing here ran in CI.

The pairs themselves are real equivalences and worth keeping, so they live here
as what they are — a regression test. Each pair is a rewrite an optimizer is
entitled to make; if the two sides disagree, the engine has a logic bug.

Rows are compared as an order-insensitive MULTISET. The original compared sets,
which discards duplicates and so could not see a query emitting a row too many
or too few.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

# (original, rewritten, what the rewrite is)
EQUIVALENT_QUERIES = [
    (
        "SELECT * FROM $planets WHERE 1 = 1",
        "SELECT * FROM $planets",
        "a tautology in WHERE is redundant",
    ),
    (
        "SELECT * FROM $planets WHERE 1 = 1 LIMIT 5",
        "SELECT * FROM $planets LIMIT 5",
        "a tautology in WHERE is redundant under LIMIT",
    ),
    (
        "SELECT * FROM $planets WHERE 0 = 1",
        "SELECT * FROM $planets WHERE 1 = 0",
        "both contradictions return nothing",
    ),
    (
        "SELECT COUNT(*) FROM $planets WHERE 0 = 1",
        "SELECT 0",
        "counting a contradiction is zero",
    ),
    (
        "SELECT name FROM $planets WHERE id IN (1, 2, 3)",
        "SELECT name FROM $planets WHERE id = 1 OR id = 2 OR id = 3",
        "IN over literals is an OR chain",
    ),
    (
        "SELECT COUNT(*) FROM $planets WHERE id IN (1, 2, 3)",
        "SELECT COUNT(*) FROM $planets WHERE id = 1 OR id = 2 OR id = 3",
        "IN over literals is an OR chain, under an aggregate",
    ),
    (
        "SELECT COUNT(*) FROM $planets WHERE id IN (1, 2, 3)",
        "SELECT COUNT(*) FROM $planets WHERE id IN (SELECT id FROM (VALUES (1), (2), (3)) AS T(id))",
        "IN over literals matches IN over a subquery of the same values",
    ),
    (
        "SELECT COUNT(*) FROM $planets WHERE id IN (1, 2, 3)",
        "SELECT COUNT(*) FROM $planets WHERE 1 = 1 AND id IN (1, 2, 3)",
        "a tautology conjoined to IN is redundant",
    ),
    (
        "SELECT COUNT(*) FROM $planets p WHERE id IN (SELECT id FROM $planets WHERE id < 5)",
        "SELECT COUNT(*) FROM $planets WHERE id < 5",
        "a self-referencing IN subquery is the filter itself",
    ),
    (
        "SELECT COUNT(*) FROM $planets p1 JOIN $planets p2 ON p1.id = p2.id",
        "SELECT COUNT(*) FROM $planets",
        "a self-join on a unique key preserves cardinality",
    ),
    (
        "SELECT COUNT(*) FROM $planets WHERE id IS NOT NULL",
        "SELECT COUNT(*) FROM $planets",
        "IS NOT NULL on a non-nullable column is a tautology",
    ),
    (
        "SELECT COUNT(CASE WHEN id IS NOT NULL THEN 1 END) FROM $planets",
        "SELECT COUNT(*) FROM $planets",
        "counting a CASE that never yields NULL is COUNT(*)",
    ),
    (
        "SELECT SUM(CASE WHEN id > 0 THEN 1 ELSE 0 END) FROM $planets",
        "SELECT COUNT(*) FROM $planets",
        "summing a CASE that always yields 1 is COUNT(*)",
    ),
    (
        "SELECT COUNT(*) FROM $planets p1 WHERE id IN (SELECT id FROM $planets p2 LIMIT 5)",
        "SELECT 5",
        "an IN subquery limited to 5 of 9 unique keys matches 5 rows",
    ),
    (
        "SELECT COUNT(*) FROM (SELECT DISTINCT id FROM $planets) AS T",
        "SELECT COUNT(DISTINCT id) FROM $planets",
        "counting a DISTINCT subquery is COUNT(DISTINCT)",
    ),
]


def _multiset(sql: str):
    """Rows of `sql` as an order-insensitive multiset.

    Rendered positionally so the two sides compare on values, not column names,
    and as a sorted list rather than a set so duplicates still count.
    """
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(len(morsel)):
            rows.append(repr(morsel[i]))
    return sorted(rows)


@pytest.mark.parametrize("original,rewritten,description", EQUIVALENT_QUERIES)
def test_query_equivalence(original: str, rewritten: str, description: str):
    left = _multiset(original)
    right = _multiset(rewritten)
    assert left == right, (
        f"{description}\n"
        f"  {original}\n    -> {len(left)} rows\n"
        f"  {rewritten}\n    -> {len(right)} rows"
    )


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__])
