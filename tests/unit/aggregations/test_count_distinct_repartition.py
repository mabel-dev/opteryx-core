"""
Grouped COUNT(DISTINCT) with few groups and many distinct pairs.

GroupBySink dedups (group, value) pairs in the GROUP's hash partition, so a handful
of groups used to funnel every pair through one or two partitions at merge time.
Finalize now repartitions those pairs by their own hash (cd_repartition in
native_group_sinks.hpp) once there are >= 65,536 pairs over <= 65,536 groups. These
queries cross that threshold; each expected count is derived arithmetically, and
every grouped answer is also checked against the DISTINCT-subquery spelling, which
takes the DistinctSink path and never touches the repartition.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import opteryx

N = 300_000  # rows; value i % 100_000 gives 100,000 distinct values overall


def _rows(sql: str):
    session = opteryx.session()
    try:
        rows = []
        for morsel in session.execute_to_morsels(sql):
            cols = [morsel.column(c).to_pylist() for c in morsel.column_names]
            rows.extend(zip(*cols))
        return sorted(rows, key=lambda r: tuple((x is None, x) for x in r))
    finally:
        session.close()


SERIES = f"(SELECT g AS i FROM generate_series(0, {N - 1}) AS g) AS s"


def test_few_groups_integer_values():
    # 7 groups; value i % 100_000 — every value lands in every group whose residues
    # it meets, so each group's distinct count is exact arithmetic.
    got = _rows(f"SELECT i % 7 AS k, COUNT(DISTINCT i % 100000) AS d FROM {SERIES} GROUP BY k")
    expected = []
    for k in range(7):
        expected.append((k, len({i % 100_000 for i in range(k, N, 7)})))
    assert got == expected


def test_few_groups_string_values_match_distinct_subquery():
    grouped = _rows(
        f"SELECT i % 5 AS k, COUNT(DISTINCT CAST(i % 120000 AS VARCHAR) || 'did') AS d "
        f"FROM {SERIES} GROUP BY k"
    )
    via_distinct = _rows(
        f"SELECT k, COUNT(*) AS d FROM (SELECT DISTINCT i % 5 AS k, "
        f"CAST(i % 120000 AS VARCHAR) || 'did' AS v FROM {SERIES}) AS t GROUP BY k"
    )
    assert grouped == via_distinct
    assert sum(d for _, d in grouped) > 65_536  # actually crossed the threshold


def test_duplicate_pairs_across_sources_count_once():
    # Three identical branches: each (group, value) pair reaches finalize from three
    # sources and must be counted once. 120_000 is a multiple of 5, so each group
    # holds exactly the 24_000 values congruent to it.
    union = " UNION ALL ".join([f"SELECT i FROM {SERIES}"] * 3)
    got = _rows(f"SELECT i % 5 AS k, COUNT(DISTINCT i % 120000) AS d FROM ({union}) AS t GROUP BY k")
    assert got == [(k, 24_000) for k in range(5)]


def test_skewed_groups_with_nulls_and_count_star():
    # One dominant group (k = 0 for 90% of rows) and NULL operands every 10th row:
    # COUNT(DISTINCT) ignores NULL, COUNT(*) does not.
    got = _rows(
        f"SELECT CASE WHEN i % 10 = 9 THEN i % 3 + 1 ELSE 0 END AS k, COUNT(*) AS c, "
        f"COUNT(DISTINCT CASE WHEN i % 10 = 4 THEN NULL ELSE i END) AS d "
        f"FROM {SERIES} GROUP BY k"
    )
    expected = {}
    for i in range(N):
        k = i % 3 + 1 if i % 10 == 9 else 0
        c, vals = expected.setdefault(k, [0, set()])
        expected[k][0] = c + 1
        if i % 10 != 4:
            vals.add(i)
    assert got == sorted((k, c, len(v)) for k, (c, v) in expected.items())


def test_two_count_distincts_and_sum_distinct_together():
    # Two repartitioned specs side by side with a distinct-operand spec (which keeps
    # the merge path) — they must not disturb one another.
    got = _rows(
        f"SELECT i % 4 AS k, COUNT(DISTINCT i) AS a, COUNT(DISTINCT i % 90000) AS b, "
        f"SUM(DISTINCT i % 1000) AS s FROM {SERIES} GROUP BY k"
    )
    expected = []
    for k in range(4):
        members = range(k, N, 4)
        expected.append(
            (
                k,
                len(members),
                len({i % 90_000 for i in members}),
                sum({i % 1000 for i in members}),
            )
        )
    assert got == expected


if __name__ == "__main__":  # pragma: no cover
    test_few_groups_integer_values()
    test_few_groups_string_values_match_distinct_subquery()
    test_duplicate_pairs_across_sources_count_once()
    test_skewed_groups_with_nulls_and_count_star()
    test_two_count_distincts_and_sum_distinct_together()
    print("✅ all grouped COUNT(DISTINCT) repartition tests passed")
