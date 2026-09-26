"""
GROUP BY -> ORDER BY <aggregate> LIMIT k fusion (docs/GROUPBY_TOPK_FUSION_DESIGN.md).

When a HeapSort sits directly above a GROUP BY, the compiler arms the GROUP BY sink
to emit only each hash partition's top k groups; the HeapSort still produces the
final answer. So the contract under test is: an ARMED plan returns exactly what the
UNARMED plan returns — same ordering values, same rows wherever the ORDER BY is a
total order — and the shapes that must not be armed (HAVING, a computed projection
or ORDER BY expression, a state-consuming aggregate) are not.

Arming is observed through the native `topk_pruned` counter (partitions/buckets
cut to their top k). The unarmed reference is the same query compiled with the
arming step disabled.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.managers.execution import compiler as _compiler
from opteryx.operators._operators import get_groupby_telemetry
from opteryx.operators._operators import reset_groupby_telemetry

# Enough groups that every one of the 64 hash partitions holds far more than k.
SERIES = "generate_series(1, 1000003) AS g"
# Enough groups (> 64 partitions x kGBMergeLeaf 65,536) that partitions take the
# RADIX merge path, where key values are gathered from sources by reference —
# asserted via `merge_bucketed` in test_radix_merge_path_armed.
BIG_SERIES = "generate_series(1, 6000011) AS g"


def _rows(sql):
    session = opteryx.session()
    try:
        rows = []
        for morsel in session.execute_to_morsels(sql):
            columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
            rows.extend(zip(*columns))
        return rows
    finally:
        session.close()


def _run(sql, armed, counter="topk_pruned"):
    """(rows, groupby telemetry `counter`) for `sql`, with the fusion armed or
    disabled."""
    original = _compiler._Compiler._arm_groupby_topk
    if not armed:
        _compiler._Compiler._arm_groupby_topk = lambda self, *args, **kwargs: None
    try:
        reset_groupby_telemetry()
        rows = _rows(sql)
        return rows, get_groupby_telemetry()[counter]
    finally:
        _compiler._Compiler._arm_groupby_topk = original


# (sql, ordering-value column position, total order?) — every one of these must ARM.
# fmt:off
ARMED = [
    # COUNT(*) DESC, unique counts impossible here, so compare ordering values only.
    (f"SELECT g % 100003 AS k, COUNT(*) AS c FROM {SERIES} GROUP BY g % 100003 ORDER BY c DESC LIMIT 10", 1, False),
    # SUM is unique per group -> a total order: rows must match exactly.
    (f"SELECT g % 100003 AS k, SUM(g) AS s FROM {SERIES} GROUP BY g % 100003 ORDER BY s DESC LIMIT 10", 1, True),
    (f"SELECT g % 100003 AS k, SUM(g) AS s FROM {SERIES} GROUP BY g % 100003 ORDER BY s ASC LIMIT 10", 1, True),
    # ORDER BY the aggregate expression itself (binds to the aggregate's output).
    (f"SELECT g % 100003 AS k, SUM(g) FROM {SERIES} GROUP BY g % 100003 ORDER BY SUM(g) DESC LIMIT 7", 1, True),
    # AVG (float), MAX, MIN over strings.
    (f"SELECT g % 100003 AS k, AVG(g) AS a FROM {SERIES} GROUP BY g % 100003 ORDER BY a DESC LIMIT 10", 1, True),
    (f"SELECT g % 100003 AS k, MAX(g) AS m FROM {SERIES} GROUP BY g % 100003 ORDER BY m ASC LIMIT 10", 1, True),
    (f"SELECT g % 100003 AS k, MIN(CAST(g AS VARCHAR)) AS m FROM {SERIES} GROUP BY g % 100003 ORDER BY m DESC LIMIT 10", 1, True),
    # COUNT(DISTINCT) and a DISTINCT-operand SUM.
    (f"SELECT g % 100003 AS k, COUNT(DISTINCT g % 7) AS u FROM {SERIES} GROUP BY g % 100003 ORDER BY u DESC LIMIT 10", 1, False),
    (f"SELECT g % 100003 AS k, SUM(DISTINCT g) AS s FROM {SERIES} GROUP BY g % 100003 ORDER BY s DESC LIMIT 10", 1, True),
    # NULL aggregates: groups whose every operand is NULL sum to NULL. NULL sorts
    # below values ascending, so ASC must return the NULL groups first.
    (f"SELECT g % 100003 AS k, SUM(CASE WHEN g % 100003 < 20 THEN NULL ELSE g END) AS s FROM {SERIES} GROUP BY g % 100003 ORDER BY s ASC LIMIT 25", 1, False),
    (f"SELECT g % 100003 AS k, SUM(CASE WHEN g % 100003 < 20 THEN NULL ELSE g END) AS s FROM {SERIES} GROUP BY g % 100003 ORDER BY s DESC LIMIT 25", 1, True),
    # ORDER BY continues onto a group key: heavy ties on c, broken by k — the
    # tie-inclusive cut must keep every tied group, so the result is a total order.
    (f"SELECT g % 100003 AS k, COUNT(*) AS c FROM {SERIES} GROUP BY g % 100003 ORDER BY c DESC, k ASC LIMIT 10", 1, True),
    (f"SELECT g % 100003 AS k, COUNT(*) AS c FROM {SERIES} GROUP BY g % 100003 ORDER BY c ASC, k DESC LIMIT 10", 1, True),
    # Several aggregates, ordered by the second; a pure-select projection between.
    (f"SELECT k, s, c FROM (SELECT g % 100003 AS k, COUNT(*) AS c, SUM(g) AS s FROM {SERIES} GROUP BY g % 100003) AS t ORDER BY s DESC LIMIT 10", 1, True),
]

# Radix merge path (keys by reference) — integer and string keys.
# fmt:off
RADIX = [
    f"SELECT g AS k, SUM(g) AS s FROM {BIG_SERIES} GROUP BY g ORDER BY s DESC LIMIT 10",
    f"SELECT CAST(g AS VARCHAR) AS k, SUM(g) AS s FROM {BIG_SERIES} GROUP BY CAST(g AS VARCHAR) ORDER BY s ASC LIMIT 10",
]

# Shapes that must NOT arm (topk_pruned stays 0) — and must still be right.
NOT_ARMED = [
    # HAVING: cutting before it could keep groups it removes.
    f"SELECT g % 100003 AS k, SUM(g) AS s FROM {SERIES} GROUP BY g % 100003 HAVING SUM(g) > 5 ORDER BY s DESC LIMIT 10",
    # ORDER BY an expression over the aggregate.
    f"SELECT g % 100003 AS k, SUM(g) AS s FROM {SERIES} GROUP BY g % 100003 ORDER BY s * -1 LIMIT 10",
    # ORDER BY leads with a group key.
    f"SELECT g % 100003 AS k, SUM(g) AS s FROM {SERIES} GROUP BY g % 100003 ORDER BY k DESC, s LIMIT 10",
    # A computed projection between the GROUP BY and the sort.
    f"SELECT k, s + 1 AS t FROM (SELECT g % 100003 AS k, SUM(g) AS s FROM {SERIES} GROUP BY g % 100003) AS x ORDER BY s DESC LIMIT 10",
    # A state-consuming aggregate as the ORDER BY key.
    f"SELECT g % 100003 AS k, MEDIAN(g) AS m FROM {SERIES} GROUP BY g % 100003 ORDER BY m DESC LIMIT 10",
]
# fmt:on


@pytest.mark.parametrize("sql, value_col, total_order", ARMED)
def test_armed_matches_unarmed(sql, value_col, total_order):
    armed_rows, pruned = _run(sql, armed=True)
    plain_rows, plain_pruned = _run(sql, armed=False)
    assert pruned > 0, f"expected the top-k fusion to arm:\n{sql}"
    assert plain_pruned == 0
    assert len(armed_rows) == len(plain_rows)
    # The ORDER BY values, in order, are the answer whatever the tie-breaking.
    assert [r[value_col] for r in armed_rows] == [r[value_col] for r in plain_rows], sql
    if total_order:
        assert armed_rows == plain_rows, sql
    else:
        # Ties at the LIMIT boundary may pick different groups; every returned
        # group must still be a real group with that value.
        full = set(_rows(sql.rsplit(" LIMIT ", 1)[0]))
        assert all(row in full for row in armed_rows), sql


@pytest.mark.parametrize("sql", NOT_ARMED)
def test_not_armed(sql):
    armed_rows, pruned = _run(sql, armed=True)
    plain_rows, _ = _run(sql, armed=False)
    assert pruned == 0, f"the top-k fusion must not arm here:\n{sql}"
    assert sorted(armed_rows, key=repr) == sorted(plain_rows, key=repr)


@pytest.mark.parametrize("sql", RADIX)
def test_radix_merge_path_armed(sql):
    armed_rows, pruned = _run(sql, armed=True)
    _, bucketed = _run(sql, armed=True, counter="merge_bucketed")
    plain_rows, _ = _run(sql, armed=False)
    assert bucketed > 0, f"expected the radix merge path:\n{sql}"
    assert pruned > 0
    assert armed_rows == plain_rows


def test_limit_without_order_by_not_armed():
    """No ORDER BY, no HeapSort: nothing to arm. Which 10 groups come back is
    unspecified (unordered LIMIT), so only the count is compared."""
    sql = f"SELECT g % 100003 AS k, SUM(g) AS s FROM {SERIES} GROUP BY g % 100003 LIMIT 10"
    armed_rows, pruned = _run(sql, armed=True)
    assert pruned == 0
    assert len(armed_rows) == 10


def test_limit_larger_than_group_count():
    """Armed, but no partition holds more than k groups: nothing is cut."""
    sql = f"SELECT g % 50 AS k, SUM(g) AS s FROM {SERIES} GROUP BY g % 50 ORDER BY s DESC LIMIT 1000"
    armed_rows, pruned = _run(sql, armed=True)
    plain_rows, _ = _run(sql, armed=False)
    assert pruned == 0
    assert len(armed_rows) == 50
    assert armed_rows == plain_rows


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
