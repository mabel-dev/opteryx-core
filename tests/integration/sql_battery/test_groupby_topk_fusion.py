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
    # Explicit, non-default null placement: ASC NULLS LAST keeps NULL groups OUT of
    # the top 25; DESC NULLS FIRST puts all 20 of them IN, ahead of every value.
    (f"SELECT g % 100003 AS k, SUM(CASE WHEN g % 100003 < 20 THEN NULL ELSE g END) AS s FROM {SERIES} GROUP BY g % 100003 ORDER BY s ASC NULLS LAST LIMIT 25", 1, True),
    (f"SELECT g % 100003 AS k, SUM(CASE WHEN g % 100003 < 20 THEN NULL ELSE g END) AS s FROM {SERIES} GROUP BY g % 100003 ORDER BY s DESC NULLS FIRST LIMIT 25", 1, False),
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
    # A no-aggregate GROUP BY (a DISTINCT over the keys): nothing to rank.
    f"SELECT g % 100003 AS k FROM {SERIES} GROUP BY g % 100003 ORDER BY k LIMIT 10",
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


_NULL_SUM = (
    f"SELECT g % 100003 AS k, SUM(CASE WHEN g % 100003 < 20 THEN NULL ELSE g END) AS s "
    f"FROM {SERIES} GROUP BY g % 100003 ORDER BY s"
)


@pytest.mark.parametrize(
    "order, nulls_in_answer, nulls_lead",
    [
        ("ASC", 20, True),                 # default: NULL lowest -> first
        ("ASC NULLS FIRST", 20, True),
        ("ASC NULLS LAST", 0, None),       # 100k+ non-null groups beat every NULL
        ("DESC", 0, None),                 # default: NULL lowest -> last
        ("DESC NULLS LAST", 0, None),
        ("DESC NULLS FIRST", 20, True),
    ],
)
def test_armed_null_placement_by_value(order, nulls_in_answer, nulls_lead):
    """The armed top-k cut honours the ORDER BY's null placement, pinned by VALUE: a
    parity check alone would pass if the armed and unarmed plans shared the same wrong
    placement (which is exactly what an ignored NULLS clause looked like)."""
    rows, pruned = _run(f"{_NULL_SUM} {order} LIMIT 25", armed=True)
    assert pruned > 0, f"expected the top-k fusion to arm for ORDER BY s {order}"
    values = [r[1] for r in rows]
    assert len(values) == 25
    assert sum(v is None for v in values) == nulls_in_answer, (order, values)
    if nulls_lead:
        assert all(v is None for v in values[:nulls_in_answer]), (order, values)
    non_null = [v for v in values if v is not None]
    descending = order.startswith("DESC")
    assert non_null == sorted(non_null, reverse=descending), (order, values)


# ---------------------------------------------------------------------------------
# Top-k through a projection that provably cannot raise
# (docs/GROUPBY_TOPK_THROUGH_PROJECTION_DESIGN.md). A narrow-integer GROUP BY output
# +/- an integer literal below 2**31 binds INT64 and cannot overflow, so a projection
# of it no longer blocks the fusion. Everything else a projection can compute still does.
# ---------------------------------------------------------------------------------


def _keyed(cast_type, modulus):
    """A subquery giving a GROUP BY key of a narrow integer type."""
    return f"(SELECT CAST(g % {modulus} AS {cast_type}) AS k, g FROM {SERIES}) AS t"


# fmt:off
THROUGH_PROJECTION_ARMED = [
    # (sql, ordering-value column position, total order?)
    (f"SELECT k, k + 1 AS k1, COUNT(*) AS c FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY c DESC LIMIT 10", 2, False),
    (f"SELECT k, 5 - k AS k1, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY s DESC LIMIT 10", 2, True),
    (f"SELECT k, k - 7 AS k1, SUM(g) AS s FROM {_keyed('INT16', 30011)} GROUP BY k ORDER BY s ASC LIMIT 10", 2, True),
    (f"SELECT k, k + 1 AS k1, SUM(g) AS s FROM {_keyed('UINT16', 60001)} GROUP BY k ORDER BY s DESC LIMIT 10", 2, True),
    (f"SELECT k, k + 1 AS k1, SUM(g) AS s FROM {_keyed('UINT32', 100003)} GROUP BY k ORDER BY s DESC LIMIT 10", 2, True),
    (f"SELECT k, k + 1 AS k1, SUM(g) AS s FROM {_keyed('INT8', 127)} GROUP BY k ORDER BY s DESC LIMIT 1", 2, True),
    # Several derived columns, and the largest literal the class admits.
    (f"SELECT k, k + 1 AS a, k - 2 AS b, k + 2147483647 AS c2, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY s DESC LIMIT 10", 4, True),
    # OFFSET (k = limit + offset) and ORDER BY continuing onto a key.
    (f"SELECT k, k + 1 AS k1, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY s DESC LIMIT 5 OFFSET 3", 2, True),
    (f"SELECT k, k + 1 AS k1, COUNT(*) AS c FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY c DESC, k ASC LIMIT 10", 2, True),
]

THROUGH_PROJECTION_NOT_ARMED = [
    # INT64 key: +/- a literal CAN overflow, so it stays unarmed.
    f"SELECT g % 100003 AS k, g % 100003 + 1 AS k1, SUM(g) AS s FROM {SERIES} GROUP BY g % 100003 ORDER BY s DESC LIMIT 10",
    # Anything but +/- with a small integer literal on a narrow key.
    f"SELECT k, k * 2 AS k1, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY s DESC LIMIT 10",
    f"SELECT k, k / 2 AS k1, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY s DESC LIMIT 10",
    f"SELECT k, k % 7 AS k1, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY s DESC LIMIT 10",
    f"SELECT k, CAST(k AS INT64) AS k1, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY s DESC LIMIT 10",
    f"SELECT k, ABS(k) AS k1, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY s DESC LIMIT 10",
    f"SELECT k, k + k AS k1, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY s DESC LIMIT 10",
    f"SELECT k, k + 1 + 2 AS k1, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY s DESC LIMIT 10",
    f"SELECT k, k + 1.5 AS k1, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY s DESC LIMIT 10",
    # A literal at or beyond 2**31 is outside the class.
    f"SELECT k, k + 3000000000 AS k1, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY s DESC LIMIT 10",
    # ORDER BY the derived column: no leading aggregate to rank.
    f"SELECT k, k + 1 AS k1, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k ORDER BY k1 DESC LIMIT 10",
    # HAVING still blocks it.
    f"SELECT k, k + 1 AS k1, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k HAVING SUM(g) > 5 ORDER BY s DESC LIMIT 10",
    # Known v1 limitation: projections stacked through subqueries carry their columns in
    # NESTED wrappers, which the matcher does not look through (it never did, for pass-
    # through columns either). Correct answer, just not fused.
    f"SELECT a, b, s FROM (SELECT k, k + 1 AS a, k - 1 AS b, s FROM (SELECT k, SUM(g) AS s FROM {_keyed('INT32', 100003)} GROUP BY k) AS x) AS y ORDER BY s DESC LIMIT 10",
]
# fmt:on


@pytest.mark.parametrize("sql, value_col, total_order", THROUGH_PROJECTION_ARMED)
def test_through_projection_armed_matches_unarmed(sql, value_col, total_order):
    armed_rows, pruned = _run(sql, armed=True)
    plain_rows, plain_pruned = _run(sql, armed=False)
    assert pruned > 0, f"expected the top-k fusion to arm through the projection:\n{sql}"
    assert plain_pruned == 0
    assert len(armed_rows) == len(plain_rows)
    assert [r[value_col] for r in armed_rows] == [r[value_col] for r in plain_rows], sql
    if total_order:
        assert armed_rows == plain_rows, sql


@pytest.mark.parametrize("sql", THROUGH_PROJECTION_NOT_ARMED)
def test_through_projection_not_armed(sql):
    armed_rows, pruned = _run(sql, armed=True)
    plain_rows, _ = _run(sql, armed=False)
    assert pruned == 0, f"the top-k fusion must not arm through this projection:\n{sql}"
    assert sorted(armed_rows, key=repr) == sorted(plain_rows, key=repr)


@pytest.mark.parametrize(
    "cast_type, low, high",
    [
        ("INT8", -128, 127), ("INT16", -32768, 32767), ("INT32", -2147483648, 2147483647),
        ("UINT8", 0, 255), ("UINT16", 0, 65535), ("UINT32", 0, 4294967295),
    ],
)
def test_narrow_integer_plus_minus_literal_binds_int64(cast_type, low, high):
    """The class's safety argument, checked per width: +/- a literal of magnitude
    2**31 - 1 on a column at either end of its range binds INT64 and is exact."""
    literal = 2147483647
    sql = (
        f"SELECT CAST(g * 0 + {high} AS {cast_type}) + {literal} AS hi, "
        f"CAST(g * 0 + {low} AS {cast_type}) - {literal} AS lo "
        f"FROM generate_series(1, 3) AS g"
    )
    session = opteryx.session()
    try:
        morsels = list(session.execute_to_morsels(sql))
    finally:
        session.close()
    types = [str(t) for t in morsels[0].column_types]
    assert all("INT64" in t for t in types), (cast_type, types)
    assert morsels[0].column("hi").to_pylist() == [high + literal] * 3
    assert morsels[0].column("lo").to_pylist() == [low - literal] * 3


def test_int64_overflow_still_raises_with_a_limit():
    """The reason INT64 operands stay outside the class: a group NOT in the top k
    whose derived column overflows must still raise, as it does without the fusion."""
    sql = (
        "SELECT k, k + 1 AS k1, COUNT(*) AS c FROM "
        f"(SELECT CASE WHEN g = 1 THEN 9223372036854775807 ELSE g % 100003 END AS k FROM {SERIES}) AS t "
        "GROUP BY k ORDER BY c DESC LIMIT 5"
    )
    for armed in (True, False):
        with pytest.raises(Exception, match="overflow"):
            _run(sql, armed=armed)


def test_narrowing_cast_still_raises_with_a_limit():
    """A CAST can raise, so it is outside the class: most groups' keys do not fit INT8,
    and the error the unfused plan raises must survive the LIMIT."""
    sql = (
        f"SELECT k, CAST(k AS INT8) AS k1, SUM(g) AS s FROM {_keyed('INT32', 100003)} "
        "GROUP BY k ORDER BY s DESC LIMIT 10"
    )
    with pytest.raises(Exception, match="out of range"):
        _run(sql, armed=True)
