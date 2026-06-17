"""
DRAKEN_INTERVAL mixed-unit ordering regression (native top-N sort vs DuckDB).

The interval slot is { int64 months; int64 us } where the sub-month field is
MICROSECONDS. The native sort builds a total-order key
    total_us = months * INTERVAL_MONTH_US + us   (INTERVAL_MONTH_US = 30 days in µs)
in both heap_sort.pyx and draken_vector_compare_at.

A latent bug normalized the month term with a 30-day-in-MILLISECONDS constant
(2_592_000_000) while the sub-month term was microseconds — the two terms were
1000× out of scale, so any interval with months != 0 AND a sub-month component
ordered wrong (e.g. 1 MONTH would sort BELOW 20 DAY).

DuckDB canonicalizes intervals with the same 30-day month + microsecond
resolution, so it is the oracle. Intervals are fed as (months, µs) tuples and the
reference ordering is what DuckDB produces for the equivalent intervals; the
cases mix month and sub-month components so the old bug would flip the order.
"""

import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import draken.draken_native as dn
from draken.morsels.morsel import Morsel
from opteryx.expression import NodeType
from opteryx.models.query_properties import QueryProperties
from opteryx.operators._operators import BasePlanNode, HeapSortNode, _EOS_SENTINEL, push_one

duckdb = pytest.importorskip("duckdb")

_DAY_US = 86_400_000_000
_HOUR_US = 3_600_000_000

# (months, µs) intervals mixing month + sub-month components. Chosen so the old
# ms/µs scale mix would change the ordering: e.g. (1mo) vs (20 day), and the
# exact equality (1mo + 5day) == (35 day) under the 30-day-month convention.
_IVALS = [
    (1, 0),               # 0: 1 month  (30 days)
    (0, 20 * _DAY_US),    # 1: 20 days  (< 1 month; bug ranked it larger)
    (1, 5 * _DAY_US),     # 2: 1 month + 5 days  == 35 days
    (0, 35 * _DAY_US),    # 3: 35 days           == case 2
    (2, 1 * _DAY_US),     # 4: 2 months + 1 day
    (0, 40 * _DAY_US),    # 5: 40 days
    (1, 12 * _HOUR_US),   # 6: 1 month + 12 hours
    (-1, 10 * _DAY_US),   # 7: -1 month + 10 days (negative total)
]


class _Collector(BasePlanNode):
    def __init__(self, properties=None, **kw):
        super().__init__(properties=properties, **kw)
        self.collected = []

    def _push_impl(self, morsel):
        if morsel is not None and morsel is not _EOS_SENTINEL:
            # Operators emit Cxx-backed morsels; materialize before column access.
            morsel.materialize()
            self.collected.append(morsel)


def _oc(name):
    return SimpleNamespace(node_type=NodeType.IDENTIFIER,
                           schema_column=SimpleNamespace(identity=name))


def _duckdb_order(ascending):
    """Reference ordering of the interval indices, by DuckDB's interval order."""
    con = duckdb.connect()
    union = "\nUNION ALL ".join(
        f"SELECT {i} AS idx, INTERVAL ({m}) MONTH + INTERVAL ({u}) MICROSECONDS AS v"
        for i, (m, u) in enumerate(_IVALS)
    )
    direction = "ASC" if ascending else "DESC"
    # idx as a stable tie-break so equal-total intervals have a defined order.
    sql = f"SELECT idx FROM ({union}) t ORDER BY v {direction}, idx ASC"
    return [r[0] for r in con.execute(sql).fetchall()]


def _opteryx_topn(ascending, limit):
    props = QueryProperties(query_id="q", variables={})
    node = HeapSortNode(
        props,
        order_by=[(_oc(b"iv"), ascending), (_oc(b"id"), True)],
        limit=limit,
    )
    sink = _Collector(props)
    node.set_downstream(sink)
    vid = dn.vector_from_sequence(list(range(len(_IVALS))))
    viv = dn.vector_interval_from_sequence(_IVALS)
    push_one(node, Morsel.from_vectors([b"id", b"iv"], [vid, viv]))
    push_one(node, _EOS_SENTINEL)
    out = []
    for m in sink.collected:
        out.extend(m.column(b"id").to_pylist())
    return out


def test_interval_ascending_topn_matches_duckdb():
    expected = _duckdb_order(ascending=True)
    actual = _opteryx_topn(ascending=True, limit=len(_IVALS))
    assert actual == expected, f"\nDuckDB : {expected}\nOpteryx: {actual}"


def test_interval_descending_topn_matches_duckdb():
    expected = _duckdb_order(ascending=False)
    actual = _opteryx_topn(ascending=False, limit=len(_IVALS))
    assert actual == expected, f"\nDuckDB : {expected}\nOpteryx: {actual}"


def test_interval_one_month_greater_than_twenty_days():
    # The headline bug: 1 MONTH (30 days) > 20 DAY. Under the old ms/µs scale mix
    # the month term was 1000× too small, ranking 20 DAY as the larger value.
    order = _opteryx_topn(ascending=True, limit=len(_IVALS))
    assert order.index(1) < order.index(0), f"20 DAY must sort before 1 MONTH: {order}"


def test_interval_month_plus_days_equals_days_total():
    # (1 month + 5 days) and (35 days) have equal totals under the 30-day month;
    # the id tie-break (2 before 3) must hold, proving they normalize equal.
    order = _opteryx_topn(ascending=True, limit=len(_IVALS))
    assert order.index(2) + 1 == order.index(3), f"cases 2 and 3 must be adjacent equals: {order}"


if __name__ == "__main__":  # pragma: no cover
    test_interval_ascending_topn_matches_duckdb()
    test_interval_descending_topn_matches_duckdb()
    test_interval_one_month_greater_than_twenty_days()
    test_interval_month_plus_days_equals_days_total()
    print("✅ okay")
