"""
ORDER BY <interval> LIMIT k — native interval top-N (WP-08).

Interval sort keys previously fell through to the per-comparison compare_at
Python path. WP-08 added a native total_us key (months*INTERVAL_MONTH_US + us,
matching draken_vector_compare_at) to the multi-key comparator and routed both
single- and multi-key interval sorts through it. These tests pin the ordering.

The interval slot's sub-month field carries MICROSECONDS (canonical engine unit),
so a month normalizes to 30 days × 86_400_000_000 µs/day.
"""

import os
import sys
from types import SimpleNamespace

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import draken.draken_native as dn
from draken.morsels.morsel import Morsel
from opteryx.expression import NodeType
from opteryx.models.query_properties import QueryProperties
from opteryx.operators._operators import BasePlanNode, HeapSortNode, _EOS_SENTINEL

_MONTH_US = 2592000000000


class _Collector(BasePlanNode):
    def __init__(self, properties=None, **kw):
        super().__init__(properties=properties, **kw)
        self.collected = []

    def _push_impl(self, morsel):
        if morsel is not None and morsel is not _EOS_SENTINEL:
            self.collected.append(morsel)


def _oc(name):
    return SimpleNamespace(node_type=NodeType.IDENTIFIER,
                           schema_column=SimpleNamespace(identity=name))


# (months, us) tuples; total order is months*_MONTH_US + us
_IVALS = [(0, 5000), (2, 0), (0, 1000), (1, 0), (0, 3000)]


def _total(t):
    return t[0] * _MONTH_US + t[1]


def _run(order_by, limit):
    props = QueryProperties(query_id="q", variables={})
    node = HeapSortNode(props, order_by=order_by, limit=limit)
    sink = _Collector(props)
    node.set_downstream(sink)
    vid = dn.vector_from_sequence(list(range(len(_IVALS))))
    viv = dn.vector_interval_from_sequence(_IVALS)
    node.push(Morsel.from_vectors([b"id", b"iv"], [vid, viv]))
    node.push(_EOS_SENTINEL)
    out = []
    for m in sink.collected:
        out.extend(m.column(b"id").to_pylist())
    return out


def test_interval_single_key_ascending():
    order = sorted(range(len(_IVALS)), key=lambda i: _total(_IVALS[i]))
    assert _run([(_oc(b"iv"), True)], 3) == order[:3]


def test_interval_single_key_descending():
    order = sorted(range(len(_IVALS)), key=lambda i: _total(_IVALS[i]), reverse=True)
    assert _run([(_oc(b"iv"), False)], 3) == order[:3]


def test_interval_multi_key_with_tiebreak():
    order = sorted(range(len(_IVALS)), key=lambda i: _total(_IVALS[i]))
    assert _run([(_oc(b"iv"), True), (_oc(b"id"), True)], 5) == order


def test_interval_with_equal_totals_tiebreak_by_id():
    # months=1 (==_MONTH_US) vs (0, _MONTH_US) have equal totals; id breaks the tie.
    ivals = [(1, 0), (0, _MONTH_US), (0, 0)]
    props = QueryProperties(query_id="q", variables={})
    node = HeapSortNode(props, order_by=[(_oc(b"iv"), True), (_oc(b"id"), True)], limit=3)
    sink = _Collector(props)
    node.set_downstream(sink)
    node.push(Morsel.from_vectors(
        [b"id", b"iv"],
        [dn.vector_from_sequence([0, 1, 2]), dn.vector_interval_from_sequence(ivals)],
    ))
    node.push(_EOS_SENTINEL)
    out = []
    for m in sink.collected:
        out.extend(m.column(b"id").to_pylist())
    assert out == [2, 0, 1]  # (0,0) smallest; then equal totals ordered by id 0,1


if __name__ == "__main__":  # pragma: no cover
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✓ {name}")
    print("✅ okay")
