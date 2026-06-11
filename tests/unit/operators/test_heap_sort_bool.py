"""
ORDER BY <bool> LIMIT k — heap_sort BOOL bit-packing (WP-02 regression tests).

DRAKEN_BOOL data is bit-packed 1 bit/row LSB-first. The numeric top-N reader
`_num_read_i64` previously byte-indexed it — reading the value of row idx*8
and overrunning the ~n/8-byte buffer for idx >= n/8 — producing wrong sort
order on every ORDER BY bool LIMIT query.

These tests drive HeapSortNode directly with contract-correct bit-packed
vectors. (The SQL path over parquet is exercised separately: the parquet bool
decode has its own byte-vs-bit packing bug, tracked independently.)
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


class _Collector(BasePlanNode):
    def __init__(self, properties=None, **kw):
        super().__init__(properties=properties, **kw)
        self.collected = []

    def _push_impl(self, morsel):
        if morsel is not None and morsel is not _EOS_SENTINEL:
            self.collected.append(morsel)


def _order_col(name):
    return SimpleNamespace(
        node_type=NodeType.IDENTIFIER,
        schema_column=SimpleNamespace(identity=name),
    )


def _run_topn(bools, order_by, limit, extra_cols=()):
    props = QueryProperties(query_id="q", variables={})
    node = HeapSortNode(props, order_by=order_by, limit=limit)
    sink = _Collector(props)
    node.set_downstream(sink)

    names = [b"id", b"b"] + [n for n, _ in extra_cols]
    vecs = [
        dn.vector_from_sequence(list(range(len(bools)))),
        dn.vector_from_bool_sequence(bools),
    ] + [v for _, v in extra_cols]
    node.push(Morsel.from_vectors(names, vecs))
    node.push(_EOS_SENTINEL)

    rows = []
    for m in sink.collected:
        rows.extend(zip(m.column(b"id").to_pylist(), m.column(b"b").to_pylist()))
    return rows


def test_bool_topn_descending_returns_trues():
    rows = _run_topn(
        [True] * 8 + [False] * 8,
        order_by=[(_order_col(b"b"), False)],  # descending
        limit=4,
    )
    assert len(rows) == 4
    assert all(b for _, b in rows), rows
    assert {i for i, _ in rows} <= set(range(8))


def test_bool_topn_ascending_returns_falses():
    rows = _run_topn(
        [True] * 8 + [False] * 8,
        order_by=[(_order_col(b"b"), True)],  # ascending
        limit=4,
    )
    assert len(rows) == 4
    assert all(not b for _, b in rows), rows
    assert {i for i, _ in rows} <= set(range(8, 16))


def test_bool_topn_tail_byte_rows():
    # n not a multiple of 8 exercises the final partial byte.
    rows = _run_topn(
        [True] * 9 + [False] * 2,
        order_by=[(_order_col(b"b"), True)],
        limit=3,
    )
    assert [b for _, b in rows] == [False, False, True], rows


def test_bool_topn_large_overruns_old_byte_index():
    # With byte-indexing, idx >= n/8 read past the bitmap; 10_000 rows makes
    # any such read certain to hit garbage and produce wrong membership.
    n = 10_000
    bools = [i % 3 == 0 for i in range(n)]
    rows = _run_topn(bools, order_by=[(_order_col(b"b"), False)], limit=50)
    assert len(rows) == 50
    assert all(b for _, b in rows)
    assert all(i % 3 == 0 for i, _ in rows)


def test_bool_multikey_topn():
    # bool primary key admits BOOL into the multi-key path too.
    rows = _run_topn(
        [True, False, True, False, True, False],
        order_by=[(_order_col(b"b"), False), (_order_col(b"id"), True)],
        limit=3,
    )
    assert [i for i, _ in rows] == [0, 2, 4], rows
    assert all(b for _, b in rows)


if __name__ == "__main__":  # pragma: no cover
    test_bool_topn_descending_returns_trues()
    test_bool_topn_ascending_returns_falses()
    test_bool_topn_tail_byte_rows()
    test_bool_topn_large_overruns_old_byte_index()
    test_bool_multikey_topn()
    print("✅ okay")
