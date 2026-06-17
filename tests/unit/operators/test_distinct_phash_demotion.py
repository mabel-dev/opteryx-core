"""
DISTINCT PerfectHashSet demotion (WP-01 regression tests).

The PerfectHashSet fast path commits on the first morsel (single non-null
dense INT8/INT16 key). Encodings legitimately vary per morsel (Parquet dict
pages, nullable row groups), so a later ineligible morsel must DEMOTE to the
carchar path — re-seeding it with the values already marked seen — never pass
through unfiltered. The original bug emitted ineligible morsels unchanged,
silently returning duplicate rows.
"""

import os
import sys
from types import SimpleNamespace

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import draken.draken_native as dn
from draken.morsels.morsel import Morsel
from opteryx.models.query_properties import QueryProperties
from opteryx.operators._operators import BasePlanNode, DistinctNode, push_one


class _Collector(BasePlanNode):
    def __init__(self, properties=None, **kw):
        super().__init__(properties=properties, **kw)
        self.collected = []

    def _push_impl(self, morsel):
        self.collected.append(morsel)


def _make_node():
    props = QueryProperties(query_id="q", variables={})
    on = [SimpleNamespace(schema_column=SimpleNamespace(identity=b"c"))]
    node = DistinctNode(props, on=on)
    sink = _Collector(props)
    node.set_downstream(sink)
    return node, sink


def _emitted_values(sink):
    vals = []
    for m in sink.collected:
        # Operators now emit Cxx-backed morsels; materialize (as the cursor does)
        # before engine-external PyObject column access.
        m.materialize()
        vals.extend(m.column(b"c").to_pylist())
    return vals


def test_phash_demotes_on_nullable_second_morsel():
    node, sink = _make_node()
    m1 = Morsel.from_vectors([b"c"], [dn.vector_int8_from_sequence([i % 10 for i in range(100)])])
    m2 = Morsel.from_vectors(
        [b"c"],
        [dn.vector_int8_from_sequence([None if i % 7 == 0 else i % 10 for i in range(100)])],
    )

    push_one(node, m1)
    assert node._use_phash is True
    push_one(node, m2)
    assert node._use_phash is False  # demoted

    vals = _emitted_values(sink)
    assert len(vals) == len(set(vals)), f"duplicates emitted: {sorted(vals)}"
    assert set(vals) == set(range(10)) | {None}


def test_phash_demotes_on_dict_encoded_second_morsel():
    node, sink = _make_node()
    m1 = Morsel.from_vectors([b"c"], [dn.vector_int8_from_sequence([i % 10 for i in range(100)])])
    # dict-encoded: 12 unique values, 100 rows -> data_length != length
    m2 = Morsel.from_vectors(
        [b"c"], [dn.vector_int8_from_dict(list(range(12)), [i % 12 for i in range(100)])]
    )

    push_one(node, m1)
    assert node._use_phash is True
    push_one(node, m2)
    assert node._use_phash is False  # demoted

    vals = _emitted_values(sink)
    assert len(vals) == len(set(vals)), f"duplicates emitted: {sorted(vals)}"
    assert set(vals) == set(range(12))


def test_phash_demotion_preserves_seen_values():
    # Values seen ONLY before demotion must not reappear after it.
    node, sink = _make_node()
    m1 = Morsel.from_vectors([b"c"], [dn.vector_int8_from_sequence([1, 2, 3, 4, 5])])
    m2 = Morsel.from_vectors(
        [b"c"], [dn.vector_int8_from_sequence([1, 2, 3, 4, 5, None, 6])]
    )

    push_one(node, m1)
    push_one(node, m2)

    vals = _emitted_values(sink)
    assert sorted(v for v in vals if v is not None) == [1, 2, 3, 4, 5, 6]
    assert vals.count(None) == 1


def test_phash_int16_demotes_on_nulls():
    node, sink = _make_node()
    m1 = Morsel.from_vectors(
        [b"c"], [dn.vector_int16_from_sequence([i % 300 - 150 for i in range(600)])]
    )
    m2 = Morsel.from_vectors(
        [b"c"],
        [dn.vector_int16_from_sequence([None if i % 5 == 0 else i % 300 - 150 for i in range(600)])],
    )

    push_one(node, m1)
    assert node._use_phash is True
    push_one(node, m2)
    assert node._use_phash is False

    vals = _emitted_values(sink)
    assert len(vals) == len(set(vals))
    assert set(vals) == {i - 150 for i in range(300)} | {None}


def test_phash_eligible_stream_stays_on_fast_path():
    node, sink = _make_node()
    for _ in range(5):
        m = Morsel.from_vectors(
            [b"c"], [dn.vector_int8_from_sequence([i % 20 for i in range(1000)])]
        )
        push_one(node, m)

    assert node._use_phash is True
    vals = _emitted_values(sink)
    assert sorted(vals) == list(range(20))


def test_phash_demotes_on_permutation_first_morsel():
    # A PERMUTATION INT8 vector (data_length == length, non-identity selection)
    # must NOT be scanned via the dense-physical phash path (it would dedupe in
    # physical order). The IDENTITY-flag gate (WP-04) demotes it.
    node, sink = _make_node()
    perm = [3, 1, 4, 2, 5, 0, 6, 7]
    v = dn.vector_int8_from_dict(list(range(8)), perm)  # data_length==length, flags=0
    push_one(node, Morsel.from_vectors([b"c"], [v]))

    assert node._use_phash is False  # demoted on the permutation
    vals = _emitted_values(sink)
    assert sorted(vals) == list(range(8))


def test_fully_deduped_chunks_are_not_emitted():
    # Morsel.__len__ is COLUMN count; the old guard emitted 0-row chunks.
    node, sink = _make_node()
    m1 = Morsel.from_vectors([b"c"], [dn.vector_int8_from_sequence([1, 2, 3])])
    m2 = Morsel.from_vectors([b"c"], [dn.vector_int8_from_sequence([1, 2, 3])])

    push_one(node, m1)
    push_one(node, m2)

    assert len(sink.collected) == 1  # second (empty) chunk suppressed
    assert sink.collected[0].num_rows == 3


if __name__ == "__main__":  # pragma: no cover
    test_phash_demotes_on_nullable_second_morsel()
    test_phash_demotes_on_dict_encoded_second_morsel()
    test_phash_demotion_preserves_seen_values()
    test_phash_int16_demotes_on_nulls()
    test_phash_eligible_stream_stays_on_fast_path()
    test_fully_deduped_chunks_are_not_emitted()
    print("✅ okay")
