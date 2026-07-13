import pyarrow as pa

from opteryx import EOS
import draken.draken_native as dn
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.compiled.morsel_ops.distinct import distinct
from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
from opteryx.compiled.structures.parvi_set import ParviSetWrapper
from opteryx.models.query_properties import QueryProperties
from opteryx.operators._operators import BasePlanNode, DistinctNode, push_one


class _Collector(BasePlanNode):
    """Downstream sink that records everything pushed to it (the current operators
    are push-based; the old generator `.execute()` entry point was removed)."""

    def __init__(self, properties=None, **kw):
        super().__init__(properties=properties, **kw)
        self.collected = []

    def _push_impl(self, morsel):
        self.collected.append(morsel)


def _drive(values, set_variant):
    """Push one morsel of ``values`` through a DistinctNode and return
    (node, emitted data chunks). EOS is not pushed here."""
    props = QueryProperties(query_id="q", variables={})
    node = DistinctNode(props, on=None, set_variant=set_variant)
    sink = _Collector(props)
    node.set_downstream(sink)
    push_one(node, _from_arrow(pa.table({"type": values})))
    return node, sink

_DT = dn.DrakenType


def _from_arrow(table):
    """Build a draken Morsel from a pyarrow.Table via the pyarrow-free vector path
    (draken.Morsel.from_arrow was removed with draken's pyarrow purge — §4). pyarrow
    is a test-only dependency, used to read the fixture data + its type."""
    names, vecs = [], []
    for name in table.column_names:
        col = table.column(name)
        vals = col.to_pylist()
        pat = col.type
        if pa.types.is_boolean(pat):
            dt = _DT.BOOL
        elif pa.types.is_floating(pat):
            dt = _DT.FLOAT64
        elif pa.types.is_integer(pat):
            dt = _DT.INT64
        elif pa.types.is_string(pat) or pa.types.is_large_string(pat):
            dt = _DT.VARCHAR
            vals = [v.encode("utf-8") if isinstance(v, str) else v for v in vals]
        elif pa.types.is_binary(pat) or pa.types.is_large_binary(pat):
            dt = _DT.VARCHAR
        else:
            raise TypeError(f"_from_arrow: unsupported fixture type {pat}")
        names.append(name.encode("utf-8") if isinstance(name, str) else name)
        vecs.append(vector_from_sequence(vals, dtype=dt))
    return Morsel.from_vectors(names, vecs)


def test_parvi_promotion_preserves_seen_keys_across_morsels():
    values = [f"type_{i}" for i in range(16)]
    first = _from_arrow(pa.table({"type": values}))
    second = _from_arrow(pa.table({"type": values}))

    seen = ParviSetWrapper()

    distinct(first, seen, columns=[b"type"])
    assert len(first) == 16
    assert seen.full()

    promoted = CarcharSetWrapper()
    seen.drain_into_carchar(promoted)
    seen = promoted

    distinct(second, seen, columns=[b"type"])
    assert len(second) == 0


def test_distinct_node_does_not_promote_when_exactly_sixteen_and_done():
    node, sink = _drive([f"type_{i}" for i in range(16)], "parvi")

    data = [m for m in sink.collected if m is not EOS]
    assert len(data) == 1
    assert data[0].num_rows == 16
    assert node._promoted is False

    # EOS forwards only the sentinel downstream — no additional data chunk, no promote.
    push_one(node, EOS)
    assert [m for m in sink.collected if m is not EOS] == data
    assert node._promoted is False


def test_distinct_node_promotes_on_overflow_and_keeps_same_morsel_uniques():
    node, sink = _drive([f"type_{i}" for i in range(20)], "parvi")

    data = [m for m in sink.collected if m is not EOS]
    assert len(data) == 1
    assert data[0].num_rows == 20
    assert node._promoted is True


def test_parvi_distinct_no_overflow_for_duplicates_after_full():
    values = [f"type_{i}" for i in range(16)]
    first = _from_arrow(pa.table({"type": values}))
    second = _from_arrow(pa.table({"type": values}))
    seen = ParviSetWrapper()

    overflow = distinct(first, seen, columns=[b"type"])
    assert overflow is False
    assert len(first) == 16
    assert seen.full()

    overflow = distinct(second, seen, columns=[b"type"])
    assert overflow is False
    assert len(second) == 0


def test_parvi_distinct_overflow_leaves_chunk_unchanged_for_replay():
    values = [f"type_{i}" for i in range(20)]
    morsel = _from_arrow(pa.table({"type": values}))
    seen = ParviSetWrapper()

    overflow = distinct(morsel, seen, columns=[b"type"])

    assert overflow is True
    assert len(morsel) == 20
