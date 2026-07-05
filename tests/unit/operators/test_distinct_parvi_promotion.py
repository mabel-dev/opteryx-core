import pyarrow as pa

from opteryx import EOS
from draken.morsels.morsel import Morsel
from opteryx.compiled.morsel_ops.distinct import distinct
from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
from opteryx.compiled.structures.parvi_set import ParviSetWrapper
from opteryx.models.query_properties import QueryProperties
from opteryx.operators._operators import DistinctNode


def test_parvi_promotion_preserves_seen_keys_across_morsels():
    values = [f"type_{i}" for i in range(16)]
    first = Morsel.from_arrow(pa.table({"type": values}))
    second = Morsel.from_arrow(pa.table({"type": values}))

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
    values = [f"type_{i}" for i in range(16)]
    morsel = Morsel.from_arrow(pa.table({"type": values}))
    node = DistinctNode(QueryProperties(query_id="q", variables={}), on=None, set_variant="parvi")

    out = list(node.execute(morsel))
    assert len(out) == 1
    assert len(out[0]) == 16
    assert node._promoted is False

    eos_out = list(node.execute(EOS))
    assert eos_out == []
    assert node._promoted is False


def test_distinct_node_promotes_on_overflow_and_keeps_same_morsel_uniques():
    values = [f"type_{i}" for i in range(20)]
    morsel = Morsel.from_arrow(pa.table({"type": values}))
    node = DistinctNode(QueryProperties(query_id="q2", variables={}), on=None, set_variant="parvi")

    out = list(node.execute(morsel))
    assert len(out) == 1
    assert len(out[0]) == 20
    assert node._promoted is True


def test_parvi_distinct_no_overflow_for_duplicates_after_full():
    values = [f"type_{i}" for i in range(16)]
    first = Morsel.from_arrow(pa.table({"type": values}))
    second = Morsel.from_arrow(pa.table({"type": values}))
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
    morsel = Morsel.from_arrow(pa.table({"type": values}))
    seen = ParviSetWrapper()

    overflow = distinct(morsel, seen, columns=[b"type"])

    assert overflow is True
    assert len(morsel) == 20
