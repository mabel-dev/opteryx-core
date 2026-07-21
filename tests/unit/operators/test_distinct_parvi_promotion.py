import pyarrow as pa

import draken.draken_native as dn
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.compiled.morsel_ops.distinct import distinct
from opteryx.compiled.structures.carchar_set import CarcharSetWrapper
from opteryx.compiled.structures.parvi_set import ParviSetWrapper

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
