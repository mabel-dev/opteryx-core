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


# Parvi is 64 slots as 4 group-selected groups of 16: overflow fires when a
# key's GROUP is full, not at 64 keys. 16 distinct values sit comfortably
# under the effective capacity (p5 = 40), so they never overflow; 200 distinct
# values always exceed the 64-slot ceiling and must overflow.


def test_parvi_promotion_preserves_seen_keys_across_morsels():
    values = [f"type_{i}" for i in range(16)]
    first = _from_arrow(pa.table({"type": values}))
    second = _from_arrow(pa.table({"type": values}))

    seen = ParviSetWrapper()

    overflow = distinct(first, seen, columns=[b"type"])
    assert overflow is False
    assert len(first) == 16
    assert seen.size() == 16

    # Early promotion is always legal — drain and continue on carchar.
    promoted = CarcharSetWrapper()
    seen.drain_into_carchar(promoted)
    seen = promoted

    distinct(second, seen, columns=[b"type"])
    assert len(second) == 0


def test_parvi_distinct_duplicates_never_overflow():
    values = [f"type_{i}" for i in range(16)]
    first = _from_arrow(pa.table({"type": values}))
    second = _from_arrow(pa.table({"type": values}))
    seen = ParviSetWrapper()

    overflow = distinct(first, seen, columns=[b"type"])
    assert overflow is False
    assert len(first) == 16
    assert seen.size() == 16

    overflow = distinct(second, seen, columns=[b"type"])
    assert overflow is False
    assert len(second) == 0


def test_parvi_distinct_overflow_leaves_chunk_unchanged_for_replay():
    # 200 distinct values cannot fit in 64 slots — overflow is guaranteed and
    # the morsel must be returned completely untouched for the replay.
    values = [f"type_{i}" for i in range(200)]
    morsel = _from_arrow(pa.table({"type": values}))
    seen = ParviSetWrapper()

    overflow = distinct(morsel, seen, columns=[b"type"])

    assert overflow is True
    assert len(morsel) == 200


def test_parvi_overflow_replay_needs_a_set_without_this_batch():
    # The overflow contract: the morsel is returned untouched, but the parvi
    # set HAS been mutated with a prefix of this morsel's keys — and those
    # rows were never emitted. Draining parvi into carchar and replaying the
    # SAME morsel therefore suppresses the prefix values' first occurrences
    # (data loss). The sound recoveries are (a) keep the parvi pass's partial
    # indices and continue on the drained carchar (what the native
    # DistinctSink does), or (b) replay against a set that excludes this
    # batch's inserts — a fresh set in single-shot use (what draken
    # Vector.unique does). This test pins down (b).
    values = [f"type_{i % 100}" for i in range(300)]  # 100 distinct, with dups
    morsel = _from_arrow(pa.table({"type": values}))
    seen = ParviSetWrapper()

    overflow = distinct(morsel, seen, columns=[b"type"])
    assert overflow is True
    assert len(morsel) == 300  # untouched

    fresh = CarcharSetWrapper()
    overflow = distinct(morsel, fresh, columns=[b"type"])
    assert overflow is False
    assert len(morsel) == 100  # one row per distinct value
