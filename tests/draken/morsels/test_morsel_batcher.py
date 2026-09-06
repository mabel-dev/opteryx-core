#!/usr/bin/env python
"""MorselBatcher — the single place morsel batching policy lives.

Two axes are under test: rows (no emitted batch exceeds max_rows, oversized
input is split) and bytes (no COMBINED batch exceeds the projected string-arena
budget in any one column). The byte axis is what was missing from all five
hand-rolled copies of this loop, and its absence is what let an OPTIMIZE pass
pile 262144 wide rows into one concat and hit `total arena bytes exceed 4 GB`.

Fixtures are built natively — no Arrow on the construction path.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import pytest

import draken.draken_native as dn
from draken.morsels.morsel import MORSEL_MAX_ARENA_BYTES, Morsel, MorselBatcher


def _ints(n, start=0):
    return Morsel.from_vectors([b"a"], [dn.vector_from_sequence(list(range(start, start + n)))])


def _strings(n, width, prefix=b"x"):
    """n rows of `width`-byte strings. width > 12 forces arena payload."""
    return Morsel.from_vectors(
        [b"s"], [dn.vector_from_string_sequence([prefix + bytes([i % 251]) * (width - 1) for i in range(n)])]
    )


def _drain(batcher, morsels):
    out = []
    for m in morsels:
        out.extend(batcher.push(m))
    out.extend(batcher.finish())
    return out


def test_coalesces_small_morsels_into_one():
    out = _drain(MorselBatcher(1000), [_ints(10, i * 10) for i in range(10)])
    assert len(out) == 1
    assert out[0].num_rows == 100
    assert out[0].column(b"a").to_pylist() == list(range(100))


def test_single_morsel_passes_through_untouched():
    m = _ints(10)
    out = _drain(MorselBatcher(1000), [m])
    assert len(out) == 1
    assert out[0] is m  # Morsel.combine short-circuits a one-element list


def test_row_budget_packs_tight_and_never_overflows():
    out = _drain(MorselBatcher(30), [_ints(7, i * 7) for i in range(10)])
    assert [m.num_rows for m in out] == [30, 30, 10]
    assert sum(m.num_rows for m in out) == 70
    assert [v for m in out for v in m.column(b"a").to_pylist()] == list(range(70))


def test_oversized_morsel_is_split_on_the_row_axis():
    """The sinks did NOT do this: they checked the threshold before appending,
    so one oversized morsel went through whole and produced a multi-row-group
    file with no bounds."""
    out = _drain(MorselBatcher(100), [_ints(250)])
    assert [m.num_rows for m in out] == [100, 100, 50]
    assert [v for m in out for v in m.column(b"a").to_pylist()] == list(range(250))


def test_zero_row_morsels_are_dropped():
    batcher = MorselBatcher(100)
    assert batcher.push(_ints(0)) == []
    assert batcher.push(None) == []
    assert batcher.finish() == []


def test_byte_budget_flushes_before_the_arena_limit():
    """1000 rows x 1KB = ~1MB of arena; a 300KB budget must split it."""
    batcher = MorselBatcher(1_000_000, max_arena_bytes=300_000)
    out = _drain(batcher, [_strings(100, 1024) for _ in range(10)])
    assert len(out) > 1
    assert sum(m.num_rows for m in out) == 1000
    for m in out:
        assert _projected(m) <= 300_000


def test_lone_oversized_morsel_is_emitted_not_split_on_bytes():
    """A single morsel's arena already exists with uint32 offsets, so it is
    under 4GB by construction and combine never concats it. Passing it through
    is safe; splitting it would be pointless work."""
    big = _strings(100, 1024)  # ~100KB
    out = _drain(MorselBatcher(1_000_000, max_arena_bytes=1024), [big])
    assert len(out) == 1
    assert out[0] is big


def test_dict_column_is_charged_what_concat_copies_not_what_it_owns():
    """THE test that fails if the budget is taken from Morsel.nbytes.

    concat materializes every LOGICAL row, so a dict column is copied at its
    row count; nbytes reports OWNED payload, sized by its UNIQUE count. Here
    that is a ~100x under-count, and dict-encoded strings are the parquet
    scan's default output — a budget built on owned bytes admits a batch that
    concat then refuses with `total arena bytes exceed 4 GB`.
    """
    values = [bytes([65 + i % 26]) * 1024 for i in range(50)]
    dict_morsel = Morsel.from_vectors(
        [b"s"], [dn.vector_from_string_dict_sequence([values[i % 50] for i in range(5000)])]
    )
    assert dict_morsel.nbytes < 100_000  # owned: 50 unique values
    concat_cost = 5000 * 1024  # ~5MB: what concat actually copies

    # A budget between the two numbers must split. If the batcher believed
    # nbytes, all four would land in one batch and the guarantee would be a lie.
    batcher = MorselBatcher(1_000_000, max_arena_bytes=2 * concat_cost)
    out = _drain(batcher, [dict_morsel] * 4)
    assert len(out) == 2
    assert [m.num_rows for m in out] == [10000, 10000]


def test_both_axes_together():
    batcher = MorselBatcher(150, max_arena_bytes=200_000)
    out = _drain(batcher, [_strings(100, 1024) for _ in range(6)])
    assert sum(m.num_rows for m in out) == 600
    for m in out:
        assert m.num_rows <= 150
        assert _projected(m) <= 200_000


def test_defrag_one_shot():
    out = MorselBatcher.defrag([_ints(10, i * 10) for i in range(5)], 20)
    assert [m.num_rows for m in out] == [20, 20, 10]


def test_rejects_a_budget_above_the_arena_ceiling():
    with pytest.raises(ValueError):
        MorselBatcher(100, max_arena_bytes=MORSEL_MAX_ARENA_BYTES + 1)
    with pytest.raises(ValueError):
        MorselBatcher(0)


def test_schema_mismatch_still_raises_from_combine():
    batcher = MorselBatcher(1000)
    batcher.push(_ints(5))
    with pytest.raises(ValueError):
        batcher.push(_strings(5, 20))
        batcher.finish()


def _projected(morsel):
    """Sum of long-form payload of the string column — what concat would copy."""
    return sum(len(v.encode("utf-8")) for v in morsel.column(b"s").to_pylist() if len(v) > 12)


if __name__ == "__main__":  # pragma: no cover
    import pytest as _p

    _p.main([__file__, "-v"])
