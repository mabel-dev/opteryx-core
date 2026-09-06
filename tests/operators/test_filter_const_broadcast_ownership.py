#!/usr/bin/env python
"""A filtered morsel must own its data for as long as the caller holds it.

`cxx_mask_with_consts` broadcasts a column the predicate proved constant
(`WHERE name = 'Earth'` ⇒ every surviving row's `name` is 'Earth') in O(1)
instead of gathering it. That column used to POINT at the predicate program's
literal buffer, which does not outlive the kernel call — so reading a filtered
morsel after the stream advanced was a use-after-free, and it segfaulted.

It went unseen because the cursor sliced every morsel before buffering it, and
slicing copies. The moment the cursor started buffering by reference, every
equality-filtered query crashed.

The equality predicate is what arms the broadcast; the inequality cases are the
control — they gather, and always survived.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

# Two morsels out of a UNION ALL, so there IS a next pull to invalidate the first.
BASE = (
    "SELECT * FROM (SELECT name, id FROM $planets AS A "
    "UNION ALL SELECT name, id FROM $planets AS B) AS C WHERE "
)


@pytest.mark.parametrize(
    "predicate",
    [
        "name = 'Earth'",  # string const-broadcast — the crash
        "id = 3",  # numeric const-broadcast on the OTHER column
        "id > 3",  # control: gathered, never borrowed
    ],
)
def test_filtered_morsel_survives_the_next_pull(predicate):
    session = opteryx.session()
    # max_size=1 keeps the cursor from coalescing, so each morsel is handed over
    # as the engine produced it and the test holds the engine's own buffers.
    held = []
    read_when_produced = []
    for morsel in session.execute_to_morsels(BASE + predicate, max_size=1):
        read_when_produced.append(
            (morsel.column(b"name").to_pylist(), morsel.column(b"id").to_pylist())
        )
        held.append(morsel)

    assert len(held) > 1, "need more than one morsel for the lifetime to matter"
    # The whole point: read them again now the stream has advanced past every one.
    read_after = [
        (m.column(b"name").to_pylist(), m.column(b"id").to_pylist()) for m in held
    ]
    assert read_after == read_when_produced


def test_long_string_const_broadcast_survives():
    """> 12 bytes takes the arena path, not the inline-slot path."""
    session = opteryx.session()
    sql = (
        "SELECT * FROM (SELECT name, id FROM $planets AS A "
        "UNION ALL SELECT name, id FROM $planets AS B) AS C "
        "WHERE name || '_padding_beyond_inline' = 'Earth_padding_beyond_inline'"
    )
    held = list(session.execute_to_morsels(sql, max_size=1))
    for morsel in held:
        assert morsel.column(b"name").to_pylist() == ["Earth"]


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
