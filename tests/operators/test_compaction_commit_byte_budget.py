#!/usr/bin/env python
"""The compaction sink must split a batch on BYTES, not just rows.

This is the production failure that started the work: an OPTIMIZE pass over
wide string rows buffered 262144 rows — its only ceiling — and
`Morsel.combine` refused with `concat: total arena bytes exceed 4 GB`. No row
threshold can see payload width, so no value of it was ever safe.

The sink's real budget is 1 GiB, which no test can afford to reach, so the
batcher is swapped for an identically-constructed one with a small budget. The
sink code under test is unchanged: it pushes morsels in and writes one file per
batch the batcher hands back.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import draken.draken_native as dn
import pytest
from draken.morsels.morsel import MORSEL_MAX_ARENA_BYTES, Morsel, MorselBatcher

from opteryx.models import QueryProperties
from opteryx.operators._operators import CompactionCommitNode

ROW_BYTES = 4096
ROWS_PER_MORSEL = 32
BUDGET = 256 * 1024  # ~2 morsels' worth of arena


class _RecordingConnector:
    """Stands in for the store: records what the sink asked to write."""

    def __init__(self):
        self.written = []

    def write_morsel(self, relation_name, morsel):
        self.written.append(morsel)
        return f"file_{len(self.written)}.parquet"


def _wide_morsel(seed):
    values = [
        (b"%04d" % (seed * ROWS_PER_MORSEL + i)) * (ROW_BYTES // 4)
        for i in range(ROWS_PER_MORSEL)
    ]
    return Morsel.from_vectors([b"payload"], [dn.vector_from_string_sequence(values)])


def _sink(budget):
    node = CompactionCommitNode(
        properties=QueryProperties("test-query", {}),
        relation_name="test.relation",
        connector=_RecordingConnector(),
        retired_files=[],
        baseline_snapshot_id=None,
    )
    # Same construction the sink does, with a budget a test can reach.
    node._batcher = MorselBatcher(node.coalesce_rows, max_arena_bytes=budget)
    return node


def test_wide_rows_are_split_into_several_files():
    node = _sink(BUDGET)
    for seed in range(8):
        node._consume(_wide_morsel(seed))
    node._flush_pending()

    written = node.connector.written
    assert len(written) > 1, "a row-only budget would have written exactly one file"
    assert sum(m.num_rows for m in written) == 8 * ROWS_PER_MORSEL
    for morsel in written:
        payload = morsel.column(b"payload").to_pylist()
        assert sum(len(v.encode("utf-8")) for v in payload) <= BUDGET


def test_every_row_survives_the_split_intact():
    node = _sink(BUDGET)
    expected = []
    for seed in range(8):
        morsel = _wide_morsel(seed)
        expected.extend(morsel.column(b"payload").to_pylist())
        node._consume(morsel)
    node._flush_pending()

    got = [v for m in node.connector.written for v in m.column(b"payload").to_pylist()]
    assert got == expected


def test_sink_defaults_to_the_fixed_arena_ceiling():
    """The budget is a property of the uint32 arena offset, not configuration."""
    node = _sink(MORSEL_MAX_ARENA_BYTES)
    node._consume(_wide_morsel(0))
    node._flush_pending()
    assert len(node.connector.written) == 1  # nowhere near 1 GiB
    with pytest.raises(ValueError):
        MorselBatcher(1000, max_arena_bytes=MORSEL_MAX_ARENA_BYTES + 1)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
