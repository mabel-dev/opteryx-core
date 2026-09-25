#!/usr/bin/env python
"""Writing sinks stream ROW GROUPS into target-sized data files - never a file
per batch - and leave nothing behind on failure.

DataFileStream is the one place every writing sink (INSERT / CTAS, MERGE,
OPTIMIZE) turns morsels into files. Before it, each sink handed every
262,144-row batch to `write_morsel`, which writes one file per call: INSERT and
CTAS manufactured 262,144-row files, and OPTIMIZE rewrote them into the same
shape nightly ("Compaction: 43 files -> 43 files", in production). This file
pins the shape that replaced it, through the compaction sink, which adds the
retirement and refused-commit rules on top:

- each batch the batcher hands back is one `write_row_group` on the OPEN file;
- the batcher still splits on BYTES (the production failure that created it:
  wide string rows filled 262144 rows into one `Morsel.combine`, which refused
  with `total arena bytes exceed 4 GB`);
- the file is closed and a new one opened once its uncompressed size crosses
  the target, and the last one is closed at EOS;
- a failure mid-stream aborts the open file and removes the closed ones.

The stream's real budgets (1 GiB arena, 4 GB target, 262,144 rows) are far
beyond what a test can afford, so the batcher and the target are swapped for
small ones. The code under test is unchanged.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import draken.draken_native as dn
import pytest
from draken.morsels.morsel import MORSEL_MAX_ARENA_BYTES, Morsel, MorselBatcher

from opteryx.compiled.structures.plan_steps import CompactionCommitStep
from opteryx.models import QueryProperties
from opteryx.models.file_entry import FileEntry
from opteryx.operators._operators import CompactionCommitNode, DataFileStream

ROW_BYTES = 4096
ROWS_PER_MORSEL = 32
BUDGET = 256 * 1024  # ~2 morsels' worth of arena


class _RecordingWriter:
    """One open data file: records the row groups it was handed."""

    def __init__(self, connector, path, sorted_by, write_profile):
        self.connector = connector
        self.file_path = path
        self.sorted_by = sorted_by
        self.write_profile = write_profile
        self.row_groups = []
        self.uncompressed_size_in_bytes = 0
        self.closed = False
        self.aborted = False

    def write_row_group(self, morsel):
        if self.connector.fail_on_row_group is not None and (
            len(self.row_groups) + 1 == self.connector.fail_on_row_group
        ):
            raise RuntimeError("storage said no")
        self.row_groups.append(morsel)
        self.uncompressed_size_in_bytes += morsel.nbytes

    def close(self):
        self.closed = True
        self.connector.closed.append(self)
        return FileEntry(
            file_path=self.file_path,
            file_format="PARQUET",
            record_count=sum(m.num_rows for m in self.row_groups),
            file_size_in_bytes=self.uncompressed_size_in_bytes,
            uncompressed_size_in_bytes=self.uncompressed_size_in_bytes,
            row_group_count=len(self.row_groups),
            catalog_entry={"file_path": self.file_path},
        )

    def abort(self):
        self.aborted = True
        self.connector.aborted.append(self)


class _RecordingConnector:
    """Stands in for the store: records what the sink asked to write."""

    def __init__(self):
        self.opened = []
        self.closed = []
        self.aborted = []
        self.deleted = []
        self.commits = []
        self.fail_on_row_group = None

    def open_data_file_writer(
        self,
        relation_name,
        sorted_by=None,
        sorted_descending=False,
        write_profile="fast",
        pending_schema=None,
    ):
        writer = _RecordingWriter(
            self, f"file_{len(self.opened) + 1}.parquet", sorted_by, write_profile
        )
        self.opened.append(writer)
        return writer

    def delete_data_file(self, relation_name, file_path):
        self.deleted.append(file_path)

    def compaction_commit(self, relation_name, file_entries, retired_files, **kwargs):
        self.commits.append((list(file_entries), list(retired_files)))


def _wide_morsel(seed):
    values = [
        (b"%04d" % (seed * ROWS_PER_MORSEL + i)) * (ROW_BYTES // 4)
        for i in range(ROWS_PER_MORSEL)
    ]
    return Morsel.from_vectors([b"payload"], [dn.vector_from_string_sequence(values)])


def _sink(budget, target=None, retired=("old_1.parquet",), sorted_by=None, rows=None):
    node = CompactionCommitNode(
        QueryProperties("test-query", {}),
        CompactionCommitStep(
            relation_name="test.relation",
            connector=_RecordingConnector(),
            retired_files=list(retired),
            sorted_by=sorted_by,
        ),
        target_file_bytes=target,
    )
    # Same construction the stream does, with a budget a test can reach.
    # `rows` shrinks the row ceiling so a test morsel is a whole batch (row
    # group) on its own; the real 262,144 would coalesce every test morsel
    # into one.
    stream = node._stream
    stream._batcher = MorselBatcher(rows or stream.coalesce_rows, max_arena_bytes=budget)
    return node


def _eos(node):
    from opteryx import EOS

    node._push_impl(EOS)


def _all_rows(connector):
    return [v for w in connector.closed for m in w.row_groups for v in m.column(b"payload").to_pylist()]


def test_wide_rows_are_split_into_several_row_groups_of_one_file():
    node = _sink(BUDGET)
    for seed in range(8):
        node._stream.push(_wide_morsel(seed))
    node._stream.finish()

    connector = node.connector
    assert len(connector.opened) == 1, "one file, however many batches"
    (writer,) = connector.closed
    assert len(writer.row_groups) > 1, "a row-only budget would have written one row group"
    assert sum(m.num_rows for m in writer.row_groups) == 8 * ROWS_PER_MORSEL
    for morsel in writer.row_groups:
        payload = morsel.column(b"payload").to_pylist()
        assert sum(len(v.encode("utf-8")) for v in payload) <= BUDGET


def test_every_row_survives_the_split_intact():
    node = _sink(BUDGET)
    expected = []
    for seed in range(8):
        morsel = _wide_morsel(seed)
        expected.extend(morsel.column(b"payload").to_pylist())
        node._stream.push(morsel)
    node._stream.finish()

    assert _all_rows(node.connector) == expected


def test_files_roll_at_the_target_and_the_last_closes_at_eos():
    one = _wide_morsel(0).nbytes
    # Roll once a file holds two morsels' worth; ten morsels -> five files.
    node = _sink(MORSEL_MAX_ARENA_BYTES, target=2 * one, rows=ROWS_PER_MORSEL)
    expected = []
    for seed in range(10):
        morsel = _wide_morsel(seed)
        expected.extend(morsel.column(b"payload").to_pylist())
        node._push_impl(morsel)
    _eos(node)

    connector = node.connector
    assert len(connector.closed) == 5
    assert all(w.closed for w in connector.opened)
    assert all(w.uncompressed_size_in_bytes >= 2 * one for w in connector.closed)
    assert _all_rows(connector) == expected
    assert len(connector.commits) == 1
    entries, retired = connector.commits[0]
    assert [e.file_path for e in entries] == [w.file_path for w in connector.closed]
    assert retired == ["old_1.parquet"]
    assert node.result.record_count == 5


def test_compaction_writes_with_the_storage_profile_and_its_sort_claim():
    node = _sink(
        MORSEL_MAX_ARENA_BYTES, target=_wide_morsel(0).nbytes, sorted_by="payload", rows=ROWS_PER_MORSEL
    )
    for seed in range(3):
        node._push_impl(_wide_morsel(seed))
    _eos(node)
    assert [w.sorted_by for w in node.connector.opened] == ["payload"] * 3
    assert [w.write_profile for w in node.connector.opened] == ["storage"] * 3


def test_a_plain_stream_makes_no_sort_claim_and_writes_fast():
    """INSERT / CTAS / MERGE construct the stream with its defaults."""
    connector = _RecordingConnector()
    stream = DataFileStream(connector, "test.relation", target_file_bytes=_wide_morsel(0).nbytes)
    stream._batcher = MorselBatcher(ROWS_PER_MORSEL)
    for seed in range(2):
        stream.push(_wide_morsel(seed))
    entries = stream.finish()
    assert len(entries) == 2
    assert [w.sorted_by for w in connector.opened] == [None, None]
    assert [w.write_profile for w in connector.opened] == ["fast", "fast"]


def test_a_failure_mid_stream_aborts_the_open_file_and_removes_closed_ones():
    one = _wide_morsel(0).nbytes
    node = _sink(MORSEL_MAX_ARENA_BYTES, target=2 * one, rows=ROWS_PER_MORSEL)
    node.connector.fail_on_row_group = 2  # second row group of whichever file is open

    # File 1 takes two row groups... the second raises.
    node._push_impl(_wide_morsel(0))
    with pytest.raises(RuntimeError, match="storage said no"):
        node._push_impl(_wide_morsel(1))

    connector = node.connector
    assert connector.aborted == [connector.opened[0]]
    assert connector.closed == []
    assert node._stream._writer is None
    assert node._stream.entries == []
    assert connector.commits == []


def test_a_failure_after_a_file_closed_removes_that_file_too():
    one = _wide_morsel(0).nbytes
    node = _sink(MORSEL_MAX_ARENA_BYTES, target=one, rows=ROWS_PER_MORSEL)  # one morsel per file
    node._push_impl(_wide_morsel(0))  # file 1 closes
    assert len(node.connector.closed) == 1
    node.connector.fail_on_row_group = 1
    with pytest.raises(RuntimeError, match="storage said no"):
        node._push_impl(_wide_morsel(1))

    assert node.connector.deleted == ["file_1.parquet"]
    assert node.connector.aborted == [node.connector.opened[1]]


def test_no_rows_opens_no_file_and_commits_nothing():
    node = _sink(MORSEL_MAX_ARENA_BYTES, retired=())
    _eos(node)
    assert node.connector.opened == []
    assert node.connector.commits == []
    assert node.result.record_count == 0


def test_rows_with_nothing_retired_is_refused_not_duplicated():
    node = _sink(MORSEL_MAX_ARENA_BYTES, retired=())
    node._push_impl(_wide_morsel(0))
    with pytest.raises(RuntimeError, match="retires none"):
        _eos(node)
    assert node.connector.deleted == ["file_1.parquet"]
    assert node.connector.commits == []


def test_a_refused_commit_removes_the_outputs_and_raises():
    node = _sink(MORSEL_MAX_ARENA_BYTES, target=_wide_morsel(0).nbytes, rows=ROWS_PER_MORSEL)

    def refuse(*args, **kwargs):
        raise RuntimeError("row count changed")

    node.connector.compaction_commit = refuse
    node._push_impl(_wide_morsel(0))
    node._push_impl(_wide_morsel(1))
    with pytest.raises(RuntimeError, match="row count changed"):
        _eos(node)
    assert node.connector.deleted == ["file_1.parquet", "file_2.parquet"]


def test_stream_defaults_to_the_fixed_arena_ceiling_and_the_selection_target():
    """The budget is a property of the uint32 arena offset, not configuration;
    the target is the constant selection measures files against; the row
    ceiling is one parquet row group (rugo's DEFAULT_ROWS_PER_ROW_GROUP, the
    engine's measured best morsel size)."""
    from opteryx.planner.compaction.constants import TARGET_SIZE_BYTES
    from rugo.parquet import DEFAULT_ROWS_PER_ROW_GROUP

    node = _sink(MORSEL_MAX_ARENA_BYTES)
    assert node._stream.target_file_bytes == TARGET_SIZE_BYTES
    assert node._stream.coalesce_rows == DEFAULT_ROWS_PER_ROW_GROUP == 65536
    node._stream.push(_wide_morsel(0))
    node._stream.finish()
    assert len(node.connector.closed) == 1  # nowhere near 1 GiB or 4 GB
    with pytest.raises(ValueError):
        MorselBatcher(1000, max_arena_bytes=MORSEL_MAX_ARENA_BYTES + 1)
    with pytest.raises(ValueError, match="positive"):
        _sink(MORSEL_MAX_ARENA_BYTES, target=0)
    assert DataFileStream(_RecordingConnector(), "r", coalesce_rows=10**9).coalesce_rows == 65536


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
