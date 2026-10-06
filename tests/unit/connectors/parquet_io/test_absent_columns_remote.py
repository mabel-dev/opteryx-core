"""Schema evolution over a REMOTE (HTTP) native scan.

A file written before a column was added holds fewer columns than the dataset. The
scan reads it as NULL for the missing ones, and — over a remote store, where every
byte is a counted range GET — must not pay for them: the old file is fetched in the
same blocks it would be had the column never been asked for (a column with no chunk
has nothing to be adjacent, and used to split every block into row-group fetches).

The server, the signable-HTTP filesystem shim and the grouped file layout are
test_fetch_blocks.py's, imported rather than copied.
"""

import os
import sys
import tempfile

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo import parquet as rp

from tests.unit.connectors.parquet_io.test_fetch_blocks import N
from tests.unit.connectors.parquet_io.test_fetch_blocks import R
from tests.unit.connectors.parquet_io.test_fetch_blocks import _register_http_workspace
from tests.unit.connectors.parquet_io.test_fetch_blocks import _run
from tests.unit.connectors.parquet_io.test_fetch_blocks import _server
from tests.unit.connectors.parquet_io.test_fetch_blocks import _write

BLOCKS_PER_FILE = 2   # N_RG row groups at 4 per block


@pytest.fixture(scope="module")
def evolved():
    """`part-0.parquet` holds a, s, f and sorts first, so it names the dataset's schema;
    `z_old.parquet` holds only `a` — both grouped, both N rows."""
    with tempfile.TemporaryDirectory() as tmp:
        _write(tmp, "evolved", block=4)   # part-0.parquet: a, s, f
        old = rp.write_parquet(
            Morsel.from_vectors([b"a"], [vector_from_sequence(list(range(N)), DrakenType.INT64)]),
            max_rows_per_row_group=R, row_groups_per_block=4)
        with open(os.path.join(tmp, "evolved", "z_old.parquet"), "wb") as f:
            f.write(old)
        proc, port = _server(tmp)
        try:
            _register_http_workspace("abs_remote", tmp, port, "evolved")
            yield
        finally:
            proc.kill()
            proc.wait()


def test_the_old_file_reads_null_for_a_column_it_lacks(evolved):
    rows, _, _ = _run("SELECT COUNT(*) AS n, COUNT(s) AS s FROM abs_remote.evolved")
    assert rows == [(2 * N, N)]


def test_values_of_the_new_file_are_intact(evolved):
    rows, _, _ = _run("SELECT a, s FROM abs_remote.evolved WHERE a = 9000 ORDER BY s NULLS LAST")
    assert rows == [(9000, "v%d" % (9000 % 97)), (9000, None)]


def test_a_file_lacking_every_read_column_costs_no_fetch(evolved):
    """Only `s` is read: the old file holds none of it, so it is answered from its
    footer — no range GET — and only the new file is fetched, one op per block."""
    rows, gets, ops = _run("SELECT s FROM abs_remote.evolved")
    assert len(rows) == 2 * N
    assert sum(1 for r in rows if r[0] is None) == N
    assert ops == BLOCKS_PER_FILE, (gets, ops)


def test_the_old_file_is_fetched_in_its_own_blocks(evolved):
    """Reading `a` and the absent `s`: a one-column file's `a` chunks are byte-adjacent
    across ALL its row groups, so the old file is ONE fetch block — not one per row
    group because `s` has no chunk to be adjacent (that was N_RG ops)."""
    rows, gets, ops = _run("SELECT a, s FROM abs_remote.evolved")
    assert len(rows) == 2 * N
    assert ops == BLOCKS_PER_FILE + 1, (gets, ops)   # the new file's blocks + the old file's one
