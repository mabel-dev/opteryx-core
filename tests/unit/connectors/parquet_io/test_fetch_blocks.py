"""Fetch blocks: the native scan reads a grouped column-major file with one range
GET per projected column per BLOCK, not per row group.

docs/PARQUET_GROUPED_COLUMN_MAJOR_DESIGN.md: rugo writes 64k-row row groups in
256k-row blocks, each column's chunks byte-adjacent within a block, so that a
reader fetching a column over a block issues ONE range instead of G. The reader
side of that promise is pinned here on the production path — SQL through the
compiler's native scan plan into ParquetIOPipeline, over HTTP (dev/
throttle_server.py) so every byte is a counted range GET:

  * the pipeline infers the blocks from the chunk offsets (infer_fetch_blocks),
    submits the kept row groups of a block as one fetch, and its own telemetry
    (`io_http_request_count`) shows the request count falling to the block
    count for a one-column projection and to the block count for SELECT *;
  * a row-major file (blocks of one) still costs one GET per row group per
    column run — the pre-grouping behaviour, unchanged;
  * every row group is still its own result: the values, row count and
    row-group count are identical to the local read of the same file, with and
    without predicate pruning (a partially kept block is fetched as one).
"""

import os
import subprocess
import sys
import tempfile

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo import parquet as rp

SERVER = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../dev/throttle_server.py")
)
R = 2000        # rows per row group
N_RG = 8        # row groups -> 2 blocks of 4 at G = 4
N = R * N_RG


def _morsel():
    return Morsel.from_vectors(
        [b"a", b"s", b"f"],
        [vector_from_sequence(list(range(N)), DrakenType.INT64),
         vector_from_sequence([f"v{i % 97}" for i in range(N)], DrakenType.VARCHAR),
         vector_from_sequence([i * 0.5 for i in range(N)], DrakenType.FLOAT64)],
    )


def _write(root, name, block):
    d = os.path.join(root, name)
    os.makedirs(d)
    data = rp.write_parquet(_morsel(), max_rows_per_row_group=R, row_groups_per_block=block)
    with open(os.path.join(d, "part-0.parquet"), "wb") as f:
        f.write(data)
    assert rp.read_metadata(data).num_rows == N
    return d


def _server(root):
    proc = subprocess.Popen(
        [sys.executable, SERVER, "--root", root, "--port", "0", "--rtt-ms", "0",
         "--error-rate", "0", "--seed", "1"],
        stdout=subprocess.PIPE, text=True,
    )
    ready = proc.stdout.readline()
    if not ready.startswith("READY"):
        proc.kill()
        raise RuntimeError(f"throttle server failed to start: {ready!r}")
    return proc, int(ready.strip().split("port=")[1])


def _register_http_workspace(name, local_dir, port, subdir):
    """`name.<subdir>` -> the served copies of local_dir/<subdir>/*.parquet."""
    from opteryx.connectors import FileSystemConnector, register_workspace
    from opteryx.connectors.io_systems.http_filesystem import OpteryxHttpFileSystem

    files = sorted(f for f in os.listdir(os.path.join(local_dir, subdir))
                   if f.endswith(".parquet") and "manifest" not in f)
    urls = [f"http://127.0.0.1:{port}/{subdir}/{f}" for f in files]
    sizes = {u: os.path.getsize(os.path.join(local_dir, subdir, f)) for u, f in zip(urls, files)}

    class ServedDirFileSystem(OpteryxHttpFileSystem):
        # The native scan gate admits a remote path only when the filesystem can
        # sign it (an unsigned fetch would 401 against real object storage). The
        # served URLs need no credential, so "signing" is the identity — which is
        # exactly what routes the query onto the production native path instead
        # of the trampoline.
        signs_urls = True

        def rewrite_to_signed_url(self, path, expiry_seconds=3600):
            return path

        def list_files(self, base_dir, recursive=True):
            return list(urls)

        def get_file_size(self, path):
            return sizes[path]

        def get_file_info(self, paths):
            # The connector stats the data files and the (absent) dataset
            # manifest together; answer from the listing instead of HEADing a
            # relative manifest path the HTTP filesystem cannot resolve.
            from opteryx.connectors.io_systems.http_filesystem import FileInfo, FileType

            return [FileInfo(path=p, type=FileType.File, size=sizes[p]) if p in sizes
                    else FileInfo(path=p, type=FileType.NotFound) for p in paths]

    register_workspace(name, FileSystemConnector, filesystem=ServedDirFileSystem(),
                       storage_type="HTTP")


def _run(sql):
    import opteryx

    session = opteryx.session()
    rows = []
    for m in session.execute_to_morsels(sql):
        rows.extend(zip(*[m.column(n).to_pylist() for n in m.column_names]))
    diags = session.telemetry.get("io_scan_diagnostics") or []
    gets = sum(int(d.get("http_request_count", 0)) for d in diags)
    ops = sum(int(d.get("http_fetch_ops", 0)) for d in diags)
    # The production path, not the trampoline: the block grouping under test
    # lives in NativeParquetScanSource's submission loop.
    sources = set((session.telemetry.get("scan_sources") or {}).values())
    assert sources == {"NativeParquetScanSource"}, sources
    session.close()
    return rows, gets, ops


@pytest.fixture(scope="module")
def served():
    with tempfile.TemporaryDirectory() as tmp:
        grouped = _write(tmp, "grouped", block=4)
        rowmajor = _write(tmp, "rowmajor", block=1)
        proc, port = _server(tmp)
        try:
            _register_http_workspace("fb_grouped", tmp, port, "grouped")
            _register_http_workspace("fb_rowmajor", tmp, port, "rowmajor")
            yield {"grouped": grouped, "rowmajor": rowmajor, "port": port}
        finally:
            proc.kill()
            proc.wait()


def _served_url(served, subdir):
    return f"http://127.0.0.1:{served['port']}/{subdir}/part-0.parquet"


def _local(sql_template, served, arm):
    rows, gets, _ = _run(sql_template.format(rel=f"'{served[arm]}'"))
    assert gets == 0, "a local scan issues no range GETs"
    return rows


@pytest.mark.parametrize("sql,grouped_gets,rowmajor_gets", [
    # one projected column: one GET per block vs one per row group
    ("SELECT a FROM {rel}", 2, N_RG),
    # every column: the block is one contiguous run, so still one GET per block;
    # row-major: one contiguous run per row group
    ("SELECT * FROM {rel}", 2, N_RG),
    # two non-adjacent columns (f skipped in between is ~as large as a, so the
    # 10% waste rule keeps them apart): one GET per column per block vs per row group
    ("SELECT a, f FROM {rel}", 2 * 2, N_RG * 2),
])
def test_grouped_file_costs_one_get_per_column_per_block(served, sql, grouped_gets, rowmajor_gets):
    expected = sorted(_local(sql, served, "grouped"))
    assert sorted(_local(sql, served, "rowmajor")) == expected
    rows_g, gets_g, ops_g = _run(sql.format(rel="fb_grouped.grouped"))
    rows_r, gets_r, ops_r = _run(sql.format(rel="fb_rowmajor.rowmajor"))
    assert sorted(rows_g) == expected
    assert sorted(rows_r) == expected
    assert gets_g == grouped_gets, (gets_g, ops_g)
    assert gets_r == rowmajor_gets, (gets_r, ops_r)
    # One fetch operation per block: the block IS the fetch unit.
    assert ops_g == 2
    assert ops_r == N_RG


def test_pruned_block_is_fetched_partially_as_one(served):
    # a = 9000 lives in row group 4 (rows 8000..9999), the first of block 2:
    # min/max pruning keeps ONE row group, fetched as a one-member block.
    sql = "SELECT a, s FROM {rel} WHERE a = 9000"
    expected = _local(sql, served, "grouped")
    assert expected == [(9000, f"v{9000 % 97}")]
    rows, gets, ops = _run(sql.format(rel="fb_grouped.grouped"))
    assert rows == expected
    assert ops == 1
    # a and s are adjacent in a one-member block only through the block's other
    # members' chunks (3 x a between rg4.a and rg4.s), so two ranges.
    assert gets == 2


def test_range_predicate_keeps_a_whole_block_as_one_fetch(served):
    # rows 8000..15999 = block 2 exactly; pruning keeps its four row groups.
    sql = "SELECT a FROM {rel} WHERE a >= 8000"
    expected = sorted(_local(sql, served, "grouped"))
    assert len(expected) == 4 * R
    rows, gets, ops = _run(sql.format(rel="fb_grouped.grouped"))
    assert sorted(rows) == expected
    assert (ops, gets) == (1, 1)


def _run_source(sql):
    """Like _run, but returns the scan source name instead of asserting it."""
    import opteryx

    session = opteryx.session()
    rows = []
    for m in session.execute_to_morsels(sql):
        rows.extend(zip(*[m.column(n).to_pylist() for n in m.column_names]))
    diags = session.telemetry.get("io_scan_diagnostics") or []
    gets = sum(int(d.get("http_request_count", 0)) for d in diags)
    sources = set((session.telemetry.get("scan_sources") or {}).values())
    session.close()
    return rows, gets, sources


def test_late_materialization_pass2_masks_ride_blocks(served):
    """The Q24 shape — SELECT * WHERE <pushed> ORDER BY <col> LIMIT n — runs the
    two-pass late-materialization source: pass 1 reads the predicate/sort
    columns, pass 2 re-reads the surviving row groups with a per-row mask. Both
    passes submit fetch blocks (pass 2 with masks per member), so the grouped
    file answers identically to the local read, from the native latmat source,
    with fewer GETs than the row-major one."""
    sql = "SELECT * FROM {rel} WHERE s = 'v5' ORDER BY a LIMIT 10"
    expected, gets_local, _ = _run_source(sql.format(rel=f"'{served['grouped']}'"))
    assert gets_local == 0
    assert len(expected) == 10 and all(r[1] == "v5" for r in expected)
    rows_g, gets_g, src_g = _run_source(sql.format(rel="fb_grouped.grouped"))
    rows_r, gets_r, src_r = _run_source(sql.format(rel="fb_rowmajor.rowmajor"))
    assert rows_g == expected
    assert rows_r == expected
    assert src_g == {"LatmatScanSource"}, src_g
    assert src_r == {"LatmatScanSource"}, src_r
    assert gets_g < gets_r, (gets_g, gets_r)


def test_fetch_ahead_gate_counts_blocks_not_row_groups(served):
    """[D-1, ruled 2026-09-24] `parquet_io_fetch_ahead_min_blocks` is measured
    against REMOTE FETCH BLOCKS: the grouped file has 8 row groups in 2 blocks,
    so a minimum of 3 leaves the depth off (read back 0) where the row-major
    copy — 8 one-row-group blocks — arms it; a minimum of 2 arms both."""
    from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan

    def depth(subdir, gate):
        url = _served_url(served, subdir)
        plan = open_native_scan_plan([url], ["a"], decode_workers=2, fetch_ahead=6,
                                     fetch_ahead_min_blocks=gate)
        try:
            assert plan.row_group_count == N_RG
            return plan.diagnostics()["fetch_ahead_depth"]
        finally:
            plan.close()

    assert depth("grouped", 3) == 0
    assert depth("rowmajor", 3) == 6
    assert depth("grouped", 2) == 6
    assert depth("grouped", 0) == 6


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
