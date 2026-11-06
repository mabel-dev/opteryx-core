"""Fetch blocks over a PER-COLUMN block-size file (docs/PARQUET_WRITE_FOR_READ_SPEED.md
§4.5 items 3-4): the reader needs no change, and the fetch-ahead gate counts a
long run as the work it carries.

rugo's `narrow_chunk_bytes` places narrow columns as one byte-adjacent run per
column after the wide columns' blocks of four. `infer_fetch_blocks` joins row
groups into a fetch block only while EVERY projected column is byte-adjacent, so:

  * a narrow-only projection reads the whole run as ONE fetch block — one fetch
    op, one GET for 8 row groups where the grouped file costs one per block;
  * a projection that includes a wide column falls back to blocks of four, the
    narrow columns' chunks fetched from their run as extra ranges;
  * values, row counts and pruning are identical to the local read and to the
    grouped file's;
  * `parquet_io_fetch_ahead_min_blocks` counts fetch blocks in 256k-row units,
    so one run of six 64k row groups counts two, the same as the two blocks of
    four the grouped file has — a long-run file is not gated off for being a
    handful of blocks.

Served over HTTP (dev/throttle_server.py) so every byte is a counted range GET,
through the compiler's native scan plan into ParquetIOPipeline, as
test_fetch_blocks.py does.
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
from opteryx import config
from opteryx.compiled.platform import cgroup_memory_limit_bytes, physical_memory_total_bytes
from opteryx.compiled.structures.memory_pool import configure_chunk_cache
from rugo import parquet as rp

pytestmark = pytest.mark.usefixtures("allow_local_reads")

SERVER = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../dev/throttle_server.py")
)
R = 2000        # rows per row group
N_RG = 8        # row groups -> 2 blocks of 4 for a wide column, 1 run for a narrow one
N = R * N_RG
T = 4096        # narrow threshold: lo/flag are a few hundred bytes, a/s/f 16-30 KB (uncompressed)


def _morsel(n=N):
    import random

    rng = random.Random(3)
    return Morsel.from_vectors(
        [b"a", b"s", b"lo", b"flag", b"f"],
        [vector_from_sequence(list(range(n)), DrakenType.INT64),
         vector_from_sequence([f"{rng.randrange(10**12):012d}-{i}" for i in range(n)], DrakenType.VARCHAR),
         vector_from_sequence([("x", "y", "z")[i % 3] for i in range(n)], DrakenType.VARCHAR),
         vector_from_sequence([i % 7 == 0 for i in range(n)], DrakenType.BOOL),
         vector_from_sequence([i * 0.5 for i in range(n)], DrakenType.FLOAT64)],
    )


def _write(root, name, **kw):
    d = os.path.join(root, name)
    os.makedirs(d)
    kw.setdefault("compression", "none")
    data = rp.write_parquet(_morsel(), max_rows_per_row_group=R, **kw)
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
        signs_urls = True   # routes the query onto the native path (see test_fetch_blocks.py)

        def rewrite_to_signed_url(self, path, expiry_seconds=3600):
            return path

        def list_file_infos(self, base_dir, recursive=True):
            from opteryx.connectors.io_systems._file_info import FileInfoLike

            return [FileInfoLike(path=u, size=sizes[u], mtime=0) for u in urls]

        def get_file_size(self, path):
            return sizes[path]

    register_workspace(name, FileSystemConnector, filesystem=ServedDirFileSystem(),
                       storage_type="HTTP")


def _run(sql):
    import opteryx

    session = opteryx.session(entitlements=["platform_admin"])
    rows = []
    for m in session.execute_to_morsels(sql):
        rows.extend(zip(*[m.column(n).to_pylist() for n in m.column_names]))
    diags = session.telemetry.get("io_scan_diagnostics") or []
    gets = sum(int(d.get("http_request_count", 0)) for d in diags)
    ops = sum(int(d.get("http_fetch_ops", 0)) for d in diags)
    sources = set((session.telemetry.get("scan_sources") or {}).values())
    assert sources == {"NativeParquetScanSource"}, sources
    session.close()
    return rows, gets, ops


@pytest.fixture(scope="module", autouse=True)
def chunk_cache_off():
    """GET counts are asserted on the cache-MISS path (see test_fetch_blocks.py)."""
    container = cgroup_memory_limit_bytes() or physical_memory_total_bytes()
    configure_chunk_cache(container, 0, 0)
    yield
    configure_chunk_cache(
        container, config.CHUNK_CACHE_MEMORY_PERCENT, config.CHUNK_CACHE_RESERVE_BYTES
    )


@pytest.fixture(scope="module")
def served():
    with tempfile.TemporaryDirectory() as tmp:
        pc = _write(tmp, "pc", row_groups_per_block=4, narrow_chunk_bytes=T)
        grouped = _write(tmp, "grouped", row_groups_per_block=4)
        proc, port = _server(tmp)
        try:
            _register_http_workspace("pcfb_pc", tmp, port, "pc")
            _register_http_workspace("pcfb_grouped", tmp, port, "grouped")
            yield {"pc": pc, "grouped": grouped, "port": port}
        finally:
            proc.kill()
            proc.wait()


def _local(sql_template, served, arm):
    rows, gets, _ = _run(sql_template.format(rel=f"'{served[arm]}'"))
    assert gets == 0, "a local scan issues no range GETs"
    return rows


GAP0 = " WITH(parquet_io_coalesce_gap_bytes=0)"   # blocks pinned by the waste ratio alone


@pytest.mark.parametrize("sql,pc_ops,pc_gets,grouped_ops,grouped_gets", [
    # narrow column alone: the run is one fetch block, one GET; grouped: one per block
    ("SELECT lo FROM {rel}", 1, 1, 2, 2),
    # two narrow columns: two runs, byte-adjacent, one GET
    ("SELECT lo, flag FROM {rel}", 1, 1, 2, 2),
    # a wide column: blocks of four, as the grouped file
    ("SELECT a FROM {rel}", 2, 2, 2, 2),
    # wide + narrow: blocks of four; the narrow chunks come from the run as a second
    # range per block (the s and f blocks between are far more than 10% waste) —
    # what the grouped file already pays, where s sits between a and lo
    ("SELECT a, lo FROM {rel}", 2, 4, 2, 4),
])
def test_narrow_projection_reads_the_run_as_one_block(served, sql, pc_ops, pc_gets, grouped_ops, grouped_gets):
    expected = sorted(_local(sql, served, "pc"))
    assert sorted(_local(sql, served, "grouped")) == expected
    rows_pc, gets_pc, ops_pc = _run(sql.format(rel="pcfb_pc.pc" + GAP0))
    rows_g, gets_g, ops_g = _run(sql.format(rel="pcfb_grouped.grouped" + GAP0))
    assert sorted(rows_pc) == expected
    assert sorted(rows_g) == expected
    assert (ops_pc, gets_pc) == (pc_ops, pc_gets)
    assert (ops_g, gets_g) == (grouped_ops, grouped_gets)


def test_select_star_falls_back_to_blocks_of_four(served):
    sql = "SELECT * FROM {rel}"
    expected = sorted(_local(sql, served, "pc"))
    rows, gets, ops = _run(sql.format(rel="pcfb_pc.pc"))
    assert sorted(rows) == expected
    assert ops == 2
    # the wide columns of a block are one range; the narrow slices of the runs
    # add at most one range per narrow column per block
    assert 2 <= gets <= 2 * 3


def test_pruned_row_group_costs_what_the_grouped_file_costs(served):
    # a = 9000 lives in row group 4 (rows 8000..9999): min/max pruning keeps ONE
    # row group, a one-member block on both layouts. The narrow chunk comes from
    # the run instead of the block, which moves bytes, not requests: fetch ops
    # and GETs are the grouped file's exactly.
    for sql in ("SELECT a, lo FROM {rel} WHERE a = 9000", "SELECT lo FROM {rel} WHERE a = 9000"):
        expected = _local(sql, served, "pc")
        assert len(expected) == 1 and expected[0][-1] == "x"
        rows_pc, gets_pc, ops_pc = _run(sql.format(rel="pcfb_pc.pc" + GAP0))
        rows_g, gets_g, ops_g = _run(sql.format(rel="pcfb_grouped.grouped" + GAP0))
        assert rows_pc == rows_g == expected
        assert (ops_pc, gets_pc) == (ops_g, gets_g), (sql, ops_pc, gets_pc, ops_g, gets_g)
        assert ops_pc <= 2 and gets_pc <= 3


def test_range_predicate_over_a_narrow_column_is_one_fetch(served):
    # rows 8000..15999 = block 2 exactly: pruning keeps row groups 4-7, and the
    # residual filter still reads `a`, so this is a wide projection — one block
    # of four, the a chunks as one range and the lo slice of the run as another.
    sql = "SELECT lo FROM {rel} WHERE a >= 8000"
    expected = sorted(_local(sql, served, "pc"))
    assert len(expected) == 4 * R
    rows, gets, ops = _run(sql.format(rel="pcfb_pc.pc" + GAP0))
    assert sorted(rows) == expected
    assert (ops, gets) == (1, 2)
    rows_g, gets_g, ops_g = _run(sql.format(rel="pcfb_grouped.grouped" + GAP0))
    assert sorted(rows_g) == expected
    assert (ops_g, gets_g) == (1, 2)


# ── the fetch-ahead gate on long-run files ────────────────────────────────────

RG64K = 65536
N_RG64K = 6     # 393,216 rows: 2 units of 262,144 however the row groups are blocked


def _write_64k(root, name, **kw):
    d = os.path.join(root, name)
    os.makedirs(d)
    data = rp.write_parquet(_morsel(N_RG64K * RG64K), max_rows_per_row_group=RG64K,
                            compression="none", **kw)
    assert rp.read_metadata(data).num_rows == N_RG64K * RG64K
    with open(os.path.join(d, "part-0.parquet"), "wb") as f:
        f.write(data)
    return f"{name}/part-0.parquet"


def test_fetch_ahead_gate_counts_a_long_run_in_256k_row_units():
    """`parquet_io_fetch_ahead_min_blocks` [D-1] counts fetch blocks — in units
    of the 256k-row block the calibration was made on. Six 64k row groups are
    393,216 rows = 2 units whether they are one narrow run (the per-column
    file), two blocks of four (the grouped file) or six row-major blocks of
    one (6 units: a row-major 64k file still counts one per row group, as it
    did). A minimum of 2 arms every layout; 3 arms only the row-major file."""
    from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan

    with tempfile.TemporaryDirectory() as tmp:
        pc = _write_64k(tmp, "pc", row_groups_per_block=4, narrow_chunk_bytes=T)
        grouped = _write_64k(tmp, "grouped", row_groups_per_block=4)
        rowmajor = _write_64k(tmp, "rowmajor", row_groups_per_block=1)
        proc, port = _server(tmp)
        try:
            def depth(rel, cols, gate):
                plan = open_native_scan_plan([f"http://127.0.0.1:{port}/{rel}"], cols,
                                             decode_workers=2, fetch_ahead=6,
                                             fetch_ahead_min_blocks=gate, remote_decode_workers=2)
                try:
                    assert plan.row_group_count == N_RG64K
                    return plan.diagnostics()["fetch_ahead_depth"]
                finally:
                    plan.close()

            # the narrow run: one fetch block, two units
            assert depth(pc, ["lo"], 2) == 6
            assert depth(pc, ["lo"], 3) == 0
            # a wide projection of the same file: blocks of four, two units
            assert depth(pc, ["a"], 2) == 6
            assert depth(pc, ["a"], 3) == 0
            # the grouped file: two blocks, two units — unchanged
            assert depth(grouped, ["lo"], 2) == 6
            assert depth(grouped, ["lo"], 3) == 0
            # row-major: six one-row-group blocks, six units — unchanged
            assert depth(rowmajor, ["lo"], 6) == 6
            assert depth(rowmajor, ["lo"], 7) == 0
            # 0 = no minimum, on every layout
            assert depth(pc, ["lo"], 0) == 6
        finally:
            proc.kill()
            proc.wait()


def test_fetch_ahead_gate_counts_one_unit_per_block_of_four_on_todays_layout():
    """Today's layout (64k row groups in blocks of four) is unchanged by the unit
    rule: a full block is exactly 262,144 rows = ONE unit (the boundary is
    inclusive), a partial block is one too, so nine row groups in blocks of
    four are three blocks and three units — the count [D-1] gave them."""
    from opteryx.connectors.parquet_io.pool_reader import open_native_scan_plan

    n_rg = 9   # 4 + 4 + 1
    with tempfile.TemporaryDirectory() as tmp:
        d = os.path.join(tmp, "b4")
        os.makedirs(d)
        data = rp.write_parquet(_morsel(n_rg * RG64K), max_rows_per_row_group=RG64K,
                                compression="none", row_groups_per_block=4)
        with open(os.path.join(d, "part-0.parquet"), "wb") as f:
            f.write(data)
        proc, port = _server(tmp)
        try:
            def depth(gate):
                plan = open_native_scan_plan([f"http://127.0.0.1:{port}/b4/part-0.parquet"], ["a"],
                                             decode_workers=2, fetch_ahead=6,
                                             fetch_ahead_min_blocks=gate, remote_decode_workers=2)
                try:
                    assert plan.row_group_count == n_rg
                    return plan.diagnostics()["fetch_ahead_depth"]
                finally:
                    plan.close()

            assert depth(3) == 6
            assert depth(4) == 0
        finally:
            proc.kill()
            proc.wait()


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
