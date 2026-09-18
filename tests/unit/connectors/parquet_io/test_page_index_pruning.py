"""PageIndex page pruning: the pipeline reads a column's ColumnIndex/OffsetIndex
and skips the DATA PAGES a pushed predicate cannot match — never decoding them,
and on a remote path never fetching them.

Files come from pyarrow with `write_page_index=True` and a small data page size,
so each row group holds many pages; pyarrow is the oracle for every answer.

What is pinned here:
  * the answer is identical with and without page pruning, for int equality/IN,
    string equality and starts-with, a nullable dictionary column, and a
    row group where every page is pruned;
  * the read-backs move (`page_index_pages_pruned` > 0) so the mechanism is
    proven to run, and are 0 on a file written without a page index;
  * on a remote path the bytes actually transferred DROP (`bytes_fetched`),
    which is the point of the exercise;
  * a Dict-shaped column stays Dict-shaped under the pruning mask (the column
    comes back with fewer rows, not as a materialised dense copy).
"""

import json
import os
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

SERVER = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../dev/throttle_server.py")
)

ROW_GROUP_SIZE = 20_000
N_ROW_GROUPS = 4
N = ROW_GROUP_SIZE * N_ROW_GROUPS
# ~1 KB data pages → dozens of pages per row group per column.
DATA_PAGE_SIZE = 1024


def _table():
    # x is sorted so every page has a tight, disjoint range (the best case for
    # pruning); s is a low-cardinality dict column; n is nullable dict-encoded.
    x = list(range(N))
    s = [f"k{(i // 500) % 40:02d}" for i in range(N)]           # runs of 500, 40 distinct
    n = [None if i % 7 == 0 else (i // 1000) % 13 for i in range(N)]
    return pa.table({
        "x": pa.array(x, type=pa.int64()),
        "s": pa.array(s, type=pa.string()),
        "n": pa.array(n, type=pa.int32()),
    })


def _write(tmp, page_index=True, name=None):
    name = name or f"page_index_{'on' if page_index else 'off'}_{os.getpid()}.parquet"
    path = os.path.join(tmp, name)
    pq.write_table(
        _table(), path,
        row_group_size=ROW_GROUP_SIZE,
        data_page_size=DATA_PAGE_SIZE,
        write_page_index=page_index,
        use_dictionary=["s", "n"],
        column_encoding={"x": "PLAIN"},
        write_statistics=True,
    )
    assert pq.ParquetFile(path).num_row_groups == N_ROW_GROUPS
    return path, name


def _server(root):
    proc = subprocess.Popen(
        [sys.executable, SERVER, "--root", root, "--port", "0", "--seed", "1"],
        stdout=subprocess.PIPE, text=True,
    )
    ready = proc.stdout.readline()
    if not ready.startswith("READY"):
        proc.kill()
        raise RuntimeError(f"throttle server failed to start: {ready!r}")
    return proc, int(ready.strip().split("port=")[1])


def _scan(url, columns, predicates, workers=2):
    """Drive the public wrapper with pushed predicates; return (rows, diagnostics).

    Predicates here are the SAME single-conjunct pre-filters the planner pushes
    (row-group stats pruning + dictionary decode-skip + page pruning); the rows
    that come back are NOT yet filtered by them — the filter node downstream
    does that — so the caller applies the predicate to the returned rows before
    comparing with pyarrow. What is checked is that no matching row was lost."""
    from opteryx.connectors.parquet_io.pool_reader import iter_row_groups_ipc

    fd, diag_path = tempfile.mkstemp(suffix=".jsonl")
    os.close(fd)
    os.environ["OPTERYX_IO_DIAG_JSON"] = diag_path
    out = {c: [] for c in columns}
    shapes = {c: set() for c in columns}
    try:
        gen = iter_row_groups_ipc(None, [url], columns, decode_workers=workers,
                                  predicates=predicates)
        try:
            for _scan_rg, rg in gen:
                for c in columns:
                    v = rg[c.encode()]
                    out[c].extend(v.to_pylist())
                    shapes[c].add("dict" if v.is_dict else ("constant" if v.is_constant else "dense"))
        finally:
            gen.close()
        with open(diag_path) as f:
            lines = f.read().strip().splitlines()
        assert len(lines) == 1, lines
        # Row groups complete in whichever order the workers finish them; x is
        # unique and sorted in the fixture, so restore row order by it.
        order = sorted(range(len(out[columns[0]])), key=lambda i: out[columns[0]][i])
        out = {c: [out[c][i] for i in order] for c in columns}
        return out, shapes, json.loads(lines[-1])
    finally:
        del os.environ["OPTERYX_IO_DIAG_JSON"]
        os.remove(diag_path)


def _expected(mask_expr, table=None):
    t = _table() if table is None else table
    return t.filter(mask_expr)


def _survivors(rows, keep):
    return [r for r, k in zip(rows, keep) if k]


# ── correctness with and without the index ───────────────────────────────────

@pytest.mark.parametrize("page_index", [True, False])
def test_int_equality_answer_and_readbacks(page_index):
    with tempfile.TemporaryDirectory() as tmp:
        path, _ = _write(tmp, page_index=page_index)
        needle = 33_333
        out, shapes, diag = _scan(path, ["x", "s", "n"], [("x", "Eq", needle)])
    keep = [v == needle for v in out["x"]]
    exp = _expected(pc.field("x") == needle)
    assert _survivors(out["x"], keep) == exp["x"].to_pylist()
    assert _survivors(out["s"], keep) == exp["s"].to_pylist()
    assert _survivors(out["n"], keep) == exp["n"].to_pylist()
    if page_index:
        # One row group survives row-group stats; inside it all but one page of
        # every column is skipped.
        assert diag["page_index_pages_pruned"] > 0
        assert diag["page_index_bytes_pruned"] > 0
        assert diag["page_index_fetches"] == 1
        # Far fewer rows came back than the surviving row group holds.
        assert len(out["x"]) < ROW_GROUP_SIZE
    else:
        assert diag["page_index_pages_pruned"] == 0
        assert diag["page_index_fetches"] == 0
        assert len(out["x"]) == ROW_GROUP_SIZE


def test_int_in_list_keeps_every_member():
    with tempfile.TemporaryDirectory() as tmp:
        path, _ = _write(tmp)
        members = [5, 19_999, 20_000, 61_111]   # first/last row of a row group included
        out, _, diag = _scan(path, ["x", "s"], [("x", "InList", members)])
    keep = [v in members for v in out["x"]]
    exp = _expected(pc.field("x").isin(members))
    assert _survivors(out["x"], keep) == exp["x"].to_pylist()
    assert _survivors(out["s"], keep) == exp["s"].to_pylist()
    assert diag["page_index_pages_pruned"] > 0


def test_string_equality_and_starts_with():
    with tempfile.TemporaryDirectory() as tmp:
        path, _ = _write(tmp)
        out, shapes, diag = _scan(path, ["x", "s"], [("s", "Eq", b"k07")])
        keep = [v == "k07" for v in out["s"]]
        exp = _expected(pc.field("s") == "k07")
        assert _survivors(out["x"], keep) == exp["x"].to_pylist()
        assert diag["page_index_pages_pruned"] > 0
        # The string column stays dictionary-shaped under the pruning mask.
        assert shapes["s"] <= {"dict"}, shapes

        out, _, diag = _scan(path, ["x", "s"], [("s", "_STARTS_WITH", b"k3")])
        keep = [v.startswith("k3") for v in out["s"]]
        exp = _expected(pc.starts_with(pc.field("s"), "k3"))
        assert _survivors(out["x"], keep) == exp["x"].to_pylist()
        assert diag["page_index_pages_pruned"] > 0


def test_nullable_dict_column_survives_pruning_mask():
    """Pruning on x; n (nullable, dictionary-encoded) rides along under the
    mask and must keep its nulls in the right rows."""
    with tempfile.TemporaryDirectory() as tmp:
        path, _ = _write(tmp)
        lo, hi = 40_010, 40_020
        members = list(range(lo, hi))
        out, _, diag = _scan(path, ["x", "n"], [("x", "InList", members)])
    keep = [v in members for v in out["x"]]
    exp = _expected(pc.field("x").isin(members))
    assert _survivors(out["n"], keep) == exp["n"].to_pylist()
    assert None in exp["n"].to_pylist()   # the slice really has nulls
    assert diag["page_index_pages_pruned"] > 0


def test_every_page_pruned_yields_empty_row_group_not_wrong_rows():
    """Row groups whose predicate column is entirely NULL: row-group stats carry
    no min/max for them (pyarrow writes none), so only the ColumnIndex's
    null_pages can exclude them — every page pruned, the row group comes back
    empty_filtered, and nothing wrong leaks out."""
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, f"null_pages_{os.getpid()}.parquet")
        t = _table()
        y = [None] * (ROW_GROUP_SIZE * (N_ROW_GROUPS - 1)) + list(range(ROW_GROUP_SIZE))
        t = t.append_column("y", pa.array(y, type=pa.int64()))
        pq.write_table(t, path, row_group_size=ROW_GROUP_SIZE, data_page_size=DATA_PAGE_SIZE,
                       write_page_index=True, column_encoding={"x": "PLAIN", "y": "PLAIN"},
                       use_dictionary=["s", "n"])
        needle = 12_345
        out, _, diag = _scan(path, ["x", "y"], [("y", "Eq", needle)])
    keep = [v == needle for v in out["y"]]
    exp = _expected(pc.field("y") == needle, table=t)
    assert _survivors(out["x"], keep) == exp["x"].to_pylist() == [ROW_GROUP_SIZE * 3 + needle]
    # The three all-NULL row groups were pruned outright; the last one kept
    # only the page holding the needle.
    assert diag["page_index_row_groups_pruned"] == N_ROW_GROUPS - 1
    assert diag["page_index_pages_pruned"] > 0
    assert len(out["x"]) < ROW_GROUP_SIZE


# ── the bytes really go unfetched on a remote path ───────────────────────────

def test_remote_scan_fetches_fewer_bytes_with_the_index():
    with tempfile.TemporaryDirectory() as tmp:
        path_on, name_on = _write(tmp, page_index=True)
        path_off, name_off = _write(tmp, page_index=False)
        proc, port = _server(tmp)
        try:
            url_on = f"http://127.0.0.1:{port}/{name_on}"
            url_off = f"http://127.0.0.1:{port}/{name_off}"
            needle = 47_474
            out_on, shapes_on, diag_on = _scan(url_on, ["x", "s", "n"], [("x", "Eq", needle)])
            out_off, _, diag_off = _scan(url_off, ["x", "s", "n"], [("x", "Eq", needle)])
        finally:
            proc.kill(); proc.wait()
    exp = _expected(pc.field("x") == needle)
    for out in (out_on, out_off):
        keep = [v == needle for v in out["x"]]
        assert _survivors(out["x"], keep) == exp["x"].to_pylist()
        assert _survivors(out["s"], keep) == exp["s"].to_pylist()
        assert _survivors(out["n"], keep) == exp["n"].to_pylist()
    assert diag_on["page_index_pages_pruned"] > 0
    assert diag_on["page_index_fetches"] == 1
    # The pruned pages were never requested: the index costs one small read and
    # saves far more than that in column-chunk bytes.
    assert diag_on["bytes_fetched"] + diag_on["page_index_bytes_fetched"] < diag_off["bytes_fetched"]
    assert diag_on["page_index_bytes_pruned"] > 0
    # Dict shape survives the mask on the remote path too.
    assert shapes_on["s"] <= {"dict"}, shapes_on


# ── the cost gate ────────────────────────────────────────────────────────────

def _wide_narrow_fixture(tmp):
    """One file with a tiny predicate column and two fat payload columns, so a
    NARROW projection (predicate column only) cannot repay the index read while
    a WIDE one repays it many times over."""
    path = os.path.join(tmp, f"gate_{os.getpid()}.parquet")
    n = 200_000
    t = pa.table({
        "k": pa.array([i // 1000 for i in range(n)], type=pa.int32()),       # clustered, tiny
        "fat1": pa.array([("x%06d" % i) * 12 for i in range(n)], type=pa.string()),
        "fat2": pa.array([("y%06d" % i) * 12 for i in range(n)], type=pa.string()),
    })
    pq.write_table(t, path, row_group_size=50_000, data_page_size=DATA_PAGE_SIZE,
                   write_page_index=True, use_dictionary=False)
    return path


def test_gate_declines_when_the_index_cannot_repay_itself():
    """The index is read only when it costs at most a tenth of the bytes it could
    remove. Narrow projection: declined, and NOT read. Wide: read and used.
    Either way the rows are the same — the gate never changes the answer."""
    with tempfile.TemporaryDirectory() as tmp:
        path = _wide_narrow_fixture(tmp)
        narrow, _, d_narrow = _scan(path, ["k"], [("k", "Eq", 7)])
        wide, _, d_wide = _scan(path, ["k", "fat1", "fat2"], [("k", "Eq", 7)])

    # Declined: nothing fetched, nothing pruned, the decline is COUNTED so a
    # reader can tell "chose not to look" from "there was nothing to look at".
    assert d_narrow["page_index_gate_declines"] > 0
    assert d_narrow["page_index_fetches"] == 0
    assert d_narrow["page_index_pages_pruned"] == 0

    # Repaid: read once, and it pruned.
    assert d_wide["page_index_gate_declines"] == 0
    assert d_wide["page_index_fetches"] >= 1
    assert d_wide["page_index_pages_pruned"] > 0
    assert len(wide["k"]) < len(narrow["k"])

    # Same answer through both paths.
    assert [v for v in narrow["k"] if v == 7] == [v for v in wide["k"] if v == 7] == [7] * 1000


def test_the_ab_arm_disables_pruning_entirely():
    """RUGO_PAGE_INDEX_PRUNE=0 must make the pipeline behave as if no file
    carried an index — the arm every measurement in this area is run against."""
    import subprocess

    with tempfile.TemporaryDirectory() as tmp:
        path, _ = _write(tmp)
        script = (
            "import sys; sys.path.insert(0, %r); sys.path.insert(0, %r)\n"
            "import test_page_index_pruning as t\n"
            "out, _, d = t._scan(%r, ['x', 's'], [('x', 'Eq', 33333)])\n"
            "print(len(out['x']), d['page_index_pages_pruned'], d['page_index_fetches'])\n"
            % (os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../..")),
               os.path.dirname(os.path.abspath(__file__)), path)
        )
        got = {}
        for arm in ("1", "0"):
            r = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True,
                               env={**os.environ, "RUGO_PAGE_INDEX_PRUNE": arm})
            assert r.returncode == 0, r.stderr[-2000:]
            got[arm] = [int(v) for v in r.stdout.strip().split()]

    rows_on, pruned_on, fetches_on = got["1"]
    rows_off, pruned_off, fetches_off = got["0"]
    assert pruned_on > 0 and fetches_on >= 1
    assert pruned_off == 0 and fetches_off == 0
    assert rows_on < rows_off == ROW_GROUP_SIZE


# ── the same thing, on a file RUGO wrote ─────────────────────────────────────
#
# Everything above uses pyarrow fixtures, which proved the READER. This proves
# the WRITER: rugo emits a PageIndex our own pipeline can prune with. Until the
# writer could do this, the whole optimisation was inert on our own data.


def _rugo_fixture(tmp, page_index=True, max_page_bytes=16384):
    """The same shape as _table(), written by rugo instead of pyarrow.

    `s` is low-cardinality and gets auto-dictionary-encoded, so this also pins
    that a DICT chunk splits into several data pages — a single-page dict chunk
    would index at row-group granularity and prune nothing.
    """
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from draken.vectors.vector import Vector
    from rugo.parquet import write_parquet

    t = _table()
    morsel = Morsel.from_vectors(
        ["x", "s", "n"],
        [
            Vector(vector_from_sequence(t["x"].to_pylist(), "INT64")),
            Vector(vector_from_sequence(t["s"].to_pylist(), "VARCHAR")),
            Vector(vector_from_sequence(t["n"].to_pylist(), "INT32")),
        ],
    )
    path = os.path.join(tmp, f"rugo_{'on' if page_index else 'off'}_{os.getpid()}.parquet")
    with open(path, "wb") as f:
        f.write(write_parquet(
            morsel,
            compression="zstd",
            max_rows_per_row_group=ROW_GROUP_SIZE,
            max_page_bytes=max_page_bytes,
            page_index=page_index,
        ))
    md = pq.ParquetFile(path).metadata
    assert md.num_row_groups == N_ROW_GROUPS, md.num_row_groups
    return path, md


@pytest.mark.parametrize("page_index", [True, False])
def test_rugo_written_file_prunes_pages(page_index):
    with tempfile.TemporaryDirectory() as tmp:
        path, md = _rugo_fixture(tmp, page_index=page_index)

        # The footer records both index structures per chunk, or neither.
        for c in range(md.num_columns):
            cc = md.row_group(0).column(c)
            assert cc.has_column_index is page_index
            assert cc.has_offset_index is page_index

        needle = 33_333
        out, shapes, diag = _scan(path, ["x", "s", "n"], [("x", "Eq", needle)])

    keep = [v == needle for v in out["x"]]
    exp = _expected(pc.field("x") == needle)
    assert _survivors(out["x"], keep) == exp["x"].to_pylist()
    assert _survivors(out["s"], keep) == exp["s"].to_pylist()
    assert _survivors(out["n"], keep) == exp["n"].to_pylist()

    if page_index:
        assert diag["page_index_pages_pruned"] > 0
        assert diag["page_index_bytes_pruned"] > 0
        assert diag["page_index_fetches"] == 1
        assert len(out["x"]) < ROW_GROUP_SIZE
    else:
        assert diag["page_index_pages_pruned"] == 0
        assert diag["page_index_fetches"] == 0
        assert len(out["x"]) == ROW_GROUP_SIZE


def test_rugo_dict_chunk_splits_into_pages():
    """A dictionary-encoded column must prune on ITS OWN predicate, which it can
    only do if its code stream was split across several data pages."""
    with tempfile.TemporaryDirectory() as tmp:
        path, _ = _rugo_fixture(tmp)
        out, shapes, diag = _scan(path, ["x", "s"], [("s", "Eq", b"k07")])
    keep = [v == "k07" for v in out["s"]]
    exp = _expected(pc.field("s") == "k07")
    assert _survivors(out["x"], keep) == exp["x"].to_pylist()
    assert diag["page_index_pages_pruned"] > 0
    assert shapes["s"] <= {"dict"}, shapes


def test_rugo_page_index_needs_page_splitting():
    """page_index=True over a single-page chunk writes no index: it would only
    restate the footer statistics. max_page_bytes is the switch."""
    with tempfile.TemporaryDirectory() as tmp:
        path, md = _rugo_fixture(tmp, page_index=True, max_page_bytes=0)
        for c in range(md.num_columns):
            assert md.row_group(0).column(c).has_column_index is False
            assert md.row_group(0).column(c).has_offset_index is False
        out, _, diag = _scan(path, ["x", "s"], [("x", "Eq", 33_333)])
    assert diag["page_index_pages_pruned"] == 0
    assert len(out["x"]) == ROW_GROUP_SIZE


def test_rugo_streaming_writer_index_offsets_are_absolute():
    """The streaming writer hands bytes to its sink as it goes, so the index
    tail's offsets have to be counted against the absolute file position, not
    the undrained buffer. A file whose page offsets are off by a drained prefix
    is refused by the reader, so a clean prune here is the proof."""
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from draken.vectors.vector import Vector
    from rugo.parquet import open_parquet_writer

    t = _table()
    x = t["x"].to_pylist()
    s = t["s"].to_pylist()

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, f"streamed_{os.getpid()}.parquet")
        with open(path, "wb") as f:
            with open_parquet_writer(f.write, compression="zstd",
                                     max_page_bytes=16384) as w:
                for rg in range(N_ROW_GROUPS):
                    lo, hi = rg * ROW_GROUP_SIZE, (rg + 1) * ROW_GROUP_SIZE
                    w.write_row_group(Morsel.from_vectors(
                        ["x", "s"],
                        [Vector(vector_from_sequence(x[lo:hi], "INT64")),
                         Vector(vector_from_sequence(s[lo:hi], "VARCHAR"))],
                    ))

        md = pq.ParquetFile(path).metadata
        assert md.num_row_groups == N_ROW_GROUPS
        for rg in range(N_ROW_GROUPS):
            for c in range(md.num_columns):
                assert md.row_group(rg).column(c).has_column_index
                assert md.row_group(rg).column(c).has_offset_index

        needle = 33_333
        out, _, diag = _scan(path, ["x", "s"], [("x", "Eq", needle)])

    keep = [v == needle for v in out["x"]]
    exp = _expected(pc.field("x") == needle)
    assert _survivors(out["x"], keep) == exp["x"].to_pylist()
    assert _survivors(out["s"], keep) == exp["s"].to_pylist()
    assert diag["page_index_pages_pruned"] > 0
    assert len(out["x"]) < ROW_GROUP_SIZE


def test_a_chunk_that_does_not_split_gets_no_index():
    """`max_page_bytes > 0` is a BYTE budget, not a promise of more than one
    page: whether a chunk splits depends on its width and the row group's row
    count. A chunk left with one page must get NO index — a one-entry index
    restates the footer statistics, and it inflates the index region the reader
    fetches as a single range, pushing its cost gate toward declining and
    penalising the columns that did split.

    Regression: a 1 MiB budget over a 262 144-row row group leaves every 4-byte
    column single-page, which indexed 303 of 420 ClickBench chunks for nothing.
    """
    with tempfile.TemporaryDirectory() as tmp:
        # Budget chosen so the 8-byte column splits and the others do not:
        # x needs 20_000*8 = 160_000 B to stay whole, s and n need less.
        _, md = _rugo_fixture(tmp, page_index=True, max_page_bytes=150_000)
        got = {md.schema.column(c).name: md.row_group(0).column(c).has_column_index
               for c in range(md.num_columns)}
        assert got == {"x": True, "s": False, "n": False}, got
        for c in range(md.num_columns):
            cc = md.row_group(0).column(c)
            assert cc.has_column_index is cc.has_offset_index

        # A budget no column can exceed: the knob is on and nothing is indexed.
        _, md = _rugo_fixture(tmp, page_index=True, max_page_bytes=10_000_000)
        assert not any(md.row_group(0).column(c).has_column_index
                       for c in range(md.num_columns))
