"""The grouped column-major parquet layout (docs/PARQUET_GROUPED_COLUMN_MAJOR_DESIGN.md).

rugo writes row groups of `max_rows_per_row_group` rows in BLOCKS of
`row_groups_per_block`. Within a block every column's chunks for the block's
row groups are byte-adjacent, columns in schema order; every bloom filter goes
in the file tail after the last block, column-major over the whole file; then
the page index; then the footer. The row group stays the unit of decode,
statistics and pruning, and every chunk is still located by its own footer
offsets — which is what keeps the file legal for every other reader.

Pinned here, with pyarrow as the metadata oracle:
  * the byte order of chunks and blooms is exactly the one above;
  * RowGroup.file_offset / total_compressed_size are the row group's first
    byte and the sum of its chunks (honest, as ruled);
  * G = 1 is conventional row-major placement;
  * the streaming writer and the one-shot writer produce the same layout;
  * a rugo-written grouped file reads back the same VALUES in pyarrow, DuckDB,
    Polars, rugo and opteryx (design decision D-3) — the five-reader check the
    proof of concept ran on pyarrow-written files, now on rugo's own output;
  * with-bounds bounds span every row group.
"""

import datetime
import decimal
import io
import os
import random
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow as pa  # test oracle only
import pyarrow.parquet as pq  # test oracle only

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo import parquet as rp
from rugo.rugo_native import read_rowgroup_stats

R = 1000  # rows per row group for the layout tests (small, fast)


def _morsel(n, seed=7):
    rng = random.Random(seed)
    return Morsel.from_vectors(
        [b"a", b"s", b"f", b"lo"],
        [
            vector_from_sequence(list(range(n)), DrakenType.INT64),
            vector_from_sequence([f"v{rng.randrange(100000)}-{i}" for i in range(n)], DrakenType.VARCHAR),
            vector_from_sequence([i * 0.25 for i in range(n)], DrakenType.FLOAT64),
            vector_from_sequence([("x", "y", "z")[i % 3] for i in range(n)], DrakenType.VARCHAR),
        ],
    )


def _chunks(data):
    """[rg][col] -> (start, end) of the column chunk, from the footer."""
    md = pq.ParquetFile(io.BytesIO(data)).metadata
    out = []
    for rg in range(md.num_row_groups):
        row = []
        for c in range(md.num_columns):
            col = md.row_group(rg).column(c)
            start = col.dictionary_page_offset if col.has_dictionary_page else col.data_page_offset
            row.append((start, start + col.total_compressed_size))
        out.append(row)
    return out


def _footer_start(data):
    footer_len = int.from_bytes(data[-8:-4], "little")
    return len(data) - 8 - footer_len


def _blocks(n_rg, g):
    return [list(range(b, min(b + g, n_rg))) for b in range(0, n_rg, g)]


# ── chunk order ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("n_rg,g", [(10, 4), (4, 4), (5, 4), (1, 4), (7, 3), (6, 1)])
def test_blocks_are_column_major_and_contiguous(n_rg, g):
    data = rp.write_parquet(_morsel(n_rg * R), max_rows_per_row_group=R, row_groups_per_block=g)
    ch = _chunks(data)
    assert len(ch) == n_rg
    ncols = len(ch[0])
    cursor = 4  # first byte after the PAR1 magic
    for block in _blocks(n_rg, g):
        for c in range(ncols):
            for rg in block:
                start, end = ch[rg][c]
                assert start == cursor, (block, c, rg, start, cursor)
                cursor = end
    # Every bloom filter, then the page index, then the footer, follow the last chunk.
    stats = read_rowgroup_stats(data)
    for rg in stats:
        for col in rg["columns"]:
            if col["bloom_offset"] is not None and col["bloom_offset"] >= 0:
                assert col["bloom_offset"] >= cursor


def test_g1_is_row_major():
    n_rg = 5
    data = rp.write_parquet(_morsel(n_rg * R), max_rows_per_row_group=R, row_groups_per_block=1)
    ch = _chunks(data)
    cursor = 4
    for rg in range(n_rg):
        for c in range(len(ch[0])):
            start, end = ch[rg][c]
            assert start == cursor
            cursor = end


def test_blooms_sit_in_the_tail_column_major():
    n_rg = 6
    data = rp.write_parquet(_morsel(n_rg * R), max_rows_per_row_group=R)  # default G = 4
    ch = _chunks(data)
    last_chunk_end = max(end for row in ch for _, end in row)
    stats = read_rowgroup_stats(data)
    bloomed = [c for c in range(len(ch[0]))
               if stats[0]["columns"][c]["bloom_offset"] is not None
               and stats[0]["columns"][c]["bloom_offset"] >= 0]
    assert bloomed, "expected bloom filters on the int and string columns"
    cursor = last_chunk_end
    for c in bloomed:
        for rg in range(n_rg):
            col = stats[rg]["columns"][c]
            assert col["bloom_offset"] == cursor, (c, rg, col["bloom_offset"], cursor)
            cursor += col["bloom_length"]
    assert cursor <= _footer_start(data)
    # The float column never gets a bloom filter.
    assert stats[0]["columns"][2]["bloom_offset"] in (None, -1)


def test_row_group_footer_fields_are_honest():
    for g in (1, 4):
        data = rp.write_parquet(_morsel(6 * R), max_rows_per_row_group=R, row_groups_per_block=g)
        ch = _chunks(data)
        stats = read_rowgroup_stats(data)
        for rg, row in enumerate(ch):
            assert stats[rg]["file_offset"] == min(start for start, _ in row)
            assert stats[rg]["total_compressed_size"] == sum(end - start for start, end in row)
            # Under the grouped layout the pair brackets MORE than the row
            # group's own bytes — a reader must locate chunks by their own
            # offsets, never by this span.
            if g > 1 and rg % g != g - 1 and rg + 1 < len(ch):
                assert (stats[rg]["file_offset"] + stats[rg]["total_compressed_size"]
                        < max(end for _, end in row))


def test_layout_params_are_validated():
    with pytest.raises(ValueError, match="row_groups_per_block"):
        rp.write_parquet(_morsel(10), row_groups_per_block=0)
    with pytest.raises(ValueError, match="max_rows_per_row_group"):
        rp.write_parquet(_morsel(10), max_rows_per_row_group=-1)
    with pytest.raises(ValueError, match="row_groups_per_block"):
        rp.open_parquet_writer(lambda b: None, row_groups_per_block=0)


# ── streaming == one-shot ─────────────────────────────────────────────────────

def test_streaming_writer_produces_the_same_layout_as_one_shot():
    n_rg = 6
    whole = _morsel(n_rg * R)
    one_shot = rp.write_parquet(whole, max_rows_per_row_group=R)
    chunks = []
    with rp.open_parquet_writer(chunks.append) as w:
        for lo in range(0, n_rg * R, R):
            w.write_row_group(whole.slice(lo, R))
    streamed = b"".join(chunks)
    assert _chunks(streamed) == _chunks(one_shot)
    assert read_rowgroup_stats(streamed) == read_rowgroup_stats(one_shot)
    t1 = pq.read_table(io.BytesIO(one_shot))
    t2 = pq.read_table(io.BytesIO(streamed))
    assert t1.equals(t2)


def test_with_bounds_spans_every_row_group():
    n = 6 * R
    _, bounds = rp.write_parquet_with_bounds(_morsel(n), max_rows_per_row_group=R)
    assert bounds[0] == (0, n - 1)
    assert bounds[2] == (0.0, (n - 1) * 0.25)


# ── D-3: five readers, same values ────────────────────────────────────────────

def _rich_morsel(n, seed=42):
    rng = random.Random(seed)
    i64 = [None if rng.random() < 0.1 else rng.randrange(-10**12, 10**12) for _ in range(n)]
    i32 = [rng.randrange(0, 1000) for _ in range(n)]
    f64 = [rng.gauss(0, 1) * 1e6 for _ in range(n)]
    lowcard = [("alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta")[rng.randrange(8)]
               for _ in range(n)]
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    highcard = ["".join(rng.choices(alphabet, k=rng.randint(6, 24))) + f"-{i}" for i in range(n)]
    d32 = [datetime.date(2000, 1, 1) + datetime.timedelta(days=rng.randrange(9000)) for _ in range(n)]
    boo = [rng.random() < 0.5 for _ in range(n)]
    dec = [decimal.Decimal(rng.randrange(-10**12, 10**12)).scaleb(-2) for _ in range(n)]
    return Morsel.from_vectors(
        [b"rid", b"i64_nulls", b"i32", b"f64", b"lowcard", b"highcard", b"d32", b"b", b"dec"],
        [
            vector_from_sequence(list(range(n)), DrakenType.INT64),
            vector_from_sequence(i64, DrakenType.INT64),
            vector_from_sequence(i32, DrakenType.INT32),
            vector_from_sequence(f64, DrakenType.FLOAT64),
            vector_from_sequence(lowcard, DrakenType.VARCHAR),
            vector_from_sequence(highcard, DrakenType.VARCHAR),
            vector_from_sequence(d32, DrakenType.DATE32),
            vector_from_sequence(boo, DrakenType.BOOL),
            vector_from_sequence(dec, DrakenType.DECIMAL),
        ],
    )


def _canon(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return bool(v)
    if isinstance(v, int):
        return int(v)
    if isinstance(v, float):
        return float(v)
    if isinstance(v, decimal.Decimal):
        return str(v.quantize(decimal.Decimal("0.000001")))
    if isinstance(v, datetime.datetime):
        return v.date().isoformat()
    if isinstance(v, datetime.date):
        return v.isoformat()
    if isinstance(v, bytes):
        return v.decode("utf-8")
    if isinstance(v, str):
        return v
    raise TypeError(f"unhandled value type {type(v)}: {v!r}")


def _canon_table(tbl):
    cols = {}
    for name in tbl.column_names:
        col = tbl.column(name)
        if pa.types.is_dictionary(col.type):
            col = col.cast(col.type.value_type)
        cols[name] = [_canon(v) for v in col.to_pylist()]
    return cols


def _by_rid(cols):
    order = sorted(range(len(cols["rid"])), key=lambda i: cols["rid"][i])
    return {c: [cols[c][i] for i in order] for c in cols}


def _morsels_to_arrow(morsels):
    tables = [m.to_arrow() for m in morsels]
    assert tables, "no morsels returned"
    return pa.concat_tables(tables, promote_options="default")


N_RICH = 5 * 65536 + 1234   # six row groups: one full block of four, one partial of two
FILTER_LO, FILTER_HI = 3 * 65536 + 10, 3 * 65536 + 109   # 100 rows, all inside row group 3


@pytest.fixture(scope="module")
def grouped_file(tmp_path_factory):
    """A rugo-written grouped file with the production defaults (64k row groups,
    blocks of 4, zstd, page index) over every type the five readers share."""
    m = _rich_morsel(N_RICH)
    data = rp.write_parquet(m, max_page_bytes=64 * 1024)
    d = tmp_path_factory.mktemp("grouped")
    rel_dir = d / "grouped_ds"
    rel_dir.mkdir()
    path = rel_dir / "part-0.parquet"
    path.write_bytes(data)
    expected = _by_rid(_canon_table(m.to_arrow()))
    md = pq.ParquetFile(str(path)).metadata
    assert md.num_row_groups == 6
    return str(path), str(rel_dir), expected


def _filtered(expected):
    keep = [FILTER_LO <= r <= FILTER_HI for r in expected["rid"]]
    return {c: [v for v, k in zip(expected[c], keep) if k] for c in expected}


def test_reads_back_in_pyarrow(grouped_file):
    path, _, expected = grouped_file
    assert _by_rid(_canon_table(pq.read_table(path))) == expected
    pf = pq.ParquetFile(path)
    parts = [pf.read_row_group(k) for k in range(pf.metadata.num_row_groups)]
    assert _by_rid(_canon_table(pa.concat_tables(parts))) == expected
    got = _canon_table(pq.read_table(path, columns=["highcard"]))["highcard"]
    assert sorted(got) == sorted(expected["highcard"])


def test_reads_back_in_duckdb(grouped_file):
    duckdb = pytest.importorskip("duckdb")
    path, _, expected = grouped_file
    con = duckdb.connect()
    p = path.replace("'", "''")
    assert _by_rid(_canon_table(con.execute(f"SELECT * FROM read_parquet('{p}')").fetch_arrow_table())) == expected
    got = con.execute(f"SELECT * FROM read_parquet('{p}') WHERE rid BETWEEN {FILTER_LO} AND {FILTER_HI}")
    assert _by_rid(_canon_table(got.fetch_arrow_table())) == _filtered(expected)
    con.close()


def test_reads_back_in_polars(grouped_file):
    pl = pytest.importorskip("polars")
    path, _, expected = grouped_file
    assert _by_rid(_canon_table(pl.read_parquet(path).to_arrow())) == expected
    lf = pl.scan_parquet(path).filter((pl.col("rid") >= FILTER_LO) & (pl.col("rid") <= FILTER_HI))
    assert _by_rid(_canon_table(lf.collect().to_arrow())) == _filtered(expected)


def test_reads_back_in_rugo(grouped_file):
    path, _, expected = grouped_file
    with rp.read_parquet(path) as r:
        assert _by_rid(_canon_table(_morsels_to_arrow(list(r)))) == expected
    with rp.read_parquet(path, columns=["highcard"]) as r:
        got = _canon_table(_morsels_to_arrow(list(r)))["highcard"]
    assert sorted(got) == sorted(expected["highcard"])
    with rp.read_parquet(path, predicates=[("rid", ">=", FILTER_LO), ("rid", "<=", FILTER_HI)]) as r:
        ms = list(r)
    assert len(ms) == 1, "row-group pruning must keep exactly row group 3"
    assert _by_rid(_canon_table(_morsels_to_arrow(ms))) == _filtered(expected)


def test_reads_back_in_opteryx(grouped_file):
    import opteryx

    _, rel_dir, expected = grouped_file
    # A quoted relation holding a path is read as that directory (the same
    # connector the repo's `testdata.*` datasets go through), so the temp file
    # is scanned by the native engine path with no registration.
    relation = f"'{rel_dir}'"
    got = _morsels_to_arrow(opteryx.session().execute_to_morsels(f"SELECT * FROM {relation}"))
    assert _by_rid(_canon_table(got)) == expected
    for col in ("highcard", "i64_nulls", "dec"):
        got = _canon_table(_morsels_to_arrow(
            opteryx.session().execute_to_morsels(f"SELECT {col} FROM {relation}")))[col]
        assert sorted(got, key=lambda v: (v is None, v if v is not None else 0)) == \
            sorted(expected[col], key=lambda v: (v is None, v if v is not None else 0))
    n = _morsels_to_arrow(opteryx.session().execute_to_morsels(
        f"SELECT COUNT(*) AS n FROM {relation} WHERE rid BETWEEN {FILTER_LO} AND {FILTER_HI}"
    )).column(0).to_pylist()[0]
    assert n == len(_filtered(expected)["rid"])


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
