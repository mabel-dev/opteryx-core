"""
Length-only string decode: a VARCHAR column read ONLY through length-answerable
operations (`col <> ''`, LENGTH(col)) is decoded to stubs — a value's full bytes
when it fits inline in its slot (<= 12 bytes), otherwise just its 4-byte prefix —
instead of its full payload (rugo decode_column.cpp, DecodedColumn::append_string_stub).

The stub has exactly one safe consumer: the direct string builders, which take
lengths from string_lens. Everything below checks that the answers are the ones a
full decode gives, across every file shape the decoder branches on:

  * value lengths straddle the inline boundary (0, 1, 11, 12, 13, 14, long), so a
    stub that kept the wrong number of bytes shows up as a wrong length or a wrong
    emptiness answer;
  * PLAIN and dictionary pages, a dictionary that spills to PLAIN mid-chunk (the
    foreign-writer re-derivation path the stub path replaces), data page v1 and v2,
    required / nullable / nullable-with-nulls columns, compressed and not;
  * columns WITH nulls must keep their bytes (they take the IPC pool path, which a
    stub would overread) — and still answer correctly.

Each fixture also asserts the stub path engaged (rugo's ba_stub_chunks), or did not
where it must not, so a change that quietly disarmed it cannot pass by testing nothing.
"""

import os
import random
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
import rugo.rugo_native as rp
from opteryx.connectors import DiskConnector

N = 6_000
LENGTHS = [0, 0, 1, 2, 11, 12, 13, 14, 40, 200]
ALPHABET = "abcdefgh"

_WS_COUNTER = [0]


def _values(seed, null_rate):
    rnd = random.Random(seed)
    out = []
    for _ in range(N):
        if null_rate and rnd.random() < null_rate:
            out.append(None)
            continue
        n = rnd.choice(LENGTHS)
        out.append("".join(rnd.choice(ALPHABET) for _ in range(n)))
    return out


# name: (write_table kwargs, null_rate, required, stub expected)
SHAPES = {
    "plain_v1_snappy_nulls": (dict(use_dictionary=False, data_page_version="1.0", compression="snappy"), 0.15, False, False),
    "plain_v2_zstd_nulls": (dict(use_dictionary=False, data_page_version="2.0", compression="zstd"), 0.15, False, False),
    "plain_required": (dict(use_dictionary=False, compression="none"), 0.0, True, True),
    "plain_nullable_no_nulls": (dict(use_dictionary=False, compression="snappy"), 0.0, False, True),
    "plain_v2_no_nulls": (dict(use_dictionary=False, data_page_version="2.0", compression="zstd"), 0.0, False, True),
    "dict_v1_no_nulls": (dict(use_dictionary=True, data_page_version="1.0", compression="snappy"), 0.0, False, False),
    # Dictionary outgrows its page and spills to PLAIN inside the same chunk: the
    # foreign-writer path that used to be interned back into a dictionary.
    "dict_spill_no_nulls": (dict(use_dictionary=True, dictionary_pagesize_limit=256, compression="snappy"), 0.0, False, True),
    "dict_spill_required": (dict(use_dictionary=True, dictionary_pagesize_limit=256, compression="none"), 0.0, True, True),
    "dict_spill_nulls": (dict(use_dictionary=True, dictionary_pagesize_limit=256, compression="snappy"), 0.15, False, False),
}


def _write(shape):
    kwargs, null_rate, required, _ = SHAPES[shape]
    seed = sum(map(ord, shape))
    vals = _values(seed, null_rate)
    rnd = random.Random(seed + 1)
    t_col = [rnd.choice(["x", "y", "z"]) for _ in range(N)]
    tbl = pa.table(
        {
            "k": pa.array(range(N), type=pa.int64()),
            "s": pa.array(vals, type=pa.string()),
            "t": pa.array(t_col, type=pa.string()),
        }
    )
    if required:
        fields = [f.with_nullable(False) if f.name == "s" else f for f in tbl.schema]
        tbl = tbl.cast(pa.schema(fields))
    _WS_COUNTER[0] += 1
    ws = f"ws_lenonly_{_WS_COUNTER[0]}"
    tmp = tempfile.mkdtemp()
    data_dir = os.path.join(tmp, ws, "t")
    os.makedirs(data_dir)
    path = os.path.join(data_dir, "data.parquet")
    # Small pages: many per chunk, so page boundaries and the dictionary spill occur.
    pq.write_table(tbl, path, row_group_size=2_500, data_page_size=2_048, **kwargs)

    md = pq.ParquetFile(path).metadata
    encodings = set()
    null_counts = []
    for rg in range(md.num_row_groups):
        col = md.row_group(rg).column(1)
        encodings |= set(col.encodings)
        null_counts.append(col.statistics.null_count if col.statistics is not None else None)
    if shape.startswith("dict_spill"):
        assert "RLE_DICTIONARY" in encodings and "PLAIN" in encodings, (shape, encodings)
    elif kwargs["use_dictionary"]:
        assert "RLE_DICTIONARY" in encodings, (shape, encodings)
    else:
        assert "RLE_DICTIONARY" not in encodings, (shape, encodings)
    # The decoder trusts a footer null_count of 0 (or a REQUIRED column) — assert the
    # fixture really gives the decoder that proof, or the lack of it.
    if null_rate == 0.0:
        assert all(c == 0 for c in null_counts), (shape, null_counts)
    return tmp, ws, vals, t_col


def _query(tmp, ws, sql):
    cwd = os.getcwd()
    os.chdir(tmp)
    try:
        opteryx.register_workspace(ws, DiskConnector)
        rows = []
        for morsel in opteryx.session().execute_to_morsels(sql.format(T=f"{ws}.t")):
            cols = [morsel.column(n).to_pylist() for n in morsel.column_names]
            rows.extend(zip(*cols))
        return rows
    finally:
        os.chdir(cwd)


@pytest.fixture(scope="module", params=sorted(SHAPES))
def dataset(request):
    return (request.param,) + _write(request.param)


def test_count_and_sum_of_length_where_not_empty(dataset):
    """`WHERE s <> ''` is IsNotEmpty and LENGTH(s) is length-answerable, so `s` is
    length-only; the answers must be the full-decode answers."""
    shape, tmp, ws, vals, _ = dataset
    present = [v for v in vals if v is not None and v != ""]
    rows = _query(tmp, ws, "SELECT COUNT(*), SUM(LENGTH(s)) FROM {T} WHERE s <> ''")
    assert rows == [(len(present), sum(len(v) for v in present))], shape


def test_grouped_average_length(dataset):
    shape, tmp, ws, vals, t_col = dataset
    rows = _query(
        tmp, ws, "SELECT t, AVG(LENGTH(s)), COUNT(*) FROM {T} WHERE s <> '' GROUP BY t ORDER BY t"
    )
    expected = []
    for t in ("x", "y", "z"):
        lens = [len(v) for v, tt in zip(vals, t_col) if tt == t and v is not None and v != ""]
        if lens:
            expected.append((t, sum(lens) / len(lens), len(lens)))
    assert len(rows) == len(expected), shape
    for got, want in zip(rows, expected):
        assert got[0] == want[0] and got[2] == want[2], shape
        assert abs(got[1] - want[1]) < 1e-9, shape


def test_length_projected_next_to_another_column(dataset):
    """Per-row lengths line up with the row they belong to (offset/length bookkeeping
    across pages and the dictionary spill)."""
    shape, tmp, ws, vals, _ = dataset
    rows = _query(tmp, ws, "SELECT k, LENGTH(s) FROM {T} ORDER BY k")
    assert len(rows) == N, shape
    for k, length in rows:
        want = None if vals[k] is None else len(vals[k])
        assert length == want, (shape, k)


def test_inline_boundary_emptiness(dataset):
    """Values at 11/12/13 bytes sit either side of the slot's inline limit."""
    shape, tmp, ws, vals, _ = dataset
    for width in (11, 12, 13):
        rows = _query(tmp, ws, f"SELECT COUNT(*) FROM {{T}} WHERE LENGTH(s) = {width}")
        assert rows == [(sum(1 for v in vals if v is not None and len(v) == width),)], (shape, width)


def test_stub_path_engaged_only_when_provable(dataset):
    shape, tmp, ws, vals, _ = dataset
    rp.reset_cpp_telemetry()
    _query(tmp, ws, "SELECT COUNT(*), SUM(LENGTH(s)) FROM {T} WHERE s <> ''")
    tel = rp.get_cpp_telemetry()
    expected = SHAPES[shape][3]
    if expected:
        assert tel["ba_stub_chunks"] > 0, (shape, tel)
    else:
        assert tel["ba_stub_chunks"] == 0, (shape, tel)


def test_full_payload_reads_are_unaffected(dataset):
    """The same column read RAW (not length-only) must return the real bytes."""
    shape, tmp, ws, vals, _ = dataset
    rows = _query(tmp, ws, "SELECT k, s FROM {T} ORDER BY k")
    assert [r[1] for r in rows] == vals, shape
    rp.reset_cpp_telemetry()
    _query(tmp, ws, "SELECT k, s FROM {T}")
    assert rp.get_cpp_telemetry()["ba_stub_chunks"] == 0, shape
