"""
Parquet page search (docs/PARQUET_PAGE_SEARCH_DESIGN.md): `LIKE '%x%'` and
`NOT LIKE '%x%'` evaluated on raw page bytes must return exactly the rows a
per-value `x in value` returns.

The search runs over a PLAIN byte_array value region — [len32][bytes]... — as one
buffer, ignoring value boundaries, then maps each occurrence back to its value.
Its one way to be wrong is to accept an occurrence that is not wholly inside a
value: one that straddles two values, or that overlaps a 4-byte length prefix.
The oracle data here is built to make those false candidates COMMON rather than
rare:

  * values are drawn from a tiny alphabet ("a", "b", " ") so needles occur often
    and across boundaries;
  * value lengths are drawn from a set that includes 32, 97 and 98 — the length
    prefix of such a value starts with the byte " ", "a" or "b", so the prefix
    bytes themselves look like needle bytes. A search that forgets the boundary
    check matches inside prefixes.

Every file shape the decoder takes a different branch for is covered: PLAIN and
dictionary pages, a dictionary that spills to PLAIN mid-chunk, data page v1 and
v2, nullable and required columns, compressed and not, many small pages. Each
query is also run with another column projected and another conjunct, so the
rows the search keeps are checked to line up across columns.

Each fixture asserts the search actually ran (rugo's ps_* counters), so a change
that quietly disarmed it would fail here rather than pass by testing nothing.
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
ALPHABET = "ab "
LENGTHS = [0, 1, 2, 3, 5, 32, 97, 98]
NEEDLES = ["a", "ab", "b a", "a ", " a", "aa", "bb ", "abab", "b" * 40, "ba" * 60]

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


SHAPES = {
    # name: (write_table kwargs, null_rate, required)
    "plain_v1_snappy": (dict(use_dictionary=False, data_page_version="1.0", compression="snappy"), 0.15, False),
    "plain_v2_zstd": (dict(use_dictionary=False, data_page_version="2.0", compression="zstd"), 0.15, False),
    "plain_required": (dict(use_dictionary=False, compression="none"), 0.0, True),
    "plain_nullable_no_nulls": (dict(use_dictionary=False, compression="none"), 0.0, False),
    "dict_v1": (dict(use_dictionary=True, data_page_version="1.0", compression="snappy"), 0.15, False),
    "dict_v2_required": (dict(use_dictionary=True, data_page_version="2.0", compression="zstd"), 0.0, True),
    # Dictionary outgrows its page and spills to PLAIN inside the same chunk.
    "dict_spill": (dict(use_dictionary=True, dictionary_pagesize_limit=256, compression="none"), 0.15, False),
}


def _write(shape):
    kwargs, null_rate, required = SHAPES[shape]
    seed = sum(map(ord, shape))
    vals = _values(seed, null_rate)
    rnd = random.Random(seed + 1)
    tbl = pa.table(
        {
            "k": pa.array(range(N), type=pa.int64()),
            "s": pa.array(vals, type=pa.string()),
            "t": pa.array([rnd.choice(["x", "y", "z"]) for _ in range(N)], type=pa.string()),
        }
    )
    if required:
        fields = [f.with_nullable(False) if f.name == "s" else f for f in tbl.schema]
        tbl = tbl.cast(pa.schema(fields))
    _WS_COUNTER[0] += 1
    ws = f"ws_pagesearch_{_WS_COUNTER[0]}"
    tmp = tempfile.mkdtemp()
    data_dir = os.path.join(tmp, ws, "t")
    os.makedirs(data_dir)
    path = os.path.join(data_dir, "data.parquet")
    # Small pages: many per chunk, so whole-page discards and page boundaries occur.
    pq.write_table(tbl, path, row_group_size=2_500, data_page_size=2_048, **kwargs)

    encodings = set()
    md = pq.ParquetFile(path).metadata
    for rg in range(md.num_row_groups):
        encodings |= set(md.row_group(rg).column(1).encodings)
    if shape == "dict_spill":
        assert "RLE_DICTIONARY" in encodings and "PLAIN" in encodings, encodings
    elif kwargs["use_dictionary"]:
        assert "RLE_DICTIONARY" in encodings, encodings
    else:
        assert "RLE_DICTIONARY" not in encodings, encodings
    return tmp, ws, vals


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


def _oracle(vals, needle, negate, t_filter=None, t_col=None):
    keep = []
    for k, v in enumerate(vals):
        if v is None:
            continue  # NULL [NOT] LIKE x is NULL: never kept
        hit = needle in v
        if hit == negate:
            continue
        if t_filter is not None and t_col[k] != t_filter:
            continue
        keep.append(k)
    return keep


@pytest.fixture(scope="module", params=sorted(SHAPES))
def dataset(request):
    return (request.param,) + _write(request.param)


@pytest.mark.parametrize("negate", [False, True], ids=["like", "not_like"])
@pytest.mark.parametrize("needle", NEEDLES)
def test_page_search_matches_per_value_oracle(dataset, needle, negate):
    shape, tmp, ws, vals = dataset
    op = "NOT LIKE" if negate else "LIKE"
    rows = _query(tmp, ws, f"SELECT k, s FROM {{T}} WHERE s {op} '%{needle}%' ORDER BY k")
    expected = _oracle(vals, needle, negate)
    assert [r[0] for r in rows] == expected, (shape, needle, op)
    # The surviving rows of the OTHER column must line up with the searched one.
    assert all(r[1] == vals[r[0]] for r in rows), (shape, needle, op)


@pytest.mark.parametrize("negate", [False, True], ids=["like", "not_like"])
def test_page_search_with_a_second_conjunct(dataset, negate):
    shape, tmp, ws, vals = dataset
    rnd = random.Random(sum(map(ord, shape)) + 1)
    t_col = [rnd.choice(["x", "y", "z"]) for _ in range(N)]
    op = "NOT LIKE" if negate else "LIKE"
    rows = _query(tmp, ws, f"SELECT k, t FROM {{T}} WHERE s {op} '%ab%' AND t = 'y' ORDER BY k")
    assert [r[0] for r in rows] == _oracle(vals, "ab", negate, "y", t_col), (shape, op)
    assert all(r[1] == "y" for r in rows)


def test_page_search_ran(dataset):
    """The search must actually have evaluated pages for this file shape —
    otherwise every oracle test above passes while testing nothing."""
    shape, tmp, ws, vals = dataset
    rp.reset_cpp_telemetry()
    _query(tmp, ws, "SELECT COUNT(*) FROM {T} WHERE s LIKE '%abab%'")
    tel = rp.get_cpp_telemetry()
    assert tel["ps_pages"] > 0, (shape, tel)
    assert tel["ps_rows_out"] < tel["ps_rows_in"], (shape, tel)


def test_page_search_discards_pages_without_a_hit():
    """A needle present in one value only: every other PLAIN page is dropped
    whole, and the one row still comes back."""
    rnd = random.Random(3)
    vals = ["".join(rnd.choice("ab ") for _ in range(rnd.choice(LENGTHS))) for _ in range(N)]
    vals[4321] = "zzz-the-only-needle-zzz"
    tbl = pa.table({"k": pa.array(range(N), type=pa.int64()), "s": pa.array(vals, type=pa.string())})
    _WS_COUNTER[0] += 1
    ws = f"ws_pagesearch_{_WS_COUNTER[0]}"
    tmp = tempfile.mkdtemp()
    data_dir = os.path.join(tmp, ws, "t")
    os.makedirs(data_dir)
    pq.write_table(tbl, os.path.join(data_dir, "data.parquet"), use_dictionary=False,
                   row_group_size=N, data_page_size=2_048, compression="none")
    rp.reset_cpp_telemetry()
    rows = _query(tmp, ws, "SELECT k FROM {T} WHERE s LIKE '%the-only-needle%'")
    tel = rp.get_cpp_telemetry()
    assert rows == [(4321,)]
    assert tel["ps_pages"] > 1, tel
    assert tel["ps_pages_discarded"] == tel["ps_pages"] - 1, tel
