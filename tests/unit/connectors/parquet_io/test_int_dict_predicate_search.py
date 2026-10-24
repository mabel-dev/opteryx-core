"""
Integer dictionary predicate search (docs/PARQUET_RUN_DOMAIN_PREDICATE_DESIGN.md §11):
a pushed `=`, `<>`, `IN`, `NOT IN` or range conjunct on a dictionary-encoded
int column is decided once per dictionary entry and then per code straight off
the RLE / bit-packed stream, and only the rows that can pass are decoded. The
scan's own filter still runs over the survivors, so the answer must equal a
per-row Python evaluation exactly — on every shape the decoder takes a
different branch for:

  * int16 / int32 / int64 / uint32 logical widths (all physical int32/int64);
  * required and nullable columns, with and without nulls (a null never passes);
  * a dictionary that spills to PLAIN pages mid-chunk (the writer's dictionary
    page limit), so dictionary and PLAIN pages mix in one chunk;
  * many small pages, data page v1 and v2, compressed and not;
  * a second projected column and a second conjunct, so the survivors of the
    searched column line up with the other columns' rows.

Every fixture also asserts the search actually engaged (rugo's ps_* counters):
rows were decided per code, RLE runs were filled without unpacking, and fewer
codes were unpacked than rows decided — a change that quietly disarmed the
search would fail here rather than pass by testing nothing. The no-prune
canary (every dictionary entry passes) asserts the opposite: the search stays
declined and the column decodes exactly as without it.
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

N = 50_000
# Skewed: most rows carry 0 (long RLE runs, like ClickBench AdvEngineID), a
# handful of other values appear in bursts (bit-packed groups).
VALUES = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 7, 12, 52]

_WS_COUNTER = [0]


def _values(seed, null_rate):
    rnd = random.Random(seed)
    out = []
    i = 0
    while i < N:
        if null_rate and rnd.random() < null_rate:
            out.append(None)
            i += 1
            continue
        v = rnd.choice(VALUES)
        run = rnd.choice([1, 1, 1, 2, 5, 40, 300, 2000]) if v == 0 else rnd.choice([1, 1, 2, 3])
        for _ in range(min(run, N - i)):
            out.append(v)
            i += 1
    return out[:N]


SHAPES = {
    # name: (pa type, null_rate, writer kwargs)
    "int16_required": (pa.int16(), 0.0, dict(use_dictionary=True, data_page_size=4096)),
    "int16_nullable_no_nulls": (pa.int16(), 0.0, dict(use_dictionary=True, data_page_size=4096)),
    "int32_nulls": (pa.int32(), 0.05, dict(use_dictionary=True, data_page_size=4096)),
    "int64_nulls_v2": (pa.int64(), 0.05, dict(use_dictionary=True, data_page_size=4096,
                                              data_page_version="2.0")),
    "uint32_nulls": (pa.uint32(), 0.02, dict(use_dictionary=True, data_page_size=4096)),
    "int32_zstd": (pa.int32(), 0.0, dict(use_dictionary=True, data_page_size=8192,
                                        compression="zstd")),
    # A tiny dictionary page limit makes the writer spill to PLAIN pages inside
    # the chunk once the dictionary outgrows it: dictionary and PLAIN pages mix.
    "int32_spill": (pa.int32(), 0.03, dict(use_dictionary=True, data_page_size=2048,
                                          dictionary_pagesize_limit=16)),
}


def _write(shape, tmp):
    typ, null_rate, kwargs = SHAPES[shape]
    vals = _values(sum(map(ord, shape)), null_rate)
    if shape == "int16_required":
        schema = pa.schema([pa.field("k", pa.int64(), nullable=False),
                            pa.field("v", typ, nullable=False),
                            pa.field("t", pa.string(), nullable=False)])
    else:
        schema = pa.schema([("k", pa.int64()), ("v", typ), ("t", pa.string())])
    rnd = random.Random(7)
    t_col = [rnd.choice(["x", "y", "z"]) for _ in range(N)]
    tbl = pa.table({"k": pa.array(range(N), type=pa.int64()),
                    "v": pa.array(vals, type=typ),
                    "t": pa.array(t_col, type=pa.string())}, schema=schema)
    _WS_COUNTER[0] += 1
    ws = f"ws_intsearch_{_WS_COUNTER[0]}"
    data_dir = os.path.join(tmp, ws, "t")
    os.makedirs(data_dir)
    kw = dict(kwargs)
    if "compression" not in kw:
        kw["compression"] = "none"
    pq.write_table(tbl, os.path.join(data_dir, "data.parquet"), row_group_size=N, **kw)
    return ws, vals, t_col


def _query(tmp, ws, sql):
    cwd = os.getcwd()
    os.chdir(tmp)
    try:
        opteryx.register_workspace(ws, DiskConnector)
        rows = []
        for m in opteryx.session().execute_to_morsels(sql.format(T=f"{ws}.t")):
            rows.extend(zip(*[m.column(c).to_pylist() for c in m.column_names]))
        return rows
    finally:
        os.chdir(cwd)


PREDICATES = {
    "ne":      ("v <> 0",            lambda v: v is not None and v != 0),
    "eq":      ("v = 7",             lambda v: v == 7),
    "in":      ("v IN (2, 52)",      lambda v: v in (2, 52)),
    "not_in":  ("v NOT IN (0, 12)",  lambda v: v is not None and v not in (0, 12)),
    "gt":      ("v > 0",             lambda v: v is not None and v > 0),
    "between": ("v >= 3 AND v <= 12", lambda v: v is not None and 3 <= v <= 12),
    "ne_twice": ("v <> 0 AND v <> 2", lambda v: v is not None and v not in (0, 2)),
}


@pytest.fixture(scope="module", params=sorted(SHAPES))
def dataset(request):
    tmp = tempfile.mkdtemp()
    ws, vals, t_col = _write(request.param, tmp)
    return request.param, tmp, ws, vals, t_col


@pytest.mark.parametrize("pred", sorted(PREDICATES))
def test_int_search_matches_per_row_oracle(dataset, pred):
    shape, tmp, ws, vals, _t = dataset
    sql, fn = PREDICATES[pred]
    rows = _query(tmp, ws, f"SELECT k, v FROM {{T}} WHERE {sql} ORDER BY k")
    expected = [i for i, v in enumerate(vals) if fn(v)]
    assert [r[0] for r in rows] == expected, (shape, pred)
    assert all(r[1] == vals[r[0]] for r in rows), (shape, pred)


def test_int_search_count_star(dataset):
    """The ClickBench Q01 shape: no column projected, the searched column is
    filter-only."""
    shape, tmp, ws, vals, _t = dataset
    rows = _query(tmp, ws, "SELECT COUNT(*) FROM {T} WHERE v <> 0")
    assert rows == [(sum(1 for v in vals if v is not None and v != 0),)], shape


def test_int_search_with_a_second_conjunct_and_column(dataset):
    shape, tmp, ws, vals, t_col = dataset
    rows = _query(tmp, ws, "SELECT k, t FROM {T} WHERE v <> 0 AND t = 'y' ORDER BY k")
    expected = [i for i, v in enumerate(vals) if v is not None and v != 0 and t_col[i] == "y"]
    assert [r[0] for r in rows] == expected, shape
    assert all(r[1] == "y" for r in rows), shape


def test_int_search_group_by(dataset):
    """The ClickBench Q07 shape: the searched column is also projected."""
    shape, tmp, ws, vals, _t = dataset
    rows = _query(tmp, ws, "SELECT v, COUNT(*) FROM {T} WHERE v <> 0 GROUP BY v ORDER BY v")
    expected = {}
    for v in vals:
        if v is not None and v != 0:
            expected[v] = expected.get(v, 0) + 1
    assert rows == sorted(expected.items()), shape


def test_int_search_engaged(dataset):
    """The search must have decided rows per code for this shape, filling RLE
    runs without unpacking them — otherwise the oracle tests above pass while
    testing nothing."""
    shape, tmp, ws, vals, _t = dataset
    rp.reset_cpp_telemetry()
    rows = _query(tmp, ws, "SELECT COUNT(*) FROM {T} WHERE v <> 0")
    tel = rp.get_cpp_telemetry()
    assert tel["ps_pages"] > 0, (shape, tel)
    assert tel["ps_rows_out"] < tel["ps_rows_in"], (shape, tel)
    assert tel["ps_dict_entries"] > 0 and tel["ps_dict_entries_pass"] < tel["ps_dict_entries"], (shape, tel)
    assert tel["ps_runs_filled"] > 0, (shape, tel)
    assert tel["ps_codes_unpacked"] < tel["ps_rows_in"], (shape, tel)
    assert rows[0][0] == tel["ps_rows_out"] or shape == "int32_spill", (shape, tel, rows)


def test_int_search_declined_when_every_entry_passes(dataset):
    """No-prune canary: a conjunct every dictionary entry satisfies arms nothing
    — the column decodes exactly as without the search."""
    shape, tmp, ws, vals, _t = dataset
    rp.reset_cpp_telemetry()
    rows = _query(tmp, ws, "SELECT COUNT(*) FROM {T} WHERE v <> 99")
    tel = rp.get_cpp_telemetry()
    assert rows == [(sum(1 for v in vals if v is not None),)], shape
    assert tel["ps_pages"] == 0, (shape, tel)
    assert tel["ps_runs_filled"] == 0 and tel["ps_codes_unpacked"] == 0, (shape, tel)


def test_int_search_drops_a_whole_chunk_when_no_entry_passes():
    """Every dictionary entry fails: the chunk is dropped from its dictionary
    alone, no data page is decoded, and the count is still right."""
    tmp = tempfile.mkdtemp()
    _WS_COUNTER[0] += 1
    ws = f"ws_intsearch_{_WS_COUNTER[0]}"
    data_dir = os.path.join(tmp, ws, "t")
    os.makedirs(data_dir)
    # Dictionary {0, 2, 4, 6, 8}: the needles below sit inside the footer's
    # min/max (so row-group pruning cannot answer) but in no entry.
    vals = [(i % 5) * 2 for i in range(N)]
    tbl = pa.table({"k": pa.array(range(N), type=pa.int64()), "v": pa.array(vals, type=pa.int32())})
    pq.write_table(tbl, os.path.join(data_dir, "data.parquet"), row_group_size=N // 2,
                   use_dictionary=True, compression="none")
    rp.reset_cpp_telemetry()
    rows = _query(tmp, ws, "SELECT COUNT(*) FROM {T} WHERE v > 2 AND v < 4")
    tel = rp.get_cpp_telemetry()
    assert rows == [(0,)]
    assert tel["ps_dict_entries"] == 10 and tel["ps_dict_entries_pass"] == 0, tel
    assert tel["ps_pages"] == 0, tel
    rp.reset_cpp_telemetry()
    rows = _query(tmp, ws, "SELECT COUNT(*) FROM {T} WHERE v IN (3, 5)")
    tel = rp.get_cpp_telemetry()
    assert rows == [(0,)]
    assert tel["ps_dict_entries"] == 10 and tel["ps_dict_entries_pass"] == 0, tel
    assert tel["ps_pages"] == 0, tel
    rows = _query(tmp, ws, "SELECT COUNT(*) FROM {T} WHERE v IN (3, 4)")
    assert rows == [(N // 5,)]


def test_uint64_range_is_not_searched_but_membership_is():
    """A UINT64 column's entries compare as raw 64 bits: equality is exact, a
    range is not, so `>` declines the search and `=` keeps it."""
    tmp = tempfile.mkdtemp()
    _WS_COUNTER[0] += 1
    ws = f"ws_intsearch_{_WS_COUNTER[0]}"
    data_dir = os.path.join(tmp, ws, "t")
    os.makedirs(data_dir)
    big = 2**64 - 5
    vals = [0] * (N - 100) + [big] * 50 + [3] * 50
    tbl = pa.table({"k": pa.array(range(N), type=pa.int64()), "v": pa.array(vals, type=pa.uint64())})
    pq.write_table(tbl, os.path.join(data_dir, "data.parquet"), row_group_size=N,
                   use_dictionary=True, compression="none")
    rp.reset_cpp_telemetry()
    rows = _query(tmp, ws, f"SELECT COUNT(*) FROM {{T}} WHERE v > 2")
    assert rows == [(100,)]
    tel = rp.get_cpp_telemetry()
    assert tel["ps_pages"] == 0, tel
    rp.reset_cpp_telemetry()
    rows = _query(tmp, ws, f"SELECT COUNT(*) FROM {{T}} WHERE v = {big}")
    assert rows == [(50,)]
    tel = rp.get_cpp_telemetry()
    assert tel["ps_pages"] > 0 and tel["ps_rows_out"] == 50, tel
