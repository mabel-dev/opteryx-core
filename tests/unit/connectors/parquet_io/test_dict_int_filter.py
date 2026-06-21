"""
Regression test: dictionary-encoded INT column equality/IN filters.

Dictionary-encoded int32/int64 parquet columns now reach the engine as §11
"compressed" (Dict-shaped) DrakenVectors instead of being materialised dense, so
the existing dict-aware comparison kernel fires (compare the unique values, not
every row). Additionally, when a pushed equality/IN predicate's needles are
disjoint from a row group's dictionary, the worker skips decoding that row
group's data pages entirely (Phase 2) and emits a constant all-non-match column.

These tests force multiple row groups with high-repeat (dictionary-encoded) int
columns where the needle lives in only some row groups — exercising both the
decode-skip path (needle absent) and the normal dict-decode path (needle
present) in one scan — and assert the answers match a direct computation.
"""

import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import opteryx
from opteryx.connectors import DiskConnector


_WS_COUNTER = [0]


def _unique_ws():
    _WS_COUNTER[0] += 1
    return f"ws_dictint_{_WS_COUNTER[0]}"


def _scalar(sql, ws, table, values, *, dtype, nullable=False, row_group_size=1000):
    """Write a single int column `v` (dictionary-encoded, multiple row groups),
    run `sql` against it, and return the flat result rows."""
    arr = pa.array(values, type=dtype)
    tbl = pa.table({"v": arr})
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, ws, table)
        os.makedirs(data_dir)
        # use_dictionary=True (default) + small row groups → per-RG dictionaries,
        # each spanning only part of the value range.
        pq.write_table(
            tbl,
            os.path.join(data_dir, "data.parquet"),
            use_dictionary=True,
            row_group_size=row_group_size,
        )
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            opteryx.register_workspace(ws, DiskConnector)
            out = []
            for m in opteryx.session().execute_to_morsels(sql.format(ws=ws, t=table)):
                out.extend(m.column(b"v").to_pylist())
            return out
        finally:
            os.chdir(cwd)


def _build_values():
    # 10 row groups of 1000 rows. The needle 777777 appears only in row groups
    # 3 and 7 (so the other 8 row groups are decode-skipped on `= 777777`).
    needle = 777777
    blocks = []
    for rg in range(10):
        base = rg * 1000  # distinct value range per row group (high repeat)
        block = [base + (i % 50) for i in range(1000)]  # 50 unique values / RG
        if rg in (3, 7):
            block[10] = needle
            block[990] = needle
        blocks.append(block)
    values = [x for b in blocks for x in b]
    return values, needle


def test_int64_equality_present_in_some_row_groups():
    values, needle = _build_values()
    ws = _unique_ws()
    rows = _scalar(
        "SELECT v FROM {ws}.{t} WHERE v = " + str(needle),
        ws, "t64", values, dtype=pa.int64(),
    )
    expected = [v for v in values if v == needle]
    assert sorted(rows) == sorted(expected), (len(rows), len(expected))
    assert len(rows) == 4  # 2 row groups * 2 rows each


def test_int64_equality_absent_everywhere():
    values, _ = _build_values()
    ws = _unique_ws()
    rows = _scalar(
        "SELECT v FROM {ws}.{t} WHERE v = -12345",  # in no dictionary
        ws, "t64a", values, dtype=pa.int64(),
    )
    assert rows == []


def test_int32_equality():
    values, needle = _build_values()
    ws = _unique_ws()
    rows = _scalar(
        "SELECT v FROM {ws}.{t} WHERE v = " + str(needle),
        ws, "t32", values, dtype=pa.int32(),
    )
    assert len(rows) == 4
    assert all(v == needle for v in rows)


def test_int64_in_list_mixed_present_absent():
    values, needle = _build_values()
    ws = _unique_ws()
    rows = _scalar(
        "SELECT v FROM {ws}.{t} WHERE v IN (" + str(needle) + ", -1, -2)",
        ws, "tin", values, dtype=pa.int64(),
    )
    expected = [v for v in values if v == needle]
    assert sorted(rows) == sorted(expected)
    assert len(rows) == 4


def test_int64_in_list_all_absent():
    values, _ = _build_values()
    ws = _unique_ws()
    rows = _scalar(
        "SELECT v FROM {ws}.{t} WHERE v IN (-1, -2, -3)",
        ws, "tin0", values, dtype=pa.int64(),
    )
    assert rows == []


def _temporal(sql, ws, table, arr, *, row_group_size=1000):
    """Write a single dict-encoded column `v` of a temporal type, run `sql`,
    return flat result rows."""
    tbl = pa.table({"v": arr})
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, ws, table)
        os.makedirs(data_dir)
        pq.write_table(
            tbl, os.path.join(data_dir, "data.parquet"),
            use_dictionary=True, row_group_size=row_group_size,
        )
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            opteryx.register_workspace(ws, DiskConnector)
            out = []
            for m in opteryx.session().execute_to_morsels(sql.format(ws=ws, t=table)):
                out.extend(m.column(b"v").to_pylist())
            return out
        finally:
            os.chdir(cwd)


def test_date32_dict_range_filter():
    # 10 dict-encoded row groups; dates clustered per RG so a range hits some.
    import datetime
    base = datetime.date(2013, 1, 1)
    days = []
    for rg in range(10):
        for i in range(1000):
            days.append(base + datetime.timedelta(days=rg * 30 + (i % 15)))
    arr = pa.array(days, type=pa.date32())
    ws = _unique_ws()
    rows = _temporal(
        "SELECT v FROM {ws}.{t} WHERE v >= CAST('2013-07-01' AS DATE)",
        ws, "td", arr,
    )
    expected = [d for d in days if d >= datetime.date(2013, 7, 1)]
    assert len(rows) == len(expected), (len(rows), len(expected))
    assert min(rows) >= datetime.date(2013, 7, 1)


def test_timestamp_dict_range_filter():
    import datetime
    base = datetime.datetime(2013, 1, 1)
    ts = []
    for rg in range(10):
        for i in range(1000):
            ts.append(base + datetime.timedelta(hours=rg * 100 + (i % 20)))
    arr = pa.array(ts, type=pa.timestamp("us"))
    ws = _unique_ws()
    cutoff = datetime.datetime(2013, 1, 20)
    rows = _temporal(
        "SELECT v FROM {ws}.{t} WHERE v >= CAST('2013-01-20 00:00:00' AS TIMESTAMP)",
        ws, "tt", arr,
    )
    expected = [t for t in ts if t >= cutoff]
    assert len(rows) == len(expected), (len(rows), len(expected))


def test_float64_dict_range_and_equality():
    # Dict-encoded float64 across row groups; range + equality vs direct compute.
    vals = []
    for rg in range(10):
        for i in range(1000):
            vals.append(round(rg * 100.0 + (i % 25) * 0.5, 1))
    arr = pa.array(vals, type=pa.float64())
    ws = _unique_ws()
    rows = _temporal("SELECT v FROM {ws}.{t} WHERE v >= 500.0", ws, "tf64", arr)
    expected = [v for v in vals if v >= 500.0]
    assert len(rows) == len(expected), (len(rows), len(expected))
    ws2 = _unique_ws()
    rows2 = _temporal("SELECT v FROM {ws}.{t} WHERE v = 100.5", ws2, "tf64b", arr)
    assert sorted(rows2) == sorted([v for v in vals if v == 100.5])


def test_float32_dict_data_integrity():
    # float32 dict column data integrity (projection round-trip).
    vals = []
    for rg in range(10):
        for i in range(1000):
            vals.append(float(rg * 100 + (i % 25)))
    arr = pa.array(vals, type=pa.float32())
    ws = _unique_ws()
    rows = _temporal("SELECT v FROM {ws}.{t}", ws, "tf32", arr)
    assert sorted(rows) == sorted(vals), (len(rows), len(vals))


def _float32_filter(sql, ws, table, vals, *, use_dictionary, row_group_size=1000):
    """Write a float32 column `v`, run `sql`, return flat result rows. The
    `use_dictionary` flag lets a single test cover both the plain (non-dict)
    and dict-encoded scan paths."""
    arr = pa.array(vals, type=pa.float32())
    tbl = pa.table({"v": arr})
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = os.path.join(tmp, ws, table)
        os.makedirs(data_dir)
        pq.write_table(
            tbl, os.path.join(data_dir, "data.parquet"),
            use_dictionary=use_dictionary, row_group_size=row_group_size,
        )
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            opteryx.register_workspace(ws, DiskConnector)
            out = []
            for m in opteryx.session().execute_to_morsels(sql.format(ws=ws, t=table)):
                out.extend(m.column(b"v").to_pylist())
            return out
        finally:
            os.chdir(cwd)


def test_float32_vs_float64_literal_comparison():
    # REGRESSION: comparing a float32 (REAL) column against a float64 literal
    # returned wrong answers because compare_vector dispatched on the left
    # operand's type (FLOAT32) and read the FLOAT64 constant's 8 bytes through a
    # 4-byte float* — e.g. the double 5.0 (0x4014000000000000) reads as float
    # 0.0. The fix widens the FLOAT32 operand to FLOAT64 before dispatch. Verify
    # against BOTH a plain (non-dict) and a dict-encoded float32 column.
    vals = [float(i % 25) for i in range(1000)]
    cases = [
        ("v < 250.0", lambda v: v < 250.0),
        ("v = 5.0", lambda v: v == 5.0),
        ("v = 0.0", lambda v: v == 0.0),
        ("v > 20.0", lambda v: v > 20.0),
        ("v <= 4.0", lambda v: v <= 4.0),
        ("v >= 5.0 AND v < 10.0", lambda v: 5.0 <= v < 10.0),
    ]
    for use_dict in (False, True):
        for where, pred in cases:
            ws = _unique_ws()
            rows = _float32_filter(
                "SELECT v FROM {ws}.{t} WHERE " + where, ws, "tf32f",
                vals, use_dictionary=use_dict,
            )
            expected = sorted(v for v in vals if pred(v))
            assert sorted(rows) == expected, (
                use_dict, where, len(rows), len(expected))


def test_float32_vs_float64_arithmetic():
    # REGRESSION: arithmetic between a float32 column and a float64 operand
    # (literal or column) used to throw "cross-type vector arithmetic not
    # supported" — the literal reaches the engine as a FLOAT64 constant vector,
    # so it takes the vector-vector path. The fix widens the FLOAT32 operand to
    # FLOAT64 (the numeric result type of float32 op float64) before dispatch.
    vals = [float(i % 25) for i in range(1000)]
    f32 = pa.array(vals, type=pa.float32())
    f64 = pa.array([v * 2.0 for v in vals], type=pa.float64())

    def run(sql, arr_map, use_dict, col=b"r"):
        tbl = pa.table(arr_map)
        ws = _unique_ws()
        with tempfile.TemporaryDirectory() as tmp:
            dd = os.path.join(tmp, ws, "t")
            os.makedirs(dd)
            pq.write_table(
                tbl, os.path.join(dd, "data.parquet"),
                use_dictionary=use_dict, row_group_size=1000,
            )
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                opteryx.register_workspace(ws, DiskConnector)
                out = []
                for m in opteryx.session().execute_to_morsels(sql.format(ws=ws)):
                    out.extend(m.column(col).to_pylist())
                return out
            finally:
                os.chdir(cwd)

    def approx(rows, expected):
        assert len(rows) == len(expected), (len(rows), len(expected))
        for r, e in zip(sorted(rows), sorted(expected)):
            assert abs(r - e) < 1e-4, (r, e)

    for use_dict in (False, True):
        # float32 column op float64 literal
        approx(run("SELECT v + 0.5 AS r FROM {ws}.t", {"v": f32}, use_dict),
               [v + 0.5 for v in vals])
        approx(run("SELECT v * 1.5 AS r FROM {ws}.t", {"v": f32}, use_dict),
               [v * 1.5 for v in vals])
        # float32 column op float64 column (both operand orders)
        approx(run("SELECT a + b AS r FROM {ws}.t", {"a": f32, "b": f64}, use_dict),
               [vals[i] + vals[i] * 2.0 for i in range(len(vals))])
        approx(run("SELECT b - a AS r FROM {ws}.t", {"a": f32, "b": f64}, use_dict),
               [vals[i] * 2.0 - vals[i] for i in range(len(vals))])


def _string(sql, ws, table, values, *, row_group_size=1000):
    arr = pa.array(values, type=pa.string())
    return _temporal(sql, ws, table, arr, row_group_size=row_group_size)


def test_string_dict_equality_and_inequality():
    # Dict-encoded string column; equality + <> '' filters exercise the new
    # constant->scalar reduction on the dict-shaped VARCHAR (Phase 1).
    vals = []
    for rg in range(10):
        for i in range(1000):
            vals.append("" if i % 7 == 0 else f"phrase_{rg}_{i % 30}")
    ws = _unique_ws()
    rows = _string("SELECT v FROM {ws}.{t} WHERE v = 'phrase_3_5'", ws, "ts", vals)
    assert sorted(rows) == sorted([v for v in vals if v == "phrase_3_5"])
    ws2 = _unique_ws()
    rows2 = _string("SELECT v FROM {ws}.{t} WHERE v <> ''", ws2, "ts2", vals)
    assert len(rows2) == len([v for v in vals if v != ""])


def _string_blocks():
    # 10 dict-encoded row groups; per-group prefix "rgN" so equality / LIKE-prefix
    # hit only some groups (others decode-skipped). No '_' so the LIKE→_STARTS_WITH
    # / InStr rewrites fire (underscore is a LIKE wildcard and blocks them).
    return [f"rg{rg}item{i % 40}" for rg in range(10) for i in range(1000)]


def test_string_dict_equality_skip():
    vals = _string_blocks()
    rows = _string("SELECT v FROM {ws}.{t} WHERE v = 'rg3item5'", _unique_ws(), "se", vals)
    assert sorted(rows) == sorted([v for v in vals if v == "rg3item5"])
    assert len(rows) == 25  # rg3 only (i%40==5 → 25/1000)


def test_string_dict_like_prefix_skip():
    # 'rg7%' → _STARTS_WITH('rg7'); only row group 7 survives, rest decode-skipped.
    vals = _string_blocks()
    rows = _string("SELECT v FROM {ws}.{t} WHERE v LIKE 'rg7%'", _unique_ws(), "sp", vals)
    assert sorted(rows) == sorted([v for v in vals if v.startswith("rg7")])
    assert len(rows) == 1000


def test_string_dict_like_contains_skip():
    # '%item5%' → InStr('item5').
    vals = _string_blocks()
    rows = _string("SELECT v FROM {ws}.{t} WHERE v LIKE '%item5%'", _unique_ws(), "sc", vals)
    assert sorted(rows) == sorted([v for v in vals if "item5" in v])


def test_string_dict_like_absent_everywhere():
    vals = _string_blocks()
    rows = _string("SELECT v FROM {ws}.{t} WHERE v LIKE 'nope%'", _unique_ws(), "sa", vals)
    assert rows == []


if __name__ == "__main__":
    test_int64_equality_present_in_some_row_groups()
    test_int64_equality_absent_everywhere()
    test_int32_equality()
    test_int64_in_list_mixed_present_absent()
    test_int64_in_list_all_absent()
    test_date32_dict_range_filter()
    test_timestamp_dict_range_filter()
    test_float64_dict_range_and_equality()
    test_float32_dict_data_integrity()
    test_float32_vs_float64_literal_comparison()
    test_float32_vs_float64_arithmetic()
    test_string_dict_equality_and_inequality()
    test_string_dict_equality_skip()
    test_string_dict_like_prefix_skip()
    test_string_dict_like_contains_skip()
    test_string_dict_like_absent_everywhere()
    print("✅ dict int filter regression tests passed")
