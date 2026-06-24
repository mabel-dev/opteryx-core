# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
End-to-end sorted-dictionary flag flow on the NATIVE scan path.

A sorted parquet dictionary (writer sets DictionaryPageHeader.is_sorted) must
flow file -> decode -> ColumnOut.dict_sorted -> draken DRAKEN_DICT_KEYS_SORTED,
and survive the scan -> result boundary (the cursor's slice). Verified through a
real `execute_to_morsels` scan of a written file, reading the column vector's
`dict_keys_sorted` introspection property.

Floats are deliberately never sorted (NaN / -0.0 break monotonic code ranges),
so a float dict column must NOT carry the flag.
"""

import os
import shutil
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from rugo.parquet_writer import write_parquet


def _morsel(sql):
    return list(opteryx.session().execute_to_morsels(sql))[0]


def _scan_one(tmp_dir, sql, write_sql):
    """Write `write_sql`'s morsel to a parquet file under tmp_dir, scan `sql`."""
    os.makedirs(tmp_dir, exist_ok=True)
    try:
        buf = write_parquet(_morsel(write_sql))
        with open(os.path.join(tmp_dir, "data.parquet"), "wb") as f:
            f.write(buf)
        return list(opteryx.session().execute_to_morsels(sql))[0]
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_string_dict_keys_sorted_flag_reaches_engine():
    # low-cardinality string in non-sorted first-seen order -> sorted dict on disk
    write_sql = "SELECT * FROM (VALUES " + ",".join(
        "('%s')" % v for v in (["c", "a", "b"] * 8)
    ) + ") AS t(s)"
    mm = _scan_one("_sd_scan_str", "SELECT s FROM _sd_scan_str", write_sql)
    v = mm.column(b"s")
    assert v._nb.is_dict, "expected a dict-shaped string vector from the native scan"
    assert v._nb.dict_keys_sorted, "sorted-dictionary flag did not reach the engine"
    # values still correct despite the writer's sort+remap
    assert mm.column(b"s").to_pylist() == (["c", "a", "b"] * 8)


def test_float_dict_never_marked_sorted():
    # low-cardinality float: writer must NOT sort the dictionary, so no flag
    write_sql = "SELECT * FROM (VALUES " + ",".join(
        "(%f)" % v for v in ([3.5, 1.5, 2.5] * 8)
    ) + ") AS t(f)"
    mm = _scan_one("_sd_scan_flt", "SELECT f FROM _sd_scan_flt", write_sql)
    v = mm.column(b"f")
    if v._nb.is_dict:
        assert not v._nb.dict_keys_sorted, "float dictionaries must never be marked sorted"


def _order_by(tmp_dir, write_sql, order_sql):
    os.makedirs(tmp_dir, exist_ok=True)
    try:
        buf = write_parquet(_morsel(write_sql))
        with open(os.path.join(tmp_dir, "data.parquet"), "wb") as f:
            f.write(buf)
        out = []
        for m in opteryx.session().execute_to_morsels(order_sql):
            for x in m.column(b"s").to_pylist():
                out.append(x.decode() if isinstance(x, bytes) else x)
        return out
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_order_by_sorted_dict_string_matches_value_order():
    # ORDER BY on a sorted-dict string column takes the sort-by-code fast-path;
    # the result must equal a plain value sort (asc and desc).
    vals = ["cherry", "apple", "banana", "apple", "cherry", "banana", "date", "apple"] * 3
    write_sql = "SELECT * FROM (VALUES " + ",".join(
        "(%d,'%s')" % (i, v) for i, v in enumerate(vals)
    ) + ") AS t(id, s)"
    got_asc = _order_by("_sd_ord_a", write_sql, "SELECT s FROM _sd_ord_a ORDER BY s")
    got_desc = _order_by("_sd_ord_d", write_sql, "SELECT s FROM _sd_ord_d ORDER BY s DESC")
    assert got_asc == sorted(vals), got_asc[:8]
    assert got_desc == sorted(vals, reverse=True), got_desc[:8]


def _scalar(tmp_dir, write_sql, agg_sql, col):
    os.makedirs(tmp_dir, exist_ok=True)
    try:
        buf = write_parquet(_morsel(write_sql))
        with open(os.path.join(tmp_dir, "data.parquet"), "wb") as f:
            f.write(buf)
        for m in opteryx.session().execute_to_morsels(agg_sql):
            xs = m.column(col).to_pylist()
            if xs:
                r = xs[0]
                return r.decode() if isinstance(r, bytes) else r
        return None
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_string_min_max_sorted_dict_fast_path():
    # low-card incl. prefix strings ('app' vs 'apple', length tiebreak) and nulls
    vals = ["cherry", "app", "apple", "banana", None, "apple", "cherry", "app", "date", None] * 3
    present = [v for v in vals if v is not None]
    write_sql = "SELECT * FROM (VALUES " + ",".join(
        ("(%d,'%s')" % (i, v)) if v is not None else ("(%d,NULL)" % i)
        for i, v in enumerate(vals)
    ) + ") AS t(id, s)"
    assert _scalar("_sd_mn", write_sql, "SELECT MIN(s) AS mn FROM _sd_mn", b"mn") == min(present)
    assert _scalar("_sd_mx", write_sql, "SELECT MAX(s) AS mx FROM _sd_mx", b"mx") == max(present)


def test_string_min_max_high_cardinality_non_dict():
    # all-distinct -> not dict-encoded -> exercises the non-fast-path (and the
    # finalize fix that made string MIN/MAX work at all).
    vals = ["s%03d" % i for i in range(60)]
    write_sql = "SELECT * FROM (VALUES " + ",".join(
        "(%d,'%s')" % (i, v) for i, v in enumerate(vals)
    ) + ") AS t(id, s)"
    assert _scalar("_sd_hn", write_sql, "SELECT MIN(s) AS mn FROM _sd_hn", b"mn") == min(vals)
    assert _scalar("_sd_hx", write_sql, "SELECT MAX(s) AS mx FROM _sd_hx", b"mx") == max(vals)


if __name__ == "__main__":
    test_string_dict_keys_sorted_flag_reaches_engine()
    test_float_dict_never_marked_sorted()
    test_order_by_sorted_dict_string_matches_value_order()
    test_string_min_max_sorted_dict_fast_path()
    test_string_min_max_high_cardinality_non_dict()
    print("✅ okay")
