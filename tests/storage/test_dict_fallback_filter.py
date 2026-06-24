# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression: equality filter on a dictionary-encoded string column that uses
dictionary FALLBACK (mixed RLE_DICTIONARY + PLAIN data pages — what Arrow /
Spark / DuckDB emit once a dictionary outgrows its page-size limit).

The reader's dictionary-membership decode-skip evaluated the predicate against
the dictionary page and skipped the whole row group on a miss. For a fallback
chunk the dictionary is INCOMPLETE — values that spilled to PLAIN pages are not
in it — so `WHERE s = <value-in-a-plain-page>` wrongly skipped real matches and
under-counted. The fix only allows the skip when every data page is
dictionary-encoded; this test guards both the correctness fix and that the skip
optimization still fires for genuinely-absent values on a pure-dict column.

PyArrow is the writer here (test-only dependency) because it produces the
dictionary-fallback page layout our own writer never emits.
"""

import os
import shutil
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

# 'Trump' appears 4x, scattered; thousands of distinct fillers force the
# dictionary to spill to PLAIN fallback pages — 'Trump' lands in one of them.
_VALUES = ["Trump" if i % 5000 == 2500 else "user_%d" % i for i in range(20000)]


def _write(folder: str, **writer_kwargs):
    os.makedirs(folder, exist_ok=True)
    table = pa.table({"s": pa.array(_VALUES, pa.string())})
    pq.write_table(table, os.path.join(folder, "p.parquet"), **writer_kwargs)


def _query(dataset: str, value: str):
    session = opteryx.session()
    rows = sum(
        m.num_rows
        for m in session.execute_to_morsels(f"SELECT s FROM {dataset} WHERE s = '{value}'")
    )
    scan = [v for v in session.telemetry["operations"].values() if v.get("type") == "ReadRel"][0]
    return rows, scan["parquet_rows_before_filter"]


def test_dict_fallback_equality_not_dropped():
    """A value living in a PLAIN fallback page must still match (was under-counted)."""
    folder = "dictfallback_mixed_tmp"
    try:
        _write(folder, use_dictionary=True, dictionary_pagesize_limit=4096, data_page_size=4096)
        rows, _ = _query(folder, "Trump")
        assert rows == 4, rows
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def test_pure_dict_membership_skip_still_prunes():
    """The fix must not regress the dictionary-membership decode-skip: a fully
    dict-encoded chunk must still prune an absent (but in-range) value without
    decoding any rows. Written by rugo's own writer (single all-dict data page).
    """
    from rugo.parquet_writer import write_parquet

    folder = "dictfallback_pure_tmp"
    try:
        os.makedirs(folder, exist_ok=True)
        # Low cardinality -> rugo emits a single RLE_DICTIONARY data page.
        vals = ["aa", "bb", "cc"] * 40  # min='aa', max='cc'
        sql = "SELECT * FROM (VALUES " + ",".join("('%s')" % v for v in vals) + ") AS t(s)"
        morsel = list(opteryx.session().execute_to_morsels(sql))[0]
        with open(os.path.join(folder, "p.parquet"), "wb") as fh:
            fh.write(write_parquet(morsel, dictionary=True))
        # 'ab' is absent but inside [min,max] -> only a dictionary-membership
        # skip (not min/max) can prune it.
        rows, before_filter = _query(folder, "ab")
        assert rows == 0, rows
        assert before_filter == 0, before_filter  # membership-skip pruned it
    finally:
        shutil.rmtree(folder, ignore_errors=True)


if __name__ == "__main__":
    test_dict_fallback_equality_not_dropped()
    test_pure_dict_membership_skip_still_prunes()
    print("✅ okay")
