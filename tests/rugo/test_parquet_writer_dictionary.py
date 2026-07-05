# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Dictionary encoding (RLE_DICTIONARY) for the native rugo parquet writer.

A column is dictionary-encoded when it arrives compressed-shaped (the engine
already built a dictionary -> PRESERVE, zero re-hash) or when a dense column is
low-cardinality enough to auto-dictionary. Otherwise PLAIN. `dictionary=False`
forces PLAIN everywhere.

PyArrow is the read-side oracle (the hard acceptance criterion: everything we
write must be PyArrow-readable). rugo's own reader is exercised on the
write -> read -> write PRESERVE path.
"""

import io
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from rugo.parquet import write_parquet
from rugo import parquet as rugo_parquet


def _morsel(sql: str):
    morsels = list(opteryx.session().execute_to_morsels(sql))
    assert len(morsels) >= 1
    return morsels[0]


def _meta(buf: bytes, col: int = 0):
    """(used_dict, encodings, values) for a single-row-group file."""
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(io.BytesIO(buf))
    cc = pf.metadata.row_group(0).column(col)
    used_dict = ("RLE_DICTIONARY" in cc.encodings) and (cc.dictionary_page_offset is not None)
    return used_dict, cc.encodings, pf.read().column(col).to_pylist()


# A low-cardinality column repeated well past the 2x-repetition gate.
_LOWCARD_STR = "SELECT * FROM (VALUES " + ",".join(
    "('%s')" % v for v in (["a", "b", "c"] * 8)
) + ") AS t(s)"
_LOWCARD_INT = "SELECT * FROM (VALUES " + ",".join(
    "(%d)" % v for v in ([10, 20, 30] * 8)
) + ") AS t(i)"


@pytest.mark.parametrize("compression", ["zstd", "none"])
def test_lowcard_string_auto_dictionary(compression):
    used, encs, vals = _meta(write_parquet(_morsel(_LOWCARD_STR), compression=compression))
    assert used, f"expected dictionary encoding, got {encs}"
    assert vals == (["a", "b", "c"] * 8)


@pytest.mark.parametrize("compression", ["zstd", "none"])
def test_lowcard_int_auto_dictionary(compression):
    used, encs, vals = _meta(write_parquet(_morsel(_LOWCARD_INT), compression=compression))
    assert used, f"expected dictionary encoding, got {encs}"
    assert vals == ([10, 20, 30] * 8)


def test_high_cardinality_falls_back_to_plain():
    """All-distinct values exceed the cardinality gate -> PLAIN, not dictionary."""
    sql = "SELECT * FROM (VALUES ('a'),('b'),('c'),('d'),('e'),('f')) AS t(s)"
    used, encs, vals = _meta(write_parquet(_morsel(sql)))
    assert not used, f"expected PLAIN fallback, got {encs}"
    assert vals == ["a", "b", "c", "d", "e", "f"]


def test_dictionary_false_forces_plain():
    used, encs, _ = _meta(write_parquet(_morsel(_LOWCARD_STR), dictionary=False))
    assert not used, f"dictionary=False must emit PLAIN, got {encs}"


def test_wide_dictionary_bitpacked_roundtrip():
    """20 distinct codes => bit_width 5, exercising the bit-packed index path."""
    expected = [x % 20 for x in range(80)]
    sql = "SELECT * FROM (VALUES " + ",".join("(%d)" % v for v in expected) + ") AS t(i)"
    used, encs, vals = _meta(write_parquet(_morsel(sql)))
    assert used, f"expected dictionary encoding, got {encs}"
    assert vals == expected


def test_dictionary_with_interior_nulls():
    sql = (
        "SELECT * FROM (VALUES ('x'),(NULL),('y'),('x'),('x'),(NULL),"
        "('y'),('x'),('x'),('y'),('x'),(NULL)) AS t(s)"
    )
    used, encs, vals = _meta(write_parquet(_morsel(sql)))
    assert used, f"expected dictionary encoding, got {encs}"
    assert vals == ["x", None, "y", "x", "x", None, "y", "x", "x", "y", "x", None]


@pytest.mark.parametrize("sql,expected", [
    (_LOWCARD_INT, [10, 20, 30] * 8),
    (_LOWCARD_STR, ["a", "b", "c"] * 8),
])
def test_preserve_dict_shape_roundtrip(sql, expected):
    """write (dict) -> rugo-read (yields a dict-shaped vector) -> write again
    keeps the dictionary (PRESERVE path) and the values, via both readers."""
    buf = write_parquet(_morsel(sql))
    with rugo_parquet.read_parquet(buf) as reader:
        morsel2 = list(reader)[0]
    buf2 = write_parquet(morsel2)
    used, encs, vals = _meta(buf2)
    assert used, f"PRESERVE path should keep the dictionary, got {encs}"
    assert vals == expected


def test_dict_min_max_stats_exact():
    """Dictionary columns still carry exact min/max statistics (codes-aware)."""
    import pyarrow.parquet as pq

    sql = _LOWCARD_INT  # values 10/20/30
    buf = write_parquet(_morsel(sql))
    used, _, _ = _meta(buf)
    assert used
    st = pq.ParquetFile(io.BytesIO(buf)).metadata.row_group(0).column(0).statistics
    assert st.min == 10
    assert st.max == 30
    assert st.null_count == 0


def _dictionary_values(buf: bytes, colname: str):
    """Physical dictionary values, in code order, as PyArrow sees them. Only
    BYTE_ARRAY columns can be surfaced as a DictionaryArray via read_dictionary;
    numeric dict ordering is verified through rugo's own reader (reader phase)."""
    import pyarrow.parquet as pq

    t = pq.read_table(io.BytesIO(buf), read_dictionary=[colname])
    return t.column(colname).combine_chunks().dictionary.to_pylist()


# First-seen order (c,a,b / 30,10,20) deliberately differs from sorted order,
# so a correct remap is required for the values to round-trip.
_UNSORTED_STR = "SELECT * FROM (VALUES " + ",".join(
    "('%s')" % v for v in (["c", "a", "b"] * 8)
) + ") AS t(s)"
_UNSORTED_INT = "SELECT * FROM (VALUES " + ",".join(
    "(%d)" % v for v in ([30, 10, 20] * 8)
) + ") AS t(i)"


@pytest.mark.parametrize("compression", ["zstd", "none"])
def test_sorted_dictionary_string_roundtrip(compression):
    buf = write_parquet(_morsel(_UNSORTED_STR), compression=compression)
    used, encs, vals = _meta(buf)
    assert used, f"expected dictionary encoding, got {encs}"
    assert vals == (["c", "a", "b"] * 8)  # values preserved despite remap
    assert _dictionary_values(buf, "s") == ["a", "b", "c"]  # dict ascending


@pytest.mark.parametrize("compression", ["zstd", "none"])
def test_sorted_dictionary_int_roundtrip(compression):
    buf = write_parquet(_morsel(_UNSORTED_INT), compression=compression)
    used, encs, vals = _meta(buf)
    assert used, f"expected dictionary encoding, got {encs}"
    # first-seen order (30,10,20) != sorted order: values preserved => the
    # code remap onto the sorted dictionary is correct.
    assert vals == ([30, 10, 20] * 8)


def test_sorted_dictionary_with_nulls_roundtrip():
    sql = (
        "SELECT * FROM (VALUES ('y'),(NULL),('x'),('z'),('x'),(NULL),"
        "('y'),('x'),('z'),('y'),('x'),(NULL)) AS t(s)"
    )
    buf = write_parquet(_morsel(sql))
    used, _, vals = _meta(buf)
    assert used
    assert vals == ["y", None, "x", "z", "x", None, "y", "x", "z", "y", "x", None]
    assert _dictionary_values(buf, "s") == ["x", "y", "z"]


if __name__ == "__main__":
    test_sorted_dictionary_string_roundtrip("zstd")
    test_sorted_dictionary_string_roundtrip("none")
    test_sorted_dictionary_int_roundtrip("zstd")
    test_sorted_dictionary_int_roundtrip("none")
    test_sorted_dictionary_with_nulls_roundtrip()
    test_lowcard_string_auto_dictionary("zstd")
    test_lowcard_string_auto_dictionary("none")
    test_lowcard_int_auto_dictionary("zstd")
    test_lowcard_int_auto_dictionary("none")
    test_high_cardinality_falls_back_to_plain()
    test_dictionary_false_forces_plain()
    test_wide_dictionary_bitpacked_roundtrip()
    test_dictionary_with_interior_nulls()
    test_preserve_dict_shape_roundtrip(_LOWCARD_INT, [10, 20, 30] * 8)
    test_preserve_dict_shape_roundtrip(_LOWCARD_STR, ["a", "b", "c"] * 8)
    test_dict_min_max_stats_exact()
    print("✅ okay")
