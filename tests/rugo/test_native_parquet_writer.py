# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Tests for the NATIVE (zero-pyarrow) rugo parquet writer:
`rugo.parquet_writer.write_parquet(morsel) -> bytes`.

The hard acceptance criterion is that PyArrow can read every file we write.
PyArrow is used here ONLY as the read-side oracle (tests may use pyarrow).

Scope: INT64, FLOAT64, BOOL, VARCHAR/NVARCHAR/VARBINARY, DATE32, TIMESTAMP64,
DECIMAL/DECIMAL128, and all-null (NULL) columns; PLAIN encoding, ZSTD or
uncompressed, single row group. Other physical types must fail loud.
"""

import io
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from rugo.parquet_writer import write_parquet


def _write(sql: str, compression: str = "zstd") -> bytes:
    morsels = list(opteryx.session().execute_to_morsels(sql))
    assert len(morsels) >= 1
    return write_parquet(morsels[0], compression=compression)


def _read_pyarrow(buf: bytes):
    import pyarrow.parquet as pq

    table = pq.read_table(io.BytesIO(buf))
    cols = {n: table.column(n).to_pylist() for n in table.column_names}
    types = {f.name: str(f.type) for f in table.schema}
    return cols, types


@pytest.mark.parametrize("compression", ["zstd", "none"])
def test_all_types_with_nulls(compression):
    sql = """
    SELECT * FROM (VALUES
      (1, 1.5, true, 'alpha'),
      (-2, 2.25, false, ''),
      (9000000000, -3.0, true, 'gamma'),
      (NULL, NULL, NULL, NULL),
      (7, 1e300, false, 'delta')
    ) AS t(i, d, b, s)
    """
    cols, types = _read_pyarrow(_write(sql, compression=compression))
    assert cols["i"] == [1, -2, 9000000000, None, 7]
    assert cols["d"] == [1.5, 2.25, -3.0, None, 1e300]
    assert cols["b"] == [True, False, True, None, False]
    assert cols["s"] == ["alpha", "", "gamma", None, "delta"]
    assert types == {"i": "int64", "d": "double", "b": "bool", "s": "string"}


def test_zstd_is_codec_and_compresses():
    """Default codec is ZSTD, PyArrow sees it as ZSTD, and it shrinks
    compressible data."""
    import io
    import pyarrow.parquet as pq

    sql = (
        "SELECT i, 'a_repeated_string_value' AS s FROM (VALUES "
        + ",".join("(%d)" % n for n in range(2000))
        + ") AS t(i)"
    )
    z = _write(sql, compression="zstd")
    u = _write(sql, compression="none")
    assert len(z) < len(u)  # genuinely compressed
    md = pq.ParquetFile(io.BytesIO(z)).metadata
    assert md.row_group(0).column(0).compression == "ZSTD"


def test_statistics_min_max_null_count():
    """Per-column min/max/null_count must match the reader's comparison
    semantics (signed int, IEEE double, unsigned-byte string) so row-group
    pruning is correct. Wrong stats silently drop real data."""
    import io
    import pyarrow.parquet as pq

    sql = """
    SELECT * FROM (VALUES
      (10, 1.5, 'banana'),
      (-20, -3.25, 'apple'),
      (9000000000, NULL, 'cherry'),
      (NULL, 2.0, NULL)
    ) AS t(i, d, s)
    """
    buf = _write(sql)
    md = pq.ParquetFile(io.BytesIO(buf)).metadata
    rg = md.row_group(0)
    by_name = {rg.column(c).path_in_schema: rg.column(c) for c in range(md.num_columns)}

    si = by_name["i"].statistics
    assert (si.min, si.max, si.null_count) == (-20, 9000000000, 1)
    sd = by_name["d"].statistics
    assert (sd.min, sd.max, sd.null_count) == (-3.25, 2.0, 1)
    ss = by_name["s"].statistics
    assert (ss.min, ss.max, ss.null_count) == ("apple", "cherry", 1)


def test_bloom_probe_roundtrip(tmp_path):
    """Bloom filter is byte-compatible with the reader's probe: present values
    must probe True (no false negatives); absent values mostly False."""
    from rugo.parquet_reader import read_rowgroup_stats, bloom_filter_maybe_contains

    vals = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"]
    sql = "SELECT s FROM (VALUES " + ",".join("('%s')" % v for v in vals) + ") AS t(s)"
    data = _write(sql, compression="none")  # bloom_filters=True by default

    c0 = read_rowgroup_stats(data)[0]["columns"][0]
    assert c0["bloom_offset"] >= 0 and c0["bloom_length"] > 0

    p = str(tmp_path / "b.parquet")
    with open(p, "wb") as f:
        f.write(data)
    # caller encodes the candidate to plain bytes (UTF-8 for byte_array)
    assert all(
        bloom_filter_maybe_contains(p, c0["bloom_offset"], c0["bloom_length"], v.encode("utf-8"))
        for v in vals
    )
    absent = [
        bloom_filter_maybe_contains(p, c0["bloom_offset"], c0["bloom_length"], v.encode("utf-8"))
        for v in ["ZZZ", "nope", "missing", "xyzzy", "qux", "quux"]
    ]
    assert absent.count(True) == 0  # low FPP — no false positives on this sample


def test_bloom_can_be_disabled():
    from rugo.parquet_reader import read_rowgroup_stats

    sql = "SELECT s FROM (VALUES ('a'),('b'),('c')) AS t(s)"
    on = _morsel_write(sql, bloom_filters=True)
    off = _morsel_write(sql, bloom_filters=False)
    assert read_rowgroup_stats(on)[0]["columns"][0]["bloom_offset"] >= 0
    assert read_rowgroup_stats(off)[0]["columns"][0]["bloom_offset"] < 0


def _morsel_write(sql, **kw):
    m = list(opteryx.session().execute_to_morsels(sql))[0]
    return write_parquet(m, compression="none", **kw)


def test_bad_compression_fails_loud():
    sql = "SELECT i FROM (VALUES (1),(2)) AS t(i)"
    morsel = list(opteryx.session().execute_to_morsels(sql))[0]
    with pytest.raises(ValueError, match="compression must be"):
        write_parquet(morsel, compression="snappy")


@pytest.mark.parametrize("compression", ["zstd", "none"])
def test_date_timestamp_decimal(compression):
    """Phase 3: DATE32, TIMESTAMP64, DECIMAL round-trip with correct logical
    types, values, and nulls."""
    import datetime
    import decimal

    sql = """
    SELECT
      CAST(d AS DATE) AS dt,
      CAST(CAST(d AS DATE) AS TIMESTAMP) AS ts,
      CAST(v AS DECIMAL(10,2)) AS dec
    FROM (VALUES ('2020-01-01', 123.45), ('2021-06-15', -7.0), (NULL, NULL)) AS t(d, v)
    """
    cols, types = _read_pyarrow(_write(sql, compression=compression))
    assert types["dt"] == "date32[day]"
    assert types["ts"] == "timestamp[us]"
    assert types["dec"] == "decimal128(10, 2)"
    assert cols["dt"] == [datetime.date(2020, 1, 1), datetime.date(2021, 6, 15), None]
    assert cols["ts"] == [
        datetime.datetime(2020, 1, 1),
        datetime.datetime(2021, 6, 15),
        None,
    ]
    assert cols["dec"] == [decimal.Decimal("123.45"), decimal.Decimal("-7.00"), None]


def test_interval_roundtrip():
    """INTERVAL -> FLBA(12) of (months, days, millis) little-endian."""
    import struct

    m = list(
        opteryx.session().execute_to_morsels(
            "SELECT INTERVAL '3' DAY AS d, INTERVAL '90' MINUTE AS m "
            "FROM (VALUES (1),(2)) AS t(x)"
        )
    )[0]
    cols, types = _read_pyarrow(write_parquet(m, compression="none"))
    assert types["d"] == "fixed_size_binary[12]"
    mo, da, ms = struct.unpack("<III", cols["d"][0])
    assert (mo, da, ms) == (0, 3, 0)                # 3 days
    mo, da, ms = struct.unpack("<III", cols["m"][0])
    assert (mo, da, ms) == (0, 0, 90 * 60 * 1000)   # 90 minutes in millis


def test_float32_time_via_vectors():
    """FLOAT32 widens to double; TIME32/64 keep time semantics. Built directly
    since opteryx SQL has no CAST AS FLOAT / TIME producer."""
    import datetime
    import draken.draken_native as dn
    from draken.vectors.vector import Vector
    from draken.morsels.morsel import Morsel

    def one(name, nb):
        mor = Morsel.from_vectors([name], [Vector(nb)])
        cols, types = _read_pyarrow(write_parquet(mor, compression="none"))
        return types[name], cols[name]

    ty, vals = one("f", dn.vector_float32_from_sequence([1.5, 2.25, None]))
    assert ty == "double" and vals == [1.5, 2.25, None]

    times = [datetime.time(1, 0, 0), datetime.time(23, 59, 59), None]
    ty, vals = one("t32", dn.vector_time32_from_sequence(times, "ms"))
    assert ty == "time32[ms]" and vals == times
    ty, vals = one("t64", dn.vector_time64_from_sequence(times, "us"))
    assert ty == "time64[us]" and vals == times


@pytest.mark.parametrize("compression", ["zstd", "none"])
def test_array_roundtrip(compression):
    """LIST columns: null list, empty list, and null element all round-trip
    via the 3-level parquet encoding."""
    import draken.draken_native as dn
    from draken.vectors.vector import Vector
    from draken.morsels.morsel import Morsel

    def one(name, nb):
        mor = Morsel.from_vectors([name], [Vector(nb)])
        cols, types = _read_pyarrow(write_parquet(mor, compression=compression))
        return types[name], cols[name]

    ty, vals = one("a", dn.vector_array_from_sequence([[1, 2, 3], [], None, [4, None, 6]]))
    assert ty == "list<element: int64>"
    assert vals == [[1, 2, 3], [], None, [4, None, 6]]

    ty, vals = one("s", dn.vector_array_from_sequence([["x", "yy"], None, ["z"]]))
    assert ty == "list<element: string>"
    assert vals == [["x", "yy"], None, ["z"]]


def test_nested_array_fails_loud():
    """Array-of-array (non-primitive element) is out of scope — must raise."""
    import draken.draken_native as dn
    from draken.vectors.vector import Vector
    from draken.morsels.morsel import Morsel

    m = Morsel.from_vectors(
        ["n"], [Vector(dn.vector_array_from_sequence([[[1, 2], [3]], [[4]]]))]
    )
    with pytest.raises(ValueError, match="unsupported ARRAY element type"):
        write_parquet(m)


def test_decimal_stats_numeric_order():
    """DECIMAL min/max must order by numeric value (signed unscaled), not raw
    bytes — else negative values break pruning."""
    import io
    import decimal
    import pyarrow.parquet as pq

    sql = """
    SELECT CAST(v AS DECIMAL(10,2)) AS dec
    FROM (VALUES (5.0), (-99.5), (10.25)) AS t(v)
    """
    buf = _write(sql)
    st = pq.ParquetFile(io.BytesIO(buf)).metadata.row_group(0).column(0).statistics
    assert st.min == decimal.Decimal("-99.50")
    assert st.max == decimal.Decimal("10.25")
    assert st.null_count == 0


def test_all_valid_no_nulls():
    sql = "SELECT * FROM (VALUES (1,'a'),(2,'bb'),(3,'ccc')) AS t(i, s)"
    cols, _ = _read_pyarrow(_write(sql))
    assert cols["i"] == [1, 2, 3]
    assert cols["s"] == ["a", "bb", "ccc"]


def test_interior_nulls_keep_type():
    """A column with SOME nulls keeps its physical type and round-trips. (An
    ALL-null column collapses to DRAKEN_NULL at the morsel level — see
    test_null_typed_column_writes_as_int32 — a separate, typeless case.)"""
    sql = "SELECT i FROM (VALUES (1),(CAST(NULL AS INTEGER)),(3)) AS t(i)"
    cols, types = _read_pyarrow(_write(sql))
    assert cols["i"] == [1, None, 3]
    assert types["i"] == "int64"


def test_null_typed_column_writes_as_int32():
    """An all-null projection becomes DRAKEN_NULL (typeless). The writer emits
    it as an all-null INT32 column (readable, no values)."""
    sql = "SELECT CAST(NULL AS INTEGER) AS i FROM (VALUES (1),(2),(3)) AS t(x)"
    cols, types = _read_pyarrow(_write(sql))
    assert cols["i"] == [None, None, None]
    assert types["i"] == "int32"


def test_rugo_can_parse_own_footer():
    """rugo's metadata reader must understand the footer we emit."""
    from rugo.parquet_reader import read_metadata_from_bytes

    buf = _write("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(i, s)")
    md = read_metadata_from_bytes(buf)
    assert md.num_rows == 2
    names = [c.name for c in md.schema_columns]
    assert names == ["i", "s"]


def test_unsupported_type_fails_loud():
    """An FP16 embedding column is not yet supported — must raise, not skip."""
    import draken.draken_native as dn
    from draken.vectors.vector import Vector
    from draken.morsels.morsel import Morsel

    morsel = Morsel.from_vectors(
        ["e"], [Vector(dn.vector_fp16_from_sequence([[1.0, 2.0], [3.0, 4.0]], 2))]
    )
    with pytest.raises(ValueError, match="unsupported column type"):
        write_parquet(morsel)


def test_two_string_array_columns_keep_values():
    """Regression: two list<string> columns in one morsel. The first column's
    string-element bytes used to be zeroed because the ARRAY child Vector was a
    loop-local freed when the second array column rebound it — leaving captured
    StrSlice pointers dangling (length survived, bytes did not)."""
    import draken.draken_native as dn
    from draken.vectors.vector import Vector
    from draken.morsels.morsel import Morsel

    # mostly-null first column with rare populated lists (the real-data shape)
    cves = [None, None, ["CVE-2021-3490"], None, None, ["CVE-2021-31252"]]
    tags = [["x", "y"], None, ["z"], [], ["only"], None]
    morsel = Morsel.from_vectors(
        ["cves", "tags"],
        [
            Vector(dn.vector_array_from_sequence(cves)),
            Vector(dn.vector_array_from_sequence(tags)),
        ],
    )
    cols, _ = _read_pyarrow(write_parquet(morsel))
    assert cols["cves"] == cves
    assert cols["tags"] == tags


if __name__ == "__main__":
    test_all_types_with_nulls("zstd")
    test_all_types_with_nulls("none")
    test_zstd_is_codec_and_compresses()
    test_statistics_min_max_null_count()
    test_bloom_can_be_disabled()
    test_bad_compression_fails_loud()
    test_date_timestamp_decimal("zstd")
    test_date_timestamp_decimal("none")
    test_decimal_stats_numeric_order()
    test_all_valid_no_nulls()
    test_interior_nulls_keep_type()
    test_null_typed_column_writes_as_int32()
    test_rugo_can_parse_own_footer()
    test_unsupported_type_fails_loud()
    test_two_string_array_columns_keep_values()
    print("✅ okay")
