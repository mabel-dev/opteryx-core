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
from rugo.parquet import write_parquet


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
    from rugo.rugo_native import read_rowgroup_stats, bloom_filter_maybe_contains

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


def test_bloom_probe_bytes_matches_file(tmp_path):
    """The in-memory bloom probe (TestBloomFilterBytes, used by the remote
    decode-skip) is byte-identical to the file-based probe: slice the exact bloom
    region out of a written file and confirm the bytes-probe agrees on every
    present and absent candidate. Also confirms the new adjacent layout — the
    bloom sits immediately before its column's data page."""
    from rugo.rugo_native import (
        read_rowgroup_stats,
        bloom_filter_maybe_contains,
        bloom_filter_bytes_maybe_contains,
    )

    vals = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"]
    sql = "SELECT s FROM (VALUES " + ",".join("('%s')" % v for v in vals) + ") AS t(s)"
    data = _write(sql, compression="none")  # bloom_filters=True by default

    c0 = read_rowgroup_stats(data)[0]["columns"][0]
    off, length = c0["bloom_offset"], c0["bloom_length"]
    assert off >= 0 and length > 0

    # New layout: the bloom is written immediately after the 4-byte PAR1 magic,
    # i.e. in front of this (only) column's data — not clustered at the row-group
    # tail as the old layout did (which would put it at a high offset past the
    # column data). This is what makes the adjacent single-fetch decode-skip work.
    assert off == 4, off

    bloom_bytes = data[off:off + length]

    p = str(tmp_path / "b.parquet")
    with open(p, "wb") as f:
        f.write(data)

    for v in vals + ["ZZZ", "nope", "missing", "xyzzy", "qux", "quux"]:
        vb = v.encode("utf-8")
        file_probe = bloom_filter_maybe_contains(p, off, length, vb)
        bytes_probe = bloom_filter_bytes_maybe_contains(bloom_bytes, vb)
        assert file_probe == bytes_probe, v
    # And the semantic contract still holds on the bytes probe.
    assert all(bloom_filter_bytes_maybe_contains(bloom_bytes, v.encode("utf-8")) for v in vals)


def test_bloom_can_be_disabled():
    from rugo.rugo_native import read_rowgroup_stats

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
    """FLOAT32 stays parquet `float`; TIME32/64 keep time semantics. Built
    directly since opteryx SQL has no CAST AS FLOAT / TIME producer.

    ⛔ This assertion was `== "double"` until 2026-08-21. The writer widened
    FLOAT32 to a parquet float64 column — lossless per VALUE, but the file then
    DECLARED float64, so no reader could recover the 4-byte column and rugo
    could not round-trip a FLOAT32 vector at all. A declared width that is not
    the stored width is the whole silent-wrong-rows class (see
    `tests/sql/test_narrow_width_column_predicates.py`), so the physical type
    is pinned here, not just the values."""
    import datetime
    import draken.draken_native as dn
    from draken.vectors.vector import Vector
    from draken.morsels.morsel import Morsel

    def one(name, nb):
        mor = Morsel.from_vectors([name], [Vector(nb)])
        cols, types = _read_pyarrow(write_parquet(mor, compression="none"))
        return types[name], cols[name]

    ty, vals = one("f", dn.vector_float32_from_sequence([1.5, 2.25, None]))
    assert ty == "float" and vals == [1.5, 2.25, None]

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


def test_array_column_row_group_splitting():
    """max_rows_per_row_group must actually split a schema containing an ARRAY
    column into multiple row groups, not silently collapse to one.

    Regression test for two bugs found together: (1) WriteParquet used to zero
    out max_rows_per_rg the instant any column was is_array, silently ignoring
    the caller's request with no error; (2) once real per-row-group array
    slicing was added, the footer writer read column metadata (num_levels /
    num_elements) from the GLOBAL unsliced ColumnInput instead of the
    per-row-group slice, so every row group's array column reported the
    full-file element count -- rugo's own reader then failed to decode the
    array column in ANY row group (PyArrow tolerated it; rugo did not).
    """
    import draken.draken_native as dn
    from draken.vectors.vector import Vector
    from draken.morsels.morsel import Morsel
    from rugo.parquet import read_parquet
    from rugo.rugo_native import read_rowgroup_stats

    # 23 rows so requested rg=7 (rounds up to 8) produces an uneven 8/8/7 split,
    # with nulls and empty lists straddling row-group boundaries.
    data = ([[1, 2], [], None, [3]] * 5) + [[9, 9]] * 3
    ids = list(range(len(data)))
    m = Morsel.from_vectors(
        ["id", "arr"],
        [
            Vector(dn.vector_from_sequence(ids)),
            Vector(dn.vector_array_from_sequence(data)),
        ],
    )
    buf = write_parquet(m, compression="zstd", max_rows_per_row_group=7)

    n_rg = len(read_rowgroup_stats(buf))
    assert n_rg == 3, f"expected 3 row groups (8/8/7), got {n_rg} -- splitting was silently ignored"

    # rugo must be able to read back its own file, every row group present.
    out_ids, out_arr = [], []
    with read_parquet(buf) as reader:
        for morsel in reader:
            assert b"arr" in morsel.column_names, "array column missing from a row group"
            out_ids.extend(morsel.column(b"id").to_pylist())
            out_arr.extend(morsel.column(b"arr").to_pylist())
    assert out_ids == ids
    assert out_arr == data

    # PyArrow must agree independently.
    cols, _ = _read_pyarrow(buf)
    assert cols["id"] == ids
    assert cols["arr"] == data


def _count_data_pages(buf: bytes, col_idx: int = 0, rg_idx: int = 0) -> int:
    """Raw compact-protocol PageHeader walk within one column chunk's byte
    range -- ground truth for how many data pages a chunk actually contains,
    independent of rugo's own reader (used to verify max_page_bytes actually
    split the chunk, not just silently produced one page)."""
    import pyarrow.parquet as pq

    def read_varint(b, p):
        result, shift = 0, 0
        while True:
            byte = b[p]; p += 1
            result |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                return result, p
            shift += 7

    def zigzag(n):
        return (n >> 1) ^ -(n & 1)

    def skip_struct(b, p):
        while True:
            header = b[p]; p += 1
            if header == 0:
                return p
            ftype = header & 0x0F
            delta = (header >> 4) & 0x0F
            if delta == 0:
                _, p = read_varint(b, p)
            if ftype in (4, 5, 6):
                _, p = read_varint(b, p)
            elif ftype == 7:
                p += 8
            elif ftype == 3:
                p += 1
            elif ftype == 8:
                n, p = read_varint(b, p)
                p += n
            elif ftype == 12:
                p = skip_struct(b, p)
            elif ftype in (9, 10):
                hdr = b[p]; p += 1
                elem_type, size = hdr & 0x0F, (hdr >> 4) & 0x0F
                if size == 15:
                    size, p = read_varint(b, p)
                for _ in range(size):
                    if elem_type == 12:
                        p = skip_struct(b, p)
                    elif elem_type == 8:
                        n, p = read_varint(b, p)
                        p += n
                    elif elem_type in (5, 6):
                        _, p = read_varint(b, p)
                    elif elem_type == 3:
                        p += 1

    pf = pq.ParquetFile(io.BytesIO(buf))
    col = pf.metadata.row_group(rg_idx).column(col_idx)
    start = col.dictionary_page_offset if col.has_dictionary_page else col.data_page_offset
    end = start + col.total_compressed_size
    pos = start
    n_data_pages = 0
    while pos < end:
        page_start = pos
        page_type = compressed_size = None
        last_id = 0
        p = pos
        while True:
            header = buf[p]; p += 1
            if header == 0:
                break
            ftype = header & 0x0F
            delta = (header >> 4) & 0x0F
            if delta == 0:
                zz, p = read_varint(buf, p)
                fid = zigzag(zz)
            else:
                fid = last_id + delta
            last_id = fid
            if ftype in (4, 5, 6):
                val, p = read_varint(buf, p)
                val = zigzag(val)
                if fid == 1:
                    page_type = val
                elif fid == 3:
                    compressed_size = val
            elif ftype == 12:
                p = skip_struct(buf, p)
            elif ftype == 8:
                n, p = read_varint(buf, p)
                p += n
        if page_type == 0:
            n_data_pages += 1
        pos = p + compressed_size
    return n_data_pages


@pytest.mark.parametrize("compression", ["zstd", "none"])
def test_page_splitting(compression):
    """max_page_bytes must actually split a column chunk into multiple data
    pages (verified via a raw page-header walk, independent of both writer
    and reader code) as the threshold shrinks, for both scalar and array
    columns, while every reader (rugo, PyArrow) still recovers exact data."""
    import draken.draken_native as dn
    from draken.vectors.vector import Vector
    from draken.morsels.morsel import Morsel
    from rugo.parquet import read_parquet

    # Scalar INT64 column: unsplit vs. tightly page-split.
    ints = list(range(10_000))
    m = Morsel.from_vectors(["id"], [Vector(dn.vector_from_sequence(ints))])
    buf_unsplit = write_parquet(m, compression=compression, max_page_bytes=0)
    buf_split = write_parquet(m, compression=compression, max_page_bytes=500)

    n_unsplit = _count_data_pages(buf_unsplit)
    n_split = _count_data_pages(buf_split)
    assert n_unsplit == 1
    assert n_split > 10, f"expected many pages at a 500-byte threshold, got {n_split}"

    with read_parquet(buf_split) as reader:
        out = []
        for morsel in reader:
            out.extend(morsel.column(b"id").to_pylist())
    assert out == ints
    cols, _ = _read_pyarrow(buf_split)
    assert cols["id"] == ints

    # ARRAY column: same splitting behavior, independent of scalar columns.
    data = [[i, i + 1, i + 2] for i in range(3_000)]
    ma = Morsel.from_vectors(["arr"], [Vector(dn.vector_array_from_sequence(data))])
    buf_arr_split = write_parquet(ma, compression=compression, max_page_bytes=500)
    assert _count_data_pages(buf_arr_split) > 5

    with read_parquet(buf_arr_split) as reader:
        out_arr = []
        for morsel in reader:
            out_arr.extend(morsel.column(b"arr").to_pylist())
    assert out_arr == data
    cols_a, _ = _read_pyarrow(buf_arr_split)
    assert cols_a["arr"] == data


@pytest.mark.parametrize("compression", ["zstd", "none"])
def test_nested_array_roundtrip(compression):
    """Depth-2 LIST columns (list<list<scalar>>) round-trip through the writer's
    2-level Dremel encoding, verified against PyArrow. Covers null outer/middle
    lists, empty lists, null leaf elements, and every supported leaf type."""
    import draken.draken_native as dn
    from draken.vectors.vector import Vector
    from draken.morsels.morsel import Morsel

    def one(name, nb):
        mor = Morsel.from_vectors([name], [Vector(nb)])
        cols, types = _read_pyarrow(write_parquet(mor, compression=compression))
        return types[name], cols[name]

    data_i = [[[1, 2, 3], [4, 5]], [[]], [], None, [None, [6]], [[7, None, 9]]]
    ty, vals = one("li", dn.vector_array_from_sequence(data_i))
    assert ty == "list<element: list<element: int64>>"
    assert vals == data_i

    data_s = [[["x", "yy"], ["z"]], [[None]], None]
    ty, vals = one("ls", dn.vector_array_from_sequence(data_s))
    assert ty == "list<element: list<element: string>>"
    assert vals == data_s


@pytest.mark.parametrize("compression", ["zstd", "none"])
def test_nested_uint64_array_roundtrip(compression):
    """Depth-2 list<list<uint64>>: the leaf carries an INTEGER(64, isSigned=false)
    annotation so full-range unsigned hash values (> INT64_MAX, up to UINT64_MAX)
    round-trip exactly instead of coming back as negative signed ints. Verified
    against PyArrow (the authoritative external reader)."""
    import draken.draken_native as dn
    from draken.vectors.vector import Vector
    from draken.morsels.morsel import Morsel

    data_u = [
        [[14748331363633426810, 8346750105840572524], [1]],
        [[]],
        [],
        None,
        [None, [2833214737711462458]],
        [[18446744073709551615, 0, None]],
    ]
    nb = dn.vector_array_from_sequence(
        data_u, element_type=dn.DrakenType.UINT64.value, nesting_depth=2
    )
    mor = Morsel.from_vectors(["lu"], [Vector(nb)])
    cols, types = _read_pyarrow(write_parquet(mor, compression=compression))
    assert types["lu"] == "list<element: list<element: uint64>>"
    assert cols["lu"] == data_u


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
    ALL-null column keeps its declared type too — see
    test_all_null_column_keeps_declared_type.)"""
    sql = "SELECT i FROM (VALUES (1),(CAST(NULL AS INTEGER)),(3)) AS t(i)"
    cols, types = _read_pyarrow(_write(sql))
    assert cols["i"] == [1, None, 3]
    assert types["i"] == "int64"


def test_all_null_column_keeps_declared_type():
    """An all-null projection keeps the type the CAST declared — it does NOT
    collapse to a typeless column. INTEGER is the SQL spelling of INT64 (there is
    no narrower INTEGER and no BIGINT alias), so the writer emits an all-null
    INT64 column, readable with no values."""
    sql = "SELECT CAST(NULL AS INTEGER) AS i FROM (VALUES (1),(2),(3)) AS t(x)"
    cols, types = _read_pyarrow(_write(sql))
    assert cols["i"] == [None, None, None]
    assert types["i"] == "int64"


def test_rugo_can_parse_own_footer():
    """rugo's metadata reader must understand the footer we emit."""
    from rugo.parquet import read_metadata

    buf = _write("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(i, s)")
    md = read_metadata(buf)
    assert md.num_rows == 2
    names = [c.name for c in md.schema_columns]
    assert names == ["i", "s"]


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
    test_all_null_column_keeps_declared_type()
    test_rugo_can_parse_own_footer()
    test_two_string_array_columns_keep_values()
    print("✅ okay")
