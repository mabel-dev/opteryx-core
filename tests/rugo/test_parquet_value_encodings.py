"""
Parquet value encodings DELTA_LENGTH_BYTE_ARRAY (6), BYTE_STREAM_SPLIT (9) and
ALP (10), and the type-aware encoding admission rule.

Reader: 6 and 9 fixtures are written with PyArrow (allowed in tests/ only); ALP
fixtures come from rugo's own opt-in ALP writer (PyArrow 25 cannot write it).
Each round trip is checked on both page walks — the parallel walk (required
column, many pages) and the sequential walk (nullable column) — and on
DATA_PAGE v1 and v2.

Floats are compared by their IEEE bits, not by ==, so NaN payloads and -0.0
are held to the bit-exact contract ALP promises.
"""

import io
import math
import os
import random
import struct
import sys
import tempfile
from decimal import Decimal

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import thriftpy2
from thriftpy2.protocol import TCompactProtocolFactory
from thriftpy2.utils import deserialize

import draken.draken_native as dn
import opteryx
import rugo.parquet as rp
from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector
from opteryx.connectors import DiskConnector

N_ROWS = 20000
ENC_PLAIN = 0
ENC_RLE_DICTIONARY = 8
ENC_ALP = 10

_SPEC = os.path.join(os.path.dirname(__file__), "../../rugo/src/parquet_spec/parquet.thrift")
_THRIFT = thriftpy2.load(_SPEC, module_name="parquet_value_encodings_thrift")


def _chunk_meta(buf, name="x"):
    """ColumnMetaData of column `name` in row group 0, parsed straight from the footer."""
    length = int.from_bytes(buf[-8:-4], "little")
    fmd = deserialize(_THRIFT.FileMetaData(), buf[-8 - length : -8], TCompactProtocolFactory())
    for col in fmd.row_groups[0].columns:
        if col.meta_data.path_in_schema[0] == name:
            return col.meta_data
    raise KeyError(name)


def _read(buf, col="x"):
    return [v for m in rp.read_parquet(buf) for v in m.column(col.encode()).to_pylist()]


def _float_bits(values, width):
    fmt = "<f" if width == 4 else "<d"
    return [None if v is None else struct.pack(fmt, v) for v in values]


def _pyarrow_write(table, **kw):
    sink = io.BytesIO()
    pq.write_table(table, sink, use_dictionary=False, data_page_size=4096,
                   compression="zstd", **kw)
    return sink.getvalue()


def _engine_rows(buf, sql_template):
    """Run `sql_template` (with {t}) against `buf` through the opteryx engine."""
    with tempfile.TemporaryDirectory() as tmp:
        workspace = f"ws_enc_{os.getpid()}_{random.randrange(1 << 30)}"
        data_dir = os.path.join(tmp, workspace, "t")
        os.makedirs(data_dir)
        with open(os.path.join(data_dir, "data.parquet"), "wb") as f:
            f.write(buf)
        cwd = os.getcwd()
        os.chdir(tmp)
        try:
            opteryx.register_workspace(workspace, DiskConnector)
            rows = []
            for morsel in opteryx.session().execute_to_morsels(
                sql_template.format(t=f"{workspace}.t")
            ):
                cols = [morsel.column(c).to_pylist() for c in morsel.column_names]
                rows.extend(zip(*cols))
            return rows
        finally:
            os.chdir(cwd)


# ---- BYTE_STREAM_SPLIT (9) ----------------------------------------------------------------------

_BSS_TYPES = [pa.int32(), pa.int64(), pa.float32(), pa.float64()]


def _bss_values(typ, nullable, rng):
    out = []
    for i in range(N_ROWS):
        if nullable and i % 7 == 0:
            out.append(None)
        elif pa.types.is_integer(typ):
            out.append(rng.randint(-(10**6), 10**6))
        else:
            out.append(rng.random() * 1000)
    return out


@pytest.mark.parametrize("page_version", ["1.0", "2.0"])
@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("typ", _BSS_TYPES, ids=str)
def test_byte_stream_split_round_trip(typ, nullable, page_version):
    rng = random.Random(9)
    arr = pa.array(_bss_values(typ, nullable, rng), typ)
    schema = pa.schema([pa.field("x", typ, nullable=nullable)])
    buf = _pyarrow_write(pa.table({"x": arr}, schema=schema), use_byte_stream_split=["x"],
                         data_page_version=page_version)
    assert _read(buf) == arr.to_pylist()


@pytest.mark.parametrize("precision,scale", [(10, 2), (30, 4)])
def test_byte_stream_split_decimal_flba(precision, scale):
    rng = random.Random(3)
    q = Decimal(1).scaleb(-scale)
    values = [None if i % 11 == 0 else Decimal(rng.randint(-(10**7), 10**7)).scaleb(-scale).quantize(q)
              for i in range(N_ROWS)]
    arr = pa.array(values, pa.decimal128(precision, scale))
    buf = _pyarrow_write(pa.table({"x": arr}), use_byte_stream_split=["x"])
    assert _read(buf) == values


def test_byte_stream_split_through_engine():
    rng = random.Random(4)
    values = [rng.random() * 100 for _ in range(N_ROWS)]
    schema = pa.schema([pa.field("f", pa.float64(), nullable=False)])
    buf = _pyarrow_write(pa.table({"f": values}, schema=schema), use_byte_stream_split=["f"])
    [(total, top)] = _engine_rows(buf, "SELECT SUM(f), MAX(f) FROM {t}")
    assert top == max(values)
    assert math.isclose(total, math.fsum(values), rel_tol=1e-9)


# ---- DELTA_LENGTH_BYTE_ARRAY (6) ----------------------------------------------------------------


def _strings(nullable, rng):
    return [None if nullable and i % 5 == 0 else
            "".join(chr(97 + rng.randrange(26)) for _ in range(rng.randrange(0, 24)))
            for i in range(N_ROWS)]


@pytest.mark.parametrize("page_version", ["1.0", "2.0"])
@pytest.mark.parametrize("nullable", [False, True])
def test_delta_length_byte_array_round_trip(nullable, page_version):
    values = _strings(nullable, random.Random(6))
    schema = pa.schema([pa.field("x", pa.string(), nullable=nullable)])
    buf = _pyarrow_write(pa.table({"x": values}, schema=schema),
                         column_encoding={"x": "DELTA_LENGTH_BYTE_ARRAY"},
                         data_page_version=page_version)
    assert _read(buf) == values


def test_delta_length_byte_array_like_through_engine():
    """A LIKE over a DELTA_LENGTH_BYTE_ARRAY column (the page is searched as PLAIN)."""
    values = _strings(False, random.Random(7))
    buf = _pyarrow_write(pa.table({"s": values}), column_encoding={"s": "DELTA_LENGTH_BYTE_ARRAY"})
    [(n,)] = _engine_rows(buf, "SELECT COUNT(*) FROM {t} WHERE s LIKE '%qz%'")
    assert n == sum(1 for v in values if "qz" in v)


# ---- DELTA_BINARY_PACKED wide deltas ------------------------------------------------------------


@pytest.mark.parametrize("bits", [56, 60, 63])
def test_delta_binary_packed_wide_deltas(bits):
    """Deltas needing 57-64 bits straddle nine bytes; they once decoded wrong silently."""
    rng = random.Random(bits)
    values = [rng.getrandbits(bits) - (1 << (bits - 1)) for _ in range(5000)]
    buf = _pyarrow_write(pa.table({"x": pa.array(values, pa.int64())}),
                         column_encoding={"x": "DELTA_BINARY_PACKED"})
    assert _read(buf) == values


# ---- ALP (10) -----------------------------------------------------------------------------------


def _alp_cases(rng):
    yield "decimal", [round(rng.uniform(-1000, 1000), 2) for _ in range(N_ROWS)]
    yield "integral", [float(rng.randint(-(10**6), 10**6)) for _ in range(N_ROWS)]
    yield "random", [rng.random() for _ in range(N_ROWS)]
    yield "specials", [math.nan, math.inf, -math.inf, -0.0, 0.0, 1e300, -1e-300, 5e-324, 1.5] * 1000
    yield "constant", [3.25] * 5000
    yield "short", [1.1, 2.2, 3.3]
    yield "nulls", [None if i % 3 == 0 else round(rng.uniform(-50, 50), 3) for i in range(N_ROWS)]
    yield "all_null", [None] * 100


@pytest.mark.parametrize("max_page_bytes", [0, 4096])
@pytest.mark.parametrize("dtype,width", [(DrakenType.FLOAT64, 8), (DrakenType.FLOAT32, 4)],
                         ids=["float64", "float32"])
def test_alp_round_trip_is_bit_exact(dtype, width, max_page_bytes):
    for name, values in _alp_cases(random.Random(11)):
        morsel = Morsel.from_vectors(["x"], [vector_from_sequence(values, dtype=dtype)])
        expected = morsel.column(b"x").to_pylist()
        buf = rp.write_parquet(morsel, alp=True, dictionary=False, max_page_bytes=max_page_bytes)
        got = _read(buf)
        assert _float_bits(got, width) == _float_bits(expected, width), name


def _alp_morsel(values, dtype=DrakenType.FLOAT64):
    return Morsel.from_vectors(["x"], [vector_from_sequence(values, dtype=dtype)])


def test_alp_footer_declares_the_encoding():
    buf = rp.write_parquet(_alp_morsel([1.25, 2.5, 3.75] * 100), alp=True, dictionary=False,
                           max_page_bytes=1024)
    meta = _chunk_meta(buf)
    assert ENC_ALP in meta.encodings and ENC_PLAIN not in meta.encodings
    assert [(s.page_type, s.encoding) for s in meta.encoding_stats] == [(0, ENC_ALP)]


def test_alp_dictionary_chunk_is_unchanged():
    """A low-cardinality column still dictionary-encodes; its dictionary page stays PLAIN."""
    buf = rp.write_parquet(_alp_morsel([1.5, 2.5] * 5000), alp=True, dictionary=True)
    meta = _chunk_meta(buf)
    assert ENC_ALP not in meta.encodings and ENC_RLE_DICTIONARY in meta.encodings
    assert _read(buf) == [1.5, 2.5] * 5000


def test_alp_array_leaves_round_trip():
    rows = [[1.25, 2.5], [], [math.inf, math.nan, 7.75]] * 500
    vector = Vector(dn.vector_array_from_sequence(rows, int(DrakenType.FLOAT64.value), 1))
    expected = vector.to_pylist()
    buf = rp.write_parquet(Morsel.from_vectors(["x"], [vector]), alp=True, dictionary=False)
    assert ENC_ALP in _chunk_meta(buf).encodings
    assert [_float_bits(r, 8) for r in _read(buf)] == [_float_bits(r, 8) for r in expected]


def test_alp_by_name_only_touches_named_columns():
    morsel = Morsel.from_vectors(
        ["a", "b"],
        [vector_from_sequence([0.5, 1.5] * 50, dtype=DrakenType.FLOAT64),
         vector_from_sequence([0.25, 0.75] * 50, dtype=DrakenType.FLOAT64)],
    )
    buf = rp.write_parquet(morsel, alp=["b"], dictionary=False)
    assert ENC_ALP not in _chunk_meta(buf, "a").encodings
    assert ENC_ALP in _chunk_meta(buf, "b").encodings


def test_alp_names_a_non_float_column():
    morsel = Morsel.from_vectors(["i"], [vector_from_sequence([1, 2, 3], dtype=DrakenType.INT64)])
    with pytest.raises(ValueError, match="not FLOAT32/FLOAT64"):
        rp.write_parquet(morsel, alp=["i"])


def test_alp_names_an_absent_column():
    with pytest.raises(ValueError, match="not in the morsel"):
        rp.write_parquet(_alp_morsel([1.0]), alp=["nope"])


def test_alp_through_engine():
    rng = random.Random(12)
    values = [round(rng.uniform(0, 500), 2) for _ in range(N_ROWS)]
    buf = rp.write_parquet(_alp_morsel(values), alp=True, dictionary=False, max_page_bytes=8192)
    [(total, top, low)] = _engine_rows(buf, "SELECT SUM(x), MAX(x), MIN(x) FROM {t}")
    assert (top, low) == (max(values), min(values))
    assert math.isclose(total, math.fsum(values), rel_tol=1e-9)


def test_alp_unknown_compression_mode_is_refused():
    """A future ALP mode (ALP-RD is reserved) is refused by name, never misread."""
    values = [round(0.01 * i, 2) for i in range(64)]
    buf = bytearray(rp.write_parquet(_alp_morsel(values), alp=True, dictionary=False,
                                     compression="none"))
    header = bytes([0, 0, 10]) + struct.pack("<i", len(values))
    at = buf.find(header)
    assert at > 0 and buf.find(header, at + 1) == -1
    buf[at] = 1
    with pytest.raises(RuntimeError, match="compression_mode 1 is not supported"):
        _read(bytes(buf))


# ---- type-aware admission -----------------------------------------------------------------------


def _patch_page_encoding(buf, encoding):
    """Rewrite the (single) PLAIN data page of a rugo-written file to `encoding`.

    rugo writes DataPageHeader fields 2..4 as encoding=PLAIN, RLE, RLE: compact
    thrift i32 fields with zigzag varints 0x00, 0x06, 0x06.
    """
    pattern = b"\x15\x00\x15\x06\x15\x06"
    at = buf.find(pattern)
    assert at > 0 and buf.find(pattern, at + 1) == -1
    patched = bytearray(buf)
    patched[at + 1] = encoding * 2  # zigzag of a small non-negative int
    return bytes(patched)


def test_delta_binary_packed_page_on_a_float_column_is_refused():
    buf = rp.write_parquet(_alp_morsel([1.5, 2.5, 3.5]), dictionary=False, compression="none")
    with pytest.raises(RuntimeError, match=r"unsupported page encoding 5 \(DELTA_BINARY_PACKED\) for float64"):
        _read(_patch_page_encoding(buf, 5))


def test_alp_page_on_an_int_column_is_refused():
    morsel = Morsel.from_vectors(["x"], [vector_from_sequence([1, 2, 3], dtype=DrakenType.INT64)])
    buf = rp.write_parquet(morsel, dictionary=False, compression="none", bloom_filters=False)
    with pytest.raises(RuntimeError, match=r"unsupported page encoding 10 \(ALP\) for int64"):
        _read(_patch_page_encoding(buf, ENC_ALP))


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
