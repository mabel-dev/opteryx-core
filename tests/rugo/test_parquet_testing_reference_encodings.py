"""
rugo's DELTA_LENGTH_BYTE_ARRAY, BYTE_STREAM_SPLIT and ALP decoders against the
Apache Parquet project's reference files — files rugo did not write.

Source: apache/parquet-testing, data/, commit
3acde70509f990c4ddfe34bf3ab05890657e766c, copied into testdata/parquet_tests/.
The sha256 of each file is pinned below so a changed fixture fails here
instead of silently changing what is tested.

References, per the parquet-testing data/README.md:
  - alp_extended.zstd.parquet: every ALP column (vector sizes 1024, 4096 and
    32) holds the same 9032 values as its PLAIN twin (float_plain /
    double_plain), including NaNs with payloads and +-Inf. Compared by IEEE bits.
  - byte_stream_split_extended.gzip.parquet: each BYTE_STREAM_SPLIT column
    holds the same values as its PLAIN twin.
  - byte_stream_split.zstd.parquet and delta_length_byte_array.parquet carry no
    in-file reference; PyArrow (allowed in tests/ only) is the oracle.
"""

import hashlib
import os
import struct
import sys
from pathlib import Path

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow.parquet as pq
import pytest
import thriftpy2
from thriftpy2.protocol import TCompactProtocolFactory
from thriftpy2.utils import deserialize

import rugo.parquet as rp

DATA = Path(__file__).resolve().parent.parent.parent / "testdata" / "parquet_tests"

SHA256 = {
    "alp_extended.zstd.parquet": "b14ebd02c6584a451a9d1ca2d09504156254a7fb7316d0abb0efce38590bedb0",
    "byte_stream_split.zstd.parquet": "64df58cf9a13672749cf8c852840553301f48cc45604c68b62ea8dc9437671b5",
    "byte_stream_split_extended.gzip.parquet": "86a96259c2c82e50aa2826b91d6de9514e538235343eed9c2a98318424b46cd8",
    "delta_length_byte_array.parquet": "efef768997748d27596a5c001ccd24eae42adf2825ca4d909e8fa8eb1d89f386",
}

ENC_DELTA_LENGTH_BYTE_ARRAY = 6
ENC_BYTE_STREAM_SPLIT = 9
ENC_ALP = 10

_SPEC = Path(__file__).resolve().parent.parent.parent / "rugo/src/parquet_spec/parquet.thrift"
_THRIFT = thriftpy2.load(str(_SPEC), module_name="parquet_reference_encodings_thrift")


def _load(name):
    buf = (DATA / name).read_bytes()
    assert hashlib.sha256(buf).hexdigest() == SHA256[name], f"{name} is not the pinned fixture"
    return buf


def _encodings(buf):
    """{column: set of encodings} over every row group, from the footer."""
    length = int.from_bytes(buf[-8:-4], "little")
    fmd = deserialize(_THRIFT.FileMetaData(), buf[-8 - length : -8], TCompactProtocolFactory())
    out = {}
    for rg in fmd.row_groups:
        for col in rg.columns:
            out.setdefault(col.meta_data.path_in_schema[0], set()).update(col.meta_data.encodings)
    return out


def _read(buf, columns):
    out = {name: [] for name in columns}
    for morsel in rp.read_parquet(buf, columns=columns):
        for name in columns:
            out[name].extend(morsel.column(name).to_pylist())
    return out


def _bits(values, fmt):
    return [None if v is None else struct.pack(fmt, v) for v in values]


# ---- ALP ----------------------------------------------------------------------------------------


@pytest.mark.parametrize("vector_size", ["1024", "4096", "32"])
@pytest.mark.parametrize("base,fmt", [("float", "<f"), ("double", "<d")])
def test_alp_reference_matches_plain_twin_bit_for_bit(base, fmt, vector_size):
    buf = _load("alp_extended.zstd.parquet")
    column = f"{base}_alp_{vector_size}"
    assert ENC_ALP in _encodings(buf)[column]
    cols = _read(buf, [f"{base}_plain", column])
    assert len(cols[column]) == 9032
    assert _bits(cols[column], fmt) == _bits(cols[f"{base}_plain"], fmt)


# ---- BYTE_STREAM_SPLIT --------------------------------------------------------------------------


@pytest.mark.parametrize("pair", ["float", "double", "int32", "int64", "decimal"])
def test_byte_stream_split_reference_matches_plain_twin(pair):
    buf = _load("byte_stream_split_extended.gzip.parquet")
    column = f"{pair}_byte_stream_split"
    assert ENC_BYTE_STREAM_SPLIT in _encodings(buf)[column]
    cols = _read(buf, [f"{pair}_plain", column])
    assert len(cols[column]) == 200
    assert [repr(v) for v in cols[column]] == [repr(v) for v in cols[f"{pair}_plain"]]


@pytest.mark.parametrize("pair", ["float16", "flba5"])
def test_byte_stream_split_reference_unsupported_flba_is_refused(pair):
    """float16 and plain FIXED_LEN_BYTE_ARRAY(5) have no rugo decode path in ANY
    encoding (only DECIMAL and UUID do); both twins are refused by name."""
    buf = _load("byte_stream_split_extended.gzip.parquet")
    for column in (f"{pair}_plain", f"{pair}_byte_stream_split"):
        with pytest.raises(RuntimeError, match="fixed_len_byte_array column is only decodable"):
            _read(buf, [column])


def test_byte_stream_split_reference_normals():
    buf = _load("byte_stream_split.zstd.parquet")
    encodings = _encodings(buf)
    assert ENC_BYTE_STREAM_SPLIT in encodings["f32"] and ENC_BYTE_STREAM_SPLIT in encodings["f64"]
    table = pq.read_table(DATA / "byte_stream_split.zstd.parquet")
    cols = _read(buf, ["f32", "f64"])
    assert _bits(cols["f32"], "<f") == _bits(table.column("f32").to_pylist(), "<f")
    assert _bits(cols["f64"], "<d") == _bits(table.column("f64").to_pylist(), "<d")


# ---- DELTA_LENGTH_BYTE_ARRAY --------------------------------------------------------------------


def test_delta_length_byte_array_reference():
    buf = _load("delta_length_byte_array.parquet")
    assert ENC_DELTA_LENGTH_BYTE_ARRAY in _encodings(buf)["FRUIT"]
    expected = pq.read_table(DATA / "delta_length_byte_array.parquet").column("FRUIT").to_pylist()
    got = _read(buf, ["FRUIT"])["FRUIT"]
    assert len(got) == 1000
    assert got == expected


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
