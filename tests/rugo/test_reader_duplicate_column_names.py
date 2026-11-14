# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Readers refuse a file holding two columns of one name: columns resolve by name,
so the second would be silently unreachable. rugo's own writers refuse to make
such a file, so the parquet case is byte-patched and ORC comes from pyarrow.
"""

import io
import os
import struct
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pyarrow as pa  # noqa: E402
import pyarrow.orc as po  # noqa: E402
from opteryx.third_party.cyan4973.xxhash import hash_bytes  # noqa: E402

import skene  # noqa: E402
from draken.draken_native import DrakenType  # noqa: E402
from draken.interop.vector_sequence import vector_from_sequence  # noqa: E402
from draken.morsels.morsel import Morsel  # noqa: E402
from rugo import csv, orc, parquet  # noqa: E402


def _morsel(names):
    return Morsel.from_vectors(
        names, [vector_from_sequence([1, 2], dtype=DrakenType.INT64) for _ in names]
    )


def _dup_parquet():
    # Same-length rename in the footer (parquet footers carry no checksum).
    data = parquet.write_parquet(_morsel([b"dupa", b"dupb"]))
    return data.replace(b"dupb", b"dupa")


def test_parquet_read():
    with pytest.raises(ValueError, match="parquet file: column name 'dupa' appears more than once"):
        list(parquet.read_parquet(_dup_parquet()))


@pytest.mark.parametrize("columns", [["dupa"], ["z"]])
def test_parquet_projected_read(columns):
    # Refused even when the projection names only the unambiguous column.
    data = parquet.write_parquet(_morsel([b"dupa", b"dupb", b"z"])).replace(b"dupb", b"dupa")
    with pytest.raises(ValueError, match="parquet file: column name 'dupa' appears more than once"):
        list(parquet.read_parquet(data, columns=columns))


def test_parquet_metadata():
    with pytest.raises(ValueError, match="parquet file: column name 'dupa' appears more than once"):
        parquet.read_metadata(_dup_parquet())


def test_orc_read():
    buf = io.BytesIO()
    table = pa.Table.from_arrays([pa.array([1, 2]), pa.array([3, 4])], names=["id", "id"])
    po.write_table(table, buf, compression="uncompressed")
    with pytest.raises(ValueError, match="ORC file: column name 'id' appears more than once"):
        list(orc.read_orc(buf.getvalue()))


def test_csv_header():
    with pytest.raises(ValueError, match="CSV header: column name 'id' appears more than once"):
        list(csv.read_csv(b"id,x,id\n1,2,3\n"))


def test_csv_without_header_is_positional():
    assert list(csv.read_csv(b"1,2,3\n", has_header=False))


def test_skene_write():
    with pytest.raises(skene.SkeneError, match="cannot write: column name 'id' appears more than once"):
        skene.write_morsel(_morsel([b"id", b"x", b"id"]))


def _dup_skene():
    # skene's writer refuses duplicates, so rename in the footer and re-seal it:
    # tail = footer_bytes u32, footer_checksum u64 (XXH3-64 over the footer), ...
    data = bytearray(skene.write_morsel(_morsel([b"dupa", b"dupb"])))
    assert data.count(b"dupb") == 1
    data[:] = data.replace(b"dupb", b"dupa")
    tail = len(data) - 24
    (footer_bytes,) = struct.unpack_from("<I", data, tail)
    struct.pack_into("<Q", data, tail + 4, hash_bytes(bytes(data[tail - footer_bytes : tail])))
    return bytes(data)


def test_skene_read_requested_name():
    with pytest.raises(skene.SkeneError, match="column name 'dupa' appears more than once in this file"):
        skene.read_morsel(_dup_skene(), 0, columns=["dupa"])
