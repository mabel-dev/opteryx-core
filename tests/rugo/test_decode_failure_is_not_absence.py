"""
Regression test: a column that fails to decode is never reported as absent.

The column decoder used one signal for two outcomes: success == false with an
empty error_message meant "this column is not in the row group", which the
standalone reader tolerates by leaving the column out. A decode failure that
recorded no reason looked the same, so rugo's read_parquet silently dropped a
corrupt column and returned the rest of the row group. Absence is now an
explicit flag; every other failure is an error.

Fixtures are written with PyArrow (allowed in tests/ only). Page bodies are
located with a minimal compact-thrift page-header parser, not a parquet reader.
"""

import io
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import rugo.parquet as rparquet
import rugo.rugo_native as rp

from opteryx.connectors.parquet_io.pool_reader import fetch_column_chunk_info

N_ROWS = 1000
DELTA_BYTE_ARRAY = 7


class _Thrift:
    """Just enough Thrift compact protocol to read a PageHeader."""

    def __init__(self, buf, pos):
        self.buf, self.pos = buf, pos

    def _byte(self):
        value = self.buf[self.pos]
        self.pos += 1
        return value

    def _varint(self):
        result = shift = 0
        while True:
            byte = self._byte()
            result |= (byte & 0x7F) << shift
            shift += 7
            if not byte & 0x80:
                return result

    def _zigzag(self):
        value = self._varint()
        return (value >> 1) ^ -(value & 1)

    def _value(self, ftype):
        if ftype in (1, 2):
            return ftype == 1
        if ftype == 3:
            return self._byte()
        if ftype in (4, 5, 6):
            return self._zigzag()
        if ftype == 7:
            self.pos += 8
            return None
        if ftype == 8:
            n = self._varint()
            value = self.buf[self.pos : self.pos + n]
            self.pos += n
            return value
        if ftype in (9, 10):
            header = self._byte()
            n, etype = header >> 4, header & 15
            if n == 15:
                n = self._varint()
            return [self._value(etype) for _ in range(n)]
        if ftype == 12:
            return self.struct()
        raise ValueError(f"unsupported thrift type {ftype}")

    def struct(self):
        fields, fid = {}, 0
        while True:
            header = self._byte()
            if header == 0:
                return fields
            ftype, delta = header & 15, header >> 4
            fid = fid + delta if delta else self._zigzag()
            fields[fid] = self._value(ftype)


def _file_with_corrupt_string_page():
    """(good bytes, corrupt bytes): column 's' has its DELTA_BYTE_ARRAY page body wiped."""
    table = pa.table(
        {
            "k": pa.array(range(N_ROWS), type=pa.int64()),
            "s": pa.array([f"name-{i:05d}" for i in range(N_ROWS)]),
        },
        schema=pa.schema(
            [pa.field("k", pa.int64(), nullable=False), pa.field("s", pa.string(), nullable=False)]
        ),
    )
    sink = io.BytesIO()
    pq.write_table(
        table,
        sink,
        use_dictionary=False,
        compression="none",
        column_encoding={"s": "DELTA_BYTE_ARRAY"},
    )
    good = sink.getvalue()
    raw = bytearray(good)
    pos = 4  # past the leading "PAR1"
    while True:
        reader = _Thrift(raw, pos)
        header = reader.struct()
        body, body_len = reader.pos, header[3]
        data_page_header = header.get(5)
        if data_page_header is not None and data_page_header[2] == DELTA_BYTE_ARRAY:
            break
        pos = body + body_len
    raw[body + 40 : body + body_len] = b"\x00" * (body_len - 40)
    return good, bytes(raw)


def test_corrupt_column_raises_instead_of_being_dropped():
    good, corrupt = _file_with_corrupt_string_page()
    assert [list(m.column_names) for m in rparquet.read_parquet(good)] == [[b"k", b"s"]]
    with pytest.raises(RuntimeError, match="column 's'"):
        for _ in rparquet.read_parquet(corrupt):
            pass


def test_absent_column_is_still_left_out():
    sink = io.BytesIO()
    pq.write_table(pa.table({"k": pa.array(range(10), type=pa.int64())}), sink)
    morsels = list(rparquet.read_parquet(sink.getvalue(), columns=["k", "nope"]))
    assert [list(m.column_names) for m in morsels] == [[b"k"]]
    assert [m.num_rows for m in morsels] == [10]


def test_decode_column_from_chunk_failure_raises():
    """A single-chunk decode has no 'absent' outcome: a failure raises, never None."""
    _, corrupt = _file_with_corrupt_string_page()
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        f.write(corrupt)
        f.flush()
        rg_stats = next(
            c for c in rp.read_rowgroup_stats(corrupt)[0]["columns"] if c["name"] == "s"
        )
        col_stats = {**rg_stats, **fetch_column_chunk_info(f.name, 0, ["s"])["s"]}
    base = col_stats["data_page_offset"]
    chunk = corrupt[base : base + col_stats["total_compressed_size"]]
    with pytest.raises(RuntimeError, match="column 's'"):
        rp.decode_column_from_chunk(chunk, col_stats)


if __name__ == "__main__":
    test_corrupt_column_raises_instead_of_being_dropped()
    test_absent_column_is_still_left_out()
    test_decode_column_from_chunk_failure_raises()
    print("✅ okay")
