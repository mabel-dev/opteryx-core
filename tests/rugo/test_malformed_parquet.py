"""Malformed Parquet must fail with an error - never overflow a buffer, recurse
until the stack is gone, hang, or read back as plausible values.

Each fixture is a small file rugo writes itself, rebuilt with one page header,
page body or footer altered: pages are re-serialized with the spec's own thrift
definitions and every offset and size in the footer is recomputed, so the file
is well-formed in every respect except the one under test. The unaltered
rebuild reading back exactly (`test_rebuild_is_faithful`) is what makes the
other cases mean something.
"""

import io
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
import thriftpy2
from thriftpy2.protocol import TCompactProtocolFactory
from thriftpy2.protocol.compact import TCompactProtocol
from thriftpy2.utils import deserialize
from thriftpy2.utils import serialize

import rugo.parquet as rp
from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel

_SPEC = os.path.join(os.path.dirname(__file__), "../../rugo/src/parquet_spec/parquet.thrift")
_T = thriftpy2.load(_SPEC, module_name="malformed_parquet_thrift")
_F = TCompactProtocolFactory()

VALUES = [i % 7 for i in range(100)]
PAGE_DATA, PAGE_DICTIONARY = 0, 2
CODEC_SNAPPY = 1
ENCODING_DELTA_BINARY_PACKED = 5


def _varint(n):
    out = bytearray()
    while True:
        byte, n = n & 0x7F, n >> 7
        if not n:
            out.append(byte)
            return bytes(out)
        out.append(byte | 0x80)


def _zigzag(n):
    return (n << 1) ^ (n >> 63)


def _snappy(raw, declared=None):
    """A valid raw-snappy stream (length preamble + literal elements); `declared`
    overrides the preamble's uncompressed length."""
    out = bytearray(_varint(len(raw) if declared is None else declared))
    for i in range(0, len(raw), 60):
        chunk = raw[i : i + 60]
        out.append((len(chunk) - 1) << 2)
        out += chunk
    return bytes(out)


def _written(dictionary):
    """(footer, [[page header, body], ...]) of a one-column, one-row-group file."""
    morsel = Morsel.from_vectors(["x"], [vector_from_sequence(VALUES, DrakenType.INT64)])
    buf = rp.write_parquet(morsel, compression="none", dictionary=dictionary, bloom_filters=False)
    length = int.from_bytes(buf[-8:-4], "little")
    footer = deserialize(_T.FileMetaData(), buf[-8 - length : -8], _F)
    meta = footer.row_groups[0].columns[0].meta_data
    offset = meta.dictionary_page_offset if meta.dictionary_page_offset is not None else meta.data_page_offset
    end, pages = offset + meta.total_compressed_size, []
    while offset < end:
        stream = io.BytesIO(buf[offset:])
        header = _T.PageHeader()
        header.read(TCompactProtocol(stream))
        start = offset + stream.tell()
        pages.append([header, buf[start : start + header.compressed_page_size]])
        offset = start + header.compressed_page_size
    return footer, pages


def _assemble(footer, pages, codec=None, footer_bytes=None):
    out = bytearray(b"PAR1")
    meta = footer.row_groups[0].columns[0].meta_data
    meta.dictionary_page_offset = None
    for header, body in pages:
        if header.type == PAGE_DICTIONARY:
            meta.dictionary_page_offset = len(out)
        else:
            meta.data_page_offset = len(out)
        out += serialize(header, _F) + body
    meta.total_compressed_size = meta.total_uncompressed_size = len(out) - 4
    if codec is not None:
        meta.codec = codec
    footer.row_groups[0].columns[0].file_offset = 4
    footer.row_groups[0].file_offset = 4
    footer.row_groups[0].total_compressed_size = meta.total_compressed_size
    encoded = serialize(footer, _F) if footer_bytes is None else footer_bytes(serialize(footer, _F))
    return bytes(out + encoded + len(encoded).to_bytes(4, "little") + b"PAR1")


def _set_body(page, body):
    page[1] = body
    page[0].compressed_page_size = page[0].uncompressed_page_size = len(body)


def _levels(body):
    """The OPTIONAL column's definition-level prefix (4-byte length + levels)."""
    return body[: 4 + int.from_bytes(body[:4], "little")]


def _read(buf):
    return [v for m in rp.read_parquet(buf) for v in m.column(b"x").to_pylist()]


@pytest.mark.parametrize("dictionary", [False, True])
def test_rebuild_is_faithful(dictionary):
    assert _read(_assemble(*_written(dictionary))) == VALUES


def test_snappy_page_reads():
    footer, pages = _written(False)
    page = pages[0]
    page[1] = _snappy(page[1])
    page[0].compressed_page_size = len(page[1])
    assert _read(_assemble(footer, pages, codec=CODEC_SNAPPY)) == VALUES


def test_snappy_stream_longer_than_the_page_is_refused():
    # The stream's own preamble claims 1024x the page header's size: snappy would
    # write that much into a buffer sized from the header.
    footer, pages = _written(False)
    page = pages[0]
    page[1] = _snappy(page[1], declared=len(page[1]) * 1024)
    page[0].compressed_page_size = len(page[1])
    with pytest.raises(RuntimeError, match="Snappy decompressed size mismatch"):
        _read(_assemble(footer, pages, codec=CODEC_SNAPPY))


def test_bit_packed_run_whose_size_wraps_is_refused():
    # 2^27 groups at bit width 4 is 2^32 bits: in int32 the byte count wrapped to
    # 0, passed the bounds check, and the unpack read past the page.
    footer, pages = _written(True)
    data = pages[1]
    _set_body(data, _levels(data[1]) + bytes([4]) + bytes([0x81, 0x80, 0x80, 0x80, 0x01]) + bytes(16))
    with pytest.raises(RuntimeError, match="ends before its 100 values"):
        _read(_assemble(footer, pages))


@pytest.mark.parametrize("block_size, miniblocks, width", [(1 << 25, 1, 64), (256, 1, 64), (128, 4, 8)])
def test_delta_miniblock_longer_than_the_page_is_refused(block_size, miniblocks, width):
    # A miniblock whose bytes the page does not hold. It used to end only the
    # block, and the decoder then read the miniblock's (zero) data as the next
    # block's header - "decoding" 100 copies of the first value.
    footer, pages = _written(False)
    data = pages[0]
    block = (
        _varint(block_size) + _varint(miniblocks) + _varint(100) + _varint(_zigzag(7))
        + _varint(0) + bytes([width] * miniblocks) + bytes(64)
    )
    _set_body(data, _levels(data[1]) + block)
    data[0].data_page_header.encoding = ENCODING_DELTA_BINARY_PACKED
    with pytest.raises(RuntimeError, match=r"DELTA_BINARY_PACKED page decoded \d+ of 100"):
        _read(_assemble(footer, pages))


def test_delta_width_zero_miniblock_needs_no_data():
    footer, pages = _written(False)
    data = pages[0]
    block = _varint(128) + _varint(4) + _varint(100) + _varint(_zigzag(5)) + _varint(0) + bytes(4)
    _set_body(data, _levels(data[1]) + block)
    data[0].data_page_header.encoding = ENCODING_DELTA_BINARY_PACKED
    assert _read(_assemble(footer, pages)) == [5] * 100


def test_deeply_nested_footer_is_refused_not_a_stack_overflow():
    # An unknown struct field (id 200) holding a million nested structs: each
    # level was one more recursive SkipField frame.
    def nest(encoded):
        return encoded[:-1] + bytes([0x0C]) + _varint(_zigzag(200)) + b"\x1c" * 1_000_000

    footer, pages = _written(False)
    with pytest.raises(RuntimeError, match="nesting deeper than 64 levels"):
        _read(_assemble(footer, pages, footer_bytes=nest))


def test_negative_dictionary_page_size_is_refused():
    # Skipping the dictionary page by a negative size stalled or rewound the cursor.
    footer, pages = _written(True)
    pages[0][0].compressed_page_size = -20
    with pytest.raises(RuntimeError, match="negative page size"):
        _read(_assemble(footer, pages))


def test_dictionary_shorter_than_its_header_is_refused():
    # Codes were bounds-checked against the header's count, not the entries read.
    footer, pages = _written(True)
    pages[0][0].dictionary_page_header.num_values = 50
    with pytest.raises(RuntimeError, match="declares 50 entries but holds 7"):
        _read(_assemble(footer, pages))


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
