"""
Parquet page-header walker.

PyArrow exposes neither the data page version nor per-page encodings, so the page
headers of a column chunk are read straight from the file bytes with a minimal
Thrift compact-protocol parser. Read-only, independent of every rugo/draken code
path, and strict: any inconsistency between the headers and the footer's
total_compressed_size is an error, not a best guess.
"""

from .errors import Unsupported

_PAGE_TYPES = {0: "DATA_PAGE", 1: "INDEX_PAGE", 2: "DICTIONARY_PAGE", 3: "DATA_PAGE_V2"}
_DATA_PAGE_VERSION = {"DATA_PAGE": 1, "DATA_PAGE_V2": 2}
_ENCODINGS = {
    0: "PLAIN",
    2: "PLAIN_DICTIONARY",
    3: "RLE",
    4: "BIT_PACKED",
    5: "DELTA_BINARY_PACKED",
    6: "DELTA_LENGTH_BYTE_ARRAY",
    7: "DELTA_BYTE_ARRAY",
    8: "RLE_DICTIONARY",
    9: "BYTE_STREAM_SPLIT",
}

# compact-protocol wire types
_BOOL_TRUE, _BOOL_FALSE, _BYTE, _I16, _I32, _I64, _DOUBLE, _BINARY, _LIST, _SET, _MAP, _STRUCT = range(1, 13)


def _varint(buf: bytes, pos: int) -> tuple:
    shift = 0
    result = 0
    while True:
        if pos >= len(buf):
            raise Unsupported("truncated Thrift varint in a page header")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, pos
        shift += 7


def _zigzag(n: int) -> int:
    return (n >> 1) ^ -(n & 1)


def _value(buf: bytes, pos: int, wire: int) -> tuple:
    if wire == _BOOL_TRUE:
        return True, pos
    if wire == _BOOL_FALSE:
        return False, pos
    if wire == _BYTE:
        return buf[pos], pos + 1
    if wire in (_I16, _I32, _I64):
        n, pos = _varint(buf, pos)
        return _zigzag(n), pos
    if wire == _DOUBLE:
        return buf[pos : pos + 8], pos + 8
    if wire == _BINARY:
        n, pos = _varint(buf, pos)
        return buf[pos : pos + n], pos + n
    if wire in (_LIST, _SET):
        head = buf[pos]
        pos += 1
        size = head >> 4
        elem = head & 0x0F
        if size == 15:
            size, pos = _varint(buf, pos)
        items = []
        for _ in range(size):
            if elem in (_BOOL_TRUE, _BOOL_FALSE):
                items.append(buf[pos] == _BOOL_TRUE)
                pos += 1
            else:
                item, pos = _value(buf, pos, elem)
                items.append(item)
        return items, pos
    if wire == _STRUCT:
        return _struct(buf, pos)
    raise Unsupported("unsupported Thrift wire type %d in a page header" % wire)


def _struct(buf: bytes, pos: int) -> tuple:
    fields = {}
    last = 0
    while True:
        if pos >= len(buf):
            raise Unsupported("truncated Thrift struct in a page header")
        head = buf[pos]
        pos += 1
        if head == 0:
            return fields, pos
        wire = head & 0x0F
        delta = head >> 4
        if delta:
            last += delta
        else:
            raw, pos = _varint(buf, pos)
            last = _zigzag(raw)
        fields[last], pos = _value(buf, pos, wire)


def column_chunk_pages(data: bytes, start: int, total_compressed_size: int) -> list:
    """Every page header of one column chunk, in file order."""
    end = start + total_compressed_size
    if start < 0 or end > len(data):
        raise Unsupported(
            "column chunk [%d, %d) lies outside the %d-byte file" % (start, end, len(data))
        )
    pages = []
    pos = start
    while pos < end:
        header, after = _struct(data, pos)
        if 1 not in header or 3 not in header:
            raise Unsupported("page header at offset %d lacks type/compressed_page_size" % pos)
        if header[1] not in _PAGE_TYPES:
            raise Unsupported("unknown page type %d at offset %d" % (header[1], pos))
        page_type = _PAGE_TYPES[header[1]]
        page = {
            "type": page_type,
            "offset": pos,
            "header_bytes": after - pos,
            "compressed_page_size": header[3],
            "uncompressed_page_size": header[2],
        }
        if page_type == "DATA_PAGE":
            body = header[5]
            page["num_values"] = body[1]
            page["encoding"] = _ENCODINGS[body[2]]
        elif page_type == "DATA_PAGE_V2":
            body = header[8]
            page["num_values"] = body[1]
            page["num_nulls"] = body[2]
            page["num_rows"] = body[3]
            page["encoding"] = _ENCODINGS[body[4]]
            page["is_compressed"] = body.get(7, True)
        elif page_type == "DICTIONARY_PAGE":
            body = header[7]
            page["num_values"] = body[1]
            page["encoding"] = _ENCODINGS[body[2]]
        pages.append(page)
        pos = after + header[3]
    if pos != end:
        raise Unsupported(
            "page headers end at offset %d but the column chunk ends at %d" % (pos, end)
        )
    return pages


def data_page_versions(pages: list) -> list:
    return sorted({_DATA_PAGE_VERSION[p["type"]] for p in pages if p["type"] in _DATA_PAGE_VERSION})
