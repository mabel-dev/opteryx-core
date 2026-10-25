"""
rugo.orc — read facade for ORC files (basic support: docs/ORC_READER_DESIGN.md).

    from rugo import orc

    meta = orc.read_metadata("events.orc")
    print(meta.compression, meta.num_rows, meta.columns)

    for morsel in orc.read_orc("events.orc", columns=["id", "name"]):
        ...

Primitive columns only (BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, STRING / VARCHAR /
CHAR, BINARY, DATE, TIMESTAMP, TIMESTAMP_INSTANT, DECIMAL). Compression: none, zlib,
snappy, lz4, zstd. Refused, by name: LIST / MAP / STRUCT / UNION columns, LZO, encrypted
files, nanosecond timestamp units, and a TIMESTAMP written in a non-UTC zone.
"""

from typing import Iterator, Optional, Sequence, Union

from rugo.rugo_native import read_orc as _read_orc
from rugo.rugo_native import read_orc_metadata as _read_orc_metadata

__all__ = ["read_orc", "read_metadata"]

Source = Union[str, bytes, bytearray, memoryview]


def _load(source: Source):
    if type(source) is str:
        with open(source, "rb") as f:
            return f.read()
    return source


class OrcMetadata:
    __slots__ = ("num_rows", "stripes", "compression", "columns")

    def __init__(self, num_rows: int, stripes: int, compression: str, columns: list):
        self.num_rows = num_rows
        self.stripes = stripes
        self.compression = compression   # none / zlib / snappy / lz4 / zstd
        self.columns = columns           # [{name, kind, iceberg_id, readable, refusal, ...}]

    def __repr__(self):
        return (f"OrcMetadata(rows={self.num_rows}, stripes={self.stripes}, "
                f"compression={self.compression!r}, columns={[c['name'] for c in self.columns]})")


def read_metadata(source: Source) -> OrcMetadata:
    """The file's tail - row count, stripes, compression and columns - without reading
    any stripe."""
    result = _read_orc_metadata(_load(source))
    return OrcMetadata(result["num_rows"], result["stripes"], result["compression"], result["columns"])


def read_orc(
    source: Source,
    columns: Optional[Sequence[str]] = None,
    reader: Optional[Sequence[tuple]] = None,
    match_by_id: bool = False,
) -> Iterator:
    """Read an ORC file (path or bytes), yielding one Morsel per stripe.

    columns: names to read; None = every column.
    reader: (name, field_id, type, precision, scale, nullable) per column - what the
        caller wants each to be, `type` a DrakenType ordinal. A column the file lacks
        is a NULL constant when nullable, an error when not.
    match_by_id: match `reader` columns by the file's `iceberg.id` attribute, not by name;
        a file with no `iceberg.id` at all is an error.
    """
    result = _read_orc(
        _load(source),
        None if columns is None else list(columns),
        None if reader is None else list(reader),
        match_by_id,
    )
    from draken.morsels.morsel import Morsel

    for vectors, rows in zip(result["batches"], result["batch_rows"]):
        morsel = Morsel.from_vectors(result["column_names"], vectors)
        morsel._zero_col_num_rows = rows
        yield morsel
