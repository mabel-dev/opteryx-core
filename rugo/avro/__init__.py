"""
rugo.avro — read facade for Avro object container files.

    from rugo import avro

    with avro.read_avro("launches.avro", columns=["Company", "Rocket.Name"]) as reader:
        for morsel in reader:
            ...

    meta = avro.read_metadata("launches.avro")
    print(meta.codec, meta.schema)

Reading only — rugo has no Avro writer. Design and limits: docs/AVRO_READER_DESIGN.md.
"""

import json
from typing import Optional, Sequence, Union

from rugo.rugo_native import read_avro as _read_avro
from rugo.rugo_native import read_avro_metadata as _read_avro_metadata

__all__ = ["read_avro", "read_metadata"]

Source = Union[str, bytes, bytearray, memoryview]


def _load(source: Source):
    if type(source) is str:
        with open(source, "rb") as f:
            return f.read()
    return source


def _schema_text(reader_schema) -> Optional[str]:
    if reader_schema is None or type(reader_schema) is str:
        return reader_schema
    return json.dumps(reader_schema)


class AvroMetadata:
    __slots__ = ("schema", "codec", "metadata")

    def __init__(self, schema: dict, codec: str, metadata: dict):
        self.schema = schema        # the file's writer schema, parsed
        self.codec = codec          # null / deflate / snappy / zstandard
        self.metadata = metadata    # the header's metadata map, {str: bytes}

    @property
    def columns(self) -> list:
        """Top-level field names of the writer schema."""
        return [f["name"] for f in self.schema["fields"]]

    def __repr__(self):
        return f"AvroMetadata(codec={self.codec!r}, columns={self.columns})"


class _AvroReader:
    """Context-managed reader that yields one Morsel per batch.

    A batch is whole Avro blocks packed up to 65,536 rows (a block is never split)."""

    def __init__(self, source, columns, reader_schema):
        self._source = source
        self._columns = columns
        self._reader_schema = reader_schema

    def __enter__(self) -> "_AvroReader":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def __iter__(self):
        result = _read_avro(
            _load(self._source),
            None if self._columns is None else list(self._columns),
            _schema_text(self._reader_schema),
        )
        from draken.morsels.morsel import Morsel

        for vectors in result["batches"]:
            yield Morsel.from_vectors(result["column_names"], vectors)


def read_avro(
    source: Source,
    columns: Optional[Sequence[str]] = None,
    reader_schema: Optional[Union[str, dict]] = None,
) -> _AvroReader:
    """Open an Avro object container file (path or bytes) for reading.

    Returns a context manager yielding one Morsel per batch (whole blocks, up to
    65,536 rows). A file with no records yields nothing.

    columns: names to read; a dotted name selects a field inside a record
        (`Rocket.Name`), through nullable records too (NULL where the record is NULL).
        None = every top-level field. A column the schema does not have raises.
    reader_schema: an Avro schema (dict or JSON text) to read the file as. Fields match
        by field-id when both sides carry one (Iceberg), else by name; a field the file
        lacks is its default as a constant, or NULL; int->long/float/double,
        long->float/double, float->double and string<->bytes promote. Logical types
        follow the FILE, as fastavro and the Apache implementation do.

    Types: boolean BOOL · int INT32 · long INT64 · float FLOAT32 · double FLOAT64 ·
    string VARCHAR · bytes / fixed VARBINARY · enum VARCHAR (one entry per symbol,
    positions per row) · date DATE · time-* TIME (us) · timestamp-* TIMESTAMP (us, UTC)
    · decimal DECIMAL / DECIMAL128 · array of a plain scalar ARRAY · a whole record,
    map, or array of nested values NVARCHAR JSON text (the parquet nested-JSON rules).

    Codecs: null, deflate, snappy, zstandard. Refused, by name: bzip2 and xz, unions
    other than [null, T], recursive types, uuid, decimal precision > 38,
    timestamp-nanos, local-timestamp-*, duration.
    """
    return _AvroReader(source, columns, reader_schema)


def read_metadata(source: Source) -> AvroMetadata:
    """The file's header — writer schema, codec and metadata map — without reading
    any block."""
    result = _read_avro_metadata(_load(source))
    meta = result["metadata"]
    return AvroMetadata(
        schema=json.loads(result["schema"]),
        codec=meta.get("avro.codec", b"null").decode("utf-8"),
        metadata=meta,
    )
