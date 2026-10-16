"""
rugo.avro — read/write facade for Avro object container files.

    from rugo import avro

    with avro.read_avro("launches.avro", columns=["Company", "Rocket.Name"]) as reader:
        for morsel in reader:
            ...

    meta = avro.read_metadata("launches.avro")
    print(meta.codec, meta.schema)

    data = avro.write_avro(morsel)

    with open("out.avro", "wb") as f:
        with avro.open_avro_writer(f.write) as w:
            for morsel in morsels:
                w.write_batch(morsel)

Reader design and limits: docs/AVRO_READER_DESIGN.md; writer: docs/AVRO_WRITER_DESIGN.md.
"""

import json
from typing import Callable, Iterable, Optional, Sequence, Union

from rugo.rugo_native import read_avro as _read_avro
from rugo.rugo_native import read_avro_metadata as _read_avro_metadata
from rugo.rugo_native import open_avro_writer as _open_avro_writer
from rugo.rugo_native import write_avro as _write_avro
from rugo.rugo_native import write_avro_stream as _write_avro_stream

__all__ = ["read_avro", "read_metadata", "write_avro", "open_avro_writer", "write_avro_stream"]

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
        self.codec = codec          # null / deflate / snappy / zstandard / bzip2
        self.metadata = metadata    # the header's metadata map, {str: bytes}

    @property
    def columns(self) -> list:
        """Top-level field names of the writer schema. A non-record schema (a file of
        bare values) reads as one column, `value`."""
        if type(self.schema) is dict and self.schema["type"] in ("record", "error"):
            return [f["name"] for f in self.schema["fields"]]
        return ["value"]

    def __repr__(self):
        return f"AvroMetadata(codec={self.codec!r}, columns={self.columns})"


class _AvroReader:
    """Context-managed reader that yields one Morsel per batch.

    A batch is whole Avro blocks packed up to 65,536 rows (a block is never split)."""

    def __init__(self, source, columns, reader_schema, max_threads):
        self._source = source
        self._columns = columns
        self._reader_schema = reader_schema
        self._max_threads = max_threads

    def __enter__(self) -> "_AvroReader":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def __iter__(self):
        result = _read_avro(
            _load(self._source),
            None if self._columns is None else list(self._columns),
            _schema_text(self._reader_schema),
            self._max_threads,
        )
        from draken.morsels.morsel import Morsel

        for vectors, rows in zip(result["batches"], result["batch_rows"]):
            morsel = Morsel.from_vectors(result["column_names"], vectors)
            # With no columns (a record with no fields) the vectors carry no length;
            # the row count must come from the batch itself.
            morsel._zero_col_num_rows = rows
            yield morsel


def read_avro(
    source: Source,
    columns: Optional[Sequence[str]] = None,
    reader_schema: Optional[Union[str, dict]] = None,
    max_threads: int = 0,
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
    max_threads: the file's batches (independent runs of whole Avro blocks) are
        decompressed and decoded on up to this many threads; 0 = every core. Morsels
        are yielded in file order either way.

    Types: boolean BOOL · int INT32 · long INT64 · float FLOAT32 · double FLOAT64 ·
    string VARCHAR · bytes / fixed VARBINARY · enum VARCHAR (one entry per symbol,
    positions per row) · date DATE · time-* TIME (us) · timestamp-* TIMESTAMP (us, UTC)
    · decimal DECIMAL / DECIMAL128 · uuid (string or fixed(16)) UUID
    · array of a plain scalar ARRAY · a whole record,
    map, or array of nested values NVARCHAR JSON text (the parquet nested-JSON rules).
    A file whose schema is not a record (bare strings, longs, ...) reads as one
    column, `value`.

    Codecs: null, deflate, snappy, zstandard, bzip2. Refused, by name: xz, unions
    other than [null, T], recursive types, decimal precision > 38,
    timestamp-nanos, local-timestamp-*, duration.
    """
    return _AvroReader(source, columns, reader_schema, max_threads)


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


def write_avro(
    morsel,
    codec: str = "deflate",
    block_bytes: int = 1 << 20,
    record_name: str = "record",
    sync_marker: Optional[bytes] = None,
    max_threads: int = 0,
    dictionary: bool = True,
) -> bytes:
    """Serialize a Morsel to Avro object container bytes (docs/AVRO_WRITER_DESIGN.md).

    A column with no nulls is written as plain T, one with nulls as `["null", T]`; the
    root record is `rugo.<record_name>`. The header holds `avro.schema`, `avro.codec` and
    `rugo.created_by` (this build's writer id).

    Types: BOOL boolean · INT8/16/32, UINT8/16 int · INT64, UINT32 long · FLOAT32 float
    · FLOAT64 double · VARCHAR/NVARCHAR/VARIANT string · VARBINARY bytes · DATE date ·
    TIME time-millis (s, ms) / time-micros (us) · TIMESTAMP timestamp-millis (s, ms) /
    timestamp-micros (us) · DECIMAL/DECIMAL128 fixed decimal(p, s) at the smallest width for
    p · UUID fixed(16) uuid ·
    IPV4 long with "opteryx.type": "IPV4" (rugo reads it back as IPV4) · a typeless NULL
    column an all-null int · ARRAY<T> an array of ["null", T] (nested arrays nest).
    Refused by name: UINT64, INTERVAL, VECTOR_FP16 (Avro cannot carry them) and
    nanosecond TIME / TIMESTAMP (cast to microseconds first).
    codec: 'null', 'deflate' (default), 'snappy', 'zstandard' or 'bzip2'.
    block_bytes: rows go into a block until their encoded size reaches this (default
        1 MiB), at most 65,536 rows per block.
    sync_marker: 16 bytes for the file's sync marker; None = random.
    max_threads: blocks are encoded and compressed on up to this many threads; 0 = every
        core. The bytes written are the same for any value.
    dictionary: True (default) writes a VARCHAR / NVARCHAR column as an Avro enum when
        every value is a valid Avro name, it has at most 4,096 distinct values, and the
        enum is smaller than the strings; rugo reads it back dictionary-encoded.
    A column name that is not a valid Avro name ([A-Za-z_][A-Za-z0-9_]*) or appears
    twice raises ValueError — nothing is renamed.
    """
    return _write_avro(morsel, codec=codec, block_bytes=block_bytes,
                       record_name=record_name, sync_marker=sync_marker,
                       max_threads=max_threads, dictionary=dictionary)


def open_avro_writer(
    sink: Callable[[bytes], object],
    codec: str = "deflate",
    block_bytes: int = 1 << 20,
    record_name: str = "record",
    sync_marker: Optional[bytes] = None,
    max_threads: int = 0,
):
    """Open a streaming Avro writer. `sink` is a callable taking bytes (`f.write`).

    Use as a context manager and call `write_batch(morsel)` once per batch. The first
    batch fixes the schema, and since later batches are unknown every field is
    `["null", T]` and strings stay `string` (write_avro, which sees the whole column,
    writes no-null columns plain and low-cardinality strings as enums); the sink receives the header, then every block, as each
    batch is written — memory is bounded by one batch, not the file. A later batch whose
    columns differ (names, order, types) raises ValueError naming the column and writes
    nothing. Blocks never span two batches, so the same rows written in other batch
    boundaries give a different (equally valid) block layout. Avro has no footer: after
    an exception the sink holds a valid file of the blocks it received. A writer that
    never saw a batch raises ValueError on a clean exit (no schema, no file). Options as
    write_avro.
    """
    return _open_avro_writer(sink, codec=codec, block_bytes=block_bytes,
                             record_name=record_name, sync_marker=sync_marker,
                             max_threads=max_threads)


def write_avro_stream(
    morsels: Iterable,
    sink: Callable[[bytes], object],
    codec: str = "deflate",
    block_bytes: int = 1 << 20,
    record_name: str = "record",
    sync_marker: Optional[bytes] = None,
    max_threads: int = 0,
) -> int:
    """Write every Morsel of `morsels` to `sink` as one Avro file (see open_avro_writer);
    returns the number of rows written. An empty iterable raises ValueError."""
    return _write_avro_stream(morsels, sink, codec=codec, block_bytes=block_bytes,
                              record_name=record_name, sync_marker=sync_marker,
                              max_threads=max_threads)
