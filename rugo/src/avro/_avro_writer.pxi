# distutils: language = c++
#
# Native Avro writer: Morsel -> Avro object container bytes, one-shot (write_avro) or
# streamed to a sink (open_avro_writer / write_avro_stream). The encoding is in C++
# (avro/avro_writer.hpp); this edge only unpacks each Morsel into unified DrakenVector
# pointers plus a FieldSpec per column, and hands the C++ chunks to Python.

from libc.stdint cimport uint8_t, uint32_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector

from libc.string cimport memcpy
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING

from draken.core.buffers cimport DrakenVector, DrakenType, DRAKEN_ARRAY, DRAKEN_VARCHAR, DRAKEN_NVARCHAR
from draken.morsels.morsel cimport Morsel
from draken.vectors.vector cimport Vector


cdef extern from "avro/avro_writer.hpp" namespace "rugo::avro":
    cdef enum class WriteCodec(uint8_t):
        Null
        Deflate
        Snappy
        Zstandard
        Bzip2

    cdef cppclass ElemSpec:
        DrakenType type
        uint8_t logical_kind
        int unit
        int scale
        int precision

    cdef cppclass FieldSpec:
        string name
        DrakenType type
        uint8_t logical_kind
        int unit
        int scale
        int precision
        vector[ElemSpec] elements
        cbool required
        vector[string] symbols

    cdef cppclass WriteColumn:
        const DrakenVector* vec
        vector[const DrakenVector*] levels
        const uint32_t* enum_codes

    cdef cppclass WriteOptions:
        WriteCodec codec
        size_t block_bytes
        string record_name
        cbool have_sync
        uint8_t sync[16]
        size_t max_threads

    # std::invalid_argument -> ValueError (bad codec / name / type / block size);
    # std::runtime_error -> RuntimeError; std::logic_error (a writer bug) -> the
    # Cython default mapping. Hence `except +` throughout.
    WriteCodec parse_write_codec(const string& name) except +
    cbool column_has_nulls(const DrakenVector* v) nogil
    cbool build_enum(const DrakenVector* v, size_t max_symbols, size_t max_threads,
                     vector[string]& symbols, vector[uint32_t]& codes) except + nogil
    size_t kMaxEnumSymbols

    cdef cppclass AvroWriter:
        AvroWriter(vector[FieldSpec] fields, const WriteOptions& opts) except +
        const string& schema()
        void write_batch(const vector[FieldSpec]& fields, const WriteColumn* cols,
                         uint32_t nrows) except + nogil
        vector[vector[uint8_t]] take_pending() nogil
        unsigned long long rows_written()
        unsigned long long blocks_written()


cdef inline void _avro_fill_logical(uint8_t* kind_out, int* unit_out, int* scale_out,
                                    int* prec_out, object nb) except *:
    """The logical descriptor of a nanobind Vector handle (the same properties the
    parquet edge reads). A vector with no descriptor leaves kind 0."""
    cdef object kind = nb.logical_type_kind
    if kind is None:
        return
    kind_out[0] = <uint8_t><int>kind.value
    cdef object unit = nb.logical_type_unit
    cdef object scale = nb.logical_type_scale
    cdef object prec = nb.logical_type_precision
    if unit is not None:
        unit_out[0] = _text_unit_code(unit)
    if scale is not None:
        scale_out[0] = <int>scale
    if prec is not None:
        prec_out[0] = <int>prec


cdef inline void _avro_fill_column(FieldSpec* spec, WriteColumn* col, object nm, Vector v,
                                   list keepalive) except *:
    """The field's name, type and descriptor, and — for an ARRAY — the child chain
    (array_child / array_child_type, as _fill_array_levels walks it for the text
    writers), each level's Vector kept alive in `keepalive` for the write."""
    cdef bytes nb_name = nm if type(nm) is bytes else (<str>nm).encode("utf-8")
    spec.name = string(<const char*>nb_name, len(nb_name))
    spec.type = v.unified().type
    col.vec = v.unified()
    _avro_fill_logical(&spec.logical_kind, &spec.unit, &spec.scale, &spec.precision, v._nb)
    cdef Vector cur = v
    cdef Vector cv
    cdef ElemSpec e
    while cur.unified().type == DRAKEN_ARRAY and cur._nb.array_child_type is not None:
        cv = Vector(cur._nb.array_child)
        keepalive.append(cv)
        e.type = cv.unified().type
        e.logical_kind = 0
        e.unit = 0
        e.scale = 0
        e.precision = 0
        _avro_fill_logical(&e.logical_kind, &e.unit, &e.scale, &e.precision, cv._nb)
        spec.elements.push_back(e)
        col.levels.push_back(cv.unified())
        cur = cv


cdef void _avro_options(WriteOptions* opts, str caller, str codec, size_t block_bytes,
                        str record_name, object sync_marker, size_t max_threads) except *:
    opts.codec = parse_write_codec(codec.encode("utf-8"))
    opts.block_bytes = block_bytes
    opts.record_name = record_name.encode("utf-8")
    opts.max_threads = max_threads
    cdef bytes sync
    cdef int i
    if sync_marker is None:
        opts.have_sync = False
        return
    sync = bytes(sync_marker)
    if len(sync) != 16:
        raise ValueError("%s: sync_marker must be exactly 16 bytes" % caller)
    opts.have_sync = True
    for i in range(16):
        opts.sync[i] = (<const uint8_t*>sync)[i]


cdef void _avro_unpack(Morsel morsel, str caller, vector[FieldSpec]& fields,
                       vector[WriteColumn]& cols, list keepalive) except *:
    """One FieldSpec + WriteColumn per column of `morsel`; every Vector (and every
    ARRAY level) is appended to `keepalive`, which must outlive the write."""
    cdef Py_ssize_t ncols = morsel._num_columns()
    if ncols == 0:
        raise ValueError("%s: morsel has no columns" % caller)
    cdef list names = morsel._col_names
    fields.resize(ncols)
    cols.resize(ncols)
    cdef Vector v
    cdef Py_ssize_t c
    for c in range(ncols):
        v = morsel._get_column(c)
        keepalive.append(v)
        _avro_fill_column(&fields[c], &cols[c], names[c], v, keepalive)


cdef void _avro_one_shot(vector[FieldSpec]& fields, vector[WriteColumn]& cols,
                         vector[vector[uint32_t]]& codes, bint dictionary,
                         size_t max_threads) except *:
    """What only a one-shot write can know, having the whole column: a column with no
    nulls is written as plain T (no union byte per value), and — with `dictionary` — a
    low-cardinality VARCHAR / NVARCHAR column whose values are all valid Avro names is
    written as an enum (rugo reads it back dictionary-encoded). `codes` must already
    have one entry per column; its buffers back cols[c].enum_codes for the write."""
    cdef size_t c
    cdef vector[string] symbols
    cdef const DrakenVector* v
    for c in range(fields.size()):
        v = cols[c].vec
        fields[c].required = not column_has_nulls(v)
        if dictionary and (v.type == DRAKEN_VARCHAR or v.type == DRAKEN_NVARCHAR):
            with nogil:
                if build_enum(v, kMaxEnumSymbols, max_threads, symbols, codes[c]):
                    fields[c].symbols = symbols
                    cols[c].enum_codes = codes[c].data()


def write_avro(Morsel morsel not None, str codec="deflate", size_t block_bytes=1 << 20,
               str record_name="record", sync_marker=None, size_t max_threads=0,
               bint dictionary=True):
    """Serialize a Morsel to Avro object container bytes.

    Design: docs/AVRO_WRITER_DESIGN.md. Every field is `["null", T]`; the root record
    is `rugo.<record_name>`. The header carries avro.schema, avro.codec and
    rugo.created_by (this build's writer id).

    codec: 'null', 'deflate' (default), 'snappy', 'zstandard' or 'bzip2'.
    block_bytes: a block holds rows until their encoded size reaches this many bytes
        (at most 65,536 rows); the last block of a batch may be smaller.
    record_name: the root record's name, a valid Avro name.
    sync_marker: 16 bytes to use as the file's sync marker (None = random); lets two
        writes of the same rows compare byte for byte.
    max_threads: blocks are encoded and compressed on up to this many threads (0 =
        every core). The bytes written do not depend on it.
    dictionary: True (default) writes a VARCHAR / NVARCHAR column as an Avro enum when
        every value is a valid Avro name, it has at most 4,096 distinct values, and the
        enum is smaller than the strings; rugo reads such a column back
        dictionary-encoded. False writes every string column as `string`.
    A column with no nulls is written as plain T rather than ["null", T] (a one-shot
    write sees the whole column; open_avro_writer cannot, so streamed fields keep the
    union and strings stay strings).
    Raises ValueError for a column name that is not a valid Avro name, a duplicate
    name, a type Avro cannot carry, an unknown codec, or a morsel with no columns.
    """
    cdef WriteOptions opts
    _avro_options(&opts, "write_avro", codec, block_bytes, record_name, sync_marker, max_threads)
    cdef vector[FieldSpec] fields
    cdef vector[WriteColumn] cols
    cdef list keepalive = []
    _avro_unpack(morsel, "write_avro", fields, cols, keepalive)
    cdef uint32_t nrows = <uint32_t>morsel.num_rows
    cdef vector[vector[uint32_t]] codes
    codes.resize(fields.size())
    _avro_one_shot(fields, cols, codes, dictionary, max_threads)

    cdef AvroWriter* w = new AvroWriter(fields, opts)
    cdef vector[vector[uint8_t]] chunks
    try:
        with nogil:
            w.write_batch(fields, cols.data(), nrows)
            chunks = w.take_pending()
    finally:
        del w
    # One allocation for the whole file; the chunks are copied straight into it.
    cdef size_t total = 0, off = 0, k
    for k in range(chunks.size()):
        total += chunks[k].size()
    cdef bytes result = PyBytes_FromStringAndSize(NULL, <Py_ssize_t>total)
    cdef char* dst = PyBytes_AS_STRING(result)
    with nogil:
        for k in range(chunks.size()):
            memcpy(dst + off, chunks[k].data(), chunks[k].size())
            off += chunks[k].size()
    return result


cdef class _StreamingAvroWriter:
    """A streaming Avro writer over a byte-chunk sink (see open_avro_writer).

    The schema comes from the first batch; the header and then each block are passed
    to `sink` as they are produced. A batch is atomic: it is validated and fully
    encoded before any of its bytes reach the sink. If the sink raises, the writer is
    closed — the bytes the sink missed cannot be replayed, so nothing more may follow.
    """
    cdef AvroWriter* _w
    cdef object _sink
    cdef WriteOptions _opts
    cdef bint _closed

    def __cinit__(self, object sink, str codec="deflate", size_t block_bytes=1 << 20,
                  str record_name="record", sync_marker=None, size_t max_threads=0):
        self._w = NULL
        self._closed = False
        if not callable(sink):
            raise TypeError("open_avro_writer: sink must be a callable taking bytes")
        _avro_options(&self._opts, "open_avro_writer", codec, block_bytes, record_name,
                      sync_marker, max_threads)
        self._sink = sink

    def __dealloc__(self):
        if self._w != NULL:
            del self._w
            self._w = NULL

    def write_batch(self, Morsel morsel not None):
        """Encode `morsel` as one or more blocks and pass them to the sink. The first
        batch fixes the schema; a later batch whose columns differ (names, order, types)
        raises ValueError naming the column, and writes nothing."""
        if self._closed:
            raise ValueError("write_batch: the Avro writer is closed")
        cdef vector[FieldSpec] fields
        cdef vector[WriteColumn] cols
        cdef list keepalive = []
        _avro_unpack(morsel, "write_batch", fields, cols, keepalive)
        cdef uint32_t nrows = <uint32_t>morsel.num_rows
        if self._w == NULL:
            self._w = new AvroWriter(fields, self._opts)
        cdef vector[vector[uint8_t]] chunks
        with nogil:
            self._w.write_batch(fields, cols.data(), nrows)
            chunks = self._w.take_pending()
        cdef size_t k
        try:
            for k in range(chunks.size()):
                self._sink(PyBytes_FromStringAndSize(<const char*>chunks[k].data(),
                                                     <Py_ssize_t>chunks[k].size()))
        except BaseException:
            self._closed = True
            raise

    @property
    def rows_written(self):
        return 0 if self._w == NULL else self._w.rows_written()

    @property
    def blocks_written(self):
        return 0 if self._w == NULL else self._w.blocks_written()

    def close(self):
        """Finish. Avro has no footer, so nothing is written; a writer that never saw a
        batch raises ValueError — with no schema there is no file. Idempotent."""
        if self._closed:
            return
        self._closed = True
        if self._w == NULL:
            raise ValueError(
                "open_avro_writer: no batch was written — an Avro file takes its schema "
                "from the first batch, so there is no file to finish")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # A clean exit closes (and refuses a writer that never saw a batch); on an
        # exception the sink keeps the complete blocks it already has.
        if exc_type is None:
            self.close()
        else:
            self._closed = True
        return False


def open_avro_writer(sink, str codec="deflate", size_t block_bytes=1 << 20,
                     str record_name="record", sync_marker=None, size_t max_threads=0):
    """Open a streaming Avro writer: `sink` is a callable taking bytes. Options as
    write_avro. Use as a context manager; call write_batch(morsel) per batch."""
    return _StreamingAvroWriter(sink, codec, block_bytes, record_name, sync_marker, max_threads)


def write_avro_stream(morsel_iter, sink, str codec="deflate", size_t block_bytes=1 << 20,
                      str record_name="record", sync_marker=None, size_t max_threads=0):
    """Write every Morsel of `morsel_iter` to `sink` as one Avro file; returns the
    number of rows written. An iterator that yields nothing raises ValueError (there
    is no schema to write a file with)."""
    cdef _StreamingAvroWriter w = _StreamingAvroWriter(
        sink, codec, block_bytes, record_name, sync_marker, max_threads)
    with w:
        for m in morsel_iter:
            w.write_batch(m)
    return w.rows_written
