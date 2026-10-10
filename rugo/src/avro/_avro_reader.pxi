from libc.stdint cimport uint8_t, uint32_t
from draken.core.buffers cimport DrakenType
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.utility cimport pair
from libcpp.vector cimport vector


cdef extern from "avro/avro_reader.hpp" namespace "rugo::avro":
    cppclass AvroColumn:
        pass

    cppclass AvroBatch:
        uint32_t rows
        vector[AvroColumn] columns

    cppclass AvroRead:
        string schema_json
        vector[pair[string, string]] metadata
        vector[string] column_names
        vector[AvroBatch] batches

    # except + : every malformed file, refused schema construct, or missing column
    # throws std::runtime_error -> RuntimeError.
    void read_avro_buffer(const uint8_t* data, size_t size, const vector[string]& columns,
                          cbool all_columns, const string& reader_schema_json,
                          AvroRead& out) except + nogil
    void read_avro_header(const uint8_t* data, size_t size, AvroRead& out) except + nogil

    cdef struct AvroColumnType:
        DrakenType type
        uint8_t logical_kind
        uint8_t precision
        uint8_t scale
        DrakenType child_type

    cppclass AvroStream:
        AvroStream(const uint8_t* data, size_t size, const vector[string]& columns,
                   cbool all_columns, const string& reader_schema_json) except +
        vector[string] column_names()
        vector[AvroColumnType] column_types()


# The Python edge: kept out of avro_reader.cpp so the reader compiles without Python.h.
# NEW reference or NULL+exception; the caller takes it with _rugo_steal (rugo_native.pyx).
cdef extern from "avro/_avro_column_wrap.hpp" namespace "rugo::avro":
    PyObject* wrap_avro_column(AvroColumn& col) except NULL


cdef dict _avro_metadata(AvroRead& res):
    cdef dict meta = {}
    cdef size_t i
    for i in range(res.metadata.size()):
        meta[res.metadata[i].first.decode("utf-8")] = <bytes>res.metadata[i].second
    return meta


def read_avro(data, columns=None, reader_schema=None):
    """
    Read an Avro object container file into Draken vectors.

    Design: docs/AVRO_READER_DESIGN.md (scope A).

    Parameters:
      data: bytes-like — the whole file.
      columns: list of column names; a dotted name selects a field inside a record
        (`data_file.file_path`). None = every top-level field.
      reader_schema: an Avro schema (JSON text) to read the file as: fields match by
        field-id when both sides carry one, else by name; a field the file lacks is
        its default (or NULL). None = the file's own schema.

    Returns:
      dict with keys:
        'column_names': list[str]
        'batches': list of batches, each a list of Draken Vectors (one per column);
          whole blocks are packed into a batch up to 65536 rows
        'num_rows': int
        'schema': the file's writer schema (JSON text)
        'metadata': dict[str, bytes] — the file header's metadata map
    """
    cdef const uint8_t[::1] buf = data
    cdef vector[string] cols
    cdef string rschema
    cdef cbool all_columns = columns is None
    cdef AvroRead res
    cdef size_t bi, ci
    cdef size_t n = buf.shape[0]
    cdef const uint8_t* ptr = &buf[0] if n > 0 else NULL
    if columns is not None:
        for c in columns:
            cols.push_back(c.encode("utf-8"))
    if reader_schema is not None:
        rschema = reader_schema.encode("utf-8")
    with nogil:
        read_avro_buffer(ptr, n, cols, all_columns, rschema, res)

    batches = []
    num_rows = 0
    for bi in range(res.batches.size()):
        vectors = []
        for ci in range(res.batches[bi].columns.size()):
            vectors.append(_rugo_steal(wrap_avro_column(res.batches[bi].columns[ci])))
        batches.append(vectors)
        num_rows += res.batches[bi].rows
    return {
        "column_names": [name.decode("utf-8") for name in res.column_names],
        "batches": batches,
        "num_rows": num_rows,
        "schema": res.schema_json.decode("utf-8"),
        "metadata": _avro_metadata(res),
    }


def read_avro_metadata(data):
    """
    The header of an Avro object container file, without reading any block.

    Returns dict with 'schema' (the writer schema, JSON text) and 'metadata'
    (dict[str, bytes]).
    """
    cdef const uint8_t[::1] buf = data
    cdef AvroRead res
    cdef size_t n = buf.shape[0]
    cdef const uint8_t* ptr = &buf[0] if n > 0 else NULL
    with nogil:
        read_avro_header(ptr, n, res)
    return {"schema": res.schema_json.decode("utf-8"), "metadata": _avro_metadata(res)}


def read_avro_column_types(data, reader_schema=None):
    """
    What every top-level column decodes to, from the header alone (no block is read).

    Returns a list of dicts, one per column in schema order: 'name', 'type' (a
    DrakenType), 'logical_kind' (0 NONE, 1 TIMESTAMP, 2 TIME, 3 DECIMAL — TIMESTAMP
    and TIME are microseconds, UTC), 'precision', 'scale', and 'child_type' (an
    ARRAY's element DrakenType, else None). Types are DrakenType ordinals.
    """
    cdef const uint8_t[::1] buf = data
    cdef size_t n = buf.shape[0]
    cdef const uint8_t* ptr = &buf[0] if n > 0 else NULL
    cdef vector[string] no_columns
    cdef string rschema
    if reader_schema is not None:
        rschema = reader_schema.encode("utf-8")
    cdef AvroStream* stream = new AvroStream(ptr, n, no_columns, True, rschema)
    cdef vector[AvroColumnType] types = stream.column_types()
    names = [name.decode("utf-8") for name in stream.column_names()]
    del stream
    out = []
    cdef size_t i
    for i in range(types.size()):
        out.append({
            "name": names[i],
            "type": <int>types[i].type,
            "logical_kind": types[i].logical_kind,
            "precision": types[i].precision,
            "scale": types[i].scale,
            "child_type": <int>types[i].child_type if <int>types[i].type == 80 else None,
        })
    return out
