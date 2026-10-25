from libc.stdint cimport int64_t, uint8_t, uint32_t, uint64_t
from draken.core.buffers cimport DrakenType, DRAKEN_ARRAY, DRAKEN_INT64
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector


cdef extern from "orc/orc_reader.hpp" namespace "rugo::orc":
    cdef cppclass OrcReaderColumn "rugo::orc::ReaderColumn":
        string name
        int64_t field_id
        DrakenType type
        uint8_t precision
        uint8_t scale
        DrakenType child_type
        uint8_t array_depth
        cbool nullable

    cdef cppclass OrcFileColumn "rugo::orc::FileColumn":
        string name
        string kind_name
        int64_t iceberg_id
        cbool readable
        string refusal
        DrakenType type
        uint8_t precision
        uint8_t scale
        cbool nested
        DrakenType child_type
        uint8_t array_depth

    # except + : every malformed file, refused construct, or unmatched column throws
    # std::runtime_error -> RuntimeError.
    cdef cppclass OrcFile:
        OrcFile(const uint8_t* data, size_t size) except +
        uint64_t num_rows()
        size_t stripe_count()
        uint32_t stripe_rows(size_t stripe)
        string compression_name()
        const vector[OrcFileColumn]& file_columns()
        void bind(const vector[OrcReaderColumn]& reader, const vector[string]& decode,
                  cbool match_by_id) except +
        void bind_all() except +
        AvroBatch decode_stripe(size_t stripe) except + nogil


def read_orc_metadata(data):
    """
    The tail of an ORC file (postscript and footer), without reading any stripe.

    Returns dict: 'num_rows', 'stripes', 'compression' (a name), and 'columns' — one
    dict per top-level column in file order: 'name', 'kind' (the ORC type kind),
    'iceberg_id' (the `iceberg.id` attribute, or None), 'readable', 'refusal' (why not),
    'type' (a DrakenType ordinal), 'precision', 'scale', 'nested' (a STRUCT / LIST / MAP),
    'child_type' (an ARRAY's innermost element, a DrakenType ordinal; else None), 'array_depth'
    (an ARRAY's levels of nesting; else None).
    """
    cdef const uint8_t[::1] buf = data
    cdef size_t n = buf.shape[0]
    cdef const uint8_t* ptr = &buf[0] if n > 0 else NULL
    cdef OrcFile* orc_file = new OrcFile(ptr, n)
    cdef size_t i
    cdef OrcFileColumn* col
    try:
        columns = []
        for i in range(orc_file.file_columns().size()):
            col = <OrcFileColumn*>&orc_file.file_columns()[i]
            columns.append({
                "name": col.name.decode("utf-8"),
                "kind": col.kind_name.decode("ascii"),
                "iceberg_id": col.iceberg_id if col.iceberg_id >= 0 else None,
                "readable": bool(col.readable),
                "refusal": col.refusal.decode("utf-8") if not col.readable else None,
                "type": <int>col.type,
                "precision": col.precision,
                "scale": col.scale,
                "nested": bool(col.nested),
                "child_type": <int>col.child_type if col.type == DRAKEN_ARRAY else None,
                "array_depth": col.array_depth if col.type == DRAKEN_ARRAY else None,
            })
        return {
            "num_rows": orc_file.num_rows(),
            "stripes": orc_file.stripe_count(),
            "compression": orc_file.compression_name().decode("ascii"),
            "columns": columns,
        }
    finally:
        del orc_file


def read_orc(data, columns=None, reader=None, match_by_id=False):
    """
    Read an ORC file into Draken vectors, a stripe at a time.

    Design: docs/ORC_READER_DESIGN.md (basic support).

    Parameters:
      data: bytes-like — the whole file.
      columns: names to read (reader-schema names when `reader` is given, else the
        file's); None = every column.
      reader: None reads each column as the file states it. Otherwise a list of
        (name, field_id, type, precision, scale, nullable[, child_type[, array_depth]]) tuples - what the
        caller wants each column to be (`type` a DrakenType ordinal; `child_type`, an
        ARRAY's innermost element, defaults to INT64; `array_depth`, its levels of nesting, defaults to 1; both only matter for an ARRAY). A column the
        file lacks is a NULL constant when nullable.
      match_by_id: match `reader` columns to the file's by `iceberg.id` instead of name.

    Returns dict: 'column_names', 'batches' (one list of Draken Vectors per stripe),
    'batch_rows', 'num_rows'.
    """
    cdef const uint8_t[::1] buf = data
    cdef size_t n = buf.shape[0]
    cdef const uint8_t* ptr = &buf[0] if n > 0 else NULL
    cdef OrcFile* orc_file = new OrcFile(ptr, n)
    cdef vector[OrcReaderColumn] rcols
    cdef vector[string] decode
    cdef OrcReaderColumn rc
    cdef AvroBatch batch
    cdef size_t si, ci
    cdef int child_ordinal
    cdef cbool by_id = match_by_id
    cdef const vector[OrcFileColumn]* file_cols
    try:
        file_cols = &orc_file.file_columns()
        if reader is None:
            if columns is None:
                orc_file.bind_all()
                names = [file_cols[0][ci].name.decode("utf-8") for ci in range(file_cols.size())]
            else:
                for ci in range(file_cols.size()):
                    rc.name = file_cols[0][ci].name
                    rc.field_id = -1
                    rc.type = file_cols[0][ci].type
                    rc.precision = file_cols[0][ci].precision
                    rc.scale = file_cols[0][ci].scale
                    rc.child_type = file_cols[0][ci].child_type
                    rc.array_depth = file_cols[0][ci].array_depth
                    rc.nullable = True
                    rcols.push_back(rc)
                names = list(columns)
                for name in names:
                    decode.push_back(name.encode("utf-8"))
                orc_file.bind(rcols, decode, False)
        else:
            for spec in reader:
                name, field_id, dtype, precision, scale, nullable = spec[:6]
                if len(spec) > 6:
                    child_ordinal = spec[6]
                    rc.child_type = <DrakenType>child_ordinal
                else:
                    rc.child_type = DRAKEN_INT64
                rc.array_depth = spec[7] if len(spec) > 7 else 1
                rc.name = name.encode("utf-8")
                rc.field_id = -1 if field_id is None else field_id
                rc.type = <DrakenType>dtype
                rc.precision = precision
                rc.scale = scale
                rc.nullable = nullable
                rcols.push_back(rc)
            names = [r[0] for r in reader] if columns is None else list(columns)
            for name in names:
                decode.push_back(name.encode("utf-8"))
            orc_file.bind(rcols, decode, by_id)
        batches = []
        batch_rows = []
        num_rows = 0
        for si in range(orc_file.stripe_count()):
            with nogil:
                batch = orc_file.decode_stripe(si)
            vectors = []
            for ci in range(batch.columns.size()):
                vectors.append(_rugo_steal(wrap_avro_column(batch.columns[ci])))
            batches.append(vectors)
            batch_rows.append(batch.rows)
            num_rows += batch.rows
        return {"column_names": names, "batches": batches, "batch_rows": batch_rows, "num_rows": num_rows}
    finally:
        del orc_file
