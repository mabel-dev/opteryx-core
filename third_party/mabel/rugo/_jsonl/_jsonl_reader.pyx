# cython: language_level=3, cdivision=True
# distutils: language = c++

from libc.stdint cimport uint8_t, int64_t, uint32_t, size_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.map cimport map as cmap

from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.float64_vector cimport Float64Vector
from opteryx.compiled.draken.vectors.bool_vector cimport BoolVector
from opteryx.compiled.draken.vectors.string_vector cimport StringVector, StringVectorBuilder


cdef extern from "core/parse_context.hpp" namespace "rugo::_jsonl":
    struct Predicate:
        string column
        uint8_t op
        string value

    struct ParseContext:
        vector[string] projected_columns
        vector[Predicate] predicates
        bint infer_schema
        uint32_t infer_sample_size
        bint parse_arrays
        bint parse_objects
        bint fail_on_error


cdef extern from "core/markers.hpp" namespace "rugo::_jsonl":
    struct FieldSpan:
        uint32_t key_start
        uint32_t key_end
        uint32_t value_start
        uint32_t value_end
        uint8_t type


cdef extern from "core/field_span.hpp" namespace "rugo::_jsonl":
    struct InterpreterResult:
        vector[vector[FieldSpan]] all_records
        size_t num_records_passed
        uint32_t bytes_consumed

    struct OrdinalPredictor:
        pass


cdef extern from "core/jsonl_reader.hpp" namespace "rugo::_jsonl":
    struct ReadResult:
        bint success
        string error_message
        vector[string] column_names
        vector[vector[FieldSpan]] records
        vector[uint8_t] buffer_data
        cmap[string, string] inferred_schema
        size_t num_records

    cdef cppclass JsonlReader:
        JsonlReader(const string& file_path, const ParseContext& context)
        JsonlReader(const uint8_t* buffer, size_t length, const ParseContext& context)
        ReadResult next_chunk()
        bint is_eof()
        bint has_error()
        const string& get_error()


cdef extern from "core/column_builder.hpp" namespace "rugo::_jsonl":
    cdef enum class ColumnType(uint8_t):
        Int64   = 0
        Float64 = 1
        Bool    = 2
        String  = 3
        Null    = 4

    struct ColumnResult:
        ColumnType col_type
        size_t num_rows
        vector[uint8_t] null_flags
        vector[uint8_t] data
        vector[uint8_t]  str_data
        vector[uint32_t] str_offsets
        vector[uint32_t] str_lengths

    ColumnResult extract_column(
        const uint8_t* buffer,
        const vector[vector[FieldSpan]]& records,
        const string& column_name
    )
    void merge_column(ColumnResult& dest, ColumnResult&& src)


def read_jsonl(
    data,
    columns=None,
    predicates=None,
    explicit_schema=None,
    infer_schema=True,
    infer_sample_size=5,
    parse_arrays=True,
    parse_objects=True,
    fail_on_error=True
):
    """
    Read JSONL data into Draken vectors with projection and predicate pushdown.

    Parameters:
      data: bytes or buffer-like (or file path string)
      columns: list of column names to extract (None = all)
      predicates: list of (column, op, value) tuples; op in ['==', '!=', '<', '<=', '>', '>=']

    Returns:
      dict with keys:
        'success': bool
        'column_names': list[str]
        'num_rows': int
        'columns': list of Draken Vector objects
        'schema': dict of inferred/applied types
    """
    cdef ParseContext context
    cdef JsonlReader* reader = NULL
    cdef Predicate pred
    cdef ReadResult chunk_result
    cdef list column_names = []
    cdef list chunk_buffers = []
    cdef list chunk_records = []   # list of C++ vector[vector[FieldSpan]]
    cdef size_t total_rows = 0
    cdef dict schema = {}
    cdef dict result = {
        'success': False,
        'column_names': [],
        'num_rows': 0,
        'columns': [],
        'schema': {}
    }

    try:
        # Build ParseContext
        if columns:
            for col in columns:
                context.projected_columns.push_back(col.encode('utf-8'))

        if predicates:
            for col, op, val in predicates:
                pred.column = col.encode('utf-8')
                pred.op = <uint8_t>_parse_op(op)
                pred.value = str(val).encode('utf-8')
                context.predicates.push_back(pred)

        context.infer_schema = infer_schema
        context.infer_sample_size = infer_sample_size
        context.parse_arrays = parse_arrays
        context.parse_objects = parse_objects
        context.fail_on_error = fail_on_error

        # Create reader
        if isinstance(data, str):
            reader = new JsonlReader(<string>data.encode('utf-8'), context)
        elif isinstance(data, bytes):
            reader = new JsonlReader(<const uint8_t*><bytes>data, len(data), context)
        else:
            data_bytes = bytes(data)
            reader = new JsonlReader(<const uint8_t*><bytes>data_bytes, len(data_bytes), context)

        # Accumulate chunks; each chunk keeps its own buffer so FieldSpan
        # offsets remain valid when we later call extract_column per chunk.
        while True:
            chunk_result = reader.next_chunk()

            if not chunk_result.success:
                if reader.has_error():
                    result['error'] = reader.get_error().decode('utf-8')
                break

            if chunk_result.num_records > 0:
                if not column_names:
                    for col in chunk_result.column_names:
                        column_names.append(col.decode('utf-8'))

                # Keep buffer as bytes (owns the memory) and records as C++ object
                chunk_buffers.append(bytes(chunk_result.buffer_data))
                chunk_records.append(chunk_result.records)
                total_rows += chunk_result.num_records

                if chunk_result.inferred_schema.size() > 0:
                    for key, value in chunk_result.inferred_schema:
                        schema[key.decode('utf-8')] = value.decode('utf-8')

            if reader.is_eof():
                break

        # Build Draken vectors — one C++ call per column per chunk
        if total_rows > 0 and column_names:
            vectors = _build_vectors_from_chunks(
                chunk_buffers, chunk_records, column_names, total_rows
            )
            result['columns'] = vectors
            result['column_names'] = column_names
            result['num_rows'] = total_rows
            result['schema'] = schema
            result['success'] = True

        return result

    finally:
        if reader != NULL:
            del reader


cdef uint8_t _parse_op(str op):
    ops = {
        '==': 0,  # EQ
        '!=': 1,  # NE
        '<': 2,   # LT
        '<=': 3,  # LE
        '>': 4,   # GT
        '>=': 5,  # GE
    }
    return ops.get(op, 0)


cdef list _build_vectors_from_chunks(
    list chunk_buffers,
    list chunk_records,
    list column_names,
    size_t total_rows
):
    """
    For each column, call extract_column() once per chunk (C++ does all field
    lookup and type detection), merge results, then wrap as a Draken vector.
    No Python per-row iteration.
    """
    cdef list vectors = []
    cdef ColumnResult merged
    cdef ColumnResult chunk_col
    cdef bytes buf_bytes
    cdef const uint8_t* buf_ptr
    cdef string col_name_cpp
    cdef size_t i

    for col_name in column_names:
        col_name_cpp = col_name.encode('utf-8')
        merged = ColumnResult()

        for i in range(len(chunk_buffers)):
            buf_bytes = <bytes>chunk_buffers[i]
            buf_ptr = <const uint8_t*>buf_bytes
            chunk_col = extract_column(
                buf_ptr,
                <const vector[vector[FieldSpan]]&>chunk_records[i],
                col_name_cpp
            )
            merge_column(merged, (<ColumnResult&&>chunk_col))

        vec = _draken_from_column_result(merged)
        if vec is not None:
            vectors.append(vec)

    return vectors


cdef _draken_from_column_result(ColumnResult& cr):
    """Wrap a merged ColumnResult into the appropriate Draken vector."""
    cdef size_t num_rows = cr.num_rows
    cdef size_t bitmap_bytes = (num_rows + 7) >> 3
    cdef uint8_t* null_bmp = NULL
    cdef size_t i

    if num_rows == 0:
        return None

    # Build null bitmap if any nulls exist
    cdef bint has_nulls = False
    for i in range(num_rows):
        if cr.null_flags[i] == 0:
            has_nulls = True
            break

    if has_nulls:
        null_bmp = <uint8_t*>malloc(bitmap_bytes)
        if null_bmp == NULL:
            raise MemoryError()
        memset(null_bmp, 0xFF, bitmap_bytes)
        for i in range(num_rows):
            if cr.null_flags[i] == 0:
                null_bmp[i >> 3] &= ~(<uint8_t>1 << (i & 7))

    if cr.col_type == ColumnType.Int64:
        vec = Int64Vector(num_rows)
        if cr.data.size() >= num_rows * 8:
            memcpy(<void*>vec.ptr.data, cr.data.data(), num_rows * 8)
        if has_nulls:
            vec.ptr.null_bitmap = null_bmp
        return vec

    elif cr.col_type == ColumnType.Float64:
        vec = Float64Vector(num_rows)
        if cr.data.size() >= num_rows * 8:
            memcpy(<void*>vec.ptr.data, cr.data.data(), num_rows * 8)
        if has_nulls:
            vec.ptr.null_bitmap = null_bmp
        return vec

    elif cr.col_type == ColumnType.Bool:
        vec = BoolVector(num_rows)
        cdef uint8_t* bdata = <uint8_t*>vec.ptr.data
        cdef size_t bdata_bytes = (num_rows + 7) >> 3
        if bdata != NULL and bdata_bytes > 0:
            memset(bdata, 0, bdata_bytes)
        for i in range(num_rows):
            if cr.data.size() > i and cr.data[i]:
                bdata[i >> 3] |= (<uint8_t>1 << (i & 7))
        if has_nulls:
            vec.ptr.null_bitmap = null_bmp
        return vec

    elif cr.col_type == ColumnType.String:
        if has_nulls and null_bmp != NULL:
            free(null_bmp)
            null_bmp = NULL
        # String: iterate spans — tight loop with direct C++ pointer access
        cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(num_rows, 16)
        cdef const uint8_t* sdata = cr.str_data.data() if cr.str_data.size() > 0 else NULL
        for i in range(num_rows):
            if cr.null_flags[i] == 0:
                builder.append_null()
            else:
                builder.append_bytes(
                    <const char*>(sdata + cr.str_offsets[i]),
                    cr.str_lengths[i]
                )
        return builder.finish()

    else:
        # Null column: return all-null string vector
        if has_nulls and null_bmp != NULL:
            free(null_bmp)
        cdef StringVectorBuilder nb = StringVectorBuilder.with_estimate(num_rows, 0)
        for i in range(num_rows):
            nb.append_null()
        return nb.finish()


def get_jsonl_schema(data, sample_size=5):
    """Infer schema from first N rows."""
    result = read_jsonl(
        data,
        columns=None,
        predicates=None,
        explicit_schema=None,
        infer_schema=True,
        infer_sample_size=sample_size
    )

    if result['success']:
        schema_list = []
        for col_name in result['column_names']:
            schema_list.append({
                'name': col_name,
                'type': result['schema'].get(col_name, 'object'),
                'nullable': True
            })
        return {'columns': schema_list}

    return {'columns': []}
