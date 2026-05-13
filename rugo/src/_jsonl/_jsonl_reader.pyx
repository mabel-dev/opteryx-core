# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport uint8_t, int64_t, uint32_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.map cimport map as cmap

from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.float64_vector cimport Float64Vector
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.string_vector cimport StringVector, StringVectorBuilder


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
        uint32_t key_width
        uint32_t value_start
        uint32_t value_width
        uint8_t type


cdef extern from "core/markers.hpp" namespace "rugo::_jsonl":
    struct MarkerPosition:
        uint32_t position
        uint8_t marker_type


cdef extern from "core/field_span.hpp" namespace "rugo::_jsonl":
    struct InterpreterResult:
        vector[vector[FieldSpan]] all_records
        size_t num_records_passed
        uint32_t bytes_consumed

    cppclass OrdinalPredictor:
        pass

    InterpreterResult interpret_jsonl(
        const uint8_t* buffer_data,
        size_t buffer_length,
        const vector[MarkerPosition]& markers,
        const ParseContext& context,
        OrdinalPredictor& predictor
    )

    InterpreterResult interpret_jsonl_parallel(
        const uint8_t* buffer_data,
        size_t buffer_length,
        const vector[MarkerPosition]& markers,
        const ParseContext& context,
        OrdinalPredictor& predictor,
        size_t min_rows_per_thread
    )


cdef extern from "core/structural_scan.hpp" namespace "rugo::_jsonl":
    vector[MarkerPosition] scan_structural_markers(
        const uint8_t* buffer,
        size_t length
    )


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
        vector[uint8_t] data
        vector[uint8_t] null_bitmap
        vector[uint8_t]  str_data
        vector[uint32_t] str_offsets
        vector[uint32_t] str_lengths
        uint8_t* data_ptr()
        uint8_t* bitmap_ptr()
        uint8_t* str_ptr()

    struct StringColumnResult:
        ColumnType inferred_type
        size_t num_rows
        vector[uint8_t] data
        vector[uint32_t] offsets
        vector[uint32_t] lengths
        vector[uint8_t] null_bitmap
        uint8_t* data_ptr()
        uint32_t* offset_ptr()
        uint32_t* length_ptr()
        uint8_t* bitmap_ptr()

    StringColumnResult extract_column(
        const uint8_t* buffer,
        const vector[vector[FieldSpan]]& records,
        const string& column_name,
        OrdinalPredictor& predictor
    )
    void merge_column(ColumnResult& dest, ColumnResult& src)


import os


def read_jsonl(
    data,
    columns=None,
    predicates=None,
    explicit_schema=None,
    infer_schema=True,
    infer_sample_size=5,
    parse_arrays=True,
    parse_objects=True,
    fail_on_error=True,
    use_threads=True,
    min_rows_per_thread=2048
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
    cdef const uint8_t* buf_data
    cdef size_t buf_len
    cdef vector[MarkerPosition] markers
    cdef InterpreterResult interp_result
    cdef OrdinalPredictor predictor
    cdef bint threaded_succeeded = False
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

        # Try threaded path first if requested
        threaded_succeeded = False
        if use_threads:
            # Load entire buffer
            if isinstance(data, str):
                with open(data, 'rb') as f:
                    threaded_data = f.read()
            elif isinstance(data, bytes):
                threaded_data = data
            else:
                threaded_data = bytes(data)

            if len(threaded_data) > 0:
                buf_data = <const uint8_t*>threaded_data
                buf_len = len(threaded_data)

                # Scan for markers once
                markers = scan_structural_markers(buf_data, buf_len)

                # Process in parallel
                interp_result = interpret_jsonl_parallel(
                    buf_data,
                    buf_len,
                    markers,
                    context,
                    predictor,
                    <size_t>min_rows_per_thread
                )

                # Store result as single "chunk" if successful
                if interp_result.all_records.size() > 0:
                    chunk_buffers.append(threaded_data)
                    chunk_records.append(interp_result.all_records)
                    total_rows = interp_result.num_records_passed

                    # Extract column names from first record (no projection = all columns)
                    if not column_names and interp_result.all_records.size() > 0:
                        first_record = interp_result.all_records[0]
                        for field in first_record:
                            # Decode key from buffer
                            key_bytes = threaded_data[field.key_start:field.key_start + field.key_width]
                            col_name = key_bytes.decode('utf-8', errors='ignore')
                            if col_name not in column_names:
                                column_names.append(col_name)

                    threaded_succeeded = True

        # Fall back to sequential if threaded path failed or wasn't requested
        if not threaded_succeeded:
            # Sequential path: use JsonlReader chunking
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


def benchmark_document_map(
    data: bytes,
):
    """
    Benchmark ONLY document map creation: structural scan + interpretation.
    No predicates, no projection, no vector construction.

    Returns:
      dict with:
        'num_records': int
        'scan_ms': float (structural scan time)
        'interpret_ms': float (document map building time)
        'total_ms': float
        'buffer_size_mb': float
        'sample_map': first record as list of FieldSpans
    """
    import time

    cdef:
        const uint8_t* buf_data = <const uint8_t*><bytes>data
        size_t buf_len = len(data)
        size_t num_records = 0
        ParseContext context
        OrdinalPredictor predictor
        vector[MarkerPosition] markers
        InterpreterResult interp_result

    # Step 1: Structural scan
    scan_start = time.perf_counter()
    markers = scan_structural_markers(buf_data, buf_len)
    scan_ms = (time.perf_counter() - scan_start) * 1000

    # Step 2: Document map interpretation
    interp_start = time.perf_counter()
    interp_result = interpret_jsonl(buf_data, buf_len, markers, context, predictor)
    interp_ms = (time.perf_counter() - interp_start) * 1000

    # Convert first record to Python for inspection
    sample_map = []
    if interp_result.all_records.size() > 0:
        first_record = interp_result.all_records[0]
        for field in first_record:
            sample_map.append({
                'key': (field.key_start, field.key_width),
                'value': (field.value_start, field.value_width),
                'type': field.type,
            })

    return {
        'num_records': interp_result.num_records_passed,
        'scan_ms': scan_ms,
        'interpret_ms': interp_ms,
        'total_ms': scan_ms + interp_ms,
        'buffer_size_mb': len(data) / 1024 / 1024,
        'sample_map': sample_map,
    }


def read_jsonl_raw(
    data,
    columns=None,
    predicates=None,
    infer_schema=False,
):
    """
    Read JSONL data and return raw FieldSpan records (no vector construction).
    Used for benchmarking interpretation phase in isolation.

    Returns:
      dict with keys:
        'success': bool
        'num_rows': int (records that passed predicates)
        'column_names': list[str]
        'buffer_size_mb': float
        'elapsed_ms': float
        'sample_record': first record as list of field info dicts
    """
    import time

    cdef ParseContext context
    cdef JsonlReader* reader = NULL
    cdef Predicate pred
    cdef ReadResult chunk_result
    cdef list column_names = []
    cdef size_t total_rows = 0
    cdef double total_bytes = 0
    cdef dict result = {
        'success': False,
        'num_rows': 0,
        'column_names': [],
        'buffer_size_mb': 0.0,
        'elapsed_ms': 0.0,
        'sample_record': []
    }

    try:
        start_time = time.perf_counter()

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
        context.infer_sample_size = 5
        context.parse_arrays = False
        context.parse_objects = False
        context.fail_on_error = False

        # Create reader
        if isinstance(data, str):
            reader = new JsonlReader(<string>data.encode('utf-8'), context)
        elif isinstance(data, bytes):
            reader = new JsonlReader(<const uint8_t*><bytes>data, len(data), context)
        else:
            data_bytes = bytes(data)
            reader = new JsonlReader(<const uint8_t*><bytes>data_bytes, len(data_bytes), context)

        # Read all chunks, accumulate row count
        first_record = None
        while True:
            chunk_result = reader.next_chunk()

            if not chunk_result.success:
                break

            if chunk_result.num_records > 0:
                if not column_names:
                    for col in chunk_result.column_names:
                        column_names.append(col.decode('utf-8'))

                total_rows += chunk_result.num_records
                total_bytes += chunk_result.buffer_data.size()

                # Capture first record as sample
                if first_record is None and len(chunk_result.records) > 0:
                    first_record = chunk_result.records[0]

            if reader.is_eof():
                break

        elapsed_ms = (time.perf_counter() - start_time) * 1000

        result['success'] = True
        result['num_rows'] = total_rows
        result['column_names'] = column_names
        result['buffer_size_mb'] = total_bytes / 1024 / 1024
        result['elapsed_ms'] = elapsed_ms

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
    For each column, extract as StringVector, then cast to inferred type.
    No Python per-row iteration.
    """
    cdef list vectors = []
    cdef StringColumnResult chunk_col
    cdef bytes buf_bytes
    cdef const uint8_t* buf_ptr
    cdef string col_name_cpp
    cdef size_t i
    cdef OrdinalPredictor predictor

    for col_name in column_names:
        col_name_cpp = col_name.encode('utf-8')

        # Extract first chunk to get type hint
        if len(chunk_buffers) == 0:
            continue

        buf_bytes = <bytes>chunk_buffers[0]
        buf_ptr = <const uint8_t*>buf_bytes
        chunk_col = extract_column(
            buf_ptr,
            <const vector[vector[FieldSpan]]&>chunk_records[0],
            col_name_cpp,
            predictor
        )

        # Build StringVector from extracted data
        vec = _string_vector_from_result(chunk_col)

        # Cast to inferred type and add to vectors
        if vec is not None:
            vectors.append(vec)

    return vectors


cdef _string_vector_from_result(StringColumnResult& scr):
    """Build StringVector from StringColumnResult and apply type casting."""
    cdef size_t num_rows = scr.num_rows
    cdef size_t bitmap_bytes = (num_rows + 7) >> 3
    cdef uint8_t* owned_bitmap = NULL
    cdef size_t i
    cdef StringVectorBuilder builder
    cdef StringVector string_vec
    cdef Int64Vector int_vec
    cdef Float64Vector float_vec
    cdef BoolVector bool_vec

    if num_rows == 0:
        return None

    # Copy null bitmap to owned memory
    if scr.null_bitmap.size() > 0:
        owned_bitmap = <uint8_t*>malloc(bitmap_bytes)
        if owned_bitmap == NULL:
            raise MemoryError()
        memcpy(owned_bitmap, scr.null_bitmap.data(), bitmap_bytes)

    # Build StringVector from offsets and lengths
    builder = StringVectorBuilder.with_estimate(num_rows, 16)
    for i in range(num_rows):
        if i < scr.lengths.size():
            if scr.lengths[i] == 0:
                builder.append_null()
            else:
                builder.append_bytes(
                    <const char*>(scr.data.data() + scr.offsets[i]),
                    scr.lengths[i]
                )
        else:
            builder.append_null()

    string_vec = builder.finish()
    if owned_bitmap != NULL:
        string_vec.ptr.null_bitmap = owned_bitmap

    # For now, return string vector; type casting deferred to caller
    return string_vec


cdef _draken_from_column_result(ColumnResult& cr):
    """Wrap ColumnResult into Draken vector."""
    cdef size_t num_rows = cr.num_rows
    cdef size_t bitmap_bytes = (num_rows + 7) >> 3
    cdef uint8_t* owned_bitmap = NULL
    cdef uint8_t* bdata = NULL
    cdef size_t i
    cdef Int64Vector vec_i64
    cdef Float64Vector vec_f64
    cdef BoolVector vec_bool
    cdef StringVectorBuilder builder

    if num_rows == 0:
        return None

    # If null bitmap exists, copy it to owned heap memory (vectors require ownership)
    if cr.null_bitmap.size() > 0:
        owned_bitmap = <uint8_t*>malloc(bitmap_bytes)
        if owned_bitmap == NULL:
            raise MemoryError()
        memcpy(owned_bitmap, cr.null_bitmap.data(), bitmap_bytes)

    if cr.col_type == ColumnType.Int64:
        vec_i64 = Int64Vector(num_rows)
        if cr.data.size() >= num_rows * 8:
            memcpy(<void*>vec_i64.ptr.data, cr.data.data(), num_rows * 8)
        if owned_bitmap != NULL:
            vec_i64.ptr.null_bitmap = owned_bitmap
        return vec_i64

    elif cr.col_type == ColumnType.Float64:
        vec_f64 = Float64Vector(num_rows)
        if cr.data.size() >= num_rows * 8:
            memcpy(<void*>vec_f64.ptr.data, cr.data.data(), num_rows * 8)
        if owned_bitmap != NULL:
            vec_f64.ptr.null_bitmap = owned_bitmap
        return vec_f64

    elif cr.col_type == ColumnType.Bool:
        vec_bool = BoolVector(num_rows)
        bdata = <uint8_t*>vec_bool.ptr.data
        if cr.data.size() > 0 and bdata != NULL:
            memcpy(bdata, cr.data.data(), (num_rows + 7) >> 3)
        if owned_bitmap != NULL:
            vec_bool.ptr.null_bitmap = owned_bitmap
        return vec_bool

    elif cr.col_type == ColumnType.String:
        if owned_bitmap != NULL:
            free(owned_bitmap)
        # String vector: build from spans
        builder = StringVectorBuilder.with_estimate(num_rows, 16)
        for i in range(num_rows):
            if i < cr.str_lengths.size() and cr.str_lengths[i] == 0:
                builder.append_null()
            elif i < cr.str_lengths.size() and cr.str_data.size() > 0:
                builder.append_bytes(
                    <const char*>(cr.str_data.data() + cr.str_offsets[i]),
                    cr.str_lengths[i]
                )
            else:
                builder.append_null()
        return builder.finish()

    else:
        # Null column
        if owned_bitmap != NULL:
            free(owned_bitmap)
        builder = StringVectorBuilder.with_estimate(num_rows, 0)
        for i in range(num_rows):
            builder.append_null()
        return builder.finish()


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
