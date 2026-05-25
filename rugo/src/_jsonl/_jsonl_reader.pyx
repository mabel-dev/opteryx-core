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

# Typed-vector cimports removed as part of E.31 migration (same gap registry as E.28):
#   E.28-gap-1: Integer64Vector dense constructor + ptr.data write access
#   E.28-gap-2: Float64Vector dense constructor + ptr.data write access
#   E.28-gap-3: StringVectorBuilder (constructors, append_bytes, append_null, finish)
#   E.31-gap-1: BoolVector dense constructor + ptr.data write access
from draken.vectors.vector cimport Vector
from draken.morsels.morsel cimport Morsel


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
    ) nogil

    InterpreterResult interpret_jsonl_parallel(
        const uint8_t* buffer_data,
        size_t buffer_length,
        const vector[MarkerPosition]& markers,
        const ParseContext& context,
        OrdinalPredictor& predictor,
        size_t min_rows_per_thread
    ) nogil


cdef extern from "core/structural_scan.hpp" namespace "rugo::_jsonl":
    vector[MarkerPosition] scan_structural_markers(
        const uint8_t* buffer,
        size_t length
    ) nogil


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
    cdef vector[string] column_names_cpp
    cdef list chunk_buffers = []
    cdef vector[vector[vector[FieldSpan]]] chunk_records
    cdef size_t total_rows = 0
    cdef cmap[string, string] schema_cpp
    cdef const uint8_t* buf_data
    cdef size_t buf_len
    cdef vector[MarkerPosition] markers
    cdef InterpreterResult interp_result
    cdef OrdinalPredictor predictor
    cdef bint threaded_succeeded = False
    cdef string col_name_cpp
    cdef vector[FieldSpan] first_record_cpp
    cdef FieldSpan field
    cdef size_t min_rows
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
                min_rows = <size_t>min_rows_per_thread

                # Scan for markers once
                with nogil:
                    markers = scan_structural_markers(buf_data, buf_len)

                # Process in parallel
                with nogil:
                    interp_result = interpret_jsonl_parallel(
                        buf_data,
                        buf_len,
                        markers,
                        context,
                        predictor,
                        min_rows
                    )

                # Store result as single "chunk" if successful
                if interp_result.all_records.size() > 0:
                    chunk_buffers.append(threaded_data)
                    chunk_records.push_back(interp_result.all_records)
                    total_rows = interp_result.num_records_passed

                    # Extract column names from first record (no projection = all columns)
                    if column_names_cpp.empty():
                        first_record_cpp = interp_result.all_records[0]
                        for field in first_record_cpp:
                            col_name_cpp = string(
                                <const char*>(buf_data + field.key_start),
                                <size_t>field.key_width
                            )
                            column_names_cpp.push_back(col_name_cpp)

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
                    if column_names_cpp.empty():
                        for col in chunk_result.column_names:
                            column_names_cpp.push_back(col)

                    # Keep buffer as bytes (owns the memory) and records as C++ object
                    chunk_buffers.append(bytes(chunk_result.buffer_data))
                    chunk_records.push_back(chunk_result.records)
                    total_rows += chunk_result.num_records

                    if chunk_result.inferred_schema.size() > 0:
                        for key, value in chunk_result.inferred_schema:
                            schema_cpp[key] = value

                if reader.is_eof():
                    break

        # Build Draken vectors — one C++ call per column per chunk
        if total_rows > 0 and not column_names_cpp.empty():
            vectors = _build_vectors_from_chunks(
                chunk_buffers, chunk_records, column_names_cpp, total_rows
            )
            result['columns'] = vectors
            result['column_names'] = [col.decode('utf-8') for col in column_names_cpp]
            result['num_rows'] = total_rows
            result['schema'] = {k.decode('utf-8'): v.decode('utf-8') for k, v in schema_cpp}
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
    with nogil:
        markers = scan_structural_markers(buf_data, buf_len)
    scan_ms = (time.perf_counter() - scan_start) * 1000

    # Step 2: Document map interpretation
    interp_start = time.perf_counter()
    with nogil:
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
    vector[vector[vector[FieldSpan]]]& chunk_records,
    vector[string]& column_names,
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

    for col_name_cpp in column_names:
        # Extract first chunk to get type hint
        if len(chunk_buffers) == 0:
            continue

        buf_bytes = <bytes>chunk_buffers[0]
        buf_ptr = <const uint8_t*>buf_bytes
        chunk_col = extract_column(
            buf_ptr,
            chunk_records[0],
            col_name_cpp,
            predictor
        )

        # Build StringVector from extracted data
        vec = _string_vector_from_result(chunk_col)

        # Cast to inferred type and add to vectors
        if vec is not None:
            vectors.append(vec)

    return vectors


cdef Vector _string_vector_from_result(StringColumnResult& scr):
    raise NotImplementedError(
        "rugo migration gap: StringVectorBuilder has no new-draken equivalent; "
        "tracked as E.28-gap-3."
    )


cdef Vector _draken_from_column_result(ColumnResult& cr):
    raise NotImplementedError(
        "rugo migration gap: Integer64Vector / Float64Vector / BoolVector dense constructors "
        "and StringVectorBuilder have no new-draken equivalents; "
        "tracked as E.28-gap-1, E.28-gap-2, E.31-gap-1, E.28-gap-3."
    )


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
