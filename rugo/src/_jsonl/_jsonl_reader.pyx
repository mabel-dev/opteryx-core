# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport uint8_t, int64_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.map cimport map as cmap
from libcpp.utility cimport move

# Typed-vector cimports removed as part of E.31 migration (same gap registry as E.28):
#   E.28-gap-1: Integer64Vector dense constructor + ptr.data write access
#   E.28-gap-2: Float64Vector dense constructor + ptr.data write access
#   E.28-gap-3: StringVectorBuilder (constructors, append_bytes, append_null, finish)
#   E.31-gap-1: BoolVector dense constructor + ptr.data write access


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


cdef extern from "core/interpreter.hpp" namespace "rugo::_jsonl":
    # Flat-arena document map. Opaque to Cython: spans/offsets stay in C++; the only
    # introspection the edge needs (first record's keys for column-name discovery) goes
    # through first_record_keys().
    cppclass RecordSet:
        size_t num_records()

    vector[string] first_record_keys(const RecordSet& rs, const uint8_t* buffer) nogil


cdef extern from "core/field_span.hpp" namespace "rugo::_jsonl":
    struct InterpreterResult:
        RecordSet all_records
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

    InterpreterResult interpret_jsonl_threaded(
        const uint8_t* buffer_data,
        size_t buffer_length,
        const ParseContext& context,
        OrdinalPredictor& predictor,
        size_t max_threads
    ) nogil



cdef extern from "core/structural_scan.hpp" namespace "rugo::_jsonl":
    vector[MarkerPosition] scan_structural_markers(
        const uint8_t* buffer,
        size_t length
    ) nogil



cdef extern from "core/jsonl_reader.hpp" namespace "rugo::_jsonl":
    struct PrefilterResult:
        vector[uint8_t] candidates
        size_t total_records
        size_t matched_records
    PrefilterResult volnitsky_prefilter(
        const uint8_t* buffer, size_t length,
        const uint8_t* needle, size_t needle_len
    ) nogil

    struct ReadResult:
        bint success
        string error_message
        vector[string] column_names
        RecordSet records
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
        const RecordSet& records,
        const string& column_name,
        OrdinalPredictor& predictor,
        bint copy_bytes
    )

    # Returns a NEW reference to a Draken Vector; the C++ side owns the Python edge.
    # `base` is the buffer slices are read from (scr.data_ptr() in copy mode, or the
    # original chunk buffer in no-copy mode).
    object build_varchar_vector(const uint8_t* base, StringColumnResult& scr)
    object build_typed_vector(const uint8_t* base, StringColumnResult& scr)
    void merge_string_column(StringColumnResult& dest, StringColumnResult& src)

    # Parsed column buffers (no Python); produced in parallel off the GIL, then wrapped.
    cppclass ParsedColumn:
        pass
    vector[ParsedColumn] parse_all_columns(
        const uint8_t* buffer,
        const RecordSet& records,
        const vector[string]& column_names,
        size_t max_threads
    ) nogil
    object wrap_column(ParsedColumn& pc)


import os


cdef bytes _maybe_prefilter(bytes buf, predicates):
    """Gated Volnitsky raw prefilter for a single selective string-equality predicate.
    Returns a candidate sub-buffer (surviving lines) when it applies, else the original
    buffer unchanged. SOUND: the needle is the quoted value, which a matching record always
    contains regardless of whitespace; the predicate is re-applied downstream so false
    positives are verified away. Self-disabling on non-string, short, or non-selective cases."""
    cdef PrefilterResult r
    cdef PrefilterResult sr
    cdef const uint8_t* ndl_ptr
    cdef const uint8_t* sample_ptr
    cdef const uint8_t* buf_ptr

    if not predicates or len(predicates) != 1:
        return buf
    col, op, val = predicates[0]
    if op != "==":
        return buf

    # Probe the first record: only prefilter when `col` is stored as a quoted (string)
    # value. A bare numeric/bool value isn't quoted, so a quoted needle would false-negative.
    nl = buf.find(b"\n")
    first = buf[:nl] if nl >= 0 else buf
    key = b'"' + col.encode("utf-8") + b'":'
    ki = first.find(key)
    if ki < 0:
        return buf                      # key absent / non-compact formatting -> skip (safe)
    if first[ki + len(key):ki + len(key) + 1] != b'"':
        return buf                      # bare value -> skip (numeric hazard)

    needle = b'"' + str(val).encode("utf-8") + b'"'
    if len(needle) < 8:                 # short/low-entropy value -> skip won't pay off
        return buf

    ndl_ptr = <const uint8_t*>needle

    # Selectivity sample on the first ~1MB: if the needle already hits >30% of sampled rows,
    # there's little to skip -> run the normal path instead of paying for a full prefilter.
    sample = buf[:1_000_000]
    sample_ptr = <const uint8_t*>sample
    sr = volnitsky_prefilter(sample_ptr, len(sample), ndl_ptr, len(needle))
    sample_lines = sample.count(b"\n")
    if sample_lines > 0 and sr.matched_records * 10 > sample_lines * 3:
        return buf

    buf_ptr = <const uint8_t*>buf
    r = volnitsky_prefilter(buf_ptr, len(buf), ndl_ptr, len(needle))
    if r.candidates.size() == 0:
        return b""                      # no candidates -> empty buffer -> 0 rows
    return (<char*>r.candidates.data())[:r.candidates.size()]


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
    use_prefilter=True
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
    cdef vector[RecordSet] chunk_records
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

            # Sparser-style raw prefilter: for a selective string-equality predicate, drop
            # records that cannot contain the value before any structural parsing. Sound by
            # construction (value-anchored needle), self-disabling on short/non-selective
            # filters; the predicate is still applied downstream, so false positives are
            # verified away.
            if use_prefilter and len(threaded_data) > 0:
                threaded_data = _maybe_prefilter(threaded_data, predicates)

            if len(threaded_data) > 0:
                buf_data = <const uint8_t*>threaded_data
                buf_len = len(threaded_data)
                # Parallel scan + document map: the buffer is split into newline-aligned
                # ranges processed across a thread pool, then merged in order. (Per range
                # it still does SIMD-scan -> markers -> state machine; fusing those two
                # into one pass measured ~25% slower, so they stay decoupled.)
                with nogil:
                    interp_result = interpret_jsonl_threaded(
                        buf_data, buf_len, context, predictor, 0
                    )

                # Store result as single "chunk" if successful
                if interp_result.all_records.num_records() > 0:
                    # Read column names from the first record BEFORE moving the
                    # records out (no projection = all columns).
                    if column_names_cpp.empty():
                        column_names_cpp = first_record_keys(interp_result.all_records, buf_data)

                    chunk_buffers.append(threaded_data)
                    total_rows = interp_result.num_records_passed
                    # Move (not copy) the record structure — tens of millions of
                    # FieldSpans + their per-record vectors — into the chunk store.
                    chunk_records.push_back(move(interp_result.all_records))
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

                    # Keep buffer as bytes (owns the memory); move the records in.
                    chunk_buffers.append(bytes(chunk_result.buffer_data))
                    total_rows += chunk_result.num_records
                    chunk_records.push_back(move(chunk_result.records))

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

    # First record's keys (column names) for inspection.
    sample_keys = [k.decode('utf-8') for k in first_record_keys(interp_result.all_records, buf_data)]

    return {
        'num_records': interp_result.num_records_passed,
        'scan_ms': scan_ms,
        'interpret_ms': interp_ms,
        'total_ms': scan_ms + interp_ms,
        'buffer_size_mb': len(data) / 1024 / 1024,
        'sample_keys': sample_keys,
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
    vector[RecordSet]& chunk_records,
    vector[string]& column_names,
    size_t total_rows
):
    """
    For each column, extract as StringVector, then cast to inferred type.
    No Python per-row iteration.
    """
    cdef list vectors = []
    cdef StringColumnResult chunk_col
    cdef StringColumnResult merged
    cdef bytes buf_bytes
    cdef const uint8_t* buf_ptr
    cdef string col_name_cpp
    cdef size_t ci, pi
    cdef size_t n_chunks = chunk_records.size()
    cdef OrdinalPredictor predictor
    cdef vector[ParsedColumn] parsed

    if n_chunks == 0 or len(chunk_buffers) == 0:
        return vectors

    if n_chunks == 1:
        # Common case: whole file fit in one chunk. Parse every column in parallel off
        # the GIL (no-copy — spans index straight into the buffer), then wrap each into
        # a Vector under the GIL (cheap, O(columns)).
        buf_bytes = <bytes>chunk_buffers[0]
        buf_ptr = <const uint8_t*>buf_bytes
        with nogil:
            parsed = parse_all_columns(buf_ptr, chunk_records[0], column_names, 0)
        for pi in range(parsed.size()):
            vec = wrap_column(parsed[pi])
            if vec is not None:
                vectors.append(vec)
        return vectors

    # Multi-chunk (>64MB file): the merged column outlives the individual chunk buffers,
    # so extract WITH a copy and concatenate; build from merged.data (serial).
    for col_name_cpp in column_names:
        for ci in range(n_chunks):
            buf_bytes = <bytes>chunk_buffers[ci]
            buf_ptr = <const uint8_t*>buf_bytes
            chunk_col = extract_column(buf_ptr, chunk_records[ci], col_name_cpp, predictor, True)
            if ci == 0:
                merged = chunk_col
            else:
                merge_string_column(merged, chunk_col)
        vec = build_typed_vector(merged.data_ptr(), merged)
        if vec is not None:
            vectors.append(vec)

    return vectors


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
