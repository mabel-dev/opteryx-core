









from libc.stdint cimport uint8_t, int64_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy, memchr
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.map cimport map as cmap
from libcpp.utility cimport move

from draken.core.buffers cimport (
    DrakenType,
    DRAKEN_INT64,
    DRAKEN_FLOAT64,
    DRAKEN_BOOL,
    DRAKEN_VARCHAR,
    DRAKEN_ARRAY,
    DRAKEN_VARIANT,
)

import warnings

# Type strings accepted by explicit_schema and reported in result['schema'] for inferred
# columns. Deliberately narrower than the DrakenType universe: explicit_schema only ever
# declares one of the first four (see parse_column_explicit); "array"/"variant" are
# inference-only outcomes (parse_arrays/parse_objects — see parse_array_column /
# ColumnType::Variant in column_builder.cpp) and are never valid explicit_schema entries.
_JSONL_SCHEMA_TYPES = ("int64", "double", "boolean", "string")

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
        cmap[string, string] explicit_schema
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
    # introspection the edge needs (the sampled records' keys for column-name discovery)
    # goes through sample_record_keys().
    cppclass RecordSet:
        size_t num_records()
        bint malformed
        uint32_t malformed_pos
        uint32_t malformed_count

    vector[string] sample_record_keys(
        const RecordSet& rs, const uint8_t* buffer, size_t sample_records) nogil


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


cdef extern from "core/column_builder.hpp" namespace "rugo::_jsonl":
    # Parsed column buffers (no Python); produced in parallel off the GIL, then wrapped.
    # `type`/`all_null` are read back (not opaque) to build result['schema'] without a
    # second pass over the data.
    cppclass ParsedColumn:
        DrakenType type
        bint all_null
        bint array_fallback
    # except + : parse_column_explicit (explicit_schema strict typing) throws
    # std::invalid_argument on a declared-type mismatch -> translated to Python ValueError.
    vector[ParsedColumn] parse_all_columns(
        const uint8_t* buffer,
        const RecordSet& records,
        const vector[string]& column_names,
        size_t max_threads,
        bint may_have_escapes,
        const ParseContext& context
    ) except + nogil
    object wrap_column(ParsedColumn& pc)


# Native mmap disk IO (src/cpp/disk_io.cpp) — same reader used by the Parquet path
# (rugo/src/parquet/parquet_reader.pxi:read_parquet_from_path). Avoids materialising the
# whole file as a Python bytes object: the OS pages the mapping in on demand instead of an
# eager read() copy, which matters at JSONBench's larger tiers (up to 425GB uncompressed).
cdef extern from "disk_io.h" nogil:
    int read_all_mmap(const char* path, uint8_t** dst, size_t* out_len)
    int unmap_memory_c(unsigned char* addr, size_t size)


cdef object _maybe_prefilter(const uint8_t* buf, size_t buf_len, predicates):
    """Gated Volnitsky raw prefilter for a single selective string-equality predicate.
    Returns a NEW bytes object of surviving candidate lines when prefiltering applies and
    helps, else None (caller keeps using the original buf/buf_len as-is). Only ever
    materialises small BOUNDED samples (first ~4KB, first ~1MB) as Python bytes regardless
    of buf_len — never copies the whole buffer just to decide whether to prefilter, so this
    stays cheap even when buf points at a multi-hundred-GB mmap'd file. SOUND: the needle is
    the quoted value, which a matching record always contains regardless of whitespace; the
    predicate is re-applied downstream so false positives are verified away. Self-disabling
    on non-string, short, or non-selective cases."""
    cdef PrefilterResult r
    cdef PrefilterResult sr
    cdef const uint8_t* ndl_ptr
    cdef size_t first_len
    cdef size_t sample_len
    cdef bytes first_bytes
    cdef bytes sample_bytes

    if not predicates or len(predicates) != 1:
        return None
    col, op, val = predicates[0]
    if op != "==":
        return None

    # Probe the first record: only prefilter when `col` is stored as a quoted (string)
    # value. A bare numeric/bool value isn't quoted, so a quoted needle would false-negative.
    # Bounded to 4KB — real JSONL lines are far shorter than that.
    first_len = buf_len if buf_len < 4096 else 4096
    first_bytes = (<const char*>buf)[:first_len]
    nl = first_bytes.find(b"\n")
    first = first_bytes[:nl] if nl >= 0 else first_bytes
    key = b'"' + col.encode("utf-8") + b'":'
    ki = first.find(key)
    if ki < 0:
        return None                     # key absent / non-compact formatting -> skip (safe)
    if first[ki + len(key):ki + len(key) + 1] != b'"':
        return None                     # bare value -> skip (numeric hazard)

    # val is bytes for every real Opteryx-pushed VARCHAR literal (its VARCHAR storage is
    # byte-based, not str) -- str(b'commit') == "b'commit'", the Python repr, not the
    # string's own bytes, so that needle would never be found and this prefilter would
    # silently return an empty buffer (0 rows) instead of skipping/no-oping. Same bug and
    # fix as the predicate-value encoding above.
    val_bytes = val if isinstance(val, bytes) else str(val).encode("utf-8")
    needle = b'"' + val_bytes + b'"'
    if len(needle) < 8:                 # short/low-entropy value -> skip won't pay off
        return None

    ndl_ptr = <const uint8_t*>needle

    # Selectivity sample on the first ~1MB (bounded, not the whole buffer): if the needle
    # already hits >30% of sampled rows, there's little to skip -> run the normal path
    # instead of paying for a full prefilter.
    sample_len = buf_len if buf_len < 1_000_000 else 1_000_000
    sr = volnitsky_prefilter(buf, sample_len, ndl_ptr, len(needle))
    sample_bytes = (<const char*>buf)[:sample_len]
    sample_lines = sample_bytes.count(b"\n")
    if sample_lines > 0 and sr.matched_records * 10 > sample_lines * 3:
        return None

    r = volnitsky_prefilter(buf, buf_len, ndl_ptr, len(needle))
    if r.candidates.size() == 0:
        return b""                      # no candidates -> empty buffer -> 0 rows
    return (<char*>r.candidates.data())[:r.candidates.size()]


cdef str _jsonl_malformed_error(const uint8_t* buf_data, size_t buf_len, uint32_t offset):
    """Build a 1-based-line-number error message for the first malformed record detected
    by the C++ parser. Only ever called on the fail_on_error=True error path (not hot),
    so a full scan of the bytes before `offset` to count newlines is fine."""
    cdef size_t i
    cdef size_t line = 1
    cdef size_t limit = offset if <size_t>offset < buf_len else buf_len
    cdef size_t snippet_end = offset
    cdef bytes snippet
    for i in range(limit):
        if buf_data[i] == c'\n':
            line += 1
    while snippet_end < buf_len and buf_data[snippet_end] != c'\n':
        snippet_end += 1
    snippet = (<const char*>buf_data)[offset:snippet_end][:200]
    return (
        f"Malformed JSONL at line {line} (byte offset {offset}): "
        f"{snippet.decode('utf-8', 'replace')!r}"
    )


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
    if not use_threads:
        # The non-threaded path was served by a sequential 64MB-chunked reader
        # (JsonlReader::next_chunk) that no caller in this codebase ever invoked — it has
        # been removed as dead code. Fail loud rather than silently falling back to the
        # threaded path with different characteristics (e.g. schema inference was wired
        # only into the removed path and never populated result['schema'] here either way).
        raise NotImplementedError(
            "read_jsonl(use_threads=False) is no longer supported; the sequential "
            "chunked reader it used was unreachable dead code and has been removed."
        )

    cdef ParseContext context
    cdef Predicate pred
    cdef vector[string] column_names_cpp
    cdef RecordSet records
    cdef size_t total_rows = 0
    cdef dict declared_schema = {}
    cdef const uint8_t* buf_data = NULL
    cdef size_t buf_len = 0
    cdef InterpreterResult interp_result
    cdef OrdinalPredictor predictor
    cdef dict result = {
        'success': False,
        'column_names': [],
        'num_rows': 0,
        'columns': [],
        'schema': {},
        'malformed_count': 0,
    }

    # mmap state for the file-path case (freed in the finally below). `in_memory_data`
    # keeps whichever Python bytes object buf_data currently points into alive — the
    # original in-memory input, or a fresh bytes object from the prefilter.
    cdef uint8_t* mapped_ptr = NULL
    cdef size_t mapped_len = 0
    cdef bint owns_mmap = False
    cdef int mmap_rc
    cdef bytes path_bytes
    cdef const char* c_path
    cdef bytes in_memory_data
    cdef const uint8_t[::1] buf_view
    cdef object pf_result

    # Build ParseContext
    if columns:
        for col in columns:
            context.projected_columns.push_back(col.encode('utf-8'))

    if predicates:
        for col, op, val in predicates:
            pred.column = col.encode('utf-8')
            pred.op = <uint8_t>_jsonl_parse_op(op)
            if isinstance(val, bool):
                # JSON's boolean literals are lowercase ("true"/"false"), not Python's
                # str(True) == "True" -- evaluate_predicate compares these bytes against
                # the raw JSON token, so this must match JSON's spelling, not Python's.
                pred.value = b'true' if val else b'false'
            elif isinstance(val, bytes):
                # Opteryx's bound VARCHAR literal values arrive as bytes (its VARCHAR
                # storage is byte-based, not str). str(b'commit') == "b'commit'" -- the
                # Python repr, quotes/b-prefix and all -- not the string's own bytes, so
                # this must pass the bytes through unchanged rather than str()'ing them.
                pred.value = val
            else:
                pred.value = str(val).encode('utf-8')
            context.predicates.push_back(pred)

    if explicit_schema:
        for col, declared_type in explicit_schema.items():
            if declared_type not in _JSONL_SCHEMA_TYPES:
                raise ValueError(
                    f"read_jsonl: explicit_schema[{col!r}] = {declared_type!r} is not a "
                    f"supported type; must be one of {_JSONL_SCHEMA_TYPES}"
                )
            context.explicit_schema[col.encode('utf-8')] = declared_type.encode('utf-8')
        declared_schema = dict(explicit_schema)

    # Guard before the cast to uint32_t: infer_sample_size bounds BOTH the type-inference
    # window and (since it also drives column discovery) how many records are consulted for
    # the key set, so 0 would silently yield a zero-column relation and a negative would
    # wrap to a huge window. Matches the CSV reader's identical guard on its own sample size.
    if not isinstance(infer_sample_size, int) or isinstance(infer_sample_size, bool) or infer_sample_size <= 0:
        raise ValueError("read_jsonl: infer_sample_size must be a positive integer")

    context.infer_schema = infer_schema
    context.infer_sample_size = infer_sample_size
    context.parse_arrays = parse_arrays
    context.parse_objects = parse_objects
    context.fail_on_error = fail_on_error

    try:
        if isinstance(data, str):
            # mmap the file directly — no f.read() copy, no eager materialisation of the
            # whole file as a Python bytes object. The OS pages the mapping in on demand;
            # interpret_jsonl_threaded's newline-range splitting works over a raw pointer
            # regardless of whether it's mmap'd or heap-allocated.
            path_bytes = data.encode('utf-8')
            c_path = path_bytes
            with nogil:
                mmap_rc = read_all_mmap(c_path, &mapped_ptr, &mapped_len)
            if mmap_rc != 0:
                raise OSError(-mmap_rc, f"read_all_mmap failed for {data!r}")
            owns_mmap = True
            buf_data = mapped_ptr
            buf_len = mapped_len
        elif isinstance(data, bytes):
            in_memory_data = data
            buf_data = <const uint8_t*>in_memory_data
            buf_len = len(in_memory_data)
        elif isinstance(data, (bytearray, memoryview)):
            # Zero-copy: view the caller's buffer directly instead of coercing via
            # bytes(data), which would force a full copy of a buffer the caller may
            # already hold zero-copy (e.g. an mmap'd region of their own). `buf_view`
            # keeps the buffer pinned for the rest of this call, same lifetime contract
            # as in_memory_data. The caller must not mutate it while we read.
            buf_view = memoryview(data).cast('B')
            buf_len = buf_view.shape[0]
            buf_data = &buf_view[0] if buf_len > 0 else NULL
        else:
            in_memory_data = bytes(data)
            buf_data = <const uint8_t*>in_memory_data
            buf_len = len(in_memory_data)

        # Sparser-style raw prefilter: for a selective string-equality predicate, drop
        # records that cannot contain the value before any structural parsing. Sound by
        # construction (value-anchored needle), self-disabling on short/non-selective
        # filters; the predicate is still applied downstream, so false positives are
        # verified away. Only ever touches bounded samples of buf, not the whole thing.
        if use_prefilter and buf_len > 0:
            pf_result = _maybe_prefilter(buf_data, buf_len, predicates)
            if pf_result is not None:
                # Prefilter produced a fresh, smaller bytes object — the mmap'd file (if
                # any) is no longer needed for parsing, so release it now rather than
                # holding the mapping open for the rest of the call.
                if owns_mmap:
                    with nogil:
                        unmap_memory_c(mapped_ptr, mapped_len)
                    owns_mmap = False
                in_memory_data = pf_result
                buf_data = <const uint8_t*>in_memory_data
                buf_len = len(in_memory_data)

        if buf_len > 0:
            # Parallel scan + document map: the buffer is split into newline-aligned
            # ranges processed across a thread pool, then merged in order. (Per range
            # it still does SIMD-scan -> markers -> state machine; fusing those two
            # into one pass measured ~25% slower, so they stay decoupled.)
            with nogil:
                interp_result = interpret_jsonl_threaded(
                    buf_data, buf_len, context, predictor, 0
                )

            result['malformed_count'] = interp_result.all_records.malformed_count

            if context.fail_on_error and interp_result.all_records.malformed:
                raise ValueError(_jsonl_malformed_error(
                    buf_data, buf_len, interp_result.all_records.malformed_pos
                ))

            if interp_result.all_records.num_records() > 0:
                # Read column names from the sampled records BEFORE moving the
                # records out (no projection = all columns).
                column_names_cpp = sample_record_keys(
                    interp_result.all_records, buf_data, context.infer_sample_size
                )
                total_rows = interp_result.num_records_passed
                # Move (not copy) the record structure — tens of millions of
                # FieldSpans + their per-record vectors.
                records = move(interp_result.all_records)

        # Build Draken vectors — buf_data/buf_len are still valid here (mmap released,
        # or in_memory_data kept alive, only in the finally below).
        if total_rows > 0 and not column_names_cpp.empty():
            vectors = _build_vectors(
                buf_data, buf_len, records, column_names_cpp,
                context, infer_schema, declared_schema, result['schema']
            )
            result['columns'] = vectors
            result['column_names'] = [col.decode('utf-8') for col in column_names_cpp]
            result['num_rows'] = total_rows
            result['success'] = True

        return result

    finally:
        if owns_mmap:
            with nogil:
                unmap_memory_c(mapped_ptr, mapped_len)


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

    # The sampled records' keys (column names) for inspection.
    sample_keys = [
        k.decode('utf-8')
        for k in sample_record_keys(interp_result.all_records, buf_data, context.infer_sample_size)
    ]

    return {
        'num_records': interp_result.num_records_passed,
        'scan_ms': scan_ms,
        'interpret_ms': interp_ms,
        'total_ms': scan_ms + interp_ms,
        'buffer_size_mb': len(data) / 1024 / 1024,
        'sample_keys': sample_keys,
    }


cdef uint8_t _jsonl_parse_op(str op):
    ops = {
        '==': 0,  # EQ
        '!=': 1,  # NE
        '<': 2,   # LT
        '<=': 3,  # LE
        '>': 4,   # GT
        '>=': 5,  # GE
    }
    return ops.get(op, 0)


cdef str _jsonl_schema_type_name(DrakenType t):
    # parse_typed_column/parse_column_explicit produce one of these DrakenTypes.
    # DRAKEN_ARRAY/DRAKEN_VARIANT only appear when parse_arrays/parse_objects
    # materialized the column; when either flag is False (or an array's elements were
    # out of v1 scope — nested/mixed — see array_fallback), the column falls back to
    # DRAKEN_VARCHAR ("string"), per README.md's documented JSONL caveats.
    if t == DRAKEN_INT64:
        return "int64"
    if t == DRAKEN_FLOAT64:
        return "double"
    if t == DRAKEN_BOOL:
        return "boolean"
    if t == DRAKEN_ARRAY:
        return "array"
    if t == DRAKEN_VARIANT:
        return "variant"
    return "string"


cdef list _build_vectors(
    const uint8_t* buf_ptr,
    size_t buf_len,
    RecordSet& records,
    vector[string]& column_names,
    ParseContext& context,
    bint infer_schema,
    dict declared_schema,
    dict schema_out
):
    """
    Parse every column of the buffer produced by the threaded scan+interpret path
    (interpret_jsonl_threaded always yields exactly one merged RecordSet over one buffer —
    its internal newline-range parallelism is orthogonal to this). No Python per-row
    iteration. `buf_ptr` may point into an mmap'd file or an in-memory bytes buffer; the
    caller is responsible for keeping it mapped/alive for the duration of this call.

    schema_out is populated in place: every column named in declared_schema (explicit_schema)
    is echoed back verbatim — it was declared, not inferred, so infer_schema does not gate
    it — and every other column is included only when infer_schema is true, reported as
    "null" when the column was absent/null on every row, else its resolved type.
    """
    cdef list vectors = []
    cdef size_t pi
    cdef vector[ParsedColumn] parsed
    cdef bint may_esc
    cdef str name

    if records.num_records() == 0:
        return vectors

    # Cheap buffer-wide gate: only then attempt (column-scoped) unescaping downstream.
    # memchr instead of Python `in` — buf_ptr may not be backed by a Python bytes object.
    may_esc = memchr(buf_ptr, 0x5C, buf_len) != NULL
    with nogil:
        parsed = parse_all_columns(buf_ptr, records, column_names, 0, may_esc, context)
    for pi in range(parsed.size()):
        vec = wrap_column(parsed[pi])
        if vec is not None:
            vectors.append(vec)
        name = column_names[pi].decode('utf-8')
        if parsed[pi].array_fallback:
            # parse_array_column (column_builder.cpp) runs off the GIL and cannot warn
            # itself; it flags this instead. Nested containers or a heterogeneous mix of
            # scalar kinds inside the array are out of v1 scope — the column was returned
            # as raw JSON text (DRAKEN_VARCHAR), same as parse_arrays=False.
            warnings.warn(
                f"JSONL column '{name}': array elements were nested or of mixed scalar "
                f"types (unsupported by parse_arrays); returned as raw JSON text instead",
                RuntimeWarning,
            )
        if name in declared_schema:
            schema_out[name] = declared_schema[name]
        elif infer_schema:
            schema_out[name] = "null" if parsed[pi].all_null else _jsonl_schema_type_name(parsed[pi].type)
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
