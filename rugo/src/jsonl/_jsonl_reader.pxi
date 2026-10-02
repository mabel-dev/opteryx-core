









from libc.stdint cimport uint8_t, int16_t, int64_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy, memchr
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.map cimport map as cmap
from libcpp.utility cimport move
from cpython.pycapsule cimport PyCapsule_New, PyCapsule_GetPointer

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

# Type names reported in result['schema'] for INFERRED columns. Deliberately narrower
# than the DrakenType universe because inference itself is: the speculative path only
# ever resolves to one of the first four, plus "array"/"variant" when parse_arrays/
# parse_objects materialise a column (see parse_array_column / ColumnType::Variant in
# column_builder.cpp).
#
# This is NOT the explicit_schema vocabulary. A DECLARED column accepts the platform's
# canonical type names (IPV4, UINT32, DECIMAL(18, 2), TIMESTAMP[us], DATE, ARRAY<INT64>,
# VARIANT, …) and is validated by rugo::parse_declared_type — the same C++ parser that
# then does the parsing, so what validates and what parses cannot drift. Declared names
# are echoed back into result['schema'] verbatim, which is why they need no entry here.
_JSONL_INFERRED_SCHEMA_TYPES = ("int64", "double", "boolean", "string")

# Typed-vector cimports removed as part of E.31 migration (same gap registry as E.28):
#   E.28-gap-1: Integer64Vector dense constructor + ptr.data write access
#   E.28-gap-2: Float64Vector dense constructor + ptr.data write access
#   E.28-gap-3: StringVectorBuilder (constructors, append_bytes, append_null, finish)
#   E.31-gap-1: BoolVector dense constructor + ptr.data write access


cdef extern from "declared_type.hpp" namespace "rugo":
    # explicit_schema's type vocabulary. Validation goes through the SAME parser the
    # C++ reader uses, so a name that validates here is guaranteed to resolve there.
    cdef struct DeclaredType:
        DrakenType type
        uint8_t    logical_kind
        uint8_t    unit
        int16_t    offset_minutes
        uint8_t    precision
        uint8_t    scale

    bint parse_declared_type(const string& name, DeclaredType* out) nogil
    const char* declared_type_vocabulary() nogil


cdef extern from "core/parse_context.hpp" namespace "rugo::_jsonl":
    struct Predicate:
        string column
        uint8_t op
        string value
        uint8_t kind
        vector[Predicate] members

    # cppclass (not struct): prepare_jsonl_context heap-allocates one with `new`.
    cppclass ParseContext:
        vector[string] projected_columns
        vector[Predicate] predicates
        cmap[string, string] explicit_schema
        bint infer_schema
        uint32_t infer_sample_size
        bint parse_arrays
        bint parse_objects
        bint fail_on_error
        bint intern_nested_text


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

    # std::invalid_argument (-> ValueError) on a malformed nested column request
    # (`key->>'sub'` with an empty key or sub-key; nested_column.hpp).
    vector[string] discover_column_names(
        const uint8_t* buffer, size_t buffer_length, const ParseContext& context
    ) except + nogil

    # std::invalid_argument (-> ValueError) on a predicate literal that does not fit its
    # column: its declared type, or a non-null value in the head sample.
    void check_predicate_literals(
        const uint8_t* buffer, size_t buffer_length, const ParseContext& context
    ) except + nogil


cdef extern from "core/nested_column.hpp" namespace "rugo::_jsonl":
    # A nested column request: `key->>'sub'` (as_json false) / `key->'sub'` (as_json true).
    struct ColumnSpec:
        bint nested
        bint as_json
    ColumnSpec parse_column_spec(const string& s) except +


cdef extern from "core/field_span.hpp" namespace "rugo::_jsonl":
    struct InterpreterResult:
        RecordSet all_records
        size_t num_records_passed
        uint32_t bytes_consumed

    cppclass OrdinalPredictor:
        pass

    # except + : evaluate_predicate throws std::invalid_argument (-> ValueError) on a
    # value whose JSON kind cannot be compared with the predicate literal's kind.
    InterpreterResult interpret_jsonl(
        const uint8_t* buffer_data,
        size_t buffer_length,
        const vector[MarkerPosition]& markers,
        const ParseContext& context,
        OrdinalPredictor& predictor
    ) except + nogil

    InterpreterResult interpret_jsonl_threaded(
        const uint8_t* buffer_data,
        size_t buffer_length,
        const ParseContext& context,
        OrdinalPredictor& predictor,
        size_t max_threads,
        bint use_prefilter
    ) except + nogil



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
    # The malformed-record message lives in C++ so the native engine scan
    # (src/cpp/engine/native_jsonl_scan_source.hpp) shares it verbatim.
    string malformed_error_message(
        const uint8_t* buffer, size_t length, uint32_t offset
    ) nogil


cdef extern from "core/column_builder.hpp" namespace "rugo::_jsonl":
    # Parsed column buffers (no Python); produced in parallel off the GIL, then wrapped.
    # `type`/`all_null` are read back (not opaque) to build result['schema'] without a
    # second pass over the data.
    cppclass ParsedColumn:
        DrakenType type
        bint all_null
        bint array_fallback
        bint key_absent
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


cdef bint _names_contain(const vector[string]& names, const string& name):
    cdef size_t i
    for i in range(names.size()):
        if names[i] == name:
            return True
    return False


def jsonl_is_nested_column(str name):
    """True iff `name` is a nested column request — `key->>'sub'` or `key->'sub'` —
    by rugo's own parser (nested_column.hpp), so callers never re-implement the
    notation. Raises ValueError on a malformed request (empty key or sub-key)."""
    return bool(parse_column_spec(name.encode("utf-8")).nested)


cdef dict _fill_parse_context(
    ParseContext* context,
    columns,
    predicates,
    explicit_schema,
    infer_schema,
    infer_sample_size,
    parse_arrays,
    parse_objects,
    fail_on_error,
    intern_nested_text,
):
    """Build `context` from read_jsonl's arguments, validating every one EAGERLY — before
    any bytes are read. The ONE place a JSONL ParseContext is built from Python values:
    read_jsonl uses it per call, and prepare_jsonl_context uses it once at plan time for
    the engine's native scan, so the two cannot disagree on what a request means.
    Returns the declared schema ({} when none was given)."""
    cdef Predicate pred
    cdef Predicate member
    cdef DeclaredType probe_type
    cdef dict declared_schema = {}

    if columns:
        for col in columns:
            context.projected_columns.push_back(col.encode('utf-8'))

    if predicates:
        for col, op, val in predicates:
            pred.column = col.encode('utf-8')
            pred.op = _jsonl_parse_op(op)
            pred.value = b''
            pred.members.clear()
            if pred.op == _OP_IS_NULL or pred.op == _OP_IS_NOT_NULL:
                if val is not None:
                    raise ValueError(
                        f"predicate {op!r} on {col!r} takes no value (pass None), got {val!r}"
                    )
            elif pred.op == _OP_IN or pred.op == _OP_NOT_IN:
                if not isinstance(val, (list, tuple, set, frozenset)):
                    raise ValueError(
                        f"predicate {op!r} on {col!r} takes a list, tuple or set of "
                        f"values, got {type(val).__name__}"
                    )
                # One scalar predicate per member on the same column: EQ for IN (any
                # passes), NE for NOT IN (all pass). See Predicate::members.
                member.column = pred.column
                member.op = _OP_EQ if pred.op == _OP_IN else _OP_NE
                for m in val:
                    member.kind, member.value = _predicate_literal(col, op, m)
                    pred.members.push_back(member)
            else:
                pred.kind, pred.value = _predicate_literal(col, op, val)
            context.predicates.push_back(pred)

    if explicit_schema:
        for col, declared_type in explicit_schema.items():
            if not isinstance(declared_type, str):
                raise ValueError(
                    f"read_jsonl: explicit_schema[{col!r}] = {declared_type!r} is not a "
                    f"type name; expected a string such as 'IPV4' or 'DECIMAL(18, 2)'"
                )
            # Validated here, EAGERLY, through the same parser that will do the work —
            # a bad type name must fail before any bytes are read, not part-way through
            # a multi-gigabyte file.
            declared_bytes = declared_type.encode('utf-8')
            # A nested column's type is fixed by its operator — `->>` is NVARCHAR text,
            # `->` is VARIANT JSON — so its declaration is a consistency check, not a
            # choice (nested_column.hpp).
            nested_spec = parse_column_spec(col.encode('utf-8'))
            if nested_spec.nested:
                nested_type = "VARIANT" if nested_spec.as_json else "NVARCHAR"
                if declared_type != nested_type:
                    raise ValueError(
                        f"read_jsonl: explicit_schema[{col!r}] = {declared_type!r}, but a nested "
                        f"{'`->`' if nested_spec.as_json else '`->>`'} column is {nested_type}"
                    )
                context.explicit_schema[col.encode('utf-8')] = declared_bytes
                continue
            if not parse_declared_type(declared_bytes, &probe_type):
                raise ValueError(
                    f"read_jsonl: explicit_schema[{col!r}] = {declared_type!r} is not a "
                    f"supported type; supported types are "
                    f"{declared_type_vocabulary().decode('utf-8')}"
                )
            context.explicit_schema[col.encode('utf-8')] = declared_bytes
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
    context.intern_nested_text = intern_nested_text
    return declared_schema


cdef void _free_parse_context_capsule(object capsule) noexcept:
    cdef ParseContext* context = <ParseContext*>PyCapsule_GetPointer(
        capsule, b"rugo.jsonl.ParseContext")
    del context


def prepare_jsonl_context(
    columns=None,
    predicates=None,
    explicit_schema=None,
    infer_schema=True,
    infer_sample_size=5,
    parse_arrays=True,
    parse_objects=True,
    fail_on_error=True,
    intern_nested_text=False,
):
    """Plan-time half of a native JSONL scan: validate read_jsonl's arguments and build
    the C++ ParseContext ONCE, through the same code read_jsonl uses, returned as a
    PyCapsule named "rugo.jsonl.ParseContext" that owns it. The engine copies the context
    out at plan build and decodes every chunk against it without Python."""
    cdef ParseContext* context = new ParseContext()
    try:
        _fill_parse_context(
            context, columns, predicates, explicit_schema, infer_schema, infer_sample_size,
            parse_arrays, parse_objects, fail_on_error, intern_nested_text
        )
    except BaseException:
        del context
        raise
    return PyCapsule_New(<void*>context, b"rugo.jsonl.ParseContext", _free_parse_context_capsule)


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
    use_prefilter=True,
    intern_nested_text=False,
):
    """
    Read JSONL data into Draken vectors with projection and predicate pushdown.

    Parameters:
      data: bytes or buffer-like (or file path string)
      columns: list of column names to extract (None = all)
      predicates: list of (column, op, value) tuples; op in ['==', '!=', '<', '<=', '>', '>=',
        'in', 'not in', 'is null', 'is not null']; in/not in take a list/tuple/set,
        is null/is not null take None. Unknown ops raise ValueError.

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
    cdef vector[string] column_names_cpp
    cdef RecordSet records
    cdef size_t total_rows = 0
    cdef dict declared_schema
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
        # Declared (explicit_schema) columns whose key appeared in NO record of this
        # buffer. Each is still returned, typed and all-null; this is how a caller
        # pinning one chunk's schema onto another tells "this data lacks the column
        # entirely" from "sparse here" (see ParsedColumn.key_absent).
        'absent_columns': [],
    }

    # mmap state for the file-path case (freed in the finally below). `in_memory_data`
    # keeps whichever Python bytes object buf_data currently points into alive — the
    # original in-memory input.
    cdef uint8_t* mapped_ptr = NULL
    cdef size_t mapped_len = 0
    cdef bint owns_mmap = False
    cdef int mmap_rc
    cdef bytes path_bytes
    cdef const char* c_path
    cdef bytes in_memory_data
    cdef const uint8_t[::1] buf_view
    cdef Codec codec
    cdef bint run_prefilter = use_prefilter

    declared_schema = _fill_parse_context(
        &context, columns, predicates, explicit_schema, infer_schema, infer_sample_size,
        parse_arrays, parse_objects, fail_on_error, intern_nested_text
    )

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

        # Compressed input (gzip / zstd / lz4, by magic bytes) is decompressed whole
        # before parsing; an unsupported or mislabelled codec raises. Compressed bytes
        # are never parsed as JSON text. (Streaming scans decompress chunk by chunk
        # natively instead — this entry point takes one whole buffer.)
        codec = _resolve_codec(data if isinstance(data, str) else "", buf_data, buf_len)
        if codec != Codec.NONE:
            in_memory_data = _decompress_all(codec, buf_data, buf_len)
            buf_data = <const uint8_t*>in_memory_data
            buf_len = len(in_memory_data)

        # Predicate literals are checked against their columns BEFORE any row is
        # filtered — a literal of the wrong type raises instead of answering "no rows"
        # (see check_predicate_literals). Declared columns are checked even on an empty
        # buffer.
        if not context.predicates.empty():
            with nogil:
                check_predicate_literals(buf_data, buf_len, context)

        # Column discovery reads the head of the INPUT — before the prefilter drops
        # records and independent of which records the predicates keep — so the column
        # set never depends on which rows matched. See discover_column_names.
        if buf_len > 0:
            with nogil:
                column_names_cpp = discover_column_names(buf_data, buf_len, context)

        if buf_len > 0:
            # Parallel scan + document map: the buffer is split into newline-aligned
            # ranges processed across a thread pool, then merged in order. (Per range
            # it still does SIMD-scan -> markers -> state machine; fusing those two
            # into one pass measured ~25% slower, so they stay decoupled.)
            #
            # Sparser-style raw prefilter (use_prefilter): for a selective
            # string-equality predicate, each range task drops the lines that cannot
            # match before scanning them — in place, on every thread (see
            # interpret_jsonl_threaded). Sound by construction, self-disabling on
            # short/non-selective filters; the predicates are still applied downstream.
            with nogil:
                interp_result = interpret_jsonl_threaded(
                    buf_data, buf_len, context, predictor, 0, run_prefilter
                )

            result['malformed_count'] = interp_result.all_records.malformed_count

            if context.fail_on_error and interp_result.all_records.malformed:
                raise ValueError(malformed_error_message(
                    buf_data, buf_len, interp_result.all_records.malformed_pos
                ).decode('utf-8'))

            if interp_result.all_records.num_records() > 0:
                # A DECLARED column is always built, even when no sampled record carries
                # it: a schema pinned from another chunk (or file) names columns this
                # buffer may lack entirely, and the caller relies on getting every
                # declared column back — typed, all-null, and listed in
                # result['absent_columns'] — rather than a morsel missing it. Column
                # discovery for everything else is unchanged.
                for col in declared_schema:
                    col_bytes = col.encode('utf-8')
                    if not _names_contain(column_names_cpp, col_bytes):
                        column_names_cpp.push_back(col_bytes)
                total_rows = interp_result.num_records_passed
                # Move (not copy) the record structure — tens of millions of
                # FieldSpans + their per-record vectors.
                records = move(interp_result.all_records)

        # Build Draken vectors — buf_data/buf_len are still valid here (mmap released,
        # or in_memory_data kept alive, only in the finally below).
        if total_rows > 0 and not column_names_cpp.empty():
            vectors = _build_vectors(
                buf_data, buf_len, records, column_names_cpp,
                context, infer_schema, declared_schema, result['schema'],
                result['absent_columns']
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


# Op codes shared with core/parse_context.hpp's Predicate::op.
cdef enum:
    _OP_EQ = 0
    _OP_NE = 1
    _OP_IN = 6
    _OP_NOT_IN = 7
    _OP_IS_NULL = 8
    _OP_IS_NOT_NULL = 9

_JSONL_OPS = {
    '==': 0,           # EQ
    '!=': 1,           # NE
    '<': 2,            # LT
    '<=': 3,           # LE
    '>': 4,            # GT
    '>=': 5,           # GE
    'in': 6,           # IN
    'not in': 7,       # NOT_IN
    'is null': 8,      # IS_NULL
    'is not null': 9,  # IS_NOT_NULL
}


cdef uint8_t _jsonl_parse_op(str op) except? 255:
    # An unknown operator must fail here. Mapping it to a default (the old `ops.get(op, 0)`)
    # silently evaluated `in [1, 3]` as `== "[1, 3]"` and returned zero rows.
    code = _JSONL_OPS.get(op)
    if code is None:
        raise ValueError(f"Unknown predicate operator: {op!r}")
    return <uint8_t>code


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
    dict schema_out,
    list absent_out
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
            # itself; it flags this instead. A row that is not a JSON array (a scalar or
            # object value — including a STRING whose text merely looks like an array),
            # malformed array text, nested containers or a heterogeneous mix of scalar
            # kinds inside the array are out of v1 scope — the column was returned as raw
            # JSON text (DRAKEN_VARCHAR), same as parse_arrays=False.
            warnings.warn(
                f"JSONL column '{name}': not every row is a JSON array of uniform scalar "
                f"elements (a row was a non-array value or malformed array text, or its "
                f"elements were nested or of mixed scalar types; unsupported by "
                f"parse_arrays); returned as raw JSON text instead",
                RuntimeWarning,
            )
        if name in declared_schema:
            schema_out[name] = declared_schema[name]
            if parsed[pi].key_absent:
                absent_out.append(name)
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
