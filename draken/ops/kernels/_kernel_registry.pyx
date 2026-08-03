# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True

"""Cython wrapper for C kernel registry lookup.

Provides bytecode builder and executor access to C kernel function pointers
and context struct allocation. Used during bind-time kernel resolution in
Phase 9b.

Public interface:
  lookup_kernel(name: str) -> (kernel_fn, ctx_ptr) or (None, None)
  - Looks up a kernel by name (uppercase string)
  - Returns function pointer (as opaque int) and context pointer (or None)

Context allocation (for parameterized kernels):
  alloc_cast_timestamp_ctx(unit: int) -> ctypes-opaque pointer
  alloc_binary_op_ctx(op_code: int) -> ctypes-opaque pointer
  alloc_extraction_ctx(sub_op_code: int) -> ctypes-opaque pointer
  free_context(ctx) -> None
"""

from libc.stdint cimport uint8_t, uint16_t, uint32_t, int32_t, int64_t
from libc.stddef cimport size_t
from cpython.ref cimport PyObject

# C declarations from kernel_registry.h
cdef extern from "ops/kernels/kernel_registry.h":
    ctypedef void* kernel_fn_t
    bint kernel_registry_lookup(const char* name, kernel_fn_t* out_fn, void** out_ctx)
    void kernel_registry_register(const char* name, kernel_fn_t fn)

    ctypedef struct cast_timestamp_ctx_:
        int unit
    ctypedef cast_timestamp_ctx_ cast_timestamp_ctx

    ctypedef struct cast_array_ctx_:
        int element_type
        int safe
    ctypedef cast_array_ctx_ cast_array_ctx

    ctypedef struct binary_op_ctx_:
        uint16_t op_code
    ctypedef binary_op_ctx_ binary_op_ctx

    ctypedef struct extraction_ctx_:
        int32_t sub_op_code
        int32_t nav_len
        int64_t index
    ctypedef extraction_ctx_ extraction_ctx

    cast_timestamp_ctx* kernel_alloc_cast_timestamp_ctx(int unit)
    cast_array_ctx* kernel_alloc_cast_array_ctx(int element_type, int safe)
    binary_op_ctx* kernel_alloc_binary_op_ctx(uint16_t op_code,
                                              unsigned char left_scale,
                                              unsigned char right_scale,
                                              unsigned char result_scale,
                                              unsigned char result_precision,
                                              unsigned char left_unit,
                                              unsigned char right_unit)
    ctypedef struct in_list_ctx_:
        uint32_t count
    ctypedef in_list_ctx_ in_list_ctx

    ctypedef struct substring_ctx_:
        int start
    ctypedef substring_ctx_ substring_ctx

    extraction_ctx* kernel_alloc_extraction_ctx(uint16_t sub_op_code, const char* nav,
                                                size_t nav_len, int64_t index) except +
    in_list_ctx* kernel_alloc_in_list_ctx(const uint8_t* blob, size_t blob_len)
    void* kernel_alloc_like_any_ctx(const uint8_t* blob, size_t blob_len)

    ctypedef struct like_dfa_ctx_:
        uint16_t op_code
    ctypedef like_dfa_ctx_ like_dfa_ctx
    like_dfa_ctx* kernel_alloc_like_dfa_ctx(uint16_t op_code, uint16_t threshold,
                                            const uint8_t* blob, size_t blob_len)
    substring_ctx* kernel_alloc_substring_ctx(int start, int count, uint8_t has_count)

    ctypedef struct time_bucket_ctx_:
        pass
    ctypedef time_bucket_ctx_ time_bucket_ctx

    ctypedef struct format_ctx_:
        pass
    ctypedef format_ctx_ format_ctx

    time_bucket_ctx* kernel_alloc_time_bucket_ctx(int64_t magnitude, uint8_t unit_kind,
                                                  uint8_t ts_unit)
    format_ctx* kernel_alloc_format_ctx(uint8_t ts_unit, const char* fmt, size_t fmt_len)

    ctypedef struct vector_dim_ctx_:
        pass
    ctypedef vector_dim_ctx_ vector_dim_ctx

    vector_dim_ctx* kernel_alloc_vector_dim_ctx(uint32_t dimension)

    ctypedef struct cosine_text_ctx_:
        pass
    ctypedef cosine_text_ctx_ cosine_text_ctx

    cosine_text_ctx* kernel_alloc_cosine_text_ctx(uint32_t dimension, void* embed_fn)

    ctypedef struct match_ctx_:
        pass
    ctypedef match_ctx_ match_ctx

    match_ctx* kernel_alloc_match_ctx(uint32_t dimension, void* embed_fn, double threshold)

    void kernel_free_context(void* ctx)


def lookup_kernel(str kernel_name):
    """
    Lookup kernel by name and return function pointer.

    Args:
        kernel_name: Uppercase kernel name (e.g., "ABS", "LENGTH", "ADD")

    Returns:
        Tuple (kernel_fn, ctx_ptr) where:
        - kernel_fn is an opaque integer (C function pointer)
        - ctx_ptr is an opaque integer (context struct pointer) or None
        If kernel not found, returns (None, None)

    Examples:
        fn, ctx = lookup_kernel("ABS")  # Single-arg function, no context
        fn, ctx = lookup_kernel("ADD")  # Binary op, might have context for op_code
    """
    cdef kernel_fn_t fn = NULL
    cdef void* ctx = NULL
    cdef bytes c_name = kernel_name.encode('utf-8')

    if kernel_registry_lookup(c_name, &fn, &ctx):
        return (<unsigned long long>fn, <unsigned long long>ctx if ctx != NULL else None)
    else:
        return (None, None)


def alloc_cast_timestamp_ctx(int unit):
    """
    Allocate context for INT64 → TIMESTAMP cast with unit parameter.

    Args:
        unit: 0=none, 1=ns, 2=us, 3=ms, 4=s, 5=days

    Returns:
        Opaque integer pointer to cast_timestamp_ctx struct
        Caller must free via free_context() when done
    """
    cdef cast_timestamp_ctx* ctx = kernel_alloc_cast_timestamp_ctx(unit)
    return <unsigned long long>ctx


def alloc_cast_array_ctx(int element_type, int safe):
    """
    Allocate context for CAST(json_text AS ARRAY<element_type>).

    Args:
        element_type: DrakenType tag of the declared element type
        safe: 1 for TRY_CAST (a bad row becomes NULL), 0 for a plain cast (raises)

    Returns:
        Opaque integer pointer to cast_array_ctx struct
        Caller must free via free_context() when done
    """
    cdef cast_array_ctx* ctx = kernel_alloc_cast_array_ctx(element_type, safe)
    return <unsigned long long>ctx


def alloc_binary_op_ctx(int op_code, int left_scale=0, int right_scale=0,
                        int result_scale=0, int result_precision=0,
                        int left_unit=0, int right_unit=0):
    """
    Allocate context for binary operation with op_code dispatch.

    Args:
        op_code: BCBinaryOpCode (BOP_PLUS=1, BOP_MINUS=2, etc.)
        left_scale/right_scale/result_scale: DECIMAL/DECIMAL128 scales (0 otherwise).
        result_precision: DECIMAL/DECIMAL128 result precision (descriptor; 0 otherwise).
        left_unit/right_unit: TimestampUnit (0=s,1=ms,2=us,3=ns) of TIMESTAMP/TIME
            operands (0 otherwise; date32 ignores it).

    Returns:
        Opaque integer pointer to binary_op_ctx struct
        Caller must free via free_context() when done
    """
    cdef binary_op_ctx* ctx = kernel_alloc_binary_op_ctx(
        <uint16_t>op_code,
        <unsigned char>left_scale, <unsigned char>right_scale,
        <unsigned char>result_scale, <unsigned char>result_precision,
        <unsigned char>left_unit, <unsigned char>right_unit)
    return <unsigned long long>ctx


def alloc_extraction_ctx(int sub_op_code, bytes nav=None, long long index=0):
    """
    Allocate context for extraction operation with sub_op_code dispatch.

    Everything the extraction kernels need is bound here, so the C ABI's `key`
    operand goes unused and BC_EXTRACTION pops exactly one vector.

    Args:
        sub_op_code: BCExtractionOpCode (BC_EXTR_MAP_STRING=1, etc.)
        nav: raw path/key bytes, or None. For the JSON sub-ops this is converted
            to an RFC 6901 pointer here — once per bind, not once per morsel.
        index: subscript for the MAP_* sub-ops.

    Returns:
        Opaque integer pointer to extraction_ctx struct
        Caller must free via free_context() when done

    Raises:
        ValueError (from C++ std::invalid_argument) on a malformed JSON path.
    """
    cdef const char* nav_ptr = NULL
    cdef size_t nav_len = 0
    if nav is not None:
        nav_ptr = <const char*>nav
        nav_len = <size_t>len(nav)
    cdef extraction_ctx* ctx = kernel_alloc_extraction_ctx(
        <uint16_t>sub_op_code, nav_ptr, nav_len, <int64_t>index)
    if ctx == NULL:
        return None
    return <unsigned long long>ctx


def alloc_in_list_ctx(bytes blob):
    """
    Allocate context for the IN-list membership kernel (draken_in_list).

    Args:
        blob: pre-built header+payload bytes —
            [u32 count][u8 kind][u8 negate][u16 pad][payload]
            kind 0: count x int64 sorted ascending; kind 1: count x (u32 len + bytes).

    Returns:
        Opaque integer pointer to in_list_ctx struct
        Caller must free via free_context() when done
    """
    cdef const uint8_t* p = <const uint8_t*><char*>blob
    cdef in_list_ctx* ctx = kernel_alloc_in_list_ctx(p, <size_t>len(blob))
    if ctx == NULL:
        return None
    return <unsigned long long>ctx


def alloc_like_dfa_ctx(int op_code, int threshold, bytes blob):
    """Allocate context for the length-adaptive LIKE kernel (draken_like_adaptive).

    Args:
        op_code: bit0 negate, bit1 ci.
        threshold: average string length (bytes) below which the kernel walks the
            DFA instead of the glob matcher.
        blob: a compile_like_dfa LIKE-DFA blob (verified equivalent to the glob).
    """
    cdef const uint8_t* p = <const uint8_t*><char*>blob
    cdef like_dfa_ctx* ctx = kernel_alloc_like_dfa_ctx(
        <uint16_t>op_code, <uint16_t>threshold, p, <size_t>len(blob))
    if ctx == NULL:
        return None
    return <unsigned long long>ctx


def alloc_like_any_ctx(bytes blob):
    """Allocate context for the LIKE ANY / ILIKE ANY kernel (draken_like_any).

    Args:
        blob: matcher blob from opteryx.compiled.vector_ops.compile_like_any
            (patterns bucketed into exact/prefix/suffix/contains-AC/residual;
            ci and negate carried in its flags). Copied behind a u32 length
            prefix so the kernel can bound its parse.
    """
    cdef const uint8_t* p = <const uint8_t*><char*>blob
    cdef void* ctx = kernel_alloc_like_any_ctx(p, <size_t>len(blob))
    if ctx == NULL:
        return None
    return <unsigned long long>ctx


def alloc_substring_ctx(int start, int count, int has_count):
    """Allocate context for the SUBSTRING kernel (draken_substring).

    Args:
        start: 1-based start position (SQL); adjusted to 0-based in the kernel.
        count: substring length (ignored when has_count is 0).
        has_count: 1 if a length was supplied, else 0 (substring runs to end).
    """
    cdef substring_ctx* ctx = kernel_alloc_substring_ctx(
        <int>start, <int>count, <uint8_t>has_count)
    if ctx == NULL:
        return None
    return <unsigned long long>ctx


def alloc_time_bucket_ctx(long long magnitude, int unit_kind, int ts_unit):
    """Allocate context for the TIME_BUCKET kernel (draken_time_bucket).

    Args:
        magnitude: bind-time TIME_BUCKET magnitude literal (e.g. 5 for '5 minutes').
        unit_kind: 1=second, 2=minute, 3=hour, 4=day, 5=week, 6=month,
            7=quarter, 8=year.
        ts_unit: TimestampUnit (0=s,1=ms,2=us,3=ns) of a TIMESTAMP64 `date`
            operand; ignored for DATE32 (the kernel works in microseconds).
    """
    cdef time_bucket_ctx* ctx = kernel_alloc_time_bucket_ctx(
        <int64_t>magnitude, <uint8_t>unit_kind, <uint8_t>ts_unit)
    if ctx == NULL:
        return None
    return <unsigned long long>ctx


def alloc_format_ctx(int ts_unit, bytes fmt):
    """Allocate context for the FORMAT_TIMESTAMP kernel (draken_date_format).

    Args:
        ts_unit: TimestampUnit (0=s,1=ms,2=us,3=ns) of the `date` operand; DATE32
            operands pass 2 (unused by the kernel).
        fmt: the bind-time pattern LITERAL, UTF-8 encoded.
    """
    cdef const char* fmt_ptr = <const char*>fmt
    cdef size_t fmt_len = <size_t>len(fmt)
    cdef format_ctx* ctx = kernel_alloc_format_ctx(<uint8_t>ts_unit, fmt_ptr, fmt_len)
    if ctx == NULL:
        return None
    return <unsigned long long>ctx


def alloc_vector_dim_ctx(int dimension):
    """Allocate context for the fp16 cosine kernels.

    Args:
        dimension: VECTOR width of both operands, read off the bind-time LogicalType.
            The physical DrakenVector carries no width, so the kernel cannot recover it.
    """
    cdef vector_dim_ctx* ctx = kernel_alloc_vector_dim_ctx(<uint32_t>dimension)
    if ctx == NULL:
        return None
    return <unsigned long long>ctx


def alloc_cosine_text_ctx(int dimension, unsigned long long embed_fn):
    """Allocate context for the TEXT cosine overloads.

    Args:
        dimension: the active EMBED capability's width.
        embed_fn: address of the resolved `draken_embed` kernel. The text overloads
            delegate BOTH operands to it rather than embedding themselves, so
            COSINE_SIMILARITY(a, b) and COSINE_SIMILARITY(EMBED(a), EMBED(b)) cannot
            drift apart when a capability replaces the core embedder.
    """
    cdef cosine_text_ctx* ctx = kernel_alloc_cosine_text_ctx(
        <uint32_t>dimension, <void*><unsigned long long>embed_fn)
    if ctx == NULL:
        return None
    return <unsigned long long>ctx


def alloc_match_ctx(int dimension, unsigned long long embed_fn, double threshold):
    """Allocate context for MATCH (col) AGAINST (str).

    Args:
        dimension: the active EMBED capability's width.
        embed_fn: address of the resolved `draken_embed` kernel — the same delegation the
            text cosine overloads use, so MATCH and COSINE_SIMILARITY cannot embed
            differently.
        threshold: similarity at or above which a row matches, resolved at bind time from
            the `match_threshold` session variable. Bind time, not execution time: a
            compiled plan keeps answering the question it was compiled for.
    """
    cdef match_ctx* ctx = kernel_alloc_match_ctx(
        <uint32_t>dimension, <void*><unsigned long long>embed_fn, threshold)
    if ctx == NULL:
        return None
    return <unsigned long long>ctx


def register_kernel(str name, unsigned long long fn_ptr):
    """Register an externally-compiled func_fn_t kernel (e.g. the bespoke DFA
    runner) under `name` so bind-time resolution finds it like any built-in.
    Call once at the owning module import; the address must stay valid for the
    process lifetime (it lives in that module loaded .so)."""
    kernel_registry_register(name.encode("utf-8"), <kernel_fn_t><void*>fn_ptr)
    return None


def free_context(unsigned long long ctx_ptr):
    """
    Free allocated context struct.

    Args:
        ctx_ptr: Opaque integer pointer from alloc_*_ctx()
    """
    if ctx_ptr != 0:
        kernel_free_context(<void*>ctx_ptr)
