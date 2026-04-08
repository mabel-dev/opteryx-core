# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Native DFA-style regex replacement for a narrow, explicitly supported subset.

This module executes optimizer-compiled DFA procedures over Draken StringVector
directly.

- optimizer compiles supported regex+replacement pairs into a compact blob
- execution decodes the constant program blob once
- execution interprets the decoded procedure over StringVector data
- preserve constant and dictionary encodings where possible
- no Python fallback in the hot path

The engine is intentionally generic at the execution layer. It does not contain
benchmark/domain-specific helpers such as "extract_url_host". Instead, supported
patterns are compiled into a sequence of generic operations like:

- consume literal
- consume optional literal
- capture until delimiter
- consume to end
- return capture

Supported subset (initial implementation)
-----------------------------------------
The current execution format supports the procedure shape used for:

    ^https?://(?:www\.)?([^/]+)/.*$   ->   \1

which corresponds to:

    consume("http")
    consume_optional("s")
    consume("://")
    consume_optional("www.")
    capture_until("/")
    consume_to_end()
    return_capture()

Program blobs are expected to be optimizer-produced constant literals.
Malformed or unsupported blobs raise ValueError.

This file is designed to be included from `vector_ops.pyx`.
"""

from libc.stddef cimport size_t
from libc.stdint cimport int32_t, uint8_t
from libc.string cimport memcmp, memcpy, memset

cdef extern from "simd_search.h":
    int neon_search(const char* data, size_t length, char target)
    int avx_search(const char* data, size_t length, char target)

import platform

cdef int (*simd_find_char)(const char*, size_t, char)

_arch = platform.machine().lower()
if _arch in ("arm64", "aarch64"):
    simd_find_char = neon_search
else:
    simd_find_char = avx_search

from opteryx.compiled.draken.core.buffers cimport (
    DrakenVarBuffer,
    DRAKEN_ENCODING_DICTIONARY,
)
from opteryx.compiled.draken.vectors.string_vector cimport StringVector


cdef enum DfaOpType:
    DFA_OP_CONSUME_LITERAL = 1
    DFA_OP_CONSUME_OPTIONAL_LITERAL = 2
    DFA_OP_CAPTURE_UNTIL_CHAR = 3
    DFA_OP_CONSUME_TO_END = 4
    DFA_OP_RETURN_CAPTURE = 5


cdef struct DfaOp:
    int op_type
    const char* literal
    Py_ssize_t literal_len
    char target_char


cdef struct DfaProcedure:
    DfaOp ops[8]
    int op_count


cdef inline uint8_t _read_u8(const char** p) noexcept:
    cdef uint8_t value = (<const uint8_t*>p[0])[0]
    p[0] += 1
    return value


cdef inline uint32_t _read_u32(const char** p) noexcept:
    cdef const uint8_t* src = <const uint8_t*>p[0]
    cdef uint32_t value = (
        <uint32_t>src[0]
        | (<uint32_t>src[1] << 8)
        | (<uint32_t>src[2] << 16)
        | (<uint32_t>src[3] << 24)
    )
    p[0] += 4
    return value


cdef inline void _decode_procedure(
    const char* program_ptr,
    Py_ssize_t program_len,
    DfaProcedure* proc,
) except *:
    cdef const char* p = program_ptr
    cdef const char* end = program_ptr + program_len
    cdef uint8_t version
    cdef uint8_t op_count
    cdef uint8_t op_type
    cdef uint32_t literal_len
    cdef int i

    if program_ptr == NULL or program_len < 2:
        raise ValueError("vector_dfa_replace: compiled program blob is invalid")

    version = _read_u8(&p)
    if version != 1:
        raise ValueError("vector_dfa_replace: unsupported compiled program version")

    op_count = _read_u8(&p)
    if op_count == 0 or op_count > 8:
        raise ValueError("vector_dfa_replace: compiled program op count is invalid")

    for i in range(op_count):
        if p >= end:
            raise ValueError("vector_dfa_replace: compiled program truncated")

        op_type = _read_u8(&p)
        proc.ops[i].op_type = op_type
        proc.ops[i].literal = NULL
        proc.ops[i].literal_len = 0
        proc.ops[i].target_char = <char>0

        if op_type == DFA_OP_CONSUME_LITERAL or op_type == DFA_OP_CONSUME_OPTIONAL_LITERAL:
            if end - p < 4:
                raise ValueError("vector_dfa_replace: compiled literal header truncated")
            literal_len = _read_u32(&p)
            if literal_len == 0:
                raise ValueError("vector_dfa_replace: compiled literal length is invalid")
            if end - p < literal_len:
                raise ValueError("vector_dfa_replace: compiled literal payload truncated")
            proc.ops[i].literal = p
            proc.ops[i].literal_len = <Py_ssize_t>literal_len
            p += literal_len

        elif op_type == DFA_OP_CAPTURE_UNTIL_CHAR:
            if p >= end:
                raise ValueError("vector_dfa_replace: compiled capture target missing")
            proc.ops[i].target_char = <char>_read_u8(&p)

        elif op_type == DFA_OP_CONSUME_TO_END or op_type == DFA_OP_RETURN_CAPTURE:
            pass

        else:
            raise ValueError("vector_dfa_replace: compiled program contains unsupported opcode")

    if p != end:
        raise ValueError("vector_dfa_replace: compiled program has trailing bytes")

    proc.op_count = op_count


cdef inline void _write_u8(char** p, uint8_t value) noexcept:
    (<uint8_t*>p[0])[0] = value
    p[0] += 1


cdef inline void _write_u32(char** p, uint32_t value) noexcept:
    cdef uint8_t* dst = <uint8_t*>p[0]
    dst[0] = <uint8_t>(value & 0xFF)
    dst[1] = <uint8_t>((value >> 8) & 0xFF)
    dst[2] = <uint8_t>((value >> 16) & 0xFF)
    dst[3] = <uint8_t>((value >> 24) & 0xFF)
    p[0] += 4


cdef inline bint _slice_equals(
    const char* value_ptr,
    Py_ssize_t value_len,
    const char* literal,
    Py_ssize_t literal_len,
) noexcept:
    if value_len != literal_len:
        return False
    return memcmp(value_ptr, literal, <size_t>literal_len) == 0


cdef inline void _extract_const_slice(
    StringVector vec,
    const char** data_ptr,
    Py_ssize_t* data_len,
) except *:
    if not vec._has_const:
        raise ValueError("vector_dfa_replace: compiled program must be constant encoded")
    if vec._const_is_null or vec._const_value == NULL:
        raise ValueError("vector_dfa_replace: compiled program must be non-null")

    data_ptr[0] = <const char*>vec._const_value.data
    data_len[0] = <Py_ssize_t>vec._const_value.length





cdef inline bint _execute_procedure(
    const char* src,
    Py_ssize_t src_len,
    DfaProcedure* proc,
    const char** out_ptr,
    Py_ssize_t* out_len,
) noexcept:
    cdef const char* p = src
    cdef const char* end = src + src_len
    cdef const char* capture_ptr = NULL
    cdef Py_ssize_t capture_len = 0
    cdef int i
    cdef DfaOp* op
    cdef Py_ssize_t remaining
    cdef const char* scan

    out_ptr[0] = NULL
    out_len[0] = 0

    for i in range(proc.op_count):
        op = &proc.ops[i]

        if op.op_type == DFA_OP_CONSUME_LITERAL:
            remaining = <Py_ssize_t>(end - p)
            if remaining < op.literal_len:
                return False
            if memcmp(p, op.literal, <size_t>op.literal_len) != 0:
                return False
            p += op.literal_len

        elif op.op_type == DFA_OP_CONSUME_OPTIONAL_LITERAL:
            remaining = <Py_ssize_t>(end - p)
            if remaining >= op.literal_len and memcmp(p, op.literal, <size_t>op.literal_len) == 0:
                p += op.literal_len

        elif op.op_type == DFA_OP_CAPTURE_UNTIL_CHAR:
            if p >= end:
                return False
            remaining = <Py_ssize_t>(end - p)
            if remaining <= 0:
                return False
            i = simd_find_char(p, <size_t>remaining, op.target_char)
            if i < 0:
                return False
            if i == 0:
                return False
            scan = p + i
            if scan < p or scan >= end:
                return False
            capture_ptr = p
            capture_len = <Py_ssize_t>(scan - p)
            p = scan

        elif op.op_type == DFA_OP_CONSUME_TO_END:
            if p >= end:
                return False
            p = end

        elif op.op_type == DFA_OP_RETURN_CAPTURE:
            if capture_ptr == NULL:
                return False
            out_ptr[0] = capture_ptr
            out_len[0] = capture_len
            return True

        else:
            return False

    return False


cpdef StringVector vector_dfa_replace(
    StringVector data,
    StringVector compiled_program,
):
    """
    Execute an optimizer-compiled DFA replacement over a StringVector.

    The execution kernel consumes a constant-encoded compiled program blob,
    decodes it once, and interprets the decoded procedure over the input data.

    Encoding behavior:
    - constant input    -> execute once, replicate result/null
    - dictionary input  -> transform dictionary values once, preserve row codes
    - dense input       -> row-by-row native execution
    """
    cdef const char* program_ptr = NULL
    cdef Py_ssize_t program_len = 0
    cdef DfaProcedure proc

    _extract_const_slice(compiled_program, &program_ptr, &program_len)
    _decode_procedure(program_ptr, program_len, &proc)

    cdef Py_ssize_t n = data.ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef const char* out_ptr = NULL
    cdef Py_ssize_t out_len = 0
    cdef DrakenVarBuffer* ptr
    cdef StringVector out_vec
    cdef DrakenVarBuffer* out_ptr_buf
    cdef Py_ssize_t total_bytes = 0
    cdef Py_ssize_t row_len = 0
    cdef const char* row_ptr = NULL
    cdef Py_ssize_t write_offset = 0
    cdef Py_ssize_t null_bytes = 0

    # Constant encoding: execute once, replicate.
    if data._has_const:
        if data._const_is_null or data._const_value == NULL:
            out_vec = StringVector(n, 0)
            out_ptr_buf = out_vec.ptr
            null_bytes = (n + 7) >> 3
            if null_bytes > 0 and out_ptr_buf.null_bitmap != NULL:
                memset(out_ptr_buf.null_bitmap, 0, <size_t>null_bytes)
            for i in range(n + 1):
                out_ptr_buf.offsets[i] = 0
            return out_vec

        row_ptr = <const char*>data._const_value.data
        row_len = <Py_ssize_t>data._const_value.length
        if _execute_procedure(
            row_ptr,
            row_len,
            &proc,
            &out_ptr,
            &out_len,
        ):
            total_bytes = out_len * n
        else:
            out_ptr = row_ptr
            out_len = row_len
            total_bytes = row_len * n

        out_vec = StringVector(n, total_bytes)
        out_ptr_buf = out_vec.ptr
        if out_ptr_buf.null_bitmap != NULL:
            null_bytes = (n + 7) >> 3
            if null_bytes > 0:
                memset(out_ptr_buf.null_bitmap, 0xFF, <size_t>null_bytes)

        write_offset = 0
        out_ptr_buf.offsets[0] = 0
        for i in range(n):
            if out_len > 0:
                memcpy((<char*>out_ptr_buf.data) + write_offset, out_ptr, <size_t>out_len)
            write_offset += out_len
            out_ptr_buf.offsets[i + 1] = <int32_t>write_offset
        return out_vec

    # Dictionary encoding currently falls through to the dense row path.
    # Repacking transformed dictionary values is unsafe here because null-producing
    # rewrites can invalidate the original dictionary/null-code relationship.
    # Fail-safe correctness beats unsafe preservation.
    ptr = data.ptr

    for i in range(n):
        if ptr.null_bitmap != NULL and not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
            continue

        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        row_ptr = (<const char*>ptr.data) + start
        row_len = <Py_ssize_t>(end - start)

        if _execute_procedure(
            row_ptr,
            row_len,
            &proc,
            &out_ptr,
            &out_len,
        ):
            total_bytes += out_len
        else:
            total_bytes += row_len

    out_vec = StringVector(n, total_bytes)
    out_ptr_buf = out_vec.ptr

    if ptr.null_bitmap != NULL and out_ptr_buf.null_bitmap != NULL:
        null_bytes = (n + 7) >> 3
        if null_bytes > 0:
            memcpy(out_ptr_buf.null_bitmap, ptr.null_bitmap, <size_t>null_bytes)
    elif out_ptr_buf.null_bitmap != NULL:
        null_bytes = (n + 7) >> 3
        if null_bytes > 0:
            memset(out_ptr_buf.null_bitmap, 0xFF, <size_t>null_bytes)

    write_offset = 0
    out_ptr_buf.offsets[0] = 0

    for i in range(n):
        if ptr.null_bitmap != NULL and not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
            out_ptr_buf.offsets[i + 1] = <int32_t>write_offset
            continue

        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        row_ptr = (<const char*>ptr.data) + start
        row_len = <Py_ssize_t>(end - start)

        if _execute_procedure(
            row_ptr,
            row_len,
            &proc,
            &out_ptr,
            &out_len,
        ):
            if out_len > 0:
                memcpy((<char*>out_ptr_buf.data) + write_offset, out_ptr, <size_t>out_len)
            write_offset += out_len
        else:
            if row_len > 0:
                memcpy((<char*>out_ptr_buf.data) + write_offset, row_ptr, <size_t>row_len)
            write_offset += row_len

        out_ptr_buf.offsets[i + 1] = <int32_t>write_offset

    return out_vec
