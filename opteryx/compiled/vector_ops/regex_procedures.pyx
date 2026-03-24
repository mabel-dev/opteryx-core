# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.string cimport memcpy, memcmp
from libc.stdint cimport int32_t, uint8_t

cdef extern from "stdlib.h" namespace "":
    void* malloc(size_t size) nogil
    void free(void* ptr) nogil

from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors import string_vector as string_vector_module
from opteryx.draken.core.buffers cimport DrakenVarBuffer

# Import SIMD search helpers
cdef extern from "simd_search.h" namespace "":
    int simd_search_substring(const char* data, size_t length, const char* pattern, size_t pattern_len) nogil
    int avx_search(const char* data, size_t length, char target) nogil


cdef enum OperationType:
    OP_MATCH_LITERAL = 0
    OP_MATCH_OPTIONAL_LITERAL = 1
    OP_FIND_CHAR = 2
    OP_EXTRACT_UNTIL_CHAR = 3
    OP_EXTRACT_WHILE_NOT = 4
    OP_START_CAPTURE = 5
    OP_END_CAPTURE = 6
    OP_DISCARD_REST = 7
    OP_RETURN_CAPTURE = 8


ctypedef struct Operation:
    int op_type
    const char* pattern
    size_t pattern_len
    int capture_id
    char target_char


ctypedef struct Capture:
    size_t start
    size_t end


cdef object execute_procedure_on_string(
    const char* str_data,
    size_t str_len,
    Operation* ops,
    Py_ssize_t num_ops
):
    """Execute procedure on single string.

    Returns (data_ptr, length) tuple or None on failure.
    """
    cdef size_t cursor = 0
    cdef Py_ssize_t pc = 0
    cdef Capture captures[10]  # Support up to 10 capture groups
    cdef int match_pos
    cdef size_t i

    # Initialize captures
    for i in range(10):
        captures[i].start = 0
        captures[i].end = 0

    # Execute operations
    while pc < num_ops:
        op = ops[pc]

        if op.op_type == OP_MATCH_LITERAL:
            # Must match literal at cursor
            if cursor + op.pattern_len > str_len:
                return None
            if memcmp(str_data + cursor, op.pattern, op.pattern_len) != 0:
                return None
            cursor += op.pattern_len

        elif op.op_type == OP_MATCH_OPTIONAL_LITERAL:
            # Optionally match literal
            if cursor + op.pattern_len <= str_len:
                if memcmp(str_data + cursor, op.pattern, op.pattern_len) == 0:
                    cursor += op.pattern_len

        elif op.op_type == OP_FIND_CHAR:
            # Find character using SIMD
            match_pos = avx_search(str_data + cursor, str_len - cursor, op.target_char)
            if match_pos < 0:
                return None
            cursor += match_pos

        elif op.op_type == OP_EXTRACT_UNTIL_CHAR:
            # Extract until character
            if op.capture_id >= 0:
                captures[op.capture_id].start = cursor
            match_pos = avx_search(str_data + cursor, str_len - cursor, op.target_char)
            if match_pos < 0:
                match_pos = str_len - cursor
            if op.capture_id >= 0:
                captures[op.capture_id].end = cursor + match_pos
            cursor += match_pos

        elif op.op_type == OP_EXTRACT_WHILE_NOT:
            # Extract while not character (like [^/])
            if op.capture_id >= 0:
                captures[op.capture_id].start = cursor
            match_pos = avx_search(str_data + cursor, str_len - cursor, op.target_char)
            if match_pos < 0:
                match_pos = str_len - cursor
            if op.capture_id >= 0:
                captures[op.capture_id].end = cursor + match_pos
            cursor += match_pos

        elif op.op_type == OP_START_CAPTURE:
            if op.capture_id >= 0:
                captures[op.capture_id].start = cursor

        elif op.op_type == OP_END_CAPTURE:
            if op.capture_id >= 0:
                captures[op.capture_id].end = cursor

        elif op.op_type == OP_DISCARD_REST:
            # Skip to end (for .* patterns)
            cursor = str_len

        elif op.op_type == OP_RETURN_CAPTURE:
            # Return captured group
            if op.capture_id >= 0:
                cap = captures[op.capture_id]
                if cap.start < cap.end:
                    return (str_data + cap.start, cap.end - cap.start)
            return None

        pc += 1

    # Default: return empty
    return None


cpdef StringVector execute_regex_procedure(
    StringVector data,
    object operations,
    int num_operations,
    bint fallback_to_re2
):
    """Execute compiled regex procedure on StringVector.

    Operations list format:
    [
        (op_type: int, pattern: bytes, pattern_len: int, capture_id: int, target_char: int),
        ...
    ]

    Returns modified StringVector with procedure results.
    """
    cdef DrakenVarBuffer* ptr = data.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef Py_ssize_t j
    cdef object builder
    cdef Operation* ops = NULL
    cdef Py_ssize_t ops_len = 0
    cdef object result
    cdef int32_t start, end

    # If fallback_to_re2, shouldn't reach here
    if fallback_to_re2 or operations is None or len(operations) == 0:
        return data

    # Convert Python operations to C structs
    ops_len = len(operations)

    # Allocate C array for operations
    ops = <Operation*>malloc(ops_len * sizeof(Operation))
    if ops == NULL:
        raise MemoryError()

    try:
        # Copy operation data
        for j in range(ops_len):
            op_tuple = operations[j]
            ops[j].op_type = op_tuple[0]

            if op_tuple[1] is not None:
                ops[j].pattern = <const char*>op_tuple[1]
                ops[j].pattern_len = len(op_tuple[1])
            else:
                ops[j].pattern = NULL
                ops[j].pattern_len = 0

            ops[j].capture_id = op_tuple[3] if op_tuple[3] != -1 else -1
            ops[j].target_char = op_tuple[4] if op_tuple[4] is not None else 0

        # Create builder for result
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 50)

        # Process each string
        for i in range(n):
            if ptr.null_bitmap != NULL and not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
                builder.append_null()
                continue

            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]

            result = execute_procedure_on_string(
                <const char*>ptr.data + start,
                <size_t>(end - start),
                ops,
                ops_len
            )

            if result is None:
                builder.append_null()
            else:
                builder.append_bytes(result[0], result[1])

        return builder.finish()

    finally:
        if ops != NULL:
            free(<void*>ops)
