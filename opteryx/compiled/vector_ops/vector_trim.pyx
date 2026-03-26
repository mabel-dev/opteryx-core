# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors import string_vector as string_vector_module
from opteryx.compiled.draken.core.buffers cimport ConstAccessor
from opteryx.compiled.draken.core.buffers cimport DrakenConstantStringPayload
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer
from libc.stdint cimport int32_t, uint8_t
from cpython.bytes cimport PyBytes_FromStringAndSize
from opteryx.compiled.draken.core.buffers cimport DRAKEN_ENCODING_CONSTANT


# ----------------------------------------------------------------------
# Helper: build a 256‑entry flag array of bytes that should be trimmed.
# For ASCII whitespace when chars=None, we use the same set as bytes.strip().
# ----------------------------------------------------------------------
cdef void build_trim_flags(uint8_t flags[256], object chars) except *:
    cdef int i
    cdef unsigned char c
    for i in range(256):
        flags[i] = 0

    if chars is None:
        # ASCII whitespace: space, tab, newline, carriage return, form feed, vertical tab
        flags[ord(' ')] = 1
        flags[ord('\t')] = 1
        flags[ord('\n')] = 1
        flags[ord('\r')] = 1
        flags[ord('\f')] = 1
        flags[ord('\v')] = 1
        return

    cdef const unsigned char* ptr = chars
    cdef Py_ssize_t n = len(chars)
    for i in range(n):
        c = ptr[i]
        if c > 127:
            # Non‑ASCII trimming characters are not supported in this C version.
            # Fall back to Python (or raise an exception).
            raise ValueError("trim characters must be ASCII only for C implementation")
        flags[c] = 1


cdef inline object _trim_chars_bytes(object chars):
    cdef ConstAccessor* accessor
    cdef DrakenConstantStringPayload* payload
    cdef DrakenVarBuffer* ptr
    cdef uint8_t* null_bm
    cdef int32_t start, end

    if chars is None:
        return None

    if isinstance(chars, bytes):
        return chars

    if isinstance(chars, str):
        return chars.encode("utf-8")

    if isinstance(chars, StringVector):
        if chars.encoding == DRAKEN_ENCODING_CONSTANT:
            accessor = (<StringVector>chars).const_accessor()
            if accessor == NULL or accessor.is_null != 0 or accessor.value_ptr == NULL:
                return None
            payload = <DrakenConstantStringPayload*>accessor.value_ptr
            return PyBytes_FromStringAndSize(<const char*>payload.data, payload.length)

        ptr = (<StringVector>chars).ptr
        if ptr.length != 1:
            raise TypeError("trim chars must be a constant or single-value StringVector")

        null_bm = ptr.null_bitmap
        if null_bm != NULL and not (null_bm[0] & 1):
            return None

        start = ptr.offsets[0]
        end = ptr.offsets[1]
        return PyBytes_FromStringAndSize(<const char*>ptr.data + start, end - start)

    if isinstance(chars, (list, tuple)):
        if len(chars) != 1:
            raise TypeError("trim chars must contain exactly one value")
        return _trim_chars_bytes(chars[0])

    raise TypeError(f"unsupported trim chars type {type(chars)!r}")


cdef inline ConstAccessor* _constant_string_accessor(StringVector vec) noexcept:
    if vec.encoding != DRAKEN_ENCODING_CONSTANT:
        return NULL
    return vec.const_accessor()


cdef inline bint _constant_string_value(
    StringVector vec,
    const uint8_t** data_ptr,
    int32_t* data_len,
    Py_ssize_t* row_count,
) except? False:
    cdef ConstAccessor* accessor = _constant_string_accessor(vec)
    cdef DrakenConstantStringPayload* payload

    if accessor == NULL:
        return False

    row_count[0] = accessor.length
    if accessor.is_null != 0 or accessor.value_ptr == NULL:
        data_ptr[0] = NULL
        data_len[0] = 0
        return True

    payload = <DrakenConstantStringPayload*>accessor.value_ptr
    data_ptr[0] = payload.data
    data_len[0] = payload.length
    return True


# ----------------------------------------------------------------------
# Main trimming functions
# ----------------------------------------------------------------------
cpdef StringVector vector_trim(StringVector vec, object chars=None):
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n
    cdef uint8_t* null_bm = NULL
    cdef uint8_t trim_flags[256]
    cdef object trim_chars
    cdef const uint8_t* const_data_ptr
    cdef int32_t const_data_len
    cdef bint is_constant_vec

    # Convert trim specification to a byte‑flag array.
    trim_chars = _trim_chars_bytes(chars)
    build_trim_flags(trim_flags, trim_chars)

    is_constant_vec = _constant_string_value(vec, &const_data_ptr, &const_data_len, &n)
    if not is_constant_vec:
        n = ptr.length
        null_bm = ptr.null_bitmap

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* data_ptr
    cdef int length, left, right
    cdef bytes trimmed_bytes

    if is_constant_vec:
        if const_data_ptr == NULL:
            for i in range(n):
                builder.append_null()
            return builder.finish()

        data_ptr = <uint8_t*>const_data_ptr
        length = const_data_len

        left = 0
        while left < length and trim_flags[data_ptr[left]]:
            left += 1

        right = length
        while right > left and trim_flags[data_ptr[right - 1]]:
            right -= 1

        if left < right:
            trimmed_bytes = PyBytes_FromStringAndSize(
                <const char*>(data_ptr + left), right - left
            )
        else:
            trimmed_bytes = b''

        for i in range(n):
            builder.append(trimmed_bytes)
        return builder.finish()

    for i in range(n):
        # Handle nulls
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start = ptr.offsets[i]
        end   = ptr.offsets[i + 1]
        data_ptr = <uint8_t*>ptr.data + start
        length = end - start

        # Trim left
        left = 0
        while left < length and trim_flags[data_ptr[left]]:
            left += 1

        # Trim right
        right = length
        while right > left and trim_flags[data_ptr[right - 1]]:
            right -= 1

        # Create a bytes object for the trimmed slice
        if left < right:
            trimmed_bytes = PyBytes_FromStringAndSize(
                <const char*>(data_ptr + left), right - left
            )
        else:
            trimmed_bytes = b''

        builder.append(trimmed_bytes)

    return builder.finish()


cpdef StringVector vector_ltrim(StringVector vec, object chars=None):
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n
    cdef uint8_t* null_bm = NULL
    cdef uint8_t trim_flags[256]
    cdef object trim_chars
    cdef const uint8_t* const_data_ptr
    cdef int32_t const_data_len
    cdef bint is_constant_vec

    trim_chars = _trim_chars_bytes(chars)
    build_trim_flags(trim_flags, trim_chars)

    is_constant_vec = _constant_string_value(vec, &const_data_ptr, &const_data_len, &n)
    if not is_constant_vec:
        n = ptr.length
        null_bm = ptr.null_bitmap

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* data_ptr
    cdef int length, left
    cdef bytes trimmed_bytes

    if is_constant_vec:
        if const_data_ptr == NULL:
            for i in range(n):
                builder.append_null()
            return builder.finish()

        data_ptr = <uint8_t*>const_data_ptr
        length = const_data_len

        left = 0
        while left < length and trim_flags[data_ptr[left]]:
            left += 1

        if left < length:
            trimmed_bytes = PyBytes_FromStringAndSize(
                <const char*>(data_ptr + left), length - left
            )
        else:
            trimmed_bytes = b''

        for i in range(n):
            builder.append(trimmed_bytes)
        return builder.finish()

    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start = ptr.offsets[i]
        end   = ptr.offsets[i + 1]
        data_ptr = <uint8_t*>ptr.data + start
        length = end - start

        left = 0
        while left < length and trim_flags[data_ptr[left]]:
            left += 1

        if left < length:
            trimmed_bytes = PyBytes_FromStringAndSize(
                <const char*>(data_ptr + left), length - left
            )
        else:
            trimmed_bytes = b''

        builder.append(trimmed_bytes)

    return builder.finish()


cpdef StringVector vector_rtrim(StringVector vec, object chars=None):
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n
    cdef uint8_t* null_bm = NULL
    cdef uint8_t trim_flags[256]
    cdef object trim_chars
    cdef const uint8_t* const_data_ptr
    cdef int32_t const_data_len
    cdef bint is_constant_vec

    trim_chars = _trim_chars_bytes(chars)
    build_trim_flags(trim_flags, trim_chars)

    is_constant_vec = _constant_string_value(vec, &const_data_ptr, &const_data_len, &n)
    if not is_constant_vec:
        n = ptr.length
        null_bm = ptr.null_bitmap

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* data_ptr
    cdef int length, right
    cdef bytes trimmed_bytes

    if is_constant_vec:
        if const_data_ptr == NULL:
            for i in range(n):
                builder.append_null()
            return builder.finish()

        data_ptr = <uint8_t*>const_data_ptr
        length = const_data_len

        right = length
        while right > 0 and trim_flags[data_ptr[right - 1]]:
            right -= 1

        if right > 0:
            trimmed_bytes = PyBytes_FromStringAndSize(
                <const char*>data_ptr, right
            )
        else:
            trimmed_bytes = b''

        for i in range(n):
            builder.append(trimmed_bytes)
        return builder.finish()

    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start = ptr.offsets[i]
        end   = ptr.offsets[i + 1]
        data_ptr = <uint8_t*>ptr.data + start
        length = end - start

        right = length
        while right > 0 and trim_flags[data_ptr[right - 1]]:
            right -= 1

        if right > 0:
            trimmed_bytes = PyBytes_FromStringAndSize(
                <const char*>data_ptr, right
            )
        else:
            trimmed_bytes = b''

        builder.append(trimmed_bytes)

    return builder.finish()
