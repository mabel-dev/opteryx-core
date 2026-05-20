# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from draken.vectors.string_vector cimport StringVector
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport DrakenVector
from draken.core.buffers cimport DrakenStringArena, DrakenStringSlot, str_length, str_data
from libc.stdint cimport int32_t, uint8_t, uint32_t
from cpython.bytes cimport PyBytes_FromStringAndSize


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
    cdef DrakenVector* uv
    cdef DrakenStringArena* tc_arena
    cdef DrakenStringSlot* tc_slot
    cdef uint32_t* sel
    cdef uint8_t* null_bm

    if chars is None:
        return None

    if isinstance(chars, bytes):
        return chars

    if isinstance(chars, str):
        return chars.encode("utf-8")

    if isinstance(chars, StringVector):
        uv = (<StringVector>chars).unified()

        # Must be a single logical row (constant or 1-row dense/dict)
        if uv.length != 1:
            raise TypeError("trim chars must be a constant or single-value StringVector")

        null_bm = uv.validity
        if null_bm != NULL and not (null_bm[0] & 1):
            return None

        tc_arena = <DrakenStringArena*>uv.data
        sel = <uint32_t*>uv.selection
        tc_slot = &tc_arena.slots[sel[0]]
        return PyBytes_FromStringAndSize(
            <const char*>str_data(tc_slot, tc_arena.arena),
            <Py_ssize_t>str_length(tc_slot),
        )

    if isinstance(chars, (list, tuple)):
        if len(chars) != 1:
            raise TypeError("trim chars must contain exactly one value")
        return _trim_chars_bytes(chars[0])

    raise TypeError(f"unsupported trim chars type {type(chars)!r}")


# ----------------------------------------------------------------------
# Main trimming functions
# ----------------------------------------------------------------------
cpdef StringVector vector_trim(StringVector vec, object chars=None):
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef uint8_t trim_flags[256]
    cdef object trim_chars
    cdef Py_ssize_t i
    cdef DrakenStringSlot* slot
    cdef uint32_t slen
    cdef const uint8_t* sdata
    cdef uint8_t* data_ptr
    cdef int length, left, right
    cdef bytes trimmed_bytes

    trim_chars = _trim_chars_bytes(chars)
    build_trim_flags(trim_flags, trim_chars)

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        slot = &arena.slots[sel[i]]
        slen = str_length(slot)
        sdata = str_data(slot, arena.arena)
        data_ptr = <uint8_t*>sdata
        length = <int>slen

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

        builder.append(trimmed_bytes)

    return builder.finish()


cpdef StringVector vector_ltrim(StringVector vec, object chars=None):
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef uint8_t trim_flags[256]
    cdef object trim_chars
    cdef Py_ssize_t i
    cdef DrakenStringSlot* slot
    cdef uint32_t slen
    cdef const uint8_t* sdata
    cdef uint8_t* data_ptr
    cdef int length, left
    cdef bytes trimmed_bytes

    trim_chars = _trim_chars_bytes(chars)
    build_trim_flags(trim_flags, trim_chars)

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        slot = &arena.slots[sel[i]]
        slen = str_length(slot)
        sdata = str_data(slot, arena.arena)
        data_ptr = <uint8_t*>sdata
        length = <int>slen

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
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef uint8_t trim_flags[256]
    cdef object trim_chars
    cdef Py_ssize_t i
    cdef DrakenStringSlot* slot
    cdef uint32_t slen
    cdef const uint8_t* sdata
    cdef uint8_t* data_ptr
    cdef int length, right
    cdef bytes trimmed_bytes

    trim_chars = _trim_chars_bytes(chars)
    build_trim_flags(trim_flags, trim_chars)

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        slot = &arena.slots[sel[i]]
        slen = str_length(slot)
        sdata = str_data(slot, arena.arena)
        data_ptr = <uint8_t*>sdata
        length = <int>slen

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
