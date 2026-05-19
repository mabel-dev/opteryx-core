# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from draken.vectors.string_vector cimport StringVector, from_packed_dict
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport DrakenConstantStringPayload
from draken.core.buffers cimport DrakenVarBuffer
from draken.core.buffers cimport DrakenVector
from draken.core.buffers cimport DrakenGermanArena, GermanString, gs_length, gs_data
from libc.stdint cimport int32_t, uint8_t
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
    cdef DrakenConstantStringPayload* payload
    cdef DrakenVector* uv
    cdef DrakenVarBuffer* vbuf
    cdef uint8_t* null_bm
    cdef int32_t start, end

    if chars is None:
        return None

    if isinstance(chars, bytes):
        return chars

    if isinstance(chars, str):
        return chars.encode("utf-8")

    if isinstance(chars, StringVector):
        uv = (<StringVector>chars).unified()
        if (<StringVector>chars).ptr.offsets == NULL:  # constant
            if uv.validity != NULL:  # null constant
                return None
            payload = <DrakenConstantStringPayload*>uv.data
            return PyBytes_FromStringAndSize(<const char*>payload.data, payload.length)

        # non-constant: must be a single-row dense vector
        if uv.length != 1:
            raise TypeError("trim chars must be a constant or single-value StringVector")

        null_bm = uv.validity
        if null_bm != NULL and not (null_bm[0] & 1):
            return None

        vbuf = <DrakenVarBuffer*>uv.data
        start = vbuf.offsets[0]
        end = vbuf.offsets[1]
        return PyBytes_FromStringAndSize(<const char*>vbuf.data + start, end - start)

    if isinstance(chars, (list, tuple)):
        if len(chars) != 1:
            raise TypeError("trim chars must contain exactly one value")
        return _trim_chars_bytes(chars[0])

    raise TypeError(f"unsupported trim chars type {type(chars)!r}")


# ----------------------------------------------------------------------
# Main trimming functions
# (helper functions _constant_string_value and _constant_string_accessor
# are defined in _helper_const.pyx)
# ----------------------------------------------------------------------
cpdef StringVector vector_trim(StringVector vec, object chars=None):
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef uint8_t* null_bm
    cdef uint8_t trim_flags[256]
    cdef object trim_chars
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* vbuf
    cdef DrakenVarBuffer* ndp
    cdef DrakenConstantStringPayload* csp

    # Convert trim specification to a byte‑flag array.
    trim_chars = _trim_chars_bytes(chars)
    build_trim_flags(trim_flags, trim_chars)

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* data_ptr
    cdef int length, left, right
    cdef bytes trimmed_bytes
    cdef DrakenGermanArena* trim_gdv
    cdef GermanString* trim_slot
    cdef const uint8_t* trim_sdata
    cdef uint32_t trim_slen

    if vec.ptr.offsets == NULL and vec._german_dict_values == NULL:  # constant
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
            return builder.finish()

        csp = <DrakenConstantStringPayload*>uv.data
        data_ptr = <uint8_t*>csp.data
        length = csp.length

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

    # Dictionary encoding: trim each unique entry, repack with same codes
    if vec._german_dict_values != NULL:  # dictionary
        trim_gdv = vec._german_dict_values
        dict_size = <Py_ssize_t>trim_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 16)
        for i in range(dict_size):
            trim_slot = &trim_gdv.slots[i]
            trim_slen = gs_length(trim_slot)
            trim_sdata = gs_data(trim_slot, trim_gdv.arena)
            data_ptr = <uint8_t*>trim_sdata
            length = <int>trim_slen
            left = 0
            while left < length and trim_flags[data_ptr[left]]:
                left += 1
            right = length
            while right > left and trim_flags[data_ptr[right - 1]]:
                right -= 1
            if left < right:
                trimmed_bytes = PyBytes_FromStringAndSize(<const char*>(data_ptr + left), right - left)
            else:
                trimmed_bytes = b''
            dict_builder.append(trimmed_bytes)
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    # Dense
    vbuf = <DrakenVarBuffer*>uv.data
    null_bm = uv.validity
    for i in range(n):
        # Handle nulls
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start = vbuf.offsets[i]
        end = vbuf.offsets[i + 1]
        data_ptr = <uint8_t*>vbuf.data + start
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
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef uint8_t* null_bm
    cdef uint8_t trim_flags[256]
    cdef object trim_chars
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* vbuf
    cdef DrakenVarBuffer* ndp
    cdef DrakenConstantStringPayload* csp

    trim_chars = _trim_chars_bytes(chars)
    build_trim_flags(trim_flags, trim_chars)

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* data_ptr
    cdef int length, left
    cdef bytes trimmed_bytes
    cdef DrakenGermanArena* ltrim_gdv
    cdef GermanString* ltrim_slot
    cdef const uint8_t* ltrim_sdata
    cdef uint32_t ltrim_slen

    if vec.ptr.offsets == NULL and vec._german_dict_values == NULL:  # constant
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
            return builder.finish()

        csp = <DrakenConstantStringPayload*>uv.data
        data_ptr = <uint8_t*>csp.data
        length = csp.length

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

    # Dictionary encoding: ltrim each unique entry, repack with same codes
    if vec._german_dict_values != NULL:  # dictionary
        ltrim_gdv = vec._german_dict_values
        dict_size = <Py_ssize_t>ltrim_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 16)
        for i in range(dict_size):
            ltrim_slot = &ltrim_gdv.slots[i]
            ltrim_slen = gs_length(ltrim_slot)
            ltrim_sdata = gs_data(ltrim_slot, ltrim_gdv.arena)
            data_ptr = <uint8_t*>ltrim_sdata
            length = <int>ltrim_slen
            left = 0
            while left < length and trim_flags[data_ptr[left]]:
                left += 1
            if left < length:
                trimmed_bytes = PyBytes_FromStringAndSize(<const char*>(data_ptr + left), length - left)
            else:
                trimmed_bytes = b''
            dict_builder.append(trimmed_bytes)
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    # Dense
    vbuf = <DrakenVarBuffer*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start = vbuf.offsets[i]
        end = vbuf.offsets[i + 1]
        data_ptr = <uint8_t*>vbuf.data + start
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
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef uint8_t* null_bm
    cdef uint8_t trim_flags[256]
    cdef object trim_chars
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* vbuf
    cdef DrakenVarBuffer* ndp
    cdef DrakenConstantStringPayload* csp

    trim_chars = _trim_chars_bytes(chars)
    build_trim_flags(trim_flags, trim_chars)

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* data_ptr
    cdef int length, right
    cdef bytes trimmed_bytes
    cdef DrakenGermanArena* rtrim_gdv
    cdef GermanString* rtrim_slot
    cdef const uint8_t* rtrim_sdata
    cdef uint32_t rtrim_slen

    if vec.ptr.offsets == NULL and vec._german_dict_values == NULL:  # constant
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
            return builder.finish()

        csp = <DrakenConstantStringPayload*>uv.data
        data_ptr = <uint8_t*>csp.data
        length = csp.length

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

    # Dictionary encoding: rtrim each unique entry, repack with same codes
    if vec._german_dict_values != NULL:  # dictionary
        rtrim_gdv = vec._german_dict_values
        dict_size = <Py_ssize_t>rtrim_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 16)
        for i in range(dict_size):
            rtrim_slot = &rtrim_gdv.slots[i]
            rtrim_slen = gs_length(rtrim_slot)
            rtrim_sdata = gs_data(rtrim_slot, rtrim_gdv.arena)
            data_ptr = <uint8_t*>rtrim_sdata
            length = <int>rtrim_slen
            right = length
            while right > 0 and trim_flags[data_ptr[right - 1]]:
                right -= 1
            if right > 0:
                trimmed_bytes = PyBytes_FromStringAndSize(<const char*>data_ptr, right)
            else:
                trimmed_bytes = b''
            dict_builder.append(trimmed_bytes)
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    # Dense
    vbuf = <DrakenVarBuffer*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start = vbuf.offsets[i]
        end = vbuf.offsets[i + 1]
        data_ptr = <uint8_t*>vbuf.data + start
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
