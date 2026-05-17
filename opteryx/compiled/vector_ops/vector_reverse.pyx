# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport int32_t, uint8_t

from draken.vectors.string_vector cimport StringVector, from_packed_dict
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload, DrakenVector


cpdef StringVector vector_reverse(StringVector vec):
    """Reverse each string element in a StringVector (Unicode codepoint-aware)."""
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef bytes raw
    cdef str text
    cdef bytes result
    cdef DrakenVarBuffer* vbuf
    cdef uint8_t* null_bm
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef DrakenConstantStringPayload* csp

    # Constant encoding: process once, replicate
    if uv.data_length == 1:  # constant
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = <DrakenConstantStringPayload*>uv.data
            raw = bytes((<uint8_t*>csp.data)[:csp.length])
            text = raw.decode('utf-8', errors='replace')
            result = text[::-1].encode('utf-8')
            for i in range(n):
                builder.append(result)
        return builder.finish()

    # Dictionary encoding: transform each unique entry, repack with same codes
    if uv.selection != NULL:  # dictionary
        vbuf = <DrakenVarBuffer*>uv.data
        dict_size = <Py_ssize_t>vbuf.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 16)
        for i in range(dict_size):
            start = vbuf.offsets[i]
            end = vbuf.offsets[i + 1]
            raw = bytes((<uint8_t*>vbuf.data + start)[:end - start])
            text = raw.decode('utf-8', errors='replace')
            dict_builder.append(text[::-1].encode('utf-8'))
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, uv.sel_width, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    # Dense encoding: row by row
    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)
    vbuf = <DrakenVarBuffer*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        start = vbuf.offsets[i]
        end = vbuf.offsets[i + 1]
        raw = bytes((<uint8_t*>vbuf.data + start)[:end - start])
        text = raw.decode('utf-8', errors='replace')
        builder.append(text[::-1].encode('utf-8'))
    return builder.finish()
