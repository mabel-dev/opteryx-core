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
from draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload, DrakenVector, DrakenGermanArena, GermanString, gs_length, gs_data


cdef inline str _initcap_string(str text):
    cdef Py_ssize_t i, length = len(text)
    if length == 0:
        return text
    cdef list builder = []
    cdef str ch
    cdef bint in_word = False
    for i in range(length):
        ch = text[i]
        if ch.isalpha():
            builder.append(ch.upper() if not in_word else ch.lower())
            in_word = True
        elif ch.isdigit():
            builder.append(ch)
            in_word = True
        else:
            builder.append(ch)
            in_word = False
    return "".join(builder)


cpdef StringVector vector_initcap(StringVector vec):
    """Apply INITCAP transformation to each element of a StringVector."""
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef bytes raw
    cdef str text, transformed
    cdef DrakenVarBuffer* vbuf
    cdef uint8_t* null_bm
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef DrakenConstantStringPayload* csp
    cdef DrakenGermanArena* ic_gdv
    cdef GermanString* ic_slot
    cdef const uint8_t* ic_sdata
    cdef uint32_t ic_slen

    # Constant encoding: process once, replicate
    if vec.ptr.offsets == NULL and vec._german_dict_values == NULL:  # constant
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = <DrakenConstantStringPayload*>uv.data
            raw = bytes((<uint8_t*>csp.data)[:csp.length])
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                text = raw.decode("utf-8", "replace")
            transformed = _initcap_string(text)
            result = transformed.encode("utf-8")
            for i in range(n):
                builder.append(result)
        return builder.finish()

    # Dictionary encoding: transform each unique entry, repack with same codes
    if vec._german_dict_values != NULL:  # dictionary
        ic_gdv = vec._german_dict_values
        dict_size = <Py_ssize_t>ic_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 16)
        for i in range(dict_size):
            ic_slot = &ic_gdv.slots[i]
            ic_slen = gs_length(ic_slot)
            ic_sdata = gs_data(ic_slot, ic_gdv.arena)
            raw = bytes(ic_sdata[:ic_slen])
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                text = raw.decode("utf-8", "replace")
            dict_builder.append(_initcap_string(text).encode("utf-8"))
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
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
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("utf-8", "replace")
        transformed = _initcap_string(text)
        builder.append(transformed.encode("utf-8"))
    return builder.finish()
