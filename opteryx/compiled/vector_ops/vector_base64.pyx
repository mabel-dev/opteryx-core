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
from draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload, DrakenVector, DrakenGermanArena, GermanString, gs_length, gs_data
from opteryx.third_party.mabel.base64 cimport encode as b64_encode, decode as b64_decode


cpdef StringVector vector_base64_encode(StringVector data):
    cdef DrakenVector* uv = data.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* null_bm
    cdef bytes input_bytes, encoded_bytes
    cdef DrakenVarBuffer* vbuf
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef DrakenConstantStringPayload* csp
    cdef DrakenGermanArena* b64e_gdv
    cdef GermanString* b64e_slot
    cdef const uint8_t* b64e_sdata
    cdef uint32_t b64e_slen

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    if data.ptr.offsets == NULL and data._german_dict_values == NULL:  # constant
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = <DrakenConstantStringPayload*>uv.data
            input_bytes = bytes((<uint8_t*>csp.data)[:csp.length])
            encoded_bytes = b64_encode(input_bytes)
            for i in range(n):
                builder.append(encoded_bytes)
        return builder.finish()

    if data._german_dict_values != NULL:  # dictionary
        b64e_gdv = data._german_dict_values
        dict_size = <Py_ssize_t>b64e_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 32)
        for i in range(dict_size):
            b64e_slot = &b64e_gdv.slots[i]
            b64e_slen = gs_length(b64e_slot)
            b64e_sdata = gs_data(b64e_slot, b64e_gdv.arena)
            input_bytes = bytes(b64e_sdata[:b64e_slen])
            dict_builder.append(b64_encode(input_bytes))
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    # dense
    vbuf = <DrakenVarBuffer*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            start = vbuf.offsets[i]
            end = vbuf.offsets[i + 1]
            input_bytes = bytes((<uint8_t*>vbuf.data)[start:end])
            builder.append(b64_encode(input_bytes))

    return builder.finish()


cpdef StringVector vector_base64_decode(StringVector data):
    cdef DrakenVector* uv = data.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* null_bm
    cdef bytes input_bytes, decoded_bytes
    cdef DrakenVarBuffer* vbuf
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef DrakenConstantStringPayload* csp
    cdef DrakenGermanArena* b64d_gdv
    cdef GermanString* b64d_slot
    cdef const uint8_t* b64d_sdata
    cdef uint32_t b64d_slen

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    if data.ptr.offsets == NULL and data._german_dict_values == NULL:  # constant
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = <DrakenConstantStringPayload*>uv.data
            input_bytes = bytes((<uint8_t*>csp.data)[:csp.length])
            decoded_bytes = b64_decode(input_bytes)
            for i in range(n):
                builder.append(decoded_bytes)
        return builder.finish()

    if data._german_dict_values != NULL:  # dictionary
        b64d_gdv = data._german_dict_values
        dict_size = <Py_ssize_t>b64d_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 32)
        for i in range(dict_size):
            b64d_slot = &b64d_gdv.slots[i]
            b64d_slen = gs_length(b64d_slot)
            b64d_sdata = gs_data(b64d_slot, b64d_gdv.arena)
            input_bytes = bytes(b64d_sdata[:b64d_slen])
            dict_builder.append(b64_decode(input_bytes))
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    # dense
    vbuf = <DrakenVarBuffer*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            start = vbuf.offsets[i]
            end = vbuf.offsets[i + 1]
            input_bytes = bytes((<uint8_t*>vbuf.data)[start:end])
            builder.append(b64_decode(input_bytes))

    return builder.finish()
