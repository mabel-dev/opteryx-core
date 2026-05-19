# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""Vectorized HEX (base16) encoding/decoding using mabel.base16 Cython module."""

from draken.vectors.string_vector cimport StringVector, from_packed_dict
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload, DrakenVector, DrakenGermanArena, GermanString, gs_length, gs_data
from opteryx.third_party.mabel.base16 import encode as b16_encode, decode as b16_decode


cpdef StringVector vector_hex_encode(StringVector data):
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
    cdef DrakenGermanArena* hex_enc_gdv
    cdef GermanString* hex_enc_slot
    cdef const uint8_t* hex_enc_sdata
    cdef uint32_t hex_enc_slen

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    if data.ptr.offsets == NULL and data._german_dict_values == NULL:  # constant
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = <DrakenConstantStringPayload*>uv.data
            input_bytes = bytes((<uint8_t*>csp.data)[:csp.length])
            encoded_bytes = b16_encode(input_bytes)
            for i in range(n):
                builder.append(encoded_bytes)
        return builder.finish()

    if data._german_dict_values != NULL:  # dictionary
        hex_enc_gdv = data._german_dict_values
        dict_size = <Py_ssize_t>hex_enc_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 32)
        for i in range(dict_size):
            hex_enc_slot = &hex_enc_gdv.slots[i]
            hex_enc_slen = gs_length(hex_enc_slot)
            hex_enc_sdata = gs_data(hex_enc_slot, hex_enc_gdv.arena)
            input_bytes = bytes(hex_enc_sdata[:hex_enc_slen])
            dict_builder.append(b16_encode(input_bytes))
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
            builder.append(b16_encode(input_bytes))

    return builder.finish()


cpdef StringVector vector_hex_decode(StringVector data):
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
    cdef DrakenGermanArena* hex_dec_gdv
    cdef GermanString* hex_dec_slot
    cdef const uint8_t* hex_dec_sdata
    cdef uint32_t hex_dec_slen

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    if data.ptr.offsets == NULL and data._german_dict_values == NULL:  # constant
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = <DrakenConstantStringPayload*>uv.data
            input_bytes = bytes((<uint8_t*>csp.data)[:csp.length])
            decoded_bytes = b16_decode(input_bytes)
            for i in range(n):
                builder.append(decoded_bytes)
        return builder.finish()

    if data._german_dict_values != NULL:  # dictionary
        hex_dec_gdv = data._german_dict_values
        dict_size = <Py_ssize_t>hex_dec_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 32)
        for i in range(dict_size):
            hex_dec_slot = &hex_dec_gdv.slots[i]
            hex_dec_slen = gs_length(hex_dec_slot)
            hex_dec_sdata = gs_data(hex_dec_slot, hex_dec_gdv.arena)
            input_bytes = bytes(hex_dec_sdata[:hex_dec_slen])
            dict_builder.append(b16_decode(input_bytes))
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
            builder.append(b16_decode(input_bytes))

    return builder.finish()
