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
from draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload, DrakenVector, DrakenStringArena, DrakenStringSlot, str_length, str_data
from opteryx.third_party.mabel.base64 cimport encode as b64_encode, decode as b64_decode
from draken.vectors.string_vector cimport _ConstView
from draken.vectors.string_vector cimport _const_view
from draken.core.buffers cimport DrakenStringArena


cpdef StringVector vector_base64_encode(StringVector data):
    cdef DrakenVector* uv = data.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* null_bm
    cdef bytes input_bytes, encoded_bytes
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef _ConstView csp
    cdef DrakenStringArena* b64e_gdv
    cdef DrakenStringSlot* b64e_slot
    cdef const uint8_t* b64e_sdata
    cdef uint32_t b64e_slen
    cdef DrakenStringArena* b64e_dense_arena
    cdef DrakenStringSlot* b64e_dense_slot
    cdef const uint8_t* b64e_dense_sdata
    cdef uint32_t b64e_dense_slen

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    if data._unified_view.data_length == 1:  # constant
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = _const_view(<DrakenStringArena*>uv.data)
            input_bytes = bytes((<uint8_t*>csp.data)[:csp.length])
            encoded_bytes = b64_encode(input_bytes)
            for i in range(n):
                builder.append(encoded_bytes)
        return builder.finish()

    if data._unified_view.data_length < data._unified_view.length:  # dictionary
        b64e_gdv = <DrakenStringArena*>data._unified_view.data
        dict_size = <Py_ssize_t>b64e_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 32)
        for i in range(dict_size):
            b64e_slot = &b64e_gdv.slots[i]
            b64e_slen = str_length(b64e_slot)
            b64e_sdata = str_data(b64e_slot, b64e_gdv.arena)
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
    b64e_dense_arena = <DrakenStringArena*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            b64e_dense_slot = &b64e_dense_arena.slots[i]
            b64e_dense_sdata = str_data(b64e_dense_slot, b64e_dense_arena.arena)
            b64e_dense_slen = str_length(b64e_dense_slot)
            input_bytes = bytes(b64e_dense_sdata[:b64e_dense_slen])
            builder.append(b64_encode(input_bytes))

    return builder.finish()


cpdef StringVector vector_base64_decode(StringVector data):
    cdef DrakenVector* uv = data.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* null_bm
    cdef bytes input_bytes, decoded_bytes
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef _ConstView csp
    cdef DrakenStringArena* b64d_gdv
    cdef DrakenStringSlot* b64d_slot
    cdef const uint8_t* b64d_sdata
    cdef uint32_t b64d_slen
    cdef DrakenStringArena* b64d_dense_arena
    cdef DrakenStringSlot* b64d_dense_slot
    cdef const uint8_t* b64d_dense_sdata
    cdef uint32_t b64d_dense_slen

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    if data._unified_view.data_length == 1:  # constant
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = _const_view(<DrakenStringArena*>uv.data)
            input_bytes = bytes((<uint8_t*>csp.data)[:csp.length])
            decoded_bytes = b64_decode(input_bytes)
            for i in range(n):
                builder.append(decoded_bytes)
        return builder.finish()

    if data._unified_view.data_length < data._unified_view.length:  # dictionary
        b64d_gdv = <DrakenStringArena*>data._unified_view.data
        dict_size = <Py_ssize_t>b64d_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 32)
        for i in range(dict_size):
            b64d_slot = &b64d_gdv.slots[i]
            b64d_slen = str_length(b64d_slot)
            b64d_sdata = str_data(b64d_slot, b64d_gdv.arena)
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
    b64d_dense_arena = <DrakenStringArena*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            b64d_dense_slot = &b64d_dense_arena.slots[i]
            b64d_dense_sdata = str_data(b64d_dense_slot, b64d_dense_arena.arena)
            b64d_dense_slen = str_length(b64d_dense_slot)
            input_bytes = bytes(b64d_dense_sdata[:b64d_dense_slen])
            builder.append(b64_decode(input_bytes))

    return builder.finish()
