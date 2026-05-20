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
from opteryx.third_party.mabel.base85 cimport encode as b85_encode, decode as b85_decode
from draken.vectors.string_vector cimport _ConstView
from draken.vectors.string_vector cimport _const_view
from draken.core.buffers cimport DrakenStringArena


cpdef StringVector vector_base85_encode(StringVector data):
    cdef DrakenVector* uv = data.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* null_bm
    cdef bytes input_bytes, encoded_bytes
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef _ConstView csp
    cdef DrakenStringArena* b85e_gdv
    cdef DrakenStringSlot* b85e_slot
    cdef const uint8_t* b85e_sdata
    cdef uint32_t b85e_slen
    cdef DrakenStringArena* b85e_dense_arena
    cdef DrakenStringSlot* b85e_dense_slot
    cdef const uint8_t* b85e_dense_sdata
    cdef uint32_t b85e_dense_slen

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    if data._unified_view.data_length == 1:  # constant
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = _const_view(<DrakenStringArena*>uv.data)
            input_bytes = bytes((<uint8_t*>csp.data)[:csp.length])
            encoded_bytes = b85_encode(input_bytes)
            for i in range(n):
                builder.append(encoded_bytes)
        return builder.finish()

    if data._unified_view.data_length < data._unified_view.length:  # dictionary
        b85e_gdv = <DrakenStringArena*>data._unified_view.data
        dict_size = <Py_ssize_t>b85e_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 32)
        for i in range(dict_size):
            b85e_slot = &b85e_gdv.slots[i]
            b85e_slen = str_length(b85e_slot)
            b85e_sdata = str_data(b85e_slot, b85e_gdv.arena)
            input_bytes = bytes(b85e_sdata[:b85e_slen])
            dict_builder.append(b85_encode(input_bytes))
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    # dense
    b85e_dense_arena = <DrakenStringArena*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            b85e_dense_slot = &b85e_dense_arena.slots[i]
            b85e_dense_sdata = str_data(b85e_dense_slot, b85e_dense_arena.arena)
            b85e_dense_slen = str_length(b85e_dense_slot)
            input_bytes = bytes(b85e_dense_sdata[:b85e_dense_slen])
            builder.append(b85_encode(input_bytes))

    return builder.finish()


cpdef StringVector vector_base85_decode(StringVector data):
    cdef DrakenVector* uv = data.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef uint8_t* null_bm
    cdef bytes input_bytes, decoded_bytes
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef _ConstView csp
    cdef DrakenStringArena* b85d_gdv
    cdef DrakenStringSlot* b85d_slot
    cdef const uint8_t* b85d_sdata
    cdef uint32_t b85d_slen
    cdef DrakenStringArena* b85d_dense_arena
    cdef DrakenStringSlot* b85d_dense_slot
    cdef const uint8_t* b85d_dense_sdata
    cdef uint32_t b85d_dense_slen

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    if data._unified_view.data_length == 1:  # constant
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = _const_view(<DrakenStringArena*>uv.data)
            input_bytes = bytes((<uint8_t*>csp.data)[:csp.length])
            decoded_bytes = b85_decode(input_bytes)
            for i in range(n):
                builder.append(decoded_bytes)
        return builder.finish()

    if data._unified_view.data_length < data._unified_view.length:  # dictionary
        b85d_gdv = <DrakenStringArena*>data._unified_view.data
        dict_size = <Py_ssize_t>b85d_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 32)
        for i in range(dict_size):
            b85d_slot = &b85d_gdv.slots[i]
            b85d_slen = str_length(b85d_slot)
            b85d_sdata = str_data(b85d_slot, b85d_gdv.arena)
            input_bytes = bytes(b85d_sdata[:b85d_slen])
            dict_builder.append(b85_decode(input_bytes))
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    # dense
    b85d_dense_arena = <DrakenStringArena*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
        else:
            b85d_dense_slot = &b85d_dense_arena.slots[i]
            b85d_dense_sdata = str_data(b85d_dense_slot, b85d_dense_arena.arena)
            b85d_dense_slen = str_length(b85d_dense_slot)
            input_bytes = bytes(b85d_dense_sdata[:b85d_dense_slen])
            builder.append(b85_decode(input_bytes))

    return builder.finish()
