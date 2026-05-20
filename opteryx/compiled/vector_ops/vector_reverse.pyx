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
from draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload, DrakenVector, DrakenStringArena, DrakenStringSlot, str_length, str_data
from draken.vectors.string_vector cimport _ConstView
from draken.vectors.string_vector cimport _const_view
from draken.core.buffers cimport DrakenStringArena


cpdef StringVector vector_reverse(StringVector vec):
    """Reverse each string element in a StringVector (Unicode codepoint-aware)."""
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef bytes raw
    cdef str text
    cdef bytes result
    cdef uint8_t* null_bm
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef _ConstView csp
    cdef DrakenStringArena* rv_gdv
    cdef DrakenStringSlot* rv_slot
    cdef const uint8_t* rv_sdata
    cdef uint32_t rv_slen
    cdef DrakenStringArena* rv_dense_arena
    cdef DrakenStringSlot* rv_dense_slot
    cdef const uint8_t* rv_dense_sdata
    cdef uint32_t rv_dense_slen

    # Constant encoding: process once, replicate
    if vec._unified_view.data_length == 1:  # constant
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = _const_view(<DrakenStringArena*>uv.data)
            raw = bytes((<uint8_t*>csp.data)[:csp.length])
            text = raw.decode('utf-8', errors='replace')
            result = text[::-1].encode('utf-8')
            for i in range(n):
                builder.append(result)
        return builder.finish()

    # Dictionary encoding: transform each unique entry, repack with same codes
    if vec._unified_view.data_length < vec._unified_view.length:  # dictionary
        rv_gdv = <DrakenStringArena*>vec._unified_view.data
        dict_size = <Py_ssize_t>rv_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 16)
        for i in range(dict_size):
            rv_slot = &rv_gdv.slots[i]
            rv_slen = str_length(rv_slot)
            rv_sdata = str_data(rv_slot, rv_gdv.arena)
            raw = bytes(rv_sdata[:rv_slen])
            text = raw.decode('utf-8', errors='replace')
            dict_builder.append(text[::-1].encode('utf-8'))
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    # Dense encoding: row by row
    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)
    rv_dense_arena = <DrakenStringArena*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        rv_dense_slot = &rv_dense_arena.slots[i]
        rv_dense_sdata = str_data(rv_dense_slot, rv_dense_arena.arena)
        rv_dense_slen = str_length(rv_dense_slot)
        raw = bytes(rv_dense_sdata[:rv_dense_slen])
        text = raw.decode('utf-8', errors='replace')
        builder.append(text[::-1].encode('utf-8'))
    return builder.finish()
