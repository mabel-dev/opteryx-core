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
from draken.core.buffers cimport DrakenVarBuffer, DrakenVector, DrakenStringArena, DrakenStringSlot, str_length, str_data
from opteryx.third_party.mabel.base64 cimport encode as b64_encode, decode as b64_decode


cpdef StringVector vector_base64_encode(StringVector data):
    cdef DrakenVector* uv = data.unified()
    cdef Py_ssize_t slot_count = <Py_ssize_t>uv.data_length
    cdef Py_ssize_t i
    cdef DrakenVarBuffer* ndp
    cdef DrakenStringArena* in_arena = <DrakenStringArena*>uv.data
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen
    cdef bytes input_bytes

    slot_builder = string_vector_module.StringVectorBuilder.with_estimate(slot_count, 32)
    for i in range(slot_count):
        slot = &in_arena.slots[i]
        slen = str_length(slot)
        sdata = str_data(slot, in_arena.arena)
        input_bytes = bytes(sdata[:slen])
        slot_builder.append(b64_encode(input_bytes))
    new_dict_sv = slot_builder.finish()
    ndp = (<StringVector>new_dict_sv).ptr
    return from_packed_dict(
        <uint8_t*>uv.selection, 4, <Py_ssize_t>uv.length,
        ndp.offsets, <const uint8_t*>ndp.data, slot_count,
        uv.validity,
    )


cpdef StringVector vector_base64_decode(StringVector data):
    cdef DrakenVector* uv = data.unified()
    cdef Py_ssize_t slot_count = <Py_ssize_t>uv.data_length
    cdef Py_ssize_t i
    cdef DrakenVarBuffer* ndp
    cdef DrakenStringArena* in_arena = <DrakenStringArena*>uv.data
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen
    cdef bytes input_bytes

    slot_builder = string_vector_module.StringVectorBuilder.with_estimate(slot_count, 32)
    for i in range(slot_count):
        slot = &in_arena.slots[i]
        slen = str_length(slot)
        sdata = str_data(slot, in_arena.arena)
        input_bytes = bytes(sdata[:slen])
        slot_builder.append(b64_decode(input_bytes))
    new_dict_sv = slot_builder.finish()
    ndp = (<StringVector>new_dict_sv).ptr
    return from_packed_dict(
        <uint8_t*>uv.selection, 4, <Py_ssize_t>uv.length,
        ndp.offsets, <const uint8_t*>ndp.data, slot_count,
        uv.validity,
    )
