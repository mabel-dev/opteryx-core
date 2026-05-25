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
from draken.core.buffers cimport DrakenVector, DrakenStringArena, DrakenStringSlot, str_length, str_data


cpdef StringVector vector_reverse(StringVector vec):
    """Reverse each string element in a StringVector (Unicode codepoint-aware)."""
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t slot_count = <Py_ssize_t>uv.data_length
    cdef Py_ssize_t i
    cdef bytes raw
    cdef str text
    cdef DrakenStringArena* in_arena = <DrakenStringArena*>uv.data
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen

    # Build reversed version of each unique dict entry.
    slot_builder = string_vector_module.StringVectorBuilder.with_estimate(slot_count, 16)
    for i in range(slot_count):
        slot = &in_arena.slots[i]
        slen = str_length(slot)
        sdata = str_data(slot, in_arena.arena)
        raw = bytes(sdata[:slen])
        text = raw.decode('utf-8', errors='replace')
        slot_builder.append(text[::-1].encode('utf-8'))

    new_dict_sv = slot_builder.finish()
    cdef StringVector new_sv = <StringVector>new_dict_sv
    cdef DrakenVector* new_uv = new_sv.unified()
    cdef DrakenStringArena* new_arena = <DrakenStringArena*>new_uv.data

    # Apply original selection against reversed dict, materializing row-by-row.
    out_builder = string_vector_module.StringVectorBuilder.with_estimate(
        <Py_ssize_t>uv.length, 16)
    for i in range(<Py_ssize_t>uv.length):
        slot = &new_arena.slots[uv.selection[i]]
        slen = str_length(slot)
        sdata = str_data(slot, new_arena.arena)
        out_builder.append(bytes(sdata[:slen]))
    return <StringVector>out_builder.finish()
