# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdint cimport uint8_t, uint32_t

from draken.vectors.string_vector cimport StringVector, StringVectorBuilder
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport DrakenVector, DrakenStringArena, DrakenStringSlot, str_length, str_data


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
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef DrakenStringSlot* slot
    cdef uint32_t slen
    cdef const uint8_t* sdata
    cdef bytes raw
    cdef str text, transformed
    cdef StringVectorBuilder builder = string_vector_module.StringVectorBuilder.with_estimate(n, 16)

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        slot = &arena.slots[sel[i]]
        slen = str_length(slot)
        sdata = str_data(slot, arena.arena)
        raw = bytes(sdata[:slen])
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("utf-8", "replace")
        transformed = _initcap_string(text)
        builder.append(transformed.encode("utf-8"))

    return builder.finish()
