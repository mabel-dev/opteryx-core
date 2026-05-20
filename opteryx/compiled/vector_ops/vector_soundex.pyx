# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stdlib cimport malloc, free
from libc.string cimport memset
from libc.stdint cimport uint8_t, uint32_t

from draken.vectors.string_vector cimport StringVector, from_packed_dict
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport DrakenVarBuffer, DrakenVector, DrakenStringArena, DrakenStringSlot, str_length, str_data


cpdef StringVector vector_soundex(StringVector vec):
    """
    Compute Soundex codes for each element of a StringVector.

    Returns:
        StringVector: Soundex codes (e.g. 'A123' or NULL for empty/null input).
    """
    from opteryx.third_party.fuzzy import soundex

    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* in_arena = <DrakenStringArena*>uv.data
    cdef Py_ssize_t slot_count = <Py_ssize_t>uv.data_length
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i, row
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen
    cdef bytes raw
    cdef object code
    cdef bytes encoded
    cdef DrakenVarBuffer* ndp
    cdef uint8_t* in_validity
    cdef const uint32_t* sel
    cdef Py_ssize_t nbytes
    cdef uint32_t code_idx
    cdef bint input_valid, any_null
    cdef uint8_t* slot_produces_null = NULL
    cdef uint8_t* out_validity = NULL

    slot_produces_null = <uint8_t*>malloc(<size_t>(slot_count if slot_count > 0 else 1))
    if slot_produces_null == NULL:
        raise MemoryError()

    try:
        # 1. Per-slot transform; track which slots are empty (empty input → null output).
        out_dict_builder = string_vector_module.StringVectorBuilder.with_estimate(slot_count, 4)
        for i in range(slot_count):
            slot = &in_arena.slots[i]
            slen = str_length(slot)
            if slen == 0:
                slot_produces_null[i] = 1
                out_dict_builder.append(b"")
                continue
            sdata = str_data(slot, in_arena.arena)
            raw = bytes(sdata[:slen])
            code = soundex(raw)
            encoded = code if code else b""
            out_dict_builder.append(encoded)
            slot_produces_null[i] = 0
        new_dict_sv = out_dict_builder.finish()

        # 2. Per-row validity: (input row valid) AND (slot is non-empty).
        in_validity = uv.validity
        sel = uv.selection
        any_null = False
        if n != 0:
            nbytes = (n + 7) >> 3
            out_validity = <uint8_t*>malloc(<size_t>nbytes)
            if out_validity == NULL:
                raise MemoryError()
            memset(out_validity, 0, <size_t>nbytes)
            for row in range(n):
                if in_validity != NULL:
                    input_valid = ((in_validity[row >> 3] >> (row & 7)) & 1) != 0
                else:
                    input_valid = True
                code_idx = sel[row]
                if input_valid and not slot_produces_null[code_idx]:
                    out_validity[row >> 3] |= <uint8_t>(1 << (row & 7))
                else:
                    any_null = True

        # 3. Wrap into a vector. Pass NULL validity when all rows are valid.
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            ndp.offsets, <const uint8_t*>ndp.data, slot_count,
            out_validity if any_null else NULL,
        )
    finally:
        free(slot_produces_null)
        if out_validity != NULL:
            free(out_validity)
