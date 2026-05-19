# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# _string_vec_iter.pyx — Encoding-aware string vector access helpers.
#
# Auto-included FIRST in vector_ops.pyx because the underscore prefix sorts
# before all letter-prefixed filenames. All symbols defined here are available
# to every file in the consolidated module without an explicit import.
#
# API:
#   StringRow          — {data, length, is_null} for one string value
#   string_vec_get_at  — random access across all 3 encodings (const/dict/dense)
#
# Single-input string-to-string functions use the three-branch pattern directly:
#   1. const:  process one value, replicate n times
#   2. dict:   process each dict entry once (O(dict_size)), repack with same codes
#   3. dense:  iterate rows
#
# Multi-input functions use string_vec_get_at for O(1) per-row access on any
# encoding for secondary inputs (needle, search, replace_val, etc.).

from libc.stdint cimport int32_t, uint32_t
from draken.vectors.string_vector cimport StringVector
from draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload, DrakenVector, DrakenGermanArena, GermanString, gs_length, gs_data


cdef struct StringRow:
    const char* data     # NULL when is_null is True
    Py_ssize_t length
    bint is_null


# ---------------------------------------------------------------------------
# string_vec_get_at
# O(1) random access via the unified DrakenVector view.
# ---------------------------------------------------------------------------

cdef inline StringRow string_vec_get_at(StringVector vec, Py_ssize_t i) noexcept:
    cdef StringRow row
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenVarBuffer* vbuf
    cdef DrakenGermanArena* gdv
    cdef DrakenConstantStringPayload* csp
    cdef uint8_t* null_bm
    cdef uint32_t code
    cdef int32_t start
    cdef GermanString* slot

    if vec.ptr.offsets == NULL and vec._german_dict_values == NULL:  # constant
        if uv.validity != NULL:  # null constant
            row.data = NULL
            row.length = 0
            row.is_null = True
        else:
            csp = <DrakenConstantStringPayload*>uv.data
            row.data = <const char*>csp.data
            row.length = csp.length
            row.is_null = False
        return row

    null_bm = uv.validity
    if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
        row.data = NULL
        row.length = 0
        row.is_null = True
        return row

    if vec._german_dict_values != NULL:  # dictionary — backed by DrakenGermanArena
        gdv = vec._german_dict_values
        code = uv.selection[i]
        slot = &gdv.slots[code]
        row.data = <const char*>gs_data(slot, gdv.arena)
        row.length = <Py_ssize_t>gs_length(slot)
    else:  # dense — backed by DrakenVarBuffer
        vbuf = <DrakenVarBuffer*>uv.data
        start = vbuf.offsets[i]
        row.data = <const char*>vbuf.data + start
        row.length = vbuf.offsets[i + 1] - start
    row.is_null = False
    return row
