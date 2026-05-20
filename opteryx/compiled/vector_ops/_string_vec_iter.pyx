# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# _string_vec_iter.pyx — Unified string vector access helpers.
#
# Auto-included FIRST in vector_ops.pyx because the underscore prefix sorts
# before all letter-prefixed filenames. All symbols defined here are available
# to every file in the consolidated module without an explicit import.
#
# API:
#   StringRow          — {data, length, is_null} for one string value
#   string_vec_get_at  — O(1) per-row access via the unified DrakenVector view
#
# Single-input string-to-string functions use the unified loop directly:
#   uv = vec.unified(); arena = uv.data; sel = uv.selection
#   slot = &arena.slots[sel[i]]
#
# Multi-input functions use string_vec_get_at for O(1) per-row access on any
# encoding for secondary inputs (needle, search, replace_val, etc.).

from libc.stdint cimport uint32_t, uint8_t
from draken.vectors.string_vector cimport StringVector
from draken.core.buffers cimport DrakenVector, DrakenStringArena, DrakenStringSlot, str_length, str_data


cdef struct StringRow:
    const char* data     # NULL when is_null is True
    Py_ssize_t length
    bint is_null


# ---------------------------------------------------------------------------
# string_vec_get_at
# O(1) random access via the unified DrakenVector view.
# Works for all three encodings (dense, constant, dictionary) because
# unified() guarantees sel[i] is a valid slot index for every row.
# ---------------------------------------------------------------------------

cdef inline StringRow string_vec_get_at(StringVector vec, Py_ssize_t i) noexcept:
    cdef StringRow row
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef DrakenStringSlot* slot

    if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
        row.data = NULL
        row.length = 0
        row.is_null = True
        return row

    slot = &arena.slots[sel[i]]
    row.data = <const char*>str_data(slot, arena.arena)
    row.length = <Py_ssize_t>str_length(slot)
    row.is_null = False
    return row
