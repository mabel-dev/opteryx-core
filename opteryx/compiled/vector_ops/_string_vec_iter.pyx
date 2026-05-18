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
#   _read_packed_code  — dict code at row i (1/2/4-byte wide)
#
# Single-input string-to-string functions use the three-branch pattern directly:
#   1. const:  process one value, replicate n times
#   2. dict:   process each dict entry once (O(dict_size)), repack with same codes
#   3. dense:  iterate rows
#
# Multi-input functions use string_vec_get_at for O(1) per-row access on any
# encoding for secondary inputs (needle, search, replace_val, etc.).

from libc.stdint cimport int32_t, uint8_t, uint16_t, uint32_t
from draken.vectors.string_vector cimport StringVector
from draken.core.buffers cimport DrakenVarBuffer, DrakenConstantStringPayload, DrakenVector


cdef struct StringRow:
    const char* data     # NULL when is_null is True
    Py_ssize_t length
    bint is_null


# ---------------------------------------------------------------------------
# _read_packed_code
# Reads a selection-vector code at row i (1/2/4-byte wide).
# ---------------------------------------------------------------------------

cdef inline uint32_t _read_packed_code(
    const uint8_t* codes,
    uint8_t code_width,
    Py_ssize_t i,
) noexcept nogil:
    if code_width == 1:
        return (<const uint8_t*>codes)[i]
    if code_width == 2:
        return (<const uint16_t*>codes)[i]
    return (<const uint32_t*>codes)[i]


# ---------------------------------------------------------------------------
# string_vec_get_at
# O(1) random access via the unified DrakenVector view.
# ---------------------------------------------------------------------------

cdef inline StringRow string_vec_get_at(StringVector vec, Py_ssize_t i) noexcept:
    cdef StringRow row
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenVarBuffer* vbuf
    cdef DrakenConstantStringPayload* csp
    cdef uint8_t* null_bm
    cdef uint32_t code
    cdef int32_t start

    if uv.selection == NULL and vec.ptr.offsets == NULL:  # constant
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

    if uv.selection != NULL:  # dictionary
        vbuf = <DrakenVarBuffer*>uv.data
        code = _read_packed_code(<uint8_t*>uv.selection, uv.sel_width, i)
        start = vbuf.offsets[code]
        row.data = <const char*>vbuf.data + start
        row.length = vbuf.offsets[code + 1] - start
    else:  # dense
        vbuf = <DrakenVarBuffer*>uv.data
        start = vbuf.offsets[i]
        row.data = <const char*>vbuf.data + start
        row.length = vbuf.offsets[i + 1] - start
    row.is_null = False
    return row
