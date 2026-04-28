# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

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
from draken.core.buffers cimport (
    DrakenVarBuffer, DrakenConstantStringPayload,
    DrakenEncoding, DRAKEN_ENCODING_DICTIONARY,
    DictAccessor,
)


cdef struct StringRow:
    const char* data     # NULL when is_null is True
    Py_ssize_t length
    bint is_null


# ---------------------------------------------------------------------------
# _read_packed_code
# Identical to the implementation in string_vector.pyx (not exported there).
# Reads the code at row i for a dictionary-encoded vector.
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
# O(1) random access for any encoding. Use for secondary inputs in multi-input
# functions (needle, search term, replacement, etc.) so they are correct
# regardless of how the caller constructed them.
# ---------------------------------------------------------------------------

cdef inline StringRow string_vec_get_at(StringVector vec, Py_ssize_t i) noexcept:
    cdef StringRow row
    cdef DrakenVarBuffer* ptr
    cdef uint8_t* null_bm
    cdef uint32_t code
    cdef int32_t start

    if vec._has_const:
        row.is_null = vec._const_is_null
        if vec._const_is_null or vec._const_value == NULL:
            row.data = NULL
            row.length = 0
        else:
            row.data = <const char*>vec._const_value.data
            row.length = vec._const_value.length
        return row

    if vec._encoding == DRAKEN_ENCODING_DICTIONARY:
        null_bm = vec._dict_accessor.row_nulls
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            row.data = NULL
            row.length = 0
            row.is_null = True
            return row
        code = _read_packed_code(vec._dict_codes, vec._dict_code_width, i)
        start = vec._dict_values.offsets[code]
        row.data = <const char*>vec._dict_values.data + start
        row.length = vec._dict_values.offsets[code + 1] - start
        row.is_null = False
        return row

    # Dense
    ptr = vec.ptr
    null_bm = ptr.null_bitmap
    if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
        row.data = NULL
        row.length = 0
        row.is_null = True
        return row
    start = ptr.offsets[i]
    row.data = <const char*>ptr.data + start
    row.length = ptr.offsets[i + 1] - start
    row.is_null = False
    return row
