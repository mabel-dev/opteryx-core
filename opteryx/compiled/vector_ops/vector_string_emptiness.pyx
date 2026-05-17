# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Internal kernels backing the predicate-rewrite of `col = ''` / `col != ''`
# (and the equivalent `LENGTH(col) <op> N` forms) to `_IS_EMPTY(col)` /
# `_IS_NOT_EMPTY(col)`.
#
# The byte content is never inspected. Emptiness is decided purely from
# offset / length metadata:
#   - dense:  offsets[i+1] == offsets[i]
#   - dict:   dict_offsets[code+1] == dict_offsets[code]   (per-code, then row-walk)
#   - RLE:    run_str_lens[r] == 0                          (per-run, then bit-fill)
#   - const:  vec._const_value.length == 0
#
# NULL propagation matches SQL 3VL: the output null bitmap is the input's
# null bitmap copied verbatim (rows that were NULL stay NULL).
#
# _decode_dict_code is shared via _helper_string.pyx (consolidated
# at module level by the include directives in vector_ops.pyx).

from libc.stdint cimport int32_t, uint8_t, uint32_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy

from draken.core.buffers cimport (
    DrakenVarBuffer,
    DRAKEN_ENCODING_DICTIONARY,
)
from draken.vectors.string_vector cimport StringVector, _materialize_dict_string
from draken.vectors.bool_vector cimport BoolVector


cdef BoolVector _make_constant_bool(Py_ssize_t n, bint matched, bint is_null):
    """All rows share the same value (and same null-ness)."""
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*> out.ptr.data
    cdef uint8_t* out_null
    cdef uint8_t mask

    if nbytes > 0:
        memset(dst, 0, nbytes)

    if is_null:
        if nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memset(out_null, 0, nbytes)
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL
        return out

    if matched and nbytes > 0:
        memset(dst, 0xFF, nbytes)
        if (n & 7) != 0:
            mask = <uint8_t>((1 << (n & 7)) - 1)
            dst[nbytes - 1] &= mask

    out.ptr.null_bitmap = NULL
    return out


cdef BoolVector _alloc_bool_with_nulls(Py_ssize_t n, const uint8_t* src_nulls):
    """Allocate a BoolVector of length n, value bits zeroed, copying src_nulls if non-NULL."""
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*> out.ptr.data
    cdef uint8_t* out_null
    cdef uint8_t mask

    if nbytes > 0:
        memset(dst, 0, nbytes)

    if src_nulls != NULL and nbytes != 0:
        out_null = <uint8_t*> malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memcpy(out_null, src_nulls, nbytes)
        if (n & 7) != 0:
            mask = <uint8_t>((1 << (n & 7)) - 1)
            out_null[nbytes - 1] &= mask
        out.ptr.null_bitmap = out_null
    else:
        out.ptr.null_bitmap = NULL
    return out


cdef inline void _set_bit_range(uint8_t* dst, Py_ssize_t start, Py_ssize_t count) noexcept:
    """Set bits [start, start+count) in dst. Naive bit-by-bit; counts are typically small per run."""
    cdef Py_ssize_t i
    for i in range(count):
        dst[(start + i) >> 3] |= <uint8_t>(1 << ((start + i) & 7))


cdef BoolVector _emptiness_dense(StringVector vec, bint emit_when_empty):
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef BoolVector out = _alloc_bool_with_nulls(n, ptr.null_bitmap)
    cdef uint8_t* dst = <uint8_t*> out.ptr.data
    cdef int32_t* offsets = ptr.offsets
    cdef uint8_t* null_bm = ptr.null_bitmap
    cdef Py_ssize_t i
    cdef bint is_empty

    for i in range(n):
        # Skip null rows entirely — output null bitmap already says they're NULL,
        # and we leave the value bit at 0.
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            continue
        is_empty = (offsets[i + 1] == offsets[i])
        if is_empty == emit_when_empty:
            dst[i >> 3] |= <uint8_t>(1 << (i & 7))
    return out


cdef BoolVector _emptiness_dict(StringVector vec, bint emit_when_empty):
    cdef DrakenVarBuffer* dict_ptr = vec._dict_values
    cdef Py_ssize_t dict_size = dict_ptr.length
    cdef int32_t* dict_offsets = dict_ptr.offsets
    cdef uint8_t* codes = vec._dict_codes
    cdef uint8_t code_width = vec._dict_code_width
    cdef uint8_t* row_nulls = vec._dict_accessor.row_nulls
    cdef Py_ssize_t n = vec.ptr.length

    cdef BoolVector out = _alloc_bool_with_nulls(n, row_nulls)
    cdef uint8_t* dst = <uint8_t*> out.ptr.data

    # Per-code emptiness lookup table (one byte per dict entry; small, hot in cache).
    cdef uint8_t* code_match = <uint8_t*> malloc(<size_t>dict_size if dict_size > 0 else 1)
    if code_match == NULL:
        raise MemoryError()
    cdef Py_ssize_t c
    cdef bint is_empty
    cdef Py_ssize_t i
    cdef uint32_t code
    try:
        for c in range(dict_size):
            is_empty = (dict_offsets[c + 1] == dict_offsets[c])
            code_match[c] = 1 if is_empty == emit_when_empty else 0

        for i in range(n):
            if row_nulls != NULL and not ((row_nulls[i >> 3] >> (i & 7)) & 1):
                continue
            code = _decode_dict_code(codes, code_width, i)
            if code_match[code]:
                dst[i >> 3] |= <uint8_t>(1 << (i & 7))
    finally:
        free(code_match)
    return out


cdef BoolVector _string_emptiness_kernel(StringVector vec, bint emit_when_empty):
    cdef Py_ssize_t n = vec.ptr.length

    if vec._has_const:
        if vec._const_is_null:
            return _make_constant_bool(n, False, True)
        return _make_constant_bool(
            n,
            (vec._const_value.length == 0) == emit_when_empty,
            False,
        )

    if vec._encoding == DRAKEN_ENCODING_DICTIONARY and vec.ptr.data == NULL:
        return _emptiness_dict(vec, emit_when_empty)

    return _emptiness_dense(vec, emit_when_empty)


cpdef BoolVector vector_string_is_empty(StringVector vec):
    """Return a BoolVector: True where the string is empty (zero-length). NULLs propagate."""
    return _string_emptiness_kernel(vec, True)


cpdef BoolVector vector_string_is_not_empty(StringVector vec):
    """Return a BoolVector: True where the string is non-empty. NULLs propagate."""
    return _string_emptiness_kernel(vec, False)
