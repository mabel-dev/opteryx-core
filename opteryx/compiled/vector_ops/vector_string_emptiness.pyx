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
# the string length metadata via str_length().
#
# NULL propagation matches SQL 3VL: the output null bitmap is the input's
# null bitmap copied verbatim (rows that were NULL stay NULL).

from libc.stdint cimport int32_t, uint8_t, uint32_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset, memcpy

from draken.core.buffers cimport (
    DrakenVector,
    DrakenStringArena,
    DrakenStringSlot,
    str_length,
)
from draken.vectors.string_vector cimport StringVector
from draken.vectors.bool_vector cimport BoolVector


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


cdef BoolVector _string_emptiness_kernel(StringVector vec, bint emit_when_empty):
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length

    cdef BoolVector out = _alloc_bool_with_nulls(n, nulls)
    cdef uint8_t* dst = <uint8_t*> out.ptr.data
    cdef Py_ssize_t i
    cdef bint is_empty

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            continue
        is_empty = (str_length(&arena.slots[sel[i]]) == 0)
        if is_empty == emit_when_empty:
            dst[i >> 3] |= <uint8_t>(1 << (i & 7))

    return out


cpdef BoolVector vector_string_is_empty(StringVector vec):
    """Return a BoolVector: True where the string is empty (zero-length). NULLs propagate."""
    return _string_emptiness_kernel(vec, True)


cpdef BoolVector vector_string_is_not_empty(StringVector vec):
    """Return a BoolVector: True where the string is non-empty. NULLs propagate."""
    return _string_emptiness_kernel(vec, False)
