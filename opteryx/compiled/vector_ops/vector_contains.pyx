# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from draken.vectors.string_vector cimport StringVector, DrakenVarBuffer
from draken.vectors.bool_vector cimport BoolVector
from draken.core.buffers cimport DrakenVector, DrakenStringArena, DrakenStringSlot
from draken.core.buffers cimport str_length, str_data
from libc.string cimport memset, memcpy
from libc.stdlib cimport malloc, free
from libc.stddef cimport size_t
from libc.stdint cimport uint8_t, uint32_t, int32_t


# Volnitsky algorithm declarations for substring search
cdef extern from "volnitsky.h":
    cppclass VolnitskyTable:
        pass
    VolnitskyTable* volnitsky_alloc() noexcept nogil
    void volnitsky_free(VolnitskyTable* t) noexcept nogil
    void volnitsky_build(VolnitskyTable* t, const uint8_t* pat, size_t len) nogil
    bint volnitsky_contains_cs(const uint8_t* hay, size_t hay_len,
                                const uint8_t* pat, size_t pat_len,
                                const VolnitskyTable* table) noexcept nogil
    bint volnitsky_contains_ci(const uint8_t* hay, size_t hay_len,
                                const uint8_t* pat_lower, size_t pat_len,
                                const VolnitskyTable* table) noexcept nogil


cdef bint _sv_contains_cs(
    const uint8_t* haystack,
    Py_ssize_t hay_len,
    const uint8_t* needle,
    Py_ssize_t ndl_len,
    const VolnitskyTable* tbl,
) noexcept nogil:
    """Case-sensitive substring search using Volnitsky algorithm."""
    return volnitsky_contains_cs(haystack, <size_t>hay_len, needle, <size_t>ndl_len, tbl)


cdef bint _sv_contains_ci(
    const uint8_t* haystack,
    Py_ssize_t hay_len,
    const uint8_t* needle_lower,
    Py_ssize_t ndl_len,
    const VolnitskyTable* tbl,
) noexcept nogil:
    """Case-insensitive substring search using Volnitsky algorithm."""
    return volnitsky_contains_ci(haystack, <size_t>hay_len, needle_lower, <size_t>ndl_len, tbl)


cpdef BoolVector vector_contains(StringVector vec, StringVector substr, bint ignore_case=False):
    """Return mask: 1 if element contains substr, else 0. Propagates NULLs.

    `substr` is the wrapped needle literal; substring search takes a single
    needle, so the shape rule (one unique value) is enforced here via
    `data_length`. The needle bytes are read straight from the arena — no
    Python bytes object crosses into the engine.
    """
    cdef DrakenVector* nuv = substr.unified()
    if nuv.data_length != 1:
        raise ValueError(
            "vector_contains: substr must be a single value (data_length == 1)"
        )
    cdef DrakenStringArena* narena = <DrakenStringArena*>nuv.data
    cdef uint32_t* nsel = <uint32_t*>nuv.selection
    cdef DrakenStringSlot* nslot = &narena.slots[nsel[0]]
    cdef const uint8_t* ndl_ptr = str_data(nslot, narena.arena)
    cdef Py_ssize_t ndl_len = <Py_ssize_t>str_length(nslot)

    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3

    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*> out.ptr.data
    cdef uint8_t* out_null = NULL
    cdef uint8_t mask

    cdef uint8_t* ndl_lower = NULL

    cdef Py_ssize_t i, j
    cdef uint32_t code
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen
    cdef VolnitskyTable* tbl = NULL

    # A NULL needle makes every row NULL (SQL: x InStr NULL is NULL).
    if nuv.validity != NULL and (nuv.validity[0] & 1) == 0:
        return _all_null_bool(n)

    memset(dst, 0, nbytes)

    # Copy null bitmap
    if nulls != NULL and nbytes != 0:
        out_null = <uint8_t*> malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memcpy(out_null, nulls, nbytes)
        if (n & 7) != 0:
            mask = <uint8_t>((1 << (n & 7)) - 1)
            out_null[nbytes - 1] &= mask
        out.ptr.null_bitmap = out_null
    else:
        out.ptr.null_bitmap = NULL

    # Pre-lowercase needle once
    if ignore_case and ndl_len > 0:
        ndl_lower = <uint8_t*>malloc(<size_t>ndl_len)
        if ndl_lower == NULL:
            raise MemoryError()
        for j in range(ndl_len):
            ndl_lower[j] = _sv_ascii_lower(ndl_ptr[j])

    # Build Volnitsky table once for all elements in this morsel
    tbl = volnitsky_alloc()
    if tbl == NULL:
        if ndl_lower != NULL:
            free(ndl_lower)
        raise MemoryError()
    if ignore_case and ndl_lower != NULL:
        volnitsky_build(tbl, ndl_lower, <size_t>ndl_len)
    else:
        volnitsky_build(tbl, ndl_ptr, <size_t>ndl_len)

    try:
        for i in range(n):
            if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
                continue
            slot = &arena.slots[sel[i]]
            slen = str_length(slot)
            sdata = str_data(slot, arena.arena)
            if ignore_case:
                if _sv_contains_ci(
                    sdata, <Py_ssize_t>slen,
                    ndl_lower if ndl_lower != NULL else <uint8_t*>ndl_ptr,
                    ndl_len,
                    tbl,
                ):
                    dst[i >> 3] |= (1 << (i & 7))
            else:
                if _sv_contains_cs(
                    sdata, <Py_ssize_t>slen,
                    ndl_ptr, ndl_len,
                    tbl,
                ):
                    dst[i >> 3] |= (1 << (i & 7))
    finally:
        volnitsky_free(tbl)
        tbl = NULL
        if ndl_lower != NULL:
            free(ndl_lower)

    return out
