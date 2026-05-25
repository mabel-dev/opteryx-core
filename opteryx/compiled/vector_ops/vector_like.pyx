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
from draken.vectors.bool_vector cimport BoolVector, from_decoded
from draken.core.buffers cimport DrakenVector, DrakenStringArena, DrakenStringSlot, str_length, str_data
from libc.string cimport memset, memcpy
from libc.stdint cimport uint8_t, uint32_t
from libc.stddef cimport size_t

cdef extern from "core/alloc.h":
    void* draken_malloc(size_t size) nogil
    void draken_free(void* ptr) nogil


cdef inline bint _sv_byte_equals(uint8_t left, uint8_t right, bint ignore_case) noexcept nogil:
    """Compare two bytes, optionally case-insensitive."""
    if ignore_case:
        return _sv_ascii_lower(left) == _sv_ascii_lower(right)
    return left == right


cdef bint _sv_sql_like_match(
    const uint8_t* text,
    Py_ssize_t text_len,
    const uint8_t* pattern,
    Py_ssize_t pattern_len,
    bint ignore_case,
) noexcept nogil:
    """SQL LIKE matcher supporting % and _ wildcards and backslash escaping."""
    cdef Py_ssize_t ti = 0
    cdef Py_ssize_t pi = 0
    cdef Py_ssize_t last_pct = -1
    cdef Py_ssize_t last_match = 0
    cdef uint8_t pc

    while ti < text_len:
        if pi < pattern_len:
            pc = pattern[pi]
            if pc == 92 and (pi + 1) < pattern_len:  # backslash escape
                if _sv_byte_equals(text[ti], pattern[pi + 1], ignore_case):
                    ti += 1
                    pi += 2
                    continue
            elif pc == 95:  # "_" wildcard
                ti += 1
                pi += 1
                continue
            elif pc == 37:  # "%" wildcard
                last_pct = pi
                pi += 1
                last_match = ti
                continue
            elif _sv_byte_equals(text[ti], pc, ignore_case):
                ti += 1
                pi += 1
                continue

        if last_pct != -1:
            last_match += 1
            ti = last_match
            pi = last_pct + 1
            continue
        return False

    while pi < pattern_len and pattern[pi] == 37:
        pi += 1

    return pi == pattern_len


cpdef BoolVector vector_like(
    StringVector vec,
    StringVector pattern,
    bint ignore_case=False,
    bint negate=False,
):
    """Return mask: 1 if element matches SQL LIKE pattern, else 0. Propagates NULLs.

    `pattern` is the wrapped pattern literal. SQL LIKE takes a single pattern,
    so the shape rule (one unique value) is enforced here: `data_length != 1`
    fails. The pattern bytes are read straight from the arena — no Python
    bytes object crosses into the engine. The check runs with the GIL held,
    before the hot loop, so the raise is Python-free where it matters.

    If `negate` is True, the result is the row-wise NotLike: True where the
    element does NOT match the pattern. Fuses what would otherwise be a
    second full-pass `.not_vector()`.
    """
    cdef DrakenVector* puv = pattern.unified()
    if puv.data_length != 1:
        raise ValueError(
            "vector_like: pattern must be a single value (data_length == 1)"
        )
    cdef DrakenStringArena* parena = <DrakenStringArena*>puv.data
    cdef uint32_t* psel = <uint32_t*>puv.selection
    cdef DrakenStringSlot* pslot = &parena.slots[psel[0]]
    cdef const uint8_t* pat_ptr = str_data(pslot, parena.arena)
    cdef Py_ssize_t pat_len = <Py_ssize_t>str_length(pslot)

    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nb_ptr = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef uint8_t* out_null = NULL
    cdef uint8_t mask
    cdef Py_ssize_t i
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen

    # A NULL pattern makes every row NULL (SQL: x LIKE NULL is NULL).
    if puv.validity != NULL and (puv.validity[0] & 1) == 0:
        return _all_null_bool(n)

    cdef uint8_t* dst = <uint8_t*>draken_malloc(<size_t>(nbytes if nbytes > 0 else 1))
    if dst == NULL:
        raise MemoryError()

    # Initial fill matches the wanted result for the "no match" rows so the
    # inner loop only touches bits at matches.
    if negate:
        memset(dst, 0xFF, nbytes)
        if (n & 7) != 0:
            dst[nbytes - 1] &= <uint8_t>((1 << (n & 7)) - 1)
    else:
        memset(dst, 0, nbytes)

    if nb_ptr != NULL and nbytes != 0:
        out_null = <uint8_t*>draken_malloc(<size_t>nbytes)
        if out_null == NULL:
            raise MemoryError()
        memcpy(out_null, nb_ptr, nbytes)
        if (n & 7) != 0:
            mask = <uint8_t>((1 << (n & 7)) - 1)
            out_null[nbytes - 1] &= mask

    if negate:
        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            slot = &arena.slots[sel[i]]
            slen = str_length(slot)
            sdata = str_data(slot, arena.arena)
            if _sv_sql_like_match(
                sdata, <Py_ssize_t>slen,
                pat_ptr, pat_len, ignore_case,
            ):
                dst[i >> 3] &= ~(1 << (i & 7))
    else:
        for i in range(n):
            if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                continue
            slot = &arena.slots[sel[i]]
            slen = str_length(slot)
            sdata = str_data(slot, arena.arena)
            if _sv_sql_like_match(
                sdata, <Py_ssize_t>slen,
                pat_ptr, pat_len, ignore_case,
            ):
                dst[i >> 3] |= (1 << (i & 7))

    return from_decoded(<void*>dst, out_null, <size_t>n)
