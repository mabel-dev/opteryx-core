# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
Native ASCII-only STARTS_WITH / ENDS_WITH kernels for StringVector.
"""

from libc.stdint cimport uint8_t, uint32_t, int32_t
from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memcmp, memset, memcpy
from cython cimport Py_ssize_t

from draken.core.buffers cimport DrakenVector, DrakenVarBuffer, DrakenStringArena, DrakenStringSlot, str_length, str_data
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.string_vector cimport StringVector


# ----------------------------------------------------------------------
# Low-level helpers
# ----------------------------------------------------------------------
cdef inline uint8_t _ascii_lower(uint8_t b) noexcept nogil:
    # Unsigned arithmetic: for b < 65, (b - 65) wraps around to a large number > 25.
    # For 65 <= b <= 90, (b - 65) is 0..25.
    return b + (32 * ((b - 65U) <= 25U))


cdef inline void _lower_ascii_buffer(uint8_t* dst, const uint8_t* src, Py_ssize_t n) noexcept nogil:
    cdef Py_ssize_t i
    for i in range(n):
        dst[i] = _ascii_lower(src[i])


cdef inline void _set_bit(uint8_t* bits, Py_ssize_t i) noexcept nogil:
    bits[i >> 3] |= (<uint8_t>1 << (i & 7))


cdef inline void _finalize_bool_null_bitmap(BoolVector out, Py_ssize_t n, const uint8_t* src_nulls) except *:
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef uint8_t* out_null
    if src_nulls == NULL or nbytes == 0:
        out.ptr.null_bitmap = NULL
        return
    out_null = <uint8_t*>malloc(<size_t>nbytes)
    if out_null == NULL:
        raise MemoryError()
    memcpy(out_null, src_nulls, nbytes)
    if n & 7:
        out_null[nbytes - 1] &= <uint8_t>((1 << (n & 7)) - 1)
    out.ptr.null_bitmap = out_null


# ----------------------------------------------------------------------
# Matching primitives (single string against a pattern)
# ----------------------------------------------------------------------
cdef inline bint _match_prefix(const uint8_t* haystack, Py_ssize_t haystack_len,
                               const uint8_t* needle, Py_ssize_t needle_len) noexcept nogil:
    if haystack_len < needle_len:
        return False
    return memcmp(haystack, needle, needle_len) == 0


cdef inline bint _match_suffix(const uint8_t* haystack, Py_ssize_t haystack_len,
                               const uint8_t* needle, Py_ssize_t needle_len) noexcept nogil:
    if haystack_len < needle_len:
        return False
    return memcmp(haystack + haystack_len - needle_len, needle, needle_len) == 0


cdef inline bint _match_prefix_ci(const uint8_t* haystack, Py_ssize_t haystack_len,
                                  const uint8_t* needle_lower, Py_ssize_t needle_len) noexcept nogil:
    cdef Py_ssize_t i
    if haystack_len < needle_len:
        return False
    for i in range(needle_len):
        if _ascii_lower(haystack[i]) != needle_lower[i]:
            return False
    return True


cdef inline bint _match_suffix_ci(const uint8_t* haystack, Py_ssize_t haystack_len,
                                  const uint8_t* needle_lower, Py_ssize_t needle_len) noexcept nogil:
    cdef Py_ssize_t i
    if haystack_len < needle_len:
        return False
    for i in range(needle_len):
        if _ascii_lower(haystack[haystack_len - needle_len + i]) != needle_lower[i]:
            return False
    return True


# ----------------------------------------------------------------------
# Needle extraction from a constant StringVector
# ----------------------------------------------------------------------
cdef inline void _extract_const_needle(
    StringVector sv,
    const uint8_t** needle_out,
    Py_ssize_t* needle_len_out,
    const char* fn_name,
) except *:
    cdef DrakenVector* uv = sv.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    # Constant: offsets == NULL (per CLAUDE.md discriminant rule)
    if sv.ptr.offsets != NULL:
        raise ValueError(f"{fn_name} does not support non-constant needle")
    if uv.validity != NULL:  # null constant
        needle_out[0] = NULL
        needle_len_out[0] = 0
        return
    # Constant vector: single slot at index 0
    needle_out[0] = <const uint8_t*>str_data(&arena.slots[0], arena.arena)
    needle_len_out[0] = <Py_ssize_t>str_length(&arena.slots[0])


# ----------------------------------------------------------------------
# Unified kernel — one loop, all shapes
# ----------------------------------------------------------------------
cdef BoolVector _starts_ends_kernel(
    StringVector vec,
    const uint8_t* needle,
    Py_ssize_t needle_len,
    bint ignore_case,
    bint negated,
    bint is_suffix,
) except *:
    cdef DrakenVector* uv = vec.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3

    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
    cdef DrakenStringSlot* slot
    cdef size_t slen
    cdef const uint8_t* sdata
    cdef bint matched
    cdef Py_ssize_t i
    cdef uint8_t* lower_needle = NULL
    cdef uint8_t* lower_row = NULL
    cdef size_t lower_row_cap = 0

    memset(out_bits, 0, nbytes)

    if n == 0:
        _finalize_bool_null_bitmap(out, n, nulls)
        return out

    if ignore_case:
        lower_needle = <uint8_t*>malloc(<size_t>needle_len if needle_len > 0 else 1)
        if lower_needle == NULL:
            raise MemoryError()
        _lower_ascii_buffer(lower_needle, needle, needle_len)

        try:
            for i in range(n):
                if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
                    continue
                slot = &arena.slots[sel[i]]
                slen = str_length(slot)
                sdata = <const uint8_t*>str_data(slot, arena.arena)
                if slen > lower_row_cap:
                    lower_row_cap = slen + 64
                    lower_row = <uint8_t*>realloc(lower_row, lower_row_cap)
                    if lower_row == NULL:
                        raise MemoryError()
                _lower_ascii_buffer(lower_row, sdata, slen)
                if is_suffix:
                    matched = _match_suffix_ci(lower_row, slen, lower_needle, needle_len)
                else:
                    matched = _match_prefix_ci(lower_row, slen, lower_needle, needle_len)
                if negated:
                    matched = not matched
                if matched:
                    _set_bit(out_bits, i)
        finally:
            free(lower_needle)
            if lower_row != NULL:
                free(lower_row)

    else:
        for i in range(n):
            if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
                continue
            slot = &arena.slots[sel[i]]
            slen = str_length(slot)
            sdata = <const uint8_t*>str_data(slot, arena.arena)
            if is_suffix:
                matched = _match_suffix(sdata, slen, needle, needle_len)
            else:
                matched = _match_prefix(sdata, slen, needle, needle_len)
            if negated:
                matched = not matched
            if matched:
                _set_bit(out_bits, i)

    _finalize_bool_null_bitmap(out, n, nulls)
    return out


# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------
cpdef BoolVector vector_starts_with(StringVector vec, StringVector prefix):
    cdef Py_ssize_t needle_len
    cdef const uint8_t* needle
    _extract_const_needle(prefix, &needle, &needle_len, b"vector_starts_with")
    return _starts_ends_kernel(vec, needle, needle_len, False, False, False)


cpdef BoolVector vector_ci_starts_with(StringVector vec, StringVector prefix):
    cdef Py_ssize_t needle_len
    cdef const uint8_t* needle
    _extract_const_needle(prefix, &needle, &needle_len, b"vector_ci_starts_with")
    return _starts_ends_kernel(vec, needle, needle_len, True, False, False)


cpdef BoolVector vector_ends_with(StringVector vec, StringVector suffix):
    cdef Py_ssize_t needle_len
    cdef const uint8_t* needle
    _extract_const_needle(suffix, &needle, &needle_len, b"vector_ends_with")
    return _starts_ends_kernel(vec, needle, needle_len, False, False, True)


cpdef BoolVector vector_ci_ends_with(StringVector vec, StringVector suffix):
    cdef Py_ssize_t needle_len
    cdef const uint8_t* needle
    _extract_const_needle(suffix, &needle, &needle_len, b"vector_ci_ends_with")
    return _starts_ends_kernel(vec, needle, needle_len, True, False, True)
