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
from libc.stdlib cimport malloc, free
from libc.string cimport memcmp, memset
from cython cimport Py_ssize_t

from draken.core.buffers cimport DrakenVector, DrakenConstantStringPayload, DrakenVarBuffer
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


cdef inline bint _row_is_valid(const uint8_t* nulls, Py_ssize_t i) noexcept nogil:
    return nulls == NULL or ((nulls[i >> 3] >> (i & 7)) & 1) != 0


cdef inline void _set_bit(uint8_t* bits, Py_ssize_t i) noexcept nogil:
    bits[i >> 3] |= (<uint8_t>1 << (i & 7))


cdef inline void _finalize_bool_null_bitmap(BoolVector out, Py_ssize_t n, bint all_valid) except *:
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef uint8_t* out_null
    if all_valid or nbytes == 0:
        out.ptr.null_bitmap = NULL
        return
    out_null = <uint8_t*>malloc(<size_t>nbytes)
    if out_null == NULL:
        raise MemoryError()
    memset(out_null, 0xFF, <size_t>nbytes)
    if n & 7:
        out_null[nbytes - 1] &= <uint8_t>((1 << (n & 7)) - 1)
    out.ptr.null_bitmap = out_null


cdef inline uint32_t _starts_ends_read_packed_code(const uint8_t* codes, uint8_t code_width, Py_ssize_t i) noexcept nogil:
    if code_width == 1:
        return (<const uint8_t*>codes)[i]
    if code_width == 2:
        return (<const uint16_t*>codes)[i]
    return (<const uint32_t*>codes)[i]




# ----------------------------------------------------------------------
# Matching primitives (single string against a pattern)
# ----------------------------------------------------------------------
cdef inline bint _match_prefix(const uint8_t* haystack, Py_ssize_t haystack_len,
                               const uint8_t* needle, Py_ssize_t needle_len) noexcept nogil:
    return memcmp(haystack, needle, needle_len) == 0


cdef inline bint _match_suffix(const uint8_t* haystack, Py_ssize_t haystack_len,
                               const uint8_t* needle, Py_ssize_t needle_len) noexcept nogil:
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
# Constant vector case
# ----------------------------------------------------------------------
cdef BoolVector _constant_starts_ends(
    StringVector vec,
    const uint8_t* needle,
    Py_ssize_t needle_len,
    bint ignore_case,
    bint negated,
    bint is_suffix,
) except *:
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
    cdef DrakenConstantStringPayload* csp
    cdef uint8_t* const_data
    cdef Py_ssize_t const_len
    cdef bint matched
    cdef uint8_t* const_lower = NULL
    cdef uint8_t* needle_lower = NULL

    memset(out_bits, 0, nbytes)

    if uv.validity != NULL:  # null constant
        _finalize_bool_null_bitmap(out, n, False)
        return out

    csp = <DrakenConstantStringPayload*>uv.data
    const_data = <uint8_t*>csp.data
    const_len = <Py_ssize_t>csp.length

    if ignore_case:
        const_lower = <uint8_t*>malloc(<size_t>const_len)
        if const_lower == NULL:
            raise MemoryError()
        needle_lower = <uint8_t*>malloc(<size_t>needle_len)
        if needle_lower == NULL:
            free(const_lower)
            raise MemoryError()
        _lower_ascii_buffer(const_lower, const_data, const_len)
        _lower_ascii_buffer(needle_lower, needle, needle_len)
        if is_suffix:
            matched = _match_suffix_ci(const_lower, const_len, needle_lower, needle_len)
        else:
            matched = _match_prefix_ci(const_lower, const_len, needle_lower, needle_len)
        free(const_lower)
        free(needle_lower)
    else:
        if is_suffix:
            matched = _match_suffix(const_data, const_len, needle, needle_len)
        else:
            matched = _match_prefix(const_data, const_len, needle, needle_len)

    if negated:
        matched = not matched

    if matched:
        memset(out_bits, 0xFF, nbytes)
        if n & 7:
            out_bits[nbytes - 1] &= <uint8_t>((1 << (n & 7)) - 1)

    out.ptr.null_bitmap = NULL
    return out


# ----------------------------------------------------------------------
# Dense (non‑constant) vector case – branch‑free inside loop
# ----------------------------------------------------------------------
cdef BoolVector _dense_starts_ends(
    StringVector vec,
    const uint8_t* needle,
    Py_ssize_t needle_len,
    bint ignore_case,
    bint negated,
    bint is_suffix,
) except *:
    cdef Py_ssize_t n = vec.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
    cdef uint8_t* nulls = vec.ptr.null_bitmap
    cdef uint8_t* data = <uint8_t*>vec.ptr.data
    cdef int32_t* offsets = vec.ptr.offsets
    cdef Py_ssize_t i, start, length
    cdef bint all_valid = True
    cdef uint8_t* lower_needle = NULL
    cdef uint8_t* lower_data = NULL
    cdef bint matched

    memset(out_bits, 0, nbytes)

    if n == 0:
        return out

    if ignore_case:
        # Pre‑compute lowercased needle and full data once
        lower_needle = <uint8_t*>malloc(<size_t>needle_len)
        if lower_needle == NULL:
            raise MemoryError()
        _lower_ascii_buffer(lower_needle, needle, needle_len)

        lower_data = <uint8_t*>malloc(<size_t>offsets[n])
        if lower_data == NULL:
            free(lower_needle)
            raise MemoryError()
        _lower_ascii_buffer(lower_data, data, offsets[n])

        # Case‑insensitive loop – no branch inside
        if is_suffix:
            for i in range(n):
                if not _row_is_valid(nulls, i):
                    all_valid = False
                    continue
                start = offsets[i]
                length = offsets[i + 1] - start
                matched = _match_suffix_ci(lower_data + start, length, lower_needle, needle_len)
                if negated:
                    matched = not matched
                if matched:
                    _set_bit(out_bits, i)
        else:
            for i in range(n):
                if not _row_is_valid(nulls, i):
                    all_valid = False
                    continue
                start = offsets[i]
                length = offsets[i + 1] - start
                matched = _match_prefix_ci(lower_data + start, length, lower_needle, needle_len)
                if negated:
                    matched = not matched
                if matched:
                    _set_bit(out_bits, i)

        free(lower_needle)
        free(lower_data)

    else:
        # Case‑sensitive loop – no branch inside
        if is_suffix:
            for i in range(n):
                if not _row_is_valid(nulls, i):
                    all_valid = False
                    continue
                start = offsets[i]
                length = offsets[i + 1] - start
                matched = _match_suffix(data + start, length, needle, needle_len)
                if negated:
                    matched = not matched
                if matched:
                    _set_bit(out_bits, i)
        else:
            for i in range(n):
                if not _row_is_valid(nulls, i):
                    all_valid = False
                    continue
                start = offsets[i]
                length = offsets[i + 1] - start
                matched = _match_prefix(data + start, length, needle, needle_len)
                if negated:
                    matched = not matched
                if matched:
                    _set_bit(out_bits, i)

    _finalize_bool_null_bitmap(out, n, all_valid)
    return out


# ----------------------------------------------------------------------
# Dictionary‑encoded vector case – branch over dictionary only
# ----------------------------------------------------------------------
cdef BoolVector _dictionary_starts_ends(
    StringVector vec,
    const uint8_t* needle,
    Py_ssize_t needle_len,
    bint ignore_case,
    bint negated,
    bint is_suffix,
) except *:
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* out_bits = <uint8_t*>out.ptr.data
    cdef uint8_t* row_nulls = uv.validity
    cdef uint8_t* codes = <uint8_t*>uv.selection
    cdef uint8_t code_width = uv.sel_width
    cdef DrakenVarBuffer* dict_values = <DrakenVarBuffer*>uv.data
    cdef Py_ssize_t dict_size = <Py_ssize_t>dict_values.length
    cdef int32_t* dict_offsets = dict_values.offsets
    cdef uint8_t* dict_data = <uint8_t*>dict_values.data
    cdef Py_ssize_t i, d, start, length
    cdef uint32_t code
    cdef bint all_valid = True
    cdef uint8_t* dict_match = NULL
    cdef uint8_t* lower_needle = NULL
    cdef uint8_t* lower_dict = NULL
    cdef bint matched

    memset(out_bits, 0, nbytes)

    if n == 0:
        return out

    # Pre‑compute which dictionary entries match
    dict_match = <uint8_t*>malloc(<size_t>dict_size)
    if dict_match == NULL:
        raise MemoryError()
    memset(dict_match, 0, <size_t>dict_size)

    if ignore_case:
        lower_needle = <uint8_t*>malloc(<size_t>needle_len)
        if lower_needle == NULL:
            free(dict_match)
            raise MemoryError()
        _lower_ascii_buffer(lower_needle, needle, needle_len)

        lower_dict = <uint8_t*>malloc(<size_t>dict_offsets[dict_size])
        if lower_dict == NULL:
            free(dict_match)
            free(lower_needle)
            raise MemoryError()
        _lower_ascii_buffer(lower_dict, dict_data, dict_offsets[dict_size])

        # Case‑insensitive dictionary matching – branch only over dict_size (small)
        if is_suffix:
            for d in range(dict_size):
                start = dict_offsets[d]
                length = dict_offsets[d + 1] - start
                matched = _match_suffix_ci(lower_dict + start, length, lower_needle, needle_len)
                if negated:
                    matched = not matched
                if matched:
                    dict_match[d] = 1
        else:
            for d in range(dict_size):
                start = dict_offsets[d]
                length = dict_offsets[d + 1] - start
                matched = _match_prefix_ci(lower_dict + start, length, lower_needle, needle_len)
                if negated:
                    matched = not matched
                if matched:
                    dict_match[d] = 1

        free(lower_needle)
        free(lower_dict)

    else:
        # Case‑sensitive dictionary matching
        if is_suffix:
            for d in range(dict_size):
                start = dict_offsets[d]
                length = dict_offsets[d + 1] - start
                matched = _match_suffix(dict_data + start, length, needle, needle_len)
                if negated:
                    matched = not matched
                if matched:
                    dict_match[d] = 1
        else:
            for d in range(dict_size):
                start = dict_offsets[d]
                length = dict_offsets[d + 1] - start
                matched = _match_prefix(dict_data + start, length, needle, needle_len)
                if negated:
                    matched = not matched
                if matched:
                    dict_match[d] = 1

    # Row loop – no branches (just lookup in dict_match)
    for i in range(n):
        if not _row_is_valid(row_nulls, i):
            all_valid = False
            continue
        code = _starts_ends_read_packed_code(codes, code_width, i)
        if code < <uint32_t>dict_size and dict_match[code]:
            _set_bit(out_bits, i)

    free(dict_match)
    _finalize_bool_null_bitmap(out, n, all_valid)
    return out


# ----------------------------------------------------------------------
# Public API (unchanged)
# ----------------------------------------------------------------------
cdef inline void _extract_const_needle(
    StringVector sv,
    const uint8_t** needle_out,
    Py_ssize_t* needle_len_out,
    const char* fn_name,
) except *:
    cdef DrakenVector* uv = sv.unified()
    cdef DrakenConstantStringPayload* csp
    if uv.data_length != 1:  # not constant
        raise ValueError(f"{fn_name} does not support non-constant needle")
    if uv.validity != NULL:  # null constant
        needle_out[0] = NULL
        needle_len_out[0] = 0
        return
    csp = <DrakenConstantStringPayload*>uv.data
    needle_out[0] = <const uint8_t*>csp.data
    needle_len_out[0] = <Py_ssize_t>csp.length


cpdef BoolVector vector_starts_with(StringVector vec, StringVector prefix):
    cdef Py_ssize_t needle_len
    cdef const uint8_t* needle
    _extract_const_needle(prefix, &needle, &needle_len, b"vector_starts_with")

    cdef DrakenVector* uv = vec.unified()
    if uv.selection == NULL and vec.ptr.offsets == NULL:  # constant
        return _constant_starts_ends(vec, needle, needle_len, False, False, False)
    if uv.selection != NULL:  # dictionary
        return _dictionary_starts_ends(vec, needle, needle_len, False, False, False)
    return _dense_starts_ends(vec, needle, needle_len, False, False, False)


cpdef BoolVector vector_ci_starts_with(StringVector vec, StringVector prefix):
    cdef Py_ssize_t needle_len
    cdef const uint8_t* needle
    _extract_const_needle(prefix, &needle, &needle_len, b"vector_ci_starts_with")

    cdef DrakenVector* uv = vec.unified()
    if uv.selection == NULL and vec.ptr.offsets == NULL:  # constant
        return _constant_starts_ends(vec, needle, needle_len, True, False, False)
    if uv.selection != NULL:  # dictionary
        return _dictionary_starts_ends(vec, needle, needle_len, True, False, False)
    return _dense_starts_ends(vec, needle, needle_len, True, False, False)


cpdef BoolVector vector_ends_with(StringVector vec, StringVector suffix):
    cdef Py_ssize_t needle_len
    cdef const uint8_t* needle
    _extract_const_needle(suffix, &needle, &needle_len, b"vector_ends_with")

    cdef DrakenVector* uv = vec.unified()
    if uv.selection == NULL and vec.ptr.offsets == NULL:  # constant
        return _constant_starts_ends(vec, needle, needle_len, False, False, True)
    if uv.selection != NULL:  # dictionary
        return _dictionary_starts_ends(vec, needle, needle_len, False, False, True)
    return _dense_starts_ends(vec, needle, needle_len, False, False, True)


cpdef BoolVector vector_ci_ends_with(StringVector vec, StringVector suffix):
    cdef Py_ssize_t needle_len
    cdef const uint8_t* needle
    _extract_const_needle(suffix, &needle, &needle_len, b"vector_ci_ends_with")

    cdef DrakenVector* uv = vec.unified()
    if uv.selection == NULL and vec.ptr.offsets == NULL:  # constant
        return _constant_starts_ends(vec, needle, needle_len, True, False, True)
    if uv.selection != NULL:  # dictionary
        return _dictionary_starts_ends(vec, needle, needle_len, True, False, True)
    return _dense_starts_ends(vec, needle, needle_len, True, False, True)
