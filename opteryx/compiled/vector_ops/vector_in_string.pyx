# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

# Define strncasecmp as _strnicmp on Windows
cdef extern from *:
    """
    #ifdef _WIN32
    #define strncasecmp _strnicmp
    #else
    #include <ctype.h>
    #endif

    // Fast case-insensitive character comparison for ASCII
    static inline int fast_case_insensitive_eq(char a, char b) {
        if (a == b) return 1;
        // Only flip case for alphabetical characters
        if ((a >= 'a' && a <= 'z') && (b >= 'A' && b <= 'Z'))
            return (a - 32) == b;
        if ((a >= 'A' && a <= 'Z') && (b >= 'a' && b <= 'z'))
            return (a + 32) == b;
        return 0;
    }
    """

import numpy
cimport numpy
numpy.import_array()

from cpython.bytes cimport PyBytes_AsString
from libc.stdint cimport int32_t, uint8_t, uint64_t
from libc.string cimport memchr, memcpy, memset
import platform

from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.core.buffers cimport DrakenVarBuffer


cdef extern from "string.h":
    int strncasecmp(const char *s1, const char *s2, size_t n)
    int memcmp(const void *s1, const void *s2, size_t n)

cdef extern from "simd_search.h":
    int neon_search(const char *data, size_t length, char target)
    int avx_search(const char *data, size_t length, char target)
    int simd_search_substring(const char *data, size_t length, const char *pattern, size_t pattern_len)

ctypedef int (*search_func_t)(const char*, size_t, char)
cdef search_func_t searcher


# This function sets the searcher based on the CPU architecture.
def init_searcher():
    global searcher
    cdef str arch = platform.machine().lower()
    if arch.startswith("arm") or arch.startswith("aarch64") or arch.startswith("arm64"):
        searcher = neon_search
    else:
        searcher = avx_search


# Initialize the searcher once when the module is imported.
init_searcher()


cdef inline int fast_memcmp_short(const char *a, const char *b, size_t n):
    cdef uint64_t aval = 0, bval = 0
    cdef uint64_t mask

    if n == 0:
        return 0
    elif n <= 8:
        mask = ((<uint64_t>1) << (8 * n)) - 1
        memcpy(&aval, a, n)
        memcpy(&bval, b, n)
        return (aval & mask) != (bval & mask)
    else:
        return memcmp(a, b, n) != 0


cdef inline int boyer_moore_horspool(const char *haystack, size_t haystacklen,
                                     const char *needle, size_t needlelen):
    """
    Optimized case-sensitive Boyer-Moore-Horspool substring search.
    """
    cdef unsigned char skip[256]
    cdef size_t i, tail_index
    cdef unsigned char last_char

    if needlelen == 0 or haystacklen < needlelen:
        return 0

    # Fast path for single character
    if needlelen == 1:
        return memchr(haystack, needle[0], haystacklen) != NULL

    # Initialize skip table - optimized loop
    for i in range(256):
        skip[i] = needlelen

    # Populate skip table - unroll small loops
    if needlelen == 2:
        skip[<unsigned char>needle[0]] = 1
    elif needlelen == 3:
        skip[<unsigned char>needle[0]] = 2
        skip[<unsigned char>needle[1]] = 1
    else:
        for i in range(needlelen - 1):
            skip[<unsigned char>needle[i]] = needlelen - i - 1

    i = 0
    last_char = <unsigned char>needle[needlelen - 1]

    while i <= haystacklen - needlelen:
        # Check last character first for better branch prediction
        if haystack[i + needlelen - 1] == last_char:
            # Use optimized comparison for short needles
            if needlelen <= 8:
                if fast_memcmp_short(&haystack[i], needle, needlelen) == 0:
                    return 1
            else:
                if memcmp(&haystack[i], needle, needlelen) == 0:
                    return 1

        # Use precomputed skip
        tail_index = i + needlelen - 1
        i += skip[<unsigned char>haystack[tail_index]]

    return 0


cdef inline void build_bmh_skip_table(const char *needle, size_t needlelen, unsigned char *skip):
    """Optimized skip table builder"""
    cdef size_t i
    for i in range(256):
        skip[i] = needlelen

    if needlelen == 2:
        skip[<unsigned char>needle[0]] = 1
    elif needlelen == 3:
        skip[<unsigned char>needle[0]] = 2
        skip[<unsigned char>needle[1]] = 1
    else:
        for i in range(needlelen - 1):
            skip[<unsigned char>needle[i]] = needlelen - i - 1


cdef inline int boyer_moore_horspool_with_table(const char *haystack, size_t haystacklen,
                                                const char *needle, size_t needlelen,
                                                unsigned char *skip):
    """BMH with precomputed table - optimized version"""
    cdef size_t i = 0
    cdef size_t tail_index
    cdef size_t needlelen_sub1 = needlelen - 1
    cdef unsigned char last_char
    cdef unsigned char tail_char

    if needlelen == 0 or haystacklen < needlelen:
        return 0

    # Fast path for single character
    if needlelen == 1:
        return memchr(haystack, needle[0], haystacklen) != NULL

    last_char = <unsigned char>needle[needlelen_sub1]
    cdef size_t end_index = haystacklen - needlelen

    while i <= end_index:
        tail_index = i + needlelen_sub1
        tail_char = <unsigned char>haystack[tail_index]

        # Check last character first
        if tail_char == last_char:
            if haystack[i] == needle[0]:
                if needlelen <= 8:
                    if fast_memcmp_short(&haystack[i], needle, needlelen) == 0:
                        return 1
                else:
                    if memcmp(&haystack[i], needle, needlelen) == 0:
                        return 1

        i += skip[tail_char]

    return 0


cdef extern from *:
    int fast_case_insensitive_eq(char a, char b)


cdef inline int boyer_moore_horspool_case_insensitive(const char *haystack, size_t haystacklen,
                                                      const char *needle, size_t needlelen):
    """
    Optimized case-insensitive Boyer-Moore-Horspool with better ASCII handling.
    """
    cdef unsigned char skip[256]
    cdef size_t i, j, pos
    cdef char nc

    if needlelen == 0 or haystacklen < needlelen:
        return 0

    # Fast path for single character
    if needlelen == 1:
        nc = needle[0]
        for i in range(haystacklen):
            if fast_case_insensitive_eq(haystack[i], nc):
                return 1
        return 0

    # Initialize skip table
    for i in range(256):
        skip[i] = needlelen

    # Build skip table with case-insensitive consideration
    for i in range(needlelen - 1):
        nc = needle[i]
        skip[<unsigned char>nc] = needlelen - i - 1
        # Add case variants to skip table
        if nc >= 'a' and nc <= 'z':
            skip[<unsigned char>(nc - 32)] = needlelen - i - 1
        elif nc >= 'A' and nc <= 'Z':
            skip[<unsigned char>(nc + 32)] = needlelen - i - 1

    i = 0
    while i <= haystacklen - needlelen:
        pos = i + needlelen - 1

        # Check if last character matches (case-insensitive)
        if fast_case_insensitive_eq(haystack[pos], needle[needlelen - 1]):
            # Check remaining characters
            j = needlelen - 1
            while j > 0:
                if not fast_case_insensitive_eq(haystack[i + j - 1], needle[j - 1]):
                    break
                j -= 1

            if j == 0:
                return 1

        i += skip[<unsigned char>haystack[pos]]

    return 0


cdef inline void _search_draken_string_vec(
        DrakenVarBuffer* ptr,
        const char* c_pattern,
        size_t pattern_length,
        unsigned char* skip_table,
        uint8_t* dst,
        Py_ssize_t n):
    """
    Case-sensitive BMH search over every string in a DrakenVarBuffer,
    writing per-row result bits into dst (which must be pre-zeroed).
    """
    cdef:
        uint8_t* nulls = ptr.null_bitmap
        const char* data = <const char*>ptr.data
        int32_t start, end
        size_t length
        Py_ssize_t i
        int index
        char target

    if pattern_length == 1:
        target = c_pattern[0]
        for i in range(n):
            if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
                continue
            start = ptr.offsets[i]
            end = ptr.offsets[i + 1]
            length = <size_t>(end - start)
            if length > 0 and searcher(data + start, length, target) != -1:
                dst[i >> 3] |= (1 << (i & 7))
        return

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            continue
        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        length = <size_t>(end - start)
        if length < pattern_length:
            continue

        if length <= 16 and pattern_length <= 4:
            if boyer_moore_horspool_with_table(
                data + start, length, c_pattern, pattern_length, skip_table
            ):
                dst[i >> 3] |= (1 << (i & 7))
            continue

        if pattern_length >= 8 and length >= 64:
            if simd_search_substring(data + start, length, c_pattern, pattern_length) != -1:
                dst[i >> 3] |= (1 << (i & 7))
                continue

        index = searcher(data + start, length, c_pattern[0])
        if index == -1:
            continue
        if boyer_moore_horspool_with_table(
            data + start + index, length - index, c_pattern, pattern_length, skip_table
        ):
            dst[i >> 3] |= (1 << (i & 7))


cdef inline void _search_draken_string_vec_case_insensitive(
        DrakenVarBuffer* ptr,
        const char* c_pattern,
        size_t pattern_length,
        uint8_t* dst,
        Py_ssize_t n):
    """
    Case-insensitive BMH search over every string in a DrakenVarBuffer,
    writing per-row result bits into dst (which must be pre-zeroed).
    """
    cdef:
        uint8_t* nulls = ptr.null_bitmap
        const char* data = <const char*>ptr.data
        int32_t start, end
        size_t length
        Py_ssize_t i

    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            continue
        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        length = <size_t>(end - start)
        if length < pattern_length:
            continue
        if boyer_moore_horspool_case_insensitive(
            data + start, length, c_pattern, pattern_length
        ):
            dst[i >> 3] |= (1 << (i & 7))


cpdef BoolVector vector_in_string(StringVector vec, str needle):
    """
    Case-sensitive substring scan over a Draken StringVector.

    Returns a BoolVector: True at position i iff needle appears in vec[i].
    Null input rows produce False (not null) in the output.
    """
    cdef:
        DrakenVarBuffer* ptr = vec.ptr
        Py_ssize_t n = ptr.length
        Py_ssize_t nbytes = (n + 7) >> 3
        BoolVector out = BoolVector(<size_t>n)
        uint8_t* dst = <uint8_t*>out.ptr.data
        bytes needle_bytes
        const char* c_pattern
        size_t pattern_length
        unsigned char skip_table[256]

    memset(dst, 0, nbytes)

    needle_bytes = needle.encode('utf-8')
    c_pattern = PyBytes_AsString(needle_bytes)
    pattern_length = len(needle_bytes)

    if pattern_length == 0 or n == 0:
        return out

    build_bmh_skip_table(c_pattern, pattern_length, skip_table)
    _search_draken_string_vec(ptr, c_pattern, pattern_length, skip_table, dst, n)
    return out


cpdef BoolVector vector_in_string_case_insensitive(StringVector vec, str needle):
    """
    Case-insensitive substring scan over a Draken StringVector.

    Returns a BoolVector: True at position i iff needle appears (case-insensitively)
    in vec[i].  Null input rows produce False in the output.
    """
    cdef:
        DrakenVarBuffer* ptr = vec.ptr
        Py_ssize_t n = ptr.length
        Py_ssize_t nbytes = (n + 7) >> 3
        BoolVector out = BoolVector(<size_t>n)
        uint8_t* dst = <uint8_t*>out.ptr.data
        bytes needle_bytes
        const char* c_pattern
        size_t pattern_length

    memset(dst, 0, nbytes)

    needle_bytes = needle.encode('utf-8')
    c_pattern = PyBytes_AsString(needle_bytes)
    pattern_length = len(needle_bytes)

    if pattern_length == 0 or n == 0:
        return out

    _search_draken_string_vec_case_insensitive(ptr, c_pattern, pattern_length, dst, n)
    return out
