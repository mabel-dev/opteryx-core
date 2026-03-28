# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.stdint cimport int32_t
from cpython.bytes cimport PyBytes_AsStringAndSize

from libcpp.string cimport string
from libcpp.unordered_map cimport unordered_map

from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors import string_vector as string_vector_module
from opteryx.compiled.draken.core.buffers cimport DrakenVarBuffer

cdef extern from "re2/stringpiece.h" namespace "re2":
    cdef cppclass StringPiece:
        StringPiece() except +
        StringPiece(const string& other) except +
        StringPiece(const char* data, size_t length) except +

cdef extern from "re2/re2.h" namespace "re2":
    cdef cppclass RE2:
        RE2(const string& pattern) except +
        bint ok() const
        const string& error() const

        @staticmethod
        int GlobalReplace(string* target, const RE2& re, const StringPiece& rewrite)

# Module-level cache: pattern bytes → compiled RE2 pointer (never freed; process lifetime).
cdef unordered_map[string, RE2*] _re2_cache


cpdef StringVector vector_regex_replace(StringVector data, bytes pattern, bytes replacement):
    """Draken-native regex replacement using RE2 engine.

    Accepts and returns StringVector directly, eliminating Python/Arrow conversion overhead.

    Uses direct access to DrakenVarBuffer for maximum performance:
    - No Python object creation per element
    - Direct pointer access to Draken internal buffers
    - Zero-copy append via append_bytes(ptr, length)
    - Pattern caching across calls
    """
    cdef DrakenVarBuffer* ptr = data.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end

    # Compile RE2 pattern once
    cdef char* pattern_buf = <char*>0
    cdef char* repl_buf = <char*>0
    cdef Py_ssize_t pattern_len = 0
    cdef Py_ssize_t repl_len = 0
    cdef RE2* regex
    cdef StringPiece repl_piece
    cdef string pattern_str
    cdef string repl_str
    cdef string value_str
    cdef size_t buffer_capacity = 256
    cdef object builder

    # Extract pattern and replacement buffers
    PyBytes_AsStringAndSize(pattern, &pattern_buf, &pattern_len)
    PyBytes_AsStringAndSize(replacement, &repl_buf, &repl_len)

    pattern_str = string(pattern_buf, <size_t>pattern_len)
    repl_str = string(repl_buf, <size_t>repl_len)

    # Look up compiled pattern in module-level cache; compile once on first use.
    if _re2_cache.count(pattern_str) == 0:
        regex = new RE2(pattern_str)
        if not regex.ok():
            del regex
            raise ValueError("Invalid regular expression")
        _re2_cache[pattern_str] = regex
    regex = _re2_cache[pattern_str]

    repl_piece = StringPiece(repl_str)

    # Create builder with estimated capacity
    cdef Py_ssize_t estimated_bytes_per_entry = 100
    builder = string_vector_module.StringVectorBuilder.with_estimate(n, estimated_bytes_per_entry)

    # Pre-allocate string buffer
    value_str.reserve(256)

    # Process all elements directly from buffer
    for i in range(n):
        if ptr.null_bitmap != NULL and not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]

        # Reuse string buffer - reallocate if needed, doubling capacity
        if buffer_capacity < (end - start):
            buffer_capacity = <size_t>(end - start) * 2
            value_str.reserve(buffer_capacity)
        value_str.assign((<const char*>ptr.data) + start, <size_t>(end - start))

        # RE2 performs in-place replacement
        RE2.GlobalReplace(&value_str, regex[0], repl_piece)

        # Append directly from C++ string to Draken builder (zero-copy from builder's perspective)
        builder.append_bytes(value_str.c_str(), value_str.size())

    return builder.finish()
