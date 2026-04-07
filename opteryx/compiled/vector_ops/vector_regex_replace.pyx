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

from opteryx.compiled.draken.vectors.string_vector cimport StringVector, from_packed_dict
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
    Handles constant, dictionary, and dense encodings correctly.
    """
    cdef Py_ssize_t n = data.ptr.length
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
    cdef DrakenVarBuffer* ptr
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp

    PyBytes_AsStringAndSize(pattern, &pattern_buf, &pattern_len)
    PyBytes_AsStringAndSize(replacement, &repl_buf, &repl_len)

    pattern_str = string(pattern_buf, <size_t>pattern_len)
    repl_str = string(repl_buf, <size_t>repl_len)

    if _re2_cache.count(pattern_str) == 0:
        regex = new RE2(pattern_str)
        if not regex.ok():
            del regex
            raise ValueError("Invalid regular expression")
        _re2_cache[pattern_str] = regex
    regex = _re2_cache[pattern_str]

    repl_piece = StringPiece(repl_str)
    value_str.reserve(256)

    # Constant encoding: apply once, replicate
    if data._has_const:
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 100)
        if data._const_is_null or data._const_value == NULL:
            for i in range(n):
                builder.append_null()
        else:
            value_str.assign(<const char*>data._const_value.data, <size_t>data._const_value.length)
            RE2.GlobalReplace(&value_str, regex[0], repl_piece)
            for i in range(n):
                builder.append_bytes(value_str.c_str(), value_str.size())
        return builder.finish()

    # Dictionary encoding: transform each unique entry, repack with same codes
    if data._encoding == DRAKEN_ENCODING_DICTIONARY:
        dict_size = data._dict_values.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 100)
        for i in range(dict_size):
            start = data._dict_values.offsets[i]
            end = data._dict_values.offsets[i + 1]
            if buffer_capacity < <size_t>(end - start):
                buffer_capacity = <size_t>(end - start) * 2
                value_str.reserve(buffer_capacity)
            value_str.assign((<const char*>data._dict_values.data) + start, <size_t>(end - start))
            RE2.GlobalReplace(&value_str, regex[0], repl_piece)
            dict_builder.append_bytes(value_str.c_str(), value_str.size())
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            data._dict_codes, data._dict_code_width, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            data._dict_accessor.row_nulls,
        )

    # Dense encoding: row by row
    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 100)
    ptr = data.ptr
    for i in range(n):
        if ptr.null_bitmap != NULL and not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue

        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]

        if buffer_capacity < <size_t>(end - start):
            buffer_capacity = <size_t>(end - start) * 2
            value_str.reserve(buffer_capacity)
        value_str.assign((<const char*>ptr.data) + start, <size_t>(end - start))
        RE2.GlobalReplace(&value_str, regex[0], repl_piece)
        builder.append_bytes(value_str.c_str(), value_str.size())

    return builder.finish()
