# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from libc.stddef cimport size_t
from libc.stdint cimport int32_t
from cpython.bytes cimport PyBytes_AsStringAndSize

from libcpp.string cimport string
from libcpp.unordered_map cimport unordered_map

from draken.vectors.string_vector cimport StringVector
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport DrakenVarBuffer, DrakenVector, DrakenStringArena, DrakenStringSlot, str_length, str_data

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
    """
    cdef DrakenVector* uv = data.unified()
    cdef DrakenStringArena* arena = <DrakenStringArena*>uv.data
    cdef uint32_t* sel = <uint32_t*>uv.selection
    cdef uint8_t* nulls = uv.validity
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i

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
    cdef DrakenStringSlot* slot
    cdef const uint8_t* slot_data
    cdef Py_ssize_t slot_len

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

    repl_piece = StringPiece(repl_buf, <size_t>repl_len)
    value_str.reserve(256)

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 100)
    for i in range(n):
        if nulls != NULL and not ((nulls[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        slot = &arena.slots[sel[i]]
        slot_data = str_data(slot, arena.arena)
        slot_len = <Py_ssize_t>str_length(slot)
        if buffer_capacity < <size_t>slot_len:
            buffer_capacity = <size_t>slot_len * 2
            value_str.reserve(buffer_capacity)
        value_str.assign(<const char*>slot_data, <size_t>slot_len)
        RE2.GlobalReplace(&value_str, regex[0], repl_piece)
        builder.append_bytes(value_str.c_str(), value_str.size())

    return builder.finish()
