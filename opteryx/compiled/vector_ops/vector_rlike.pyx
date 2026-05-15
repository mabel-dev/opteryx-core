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
from draken.core.buffers cimport DRAKEN_ENCODING_DICTIONARY
from cpython.bytes cimport PyBytes_AsStringAndSize
from libc.string cimport memset, memcpy
from libc.stdlib cimport malloc, free
from libcpp.string cimport string


cdef extern from "re2/stringpiece.h":
    cdef cppclass StringPieceRL "re2::StringPiece":
        StringPieceRL() except +
        StringPieceRL(const char* data, size_t length) except +


cdef extern from "re2/re2.h":
    cdef cppclass RE2OptionsRL "re2::RE2::Options":
        RE2OptionsRL() except +
        void set_case_sensitive(bint)
        void set_log_errors(bint)

    cdef cppclass RE2RL "re2::RE2":
        RE2RL(const string& pattern) except +
        RE2RL(const string& pattern, const RE2OptionsRL& options) except +
        bint ok() const
        @staticmethod
        bint PartialMatch(const StringPieceRL& text, const RE2RL& re)


cpdef BoolVector vector_rlike(StringVector vec, bytes pattern, bint negate=False):
    """Return mask: 1 if element matches regex pattern, else 0. Propagates NULLs.

    Optimized for dictionary-encoded vectors: tests each unique value once.

    If `negate` is True, returns the row-wise NotRLike: True where the
    element does NOT match the pattern. Fuses what would otherwise be a
    second full-pass `.not_vector()`.

    Regex engine: RE2 (C++). RE2 is a subset of PCRE — no backreferences and
    no lookaround. Matches the engine used by vector_anyop_like and the same
    engine ClickHouse, BigQuery, and DuckDB use for REGEXP_LIKE.
    """
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef uint8_t* nb_ptr = ptr.null_bitmap
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*> out.ptr.data
    cdef uint8_t* out_null = NULL
    cdef uint8_t mask
    cdef int32_t start, end, str_len
    cdef Py_ssize_t i, dict_idx, dict_size
    cdef uint32_t code
    cdef DrakenVarBuffer* dict_values_buf
    cdef const uint8_t* dict_data
    cdef uint8_t* dict_rlike_results = NULL
    cdef const uint8_t* dict_codes
    cdef uint8_t dict_code_width
    cdef uint8_t* dict_row_nulls
    cdef char* pat_buf = <char*>0
    cdef Py_ssize_t pat_len = 0
    cdef RE2OptionsRL options
    cdef RE2RL* regex = NULL
    cdef StringPieceRL text_piece

    options = RE2OptionsRL()
    options.set_case_sensitive(True)
    options.set_log_errors(False)
    PyBytes_AsStringAndSize(pattern, &pat_buf, &pat_len)
    regex = new RE2RL(string(pat_buf, <size_t>pat_len), options)

    try:
        if not regex.ok():
            raise ValueError("Invalid REGEXP pattern")

        if vec._has_const:
            if vec._const_is_null:
                return _constant_bool_result(n, False, True)
            text_piece = StringPieceRL(
                <const char*>vec._const_value.data,
                <size_t>vec._const_value.length,
            )
            return _constant_bool_result(
                n,
                RE2RL.PartialMatch(text_piece, regex[0]) != negate,
                False,
            )

        if negate:
            memset(dst, 0xFF, nbytes)
            if (n & 7) != 0:
                dst[nbytes - 1] &= <uint8_t>((1 << (n & 7)) - 1)
        else:
            memset(dst, 0, nbytes)
        if nb_ptr != NULL and nbytes != 0:
            out_null = <uint8_t*> malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, nb_ptr, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        try:
            # Dictionary-encoded path: check each unique value once
            if vec._encoding == DRAKEN_ENCODING_DICTIONARY:
                dict_values_buf = vec._dict_values
                if dict_values_buf == NULL or dict_values_buf.data == NULL:
                    return out  # Fallback to empty result

                dict_size = <Py_ssize_t>dict_values_buf.length
                dict_codes = vec._dict_codes
                if dict_codes == NULL or dict_size == 0:
                    return out  # Fallback to empty result

                dict_code_width = vec._dict_code_width
                dict_row_nulls = vec.ptr.null_bitmap
                dict_data = <const uint8_t*>dict_values_buf.data

                # Allocate results array for each dictionary entry
                dict_rlike_results = <uint8_t*>malloc(dict_size)
                if dict_rlike_results == NULL:
                    raise MemoryError()

                # Test each unique dictionary value once
                for dict_idx in range(dict_size):
                    start = dict_values_buf.offsets[dict_idx]
                    end = dict_values_buf.offsets[dict_idx + 1]
                    str_len = end - start
                    text_piece = StringPieceRL(
                        <const char*>dict_data + start, <size_t>str_len,
                    )
                    if RE2RL.PartialMatch(text_piece, regex[0]):
                        dict_rlike_results[dict_idx] = 1
                    else:
                        dict_rlike_results[dict_idx] = 0

                # Scatter results by code index. Under `negate`, matches clear
                # bits from an all-ones init; otherwise they set bits from zero.
                if negate:
                    for i in range(n):
                        if dict_row_nulls != NULL and ((dict_row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
                            continue
                        code = _read_packed_code(dict_codes, dict_code_width, i)
                        if dict_rlike_results[code]:
                            dst[i >> 3] &= ~(1 << (i & 7))
                else:
                    for i in range(n):
                        if dict_row_nulls != NULL and ((dict_row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
                            continue
                        code = _read_packed_code(dict_codes, dict_code_width, i)
                        if dict_rlike_results[code]:
                            dst[i >> 3] |= (1 << (i & 7))

            # Dense vector path (non-dictionary, non-constant)
            else:
                if negate:
                    for i in range(n):
                        if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                            continue
                        start = ptr.offsets[i]
                        end = ptr.offsets[i + 1]
                        str_len = end - start
                        text_piece = StringPieceRL(
                            <const char*>ptr.data + start, <size_t>str_len,
                        )
                        if RE2RL.PartialMatch(text_piece, regex[0]):
                            dst[i >> 3] &= ~(1 << (i & 7))
                else:
                    for i in range(n):
                        if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                            continue
                        start = ptr.offsets[i]
                        end = ptr.offsets[i + 1]
                        str_len = end - start
                        text_piece = StringPieceRL(
                            <const char*>ptr.data + start, <size_t>str_len,
                        )
                        if RE2RL.PartialMatch(text_piece, regex[0]):
                            dst[i >> 3] |= (1 << (i & 7))
        finally:
            if dict_rlike_results != NULL:
                free(dict_rlike_results)

        return out
    finally:
        if regex != NULL:
            del regex
