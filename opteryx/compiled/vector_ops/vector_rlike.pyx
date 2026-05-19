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
from draken.core.buffers cimport DrakenVector, DrakenConstantStringPayload, DrakenGermanArena, GermanString
from draken.core.buffers cimport gs_length, gs_data
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
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*> out.ptr.data
    cdef uint8_t* out_null = NULL
    cdef uint8_t mask
    cdef int32_t start, end, str_len
    cdef Py_ssize_t i, dict_idx, dict_size
    cdef uint32_t code
    cdef DrakenVarBuffer* vbuf
    cdef DrakenGermanArena* rl_gdict
    cdef GermanString* rl_slot
    cdef const uint8_t* rl_sdata
    cdef uint32_t rl_slen
    cdef const uint8_t* dict_data
    cdef uint8_t* dict_rlike_results = NULL
    cdef uint8_t* nb_ptr
    cdef DrakenConstantStringPayload* csp
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

        if vec.ptr.offsets == NULL and vec._german_dict_values == NULL:  # constant
            if uv.validity != NULL:  # null constant
                return _constant_bool_result(n, False, True)
            csp = <DrakenConstantStringPayload*>uv.data
            text_piece = StringPieceRL(
                <const char*>csp.data,
                <size_t>csp.length,
            )
            return _constant_bool_result(
                n,
                RE2RL.PartialMatch(text_piece, regex[0]) != negate,
                False,
            )

        nb_ptr = uv.validity
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
            if vec._german_dict_values != NULL:  # dictionary
                rl_gdict = <DrakenGermanArena*>uv.data
                if rl_gdict == NULL:
                    return out

                dict_size = <Py_ssize_t>rl_gdict.length
                if dict_size == 0:
                    return out

                # Allocate results array for each dictionary entry
                dict_rlike_results = <uint8_t*>malloc(dict_size)
                if dict_rlike_results == NULL:
                    raise MemoryError()

                # Test each unique dictionary value once
                for dict_idx in range(dict_size):
                    rl_slot = &rl_gdict.slots[dict_idx]
                    rl_slen = gs_length(rl_slot)
                    rl_sdata = gs_data(rl_slot, rl_gdict.arena)
                    text_piece = StringPieceRL(
                        <const char*>rl_sdata, <size_t>rl_slen,
                    )
                    if RE2RL.PartialMatch(text_piece, regex[0]):
                        dict_rlike_results[dict_idx] = 1
                    else:
                        dict_rlike_results[dict_idx] = 0

                # Scatter results by code index
                if negate:
                    for i in range(n):
                        if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                            continue
                        code = uv.selection[i]
                        if dict_rlike_results[code]:
                            dst[i >> 3] &= ~(1 << (i & 7))
                else:
                    for i in range(n):
                        if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                            continue
                        code = uv.selection[i]
                        if dict_rlike_results[code]:
                            dst[i >> 3] |= (1 << (i & 7))

            # Dense vector path
            else:
                vbuf = <DrakenVarBuffer*>uv.data
                if negate:
                    for i in range(n):
                        if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                            continue
                        start = vbuf.offsets[i]
                        end = vbuf.offsets[i + 1]
                        str_len = end - start
                        text_piece = StringPieceRL(
                            <const char*>vbuf.data + start, <size_t>str_len,
                        )
                        if RE2RL.PartialMatch(text_piece, regex[0]):
                            dst[i >> 3] &= ~(1 << (i & 7))
                else:
                    for i in range(n):
                        if nb_ptr != NULL and ((nb_ptr[i >> 3] >> (i & 7)) & 1) == 0:
                            continue
                        start = vbuf.offsets[i]
                        end = vbuf.offsets[i + 1]
                        str_len = end - start
                        text_piece = StringPieceRL(
                            <const char*>vbuf.data + start, <size_t>str_len,
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
