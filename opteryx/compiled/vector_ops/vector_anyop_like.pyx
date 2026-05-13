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
from libc.stdint cimport uint8_t
from libc.string cimport memset, memcpy
from libc.stdlib cimport malloc
from cpython.bytes cimport PyBytes_AsStringAndSize, PyBytes_FromStringAndSize
from libcpp.string cimport string
from libcpp.vector cimport vector

from draken.core.buffers cimport DRAKEN_STRING, DrakenArrayBuffer, DrakenVarBuffer
from draken.vectors.array_vector cimport ArrayVector
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.string_vector cimport StringVector


cdef extern from "re2/stringpiece.h":
    cdef cppclass StringPieceAny "re2::StringPiece":
        StringPieceAny() except +
        StringPieceAny(const char* data, size_t length) except +


cdef extern from "re2/re2.h":
    cdef cppclass RE2OptionsAny "re2::RE2::Options":
        RE2OptionsAny() except +
        void set_case_sensitive(bint)
        void set_log_errors(bint)

    cdef cppclass RE2Any "re2::RE2":
        RE2Any(const string& pattern) except +
        RE2Any(const string& pattern, const RE2OptionsAny& options) except +
        bint ok() const
        @staticmethod
        bint PartialMatch(const StringPieceAny& text, const RE2Any& re)


cdef inline bint _bit_is_set(const uint8_t* bitmap, Py_ssize_t idx) noexcept nogil:
    return bitmap == NULL or ((bitmap[idx >> 3] >> (idx & 7)) & 1) != 0


cdef inline void _release_regex_vector(vector[RE2Any*]& compiled) noexcept:
    cdef Py_ssize_t k
    for k in range(<Py_ssize_t>compiled.size()):
        if compiled[k] != NULL:
            del compiled[k]
    compiled.clear()


cdef BoolVector _regex_match_any_literal(ArrayVector arr, object patterns, int flags=0, bint invert=False):
    """
    Draken-native SQL LIKE ANY matcher for ArrayVector(StringVector child) with literal patterns.
    """
    from opteryx.utils.sql import sql_like_to_regex

    cdef StringVector child
    cdef DrakenArrayBuffer* aptr = arr.ptr
    cdef DrakenVarBuffer* cptr
    cdef object pattern_src, p, p_str, regex_text
    cdef Py_ssize_t i, j, k, n, nbytes, row_start, row_end, start, end, text_len
    cdef BoolVector out
    cdef uint8_t* dst
    cdef uint8_t* out_null = NULL
    cdef uint8_t* row_nulls
    cdef uint8_t* child_nulls
    cdef uint8_t mask
    cdef bint matched
    cdef bint ignore_case = (flags & 2) != 0
    cdef char* pat_buf = <char*>0
    cdef Py_ssize_t pat_len = 0
    cdef bytes regex_bytes
    cdef StringPieceAny text_piece
    cdef RE2Any* regex
    cdef RE2OptionsAny options
    cdef vector[RE2Any*] compiled_patterns

    if aptr == NULL:
        raise ValueError("ArrayVector is not initialized")
    if aptr.value_type != DRAKEN_STRING or not isinstance(arr._child, StringVector):
        raise TypeError("regex_match_any expects ArrayVector with StringVector child")
    child = <StringVector>arr._child
    cptr = child.ptr
    if cptr == NULL:
        raise ValueError("ArrayVector child StringVector is not initialized")

    options = RE2OptionsAny()
    options.set_case_sensitive(not ignore_case)
    options.set_log_errors(False)

    try:
        if patterns is None:
            n = <Py_ssize_t>aptr.length
            nbytes = (n + 7) >> 3
            out = BoolVector(<size_t>n)
            dst = <uint8_t*>out.ptr.data
            if nbytes != 0:
                memset(dst, 0, nbytes)
            return out

        if isinstance(patterns, (str, bytes)):
            patterns = [patterns]

        for p in patterns:
            if p is None:
                continue
            p_str = p.decode("utf-8") if isinstance(p, bytes) else str(p)
            regex_text = sql_like_to_regex(p_str)
            regex_bytes = (<str>regex_text).encode("utf-8")
            PyBytes_AsStringAndSize(regex_bytes, &pat_buf, &pat_len)
            regex = new RE2Any(string(pat_buf, <size_t>pat_len), options)
            if not regex.ok():
                del regex
                raise ValueError("Invalid LIKE pattern")
            compiled_patterns.push_back(regex)

        n = <Py_ssize_t>aptr.length
        nbytes = (n + 7) >> 3
        out = BoolVector(<size_t>n)
        dst = <uint8_t*>out.ptr.data
        if nbytes != 0:
            memset(dst, 0, nbytes)

        row_nulls = aptr.null_bitmap
        child_nulls = cptr.null_bitmap

        if row_nulls != NULL and nbytes != 0:
            out_null = <uint8_t*>malloc(nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, row_nulls, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask
            out.ptr.null_bitmap = out_null
        else:
            out.ptr.null_bitmap = NULL

        for i in range(n):
            if row_nulls != NULL and ((row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
                continue

            matched = False
            row_start = aptr.offsets[i]
            row_end = aptr.offsets[i + 1]

            for j in range(row_start, row_end):
                if child_nulls != NULL and ((child_nulls[j >> 3] >> (j & 7)) & 1) == 0:
                    continue

                start = cptr.offsets[j]
                end = cptr.offsets[j + 1]
                text_len = end - start
                text_piece = StringPieceAny(<const char*>cptr.data + start, <size_t>text_len)

                for k in range(<Py_ssize_t>compiled_patterns.size()):
                    if RE2Any.PartialMatch(text_piece, compiled_patterns[k][0]):
                        matched = True
                        break
                if matched:
                    break

            if invert:
                matched = not matched
            if matched:
                dst[i >> 3] |= (<uint8_t>1 << (i & 7))
        return out
    finally:
        _release_regex_vector(compiled_patterns)


cdef BoolVector _regex_match_any_array_array(ArrayVector arr, ArrayVector patterns, int flags=0, bint invert=False):
    """
    Draken-native row-wise SQL LIKE ANY matcher:
      arr[i] (array<string>) LIKE ANY(patterns[i] (array<string>))
    """
    from opteryx.utils.sql import sql_like_to_regex

    cdef DrakenArrayBuffer* aptr = arr.ptr
    cdef DrakenArrayBuffer* p_aptr = patterns.ptr
    cdef StringVector child
    cdef StringVector p_child
    cdef DrakenVarBuffer* cptr
    cdef DrakenVarBuffer* p_cptr
    cdef Py_ssize_t i, j, pj, k, n, nbytes
    cdef Py_ssize_t row_start, row_end, p_row_start, p_row_end
    cdef Py_ssize_t start, end, p_start, p_end, text_len, pat_text_len
    cdef BoolVector out
    cdef uint8_t* dst
    cdef uint8_t* out_null = NULL
    cdef uint8_t* row_nulls
    cdef uint8_t* pattern_row_nulls
    cdef uint8_t* child_nulls
    cdef uint8_t* p_child_nulls
    cdef uint8_t mask
    cdef bint matched
    cdef bint ignore_case = (flags & 2) != 0
    cdef char* pat_buf = <char*>0
    cdef Py_ssize_t pat_len = 0
    cdef bytes pattern_bytes
    cdef bytes regex_bytes
    cdef object regex_text
    cdef RE2Any* regex
    cdef RE2OptionsAny options
    cdef StringPieceAny text_piece
    cdef vector[RE2Any*] row_patterns

    if aptr == NULL or p_aptr == NULL:
        raise ValueError("ArrayVector is not initialized")
    if aptr.length != p_aptr.length:
        raise ValueError("array and pattern vectors must have the same length")
    if aptr.value_type != DRAKEN_STRING or not isinstance(arr._child, StringVector):
        raise TypeError("_regex_match_any_array_array expects array ArrayVector with StringVector child")
    if p_aptr.value_type != DRAKEN_STRING or not isinstance(patterns._child, StringVector):
        raise TypeError("_regex_match_any_array_array expects patterns ArrayVector with StringVector child")

    child = <StringVector>arr._child
    p_child = <StringVector>patterns._child
    cptr = child.ptr
    p_cptr = p_child.ptr
    if cptr == NULL or p_cptr == NULL:
        raise ValueError("ArrayVector child StringVector is not initialized")

    n = <Py_ssize_t>aptr.length
    nbytes = (n + 7) >> 3
    out = BoolVector(<size_t>n)
    dst = <uint8_t*>out.ptr.data
    if nbytes != 0:
        memset(dst, 0, nbytes)

    row_nulls = aptr.null_bitmap
    pattern_row_nulls = p_aptr.null_bitmap
    child_nulls = cptr.null_bitmap
    p_child_nulls = p_cptr.null_bitmap

    if (row_nulls != NULL or pattern_row_nulls != NULL) and nbytes != 0:
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)
        out.ptr.null_bitmap = out_null
    else:
        out.ptr.null_bitmap = NULL

    options = RE2OptionsAny()
    options.set_case_sensitive(not ignore_case)
    options.set_log_errors(False)

    try:
        for i in range(n):
            if not _bit_is_set(row_nulls, i) or not _bit_is_set(pattern_row_nulls, i):
                continue

            if out_null != NULL:
                out_null[i >> 3] |= (<uint8_t>1 << (i & 7))

            matched = False
            p_row_start = p_aptr.offsets[i]
            p_row_end = p_aptr.offsets[i + 1]
            _release_regex_vector(row_patterns)

            for pj in range(p_row_start, p_row_end):
                if not _bit_is_set(p_child_nulls, pj):
                    continue
                p_start = p_cptr.offsets[pj]
                p_end = p_cptr.offsets[pj + 1]
                pat_text_len = p_end - p_start
                pattern_bytes = PyBytes_FromStringAndSize(
                    <char*>p_cptr.data + p_start, <Py_ssize_t>pat_text_len
                )
                regex_text = sql_like_to_regex(pattern_bytes.decode("utf-8", errors="replace"))
                regex_bytes = (<str>regex_text).encode("utf-8")
                PyBytes_AsStringAndSize(regex_bytes, &pat_buf, &pat_len)
                regex = new RE2Any(string(pat_buf, <size_t>pat_len), options)
                if not regex.ok():
                    del regex
                    raise ValueError("Invalid LIKE pattern")
                row_patterns.push_back(regex)

            row_start = aptr.offsets[i]
            row_end = aptr.offsets[i + 1]
            for j in range(row_start, row_end):
                if not _bit_is_set(child_nulls, j):
                    continue
                start = cptr.offsets[j]
                end = cptr.offsets[j + 1]
                text_len = end - start
                text_piece = StringPieceAny(<const char*>cptr.data + start, <size_t>text_len)
                for k in range(<Py_ssize_t>row_patterns.size()):
                    if RE2Any.PartialMatch(text_piece, row_patterns[k][0]):
                        matched = True
                        break
                if matched:
                    break

            if invert:
                matched = not matched
            if matched:
                dst[i >> 3] |= (<uint8_t>1 << (i & 7))

        if out_null != NULL and (n & 7) != 0:
            mask = <uint8_t>((1 << (n & 7)) - 1)
            out_null[nbytes - 1] &= mask
        return out
    finally:
        _release_regex_vector(row_patterns)


cpdef BoolVector regex_match_any(StringVector arr, ArrayVector patterns, int flags=0, bint invert=False):
    """
    Draken-native row-wise SQL LIKE ANY matcher:
      arr[i] (string) LIKE ANY(patterns[i] (array<string>))
    """
    from opteryx.utils.sql import sql_like_to_regex

    cdef DrakenVarBuffer* cptr = arr.ptr
    cdef DrakenArrayBuffer* p_aptr = patterns.ptr
    cdef StringVector p_child
    cdef DrakenVarBuffer* p_cptr
    cdef Py_ssize_t i, pj, k, n, nbytes
    cdef Py_ssize_t p_row_start, p_row_end, p_start, p_end, start, end, text_len, pat_text_len
    cdef BoolVector out
    cdef uint8_t* dst
    cdef uint8_t* out_null = NULL
    cdef uint8_t* row_nulls
    cdef uint8_t* pattern_row_nulls
    cdef uint8_t* p_child_nulls
    cdef uint8_t mask
    cdef bint matched
    cdef bint ignore_case = (flags & 2) != 0
    cdef char* pat_buf = <char*>0
    cdef Py_ssize_t pat_len = 0
    cdef bytes pattern_bytes
    cdef bytes regex_bytes
    cdef object regex_text
    cdef RE2Any* regex
    cdef RE2OptionsAny options
    cdef StringPieceAny text_piece
    cdef vector[RE2Any*] row_patterns

    if cptr == NULL or p_aptr == NULL:
        raise ValueError("vector is not initialized")
    if cptr.length != p_aptr.length:
        raise ValueError("array and pattern vectors must have the same length")
    if p_aptr.value_type != DRAKEN_STRING or not isinstance(patterns._child, StringVector):
        raise TypeError("regex_match_any expects patterns ArrayVector with StringVector child")

    p_child = <StringVector>patterns._child
    p_cptr = p_child.ptr
    if p_cptr == NULL:
        raise ValueError("patterns ArrayVector child StringVector is not initialized")

    n = <Py_ssize_t>cptr.length
    nbytes = (n + 7) >> 3
    out = BoolVector(<size_t>n)
    dst = <uint8_t*>out.ptr.data
    if nbytes != 0:
        memset(dst, 0, nbytes)

    row_nulls = cptr.null_bitmap
    pattern_row_nulls = p_aptr.null_bitmap
    p_child_nulls = p_cptr.null_bitmap

    if (row_nulls != NULL or pattern_row_nulls != NULL) and nbytes != 0:
        out_null = <uint8_t*>malloc(nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)
        out.ptr.null_bitmap = out_null
    else:
        out.ptr.null_bitmap = NULL

    options = RE2OptionsAny()
    options.set_case_sensitive(not ignore_case)
    options.set_log_errors(False)

    try:
        for i in range(n):
            if not _bit_is_set(row_nulls, i) or not _bit_is_set(pattern_row_nulls, i):
                continue

            if out_null != NULL:
                out_null[i >> 3] |= (<uint8_t>1 << (i & 7))

            matched = False
            p_row_start = p_aptr.offsets[i]
            p_row_end = p_aptr.offsets[i + 1]
            _release_regex_vector(row_patterns)

            for pj in range(p_row_start, p_row_end):
                if not _bit_is_set(p_child_nulls, pj):
                    continue
                p_start = p_cptr.offsets[pj]
                p_end = p_cptr.offsets[pj + 1]
                pat_text_len = p_end - p_start
                pattern_bytes = PyBytes_FromStringAndSize(
                    <char*>p_cptr.data + p_start, <Py_ssize_t>pat_text_len
                )
                regex_text = sql_like_to_regex(pattern_bytes.decode("utf-8", errors="replace"))
                regex_bytes = (<str>regex_text).encode("utf-8")
                PyBytes_AsStringAndSize(regex_bytes, &pat_buf, &pat_len)
                regex = new RE2Any(string(pat_buf, <size_t>pat_len), options)
                if not regex.ok():
                    del regex
                    raise ValueError("Invalid LIKE pattern")
                row_patterns.push_back(regex)

            start = cptr.offsets[i]
            end = cptr.offsets[i + 1]
            text_len = end - start
            text_piece = StringPieceAny(<const char*>cptr.data + start, <size_t>text_len)
            for k in range(<Py_ssize_t>row_patterns.size()):
                if RE2Any.PartialMatch(text_piece, row_patterns[k][0]):
                    matched = True
                    break

            if invert:
                matched = not matched
            if matched:
                dst[i >> 3] |= (<uint8_t>1 << (i & 7))

        if out_null != NULL and (n & 7) != 0:
            mask = <uint8_t>((1 << (n & 7)) - 1)
            out_null[nbytes - 1] &= mask
        return out
    finally:
        _release_regex_vector(row_patterns)


cpdef BoolVector vector_anyop_like(object literal, ArrayVector column):
    if isinstance(literal, ArrayVector):
        return _regex_match_any_array_array(column, <ArrayVector>literal, flags=0, invert=False)
    return _regex_match_any_literal(column, literal, flags=0, invert=False)


cpdef BoolVector vector_anyop_ilike(object literal, ArrayVector column):
    if isinstance(literal, ArrayVector):
        return _regex_match_any_array_array(column, <ArrayVector>literal, flags=2, invert=False)
    return _regex_match_any_literal(column, literal, flags=2, invert=False)
