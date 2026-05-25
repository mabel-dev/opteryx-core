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
from libc.stdint cimport int32_t, uint8_t
from libc.string cimport memset, memcpy
from cpython.bytes cimport PyBytes_AsStringAndSize, PyBytes_FromStringAndSize
from cpython.object cimport PyObject
from libcpp.string cimport string
from libcpp.vector cimport vector

from draken.core.buffers cimport DRAKEN_VARCHAR, DrakenVector
from draken.core.buffers cimport DrakenStringArena, DrakenStringSlot, str_length, str_data
from draken.vectors.array_vector cimport ArrayVector
from draken.vectors.bool_vector cimport BoolVector, from_decoded
from draken.vectors.string_vector cimport StringVector

cdef extern from "core/draken_bridge.h":
    const DrakenVector* draken_array_child_unwrap(PyObject* obj)

cdef extern from "core/alloc.h":
    void* draken_malloc(size_t size) nogil
    void draken_free(void* ptr) nogil


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

    cdef DrakenVector* arr_uv = arr.unified()
    cdef int32_t* arr_offsets = <int32_t*>arr_uv.data
    cdef const DrakenVector* child_dv = draken_array_child_unwrap(<PyObject*>arr._nb)
    cdef DrakenStringArena* child_arena
    cdef object pattern_src, p, p_str, regex_text
    cdef Py_ssize_t i, j, k, n, nbytes, row_start, row_end, text_len
    cdef uint32_t slen
    cdef const uint8_t* sptr
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

    if child_dv == NULL or child_dv.type != DRAKEN_VARCHAR:
        raise TypeError("regex_match_any expects ArrayVector with string child")
    child_arena = <DrakenStringArena*>child_dv.data

    options = RE2OptionsAny()
    options.set_case_sensitive(not ignore_case)
    options.set_log_errors(False)

    try:
        if patterns is None:
            n = <Py_ssize_t>arr_uv.length
            nbytes = (n + 7) >> 3
            dst = <uint8_t*>draken_malloc(<size_t>(nbytes if nbytes > 0 else 1))
            if dst == NULL:
                raise MemoryError()
            if nbytes != 0:
                memset(dst, 0, nbytes)
            return from_decoded(<void*>dst, NULL, <size_t>n)

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

        n = <Py_ssize_t>arr_uv.length
        nbytes = (n + 7) >> 3
        dst = <uint8_t*>draken_malloc(<size_t>(nbytes if nbytes > 0 else 1))
        if dst == NULL:
            raise MemoryError()
        if nbytes != 0:
            memset(dst, 0, nbytes)

        row_nulls = arr_uv.validity
        child_nulls = child_dv.validity

        if row_nulls != NULL and nbytes != 0:
            out_null = <uint8_t*>draken_malloc(<size_t>nbytes)
            if out_null == NULL:
                raise MemoryError()
            memcpy(out_null, row_nulls, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask

        for i in range(n):
            if row_nulls != NULL and ((row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
                continue

            matched = False
            row_start = arr_offsets[i]
            row_end = arr_offsets[i + 1]

            for j in range(row_start, row_end):
                if child_nulls != NULL and ((child_nulls[j >> 3] >> (j & 7)) & 1) == 0:
                    continue

                slen = str_length(&child_arena.slots[j])
                sptr = str_data(&child_arena.slots[j], child_arena.arena)
                text_len = <Py_ssize_t>slen
                text_piece = StringPieceAny(<const char*>sptr, <size_t>slen)

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
        return from_decoded(<void*>dst, out_null, <size_t>n)
    finally:
        _release_regex_vector(compiled_patterns)


cdef BoolVector _regex_match_any_array_array(ArrayVector arr, ArrayVector patterns, int flags=0, bint invert=False):
    """
    Draken-native row-wise SQL LIKE ANY matcher:
      arr[i] (array<string>) LIKE ANY(patterns[i] (array<string>))
    """
    from opteryx.utils.sql import sql_like_to_regex

    cdef DrakenVector* arr_uv = arr.unified()
    cdef int32_t* arr_offsets = <int32_t*>arr_uv.data
    cdef const DrakenVector* child_dv = draken_array_child_unwrap(<PyObject*>arr._nb)
    cdef DrakenVector* pat_uv = patterns.unified()
    cdef int32_t* pat_offsets = <int32_t*>pat_uv.data
    cdef const DrakenVector* p_child_dv = draken_array_child_unwrap(<PyObject*>patterns._nb)
    cdef DrakenStringArena* child_arena
    cdef DrakenStringArena* p_child_arena
    cdef Py_ssize_t i, j, pj, k, n, nbytes
    cdef Py_ssize_t row_start, row_end, p_row_start, p_row_end
    cdef uint32_t slen, p_slen
    cdef const uint8_t* sptr
    cdef const uint8_t* p_sptr
    cdef Py_ssize_t text_len, pat_text_len
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

    if child_dv == NULL or child_dv.type != DRAKEN_VARCHAR:
        raise TypeError("_regex_match_any_array_array expects arr ArrayVector with string child")
    if p_child_dv == NULL or p_child_dv.type != DRAKEN_VARCHAR:
        raise TypeError("_regex_match_any_array_array expects patterns ArrayVector with string child")
    if arr_uv.length != pat_uv.length:
        raise ValueError("array and pattern vectors must have the same length")

    child_arena = <DrakenStringArena*>child_dv.data
    p_child_arena = <DrakenStringArena*>p_child_dv.data

    options = RE2OptionsAny()
    options.set_case_sensitive(not ignore_case)
    options.set_log_errors(False)

    n = <Py_ssize_t>arr_uv.length
    nbytes = (n + 7) >> 3
    dst = <uint8_t*>draken_malloc(<size_t>(nbytes if nbytes > 0 else 1))
    if dst == NULL:
        raise MemoryError()
    if nbytes != 0:
        memset(dst, 0, nbytes)

    row_nulls = arr_uv.validity
    pattern_row_nulls = pat_uv.validity
    child_nulls = child_dv.validity
    p_child_nulls = p_child_dv.validity

    if (row_nulls != NULL or pattern_row_nulls != NULL) and nbytes != 0:
        out_null = <uint8_t*>draken_malloc(<size_t>nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

    try:
        for i in range(n):
            if not _bit_is_set(row_nulls, i) or not _bit_is_set(pattern_row_nulls, i):
                continue

            if out_null != NULL:
                out_null[i >> 3] |= (<uint8_t>1 << (i & 7))

            matched = False
            p_row_start = pat_offsets[i]
            p_row_end = pat_offsets[i + 1]
            _release_regex_vector(row_patterns)

            for pj in range(p_row_start, p_row_end):
                if not _bit_is_set(p_child_nulls, pj):
                    continue
                p_slen = str_length(&p_child_arena.slots[pj])
                p_sptr = str_data(&p_child_arena.slots[pj], p_child_arena.arena)
                pat_text_len = <Py_ssize_t>p_slen
                pattern_bytes = PyBytes_FromStringAndSize(
                    <char*>p_sptr, <Py_ssize_t>pat_text_len
                )
                regex_text = sql_like_to_regex(pattern_bytes.decode("utf-8", errors="replace"))
                regex_bytes = (<str>regex_text).encode("utf-8")
                PyBytes_AsStringAndSize(regex_bytes, &pat_buf, &pat_len)
                regex = new RE2Any(string(pat_buf, <size_t>pat_len), options)
                if not regex.ok():
                    del regex
                    raise ValueError("Invalid LIKE pattern")
                row_patterns.push_back(regex)

            row_start = arr_offsets[i]
            row_end = arr_offsets[i + 1]
            for j in range(row_start, row_end):
                if not _bit_is_set(child_nulls, j):
                    continue
                slen = str_length(&child_arena.slots[j])
                sptr = str_data(&child_arena.slots[j], child_arena.arena)
                text_len = <Py_ssize_t>slen
                text_piece = StringPieceAny(<const char*>sptr, <size_t>slen)
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
        return from_decoded(<void*>dst, out_null, <size_t>n)
    finally:
        _release_regex_vector(row_patterns)


cpdef BoolVector regex_match_any(StringVector arr, ArrayVector patterns, int flags=0, bint invert=False):
    """
    Draken-native row-wise SQL LIKE ANY matcher:
      arr[i] (string) LIKE ANY(patterns[i] (array<string>))
    """
    from opteryx.utils.sql import sql_like_to_regex

    cdef DrakenVector* arr_uv = arr.unified()
    cdef DrakenStringArena* arr_arena = <DrakenStringArena*>arr_uv.data
    cdef DrakenVector* pat_uv = patterns.unified()
    cdef int32_t* pat_offsets = <int32_t*>pat_uv.data
    cdef const DrakenVector* p_child_dv = draken_array_child_unwrap(<PyObject*>patterns._nb)
    cdef DrakenStringArena* p_child_arena
    cdef Py_ssize_t i, pj, k, n, nbytes
    cdef Py_ssize_t p_row_start, p_row_end
    cdef uint32_t slen, p_slen
    cdef const uint8_t* sptr
    cdef const uint8_t* p_sptr
    cdef Py_ssize_t text_len, pat_text_len
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

    if p_child_dv == NULL or p_child_dv.type != DRAKEN_VARCHAR:
        raise TypeError("regex_match_any expects patterns ArrayVector with StringVector child")
    if arr_uv.length != pat_uv.length:
        raise ValueError("array and pattern vectors must have the same length")

    p_child_arena = <DrakenStringArena*>p_child_dv.data

    n = <Py_ssize_t>arr_uv.length
    nbytes = (n + 7) >> 3
    dst = <uint8_t*>draken_malloc(<size_t>(nbytes if nbytes > 0 else 1))
    if dst == NULL:
        raise MemoryError()
    if nbytes != 0:
        memset(dst, 0, nbytes)

    row_nulls = arr_uv.validity
    pattern_row_nulls = pat_uv.validity
    p_child_nulls = p_child_dv.validity

    if (row_nulls != NULL or pattern_row_nulls != NULL) and nbytes != 0:
        out_null = <uint8_t*>draken_malloc(<size_t>nbytes)
        if out_null == NULL:
            raise MemoryError()
        memset(out_null, 0, nbytes)

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
            p_row_start = pat_offsets[i]
            p_row_end = pat_offsets[i + 1]
            _release_regex_vector(row_patterns)

            for pj in range(p_row_start, p_row_end):
                if not _bit_is_set(p_child_nulls, pj):
                    continue
                p_slen = str_length(&p_child_arena.slots[pj])
                p_sptr = str_data(&p_child_arena.slots[pj], p_child_arena.arena)
                pat_text_len = <Py_ssize_t>p_slen
                pattern_bytes = PyBytes_FromStringAndSize(
                    <char*>p_sptr, <Py_ssize_t>pat_text_len
                )
                regex_text = sql_like_to_regex(pattern_bytes.decode("utf-8", errors="replace"))
                regex_bytes = (<str>regex_text).encode("utf-8")
                PyBytes_AsStringAndSize(regex_bytes, &pat_buf, &pat_len)
                regex = new RE2Any(string(pat_buf, <size_t>pat_len), options)
                if not regex.ok():
                    del regex
                    raise ValueError("Invalid LIKE pattern")
                row_patterns.push_back(regex)

            slen = str_length(&arr_arena.slots[arr_uv.selection[i]])
            sptr = str_data(&arr_arena.slots[arr_uv.selection[i]], arr_arena.arena)
            text_len = <Py_ssize_t>slen
            text_piece = StringPieceAny(<const char*>sptr, <size_t>slen)
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
        return from_decoded(<void*>dst, out_null, <size_t>n)
    finally:
        _release_regex_vector(row_patterns)


cpdef BoolVector vector_anyop_like(object literal, ArrayVector column, bint negate=False):
    if isinstance(literal, ArrayVector):
        return _regex_match_any_array_array(column, <ArrayVector>literal, flags=0, invert=negate)
    return _regex_match_any_literal(column, literal, flags=0, invert=negate)


cpdef BoolVector vector_anyop_ilike(object literal, ArrayVector column, bint negate=False):
    if isinstance(literal, ArrayVector):
        return _regex_match_any_array_array(column, <ArrayVector>literal, flags=2, invert=negate)
    return _regex_match_any_literal(column, literal, flags=2, invert=negate)
