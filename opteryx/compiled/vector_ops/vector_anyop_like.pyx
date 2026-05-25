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
from libc.stdint cimport uint8_t, uint32_t, int32_t
from libc.string cimport memset, memcpy
from cpython.bytes cimport PyBytes_AsStringAndSize, PyBytes_FromStringAndSize
from cpython.object cimport PyObject
from libcpp.string cimport string
from libcpp.vector cimport vector

from draken.core.buffers cimport (
    DRAKEN_VARCHAR, DRAKEN_ARRAY, DrakenType,
    DrakenStringArena, DrakenStringSlot, str_length, str_data, DrakenVector
)
from draken.vectors.vector cimport Vector
from draken.vectors.bool_vector cimport BoolVector, from_decoded

cdef extern from "core/alloc.h":
    void* draken_malloc(size_t n) nogil
    void  draken_free(void* p) nogil

cdef extern from "core/draken_bridge.h":
    const DrakenVector* draken_vector_unwrap(PyObject* obj)
    const DrakenVector* draken_array_child_unwrap(PyObject* obj)


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


cdef BoolVector _regex_match_any_literal(Vector arr, object patterns, int flags=0, bint invert=False):
    """SQL LIKE ANY matcher: arr (DRAKEN_ARRAY of VARCHAR) vs literal pattern list."""
    from opteryx.utils.sql import sql_like_to_regex

    cdef DrakenVector* dv = arr.unified()
    cdef int32_t* offsets = <int32_t*>dv.data
    cdef const DrakenVector* child_dv = draken_array_child_unwrap(<PyObject*>arr)
    cdef DrakenStringArena* child_arena = <DrakenStringArena*>child_dv.data
    cdef uint32_t* child_sel = <uint32_t*>child_dv.selection
    cdef Py_ssize_t n = <Py_ssize_t>dv.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef size_t sz = <size_t>nbytes if nbytes > 0 else 1
    cdef uint8_t* row_nulls = dv.validity
    cdef uint8_t* child_nulls = child_dv.validity
    cdef uint8_t* dst
    cdef uint8_t* out_null = NULL
    cdef uint8_t mask
    cdef Py_ssize_t i, j, k
    cdef int32_t row_start, row_end
    cdef uint32_t sel_i
    cdef uint32_t slen
    cdef const uint8_t* sptr
    cdef bint matched
    cdef bint ignore_case = (flags & 2) != 0
    cdef char* pat_buf = <char*>0
    cdef Py_ssize_t pat_len = 0
    cdef bytes regex_bytes
    cdef RE2Any* regex
    cdef RE2OptionsAny options
    cdef StringPieceAny text_piece
    cdef vector[RE2Any*] compiled_patterns

    dst = <uint8_t*>draken_malloc(sz)
    if dst == NULL:
        raise MemoryError()
    memset(dst, 0, sz)

    options = RE2OptionsAny()
    options.set_case_sensitive(not ignore_case)
    options.set_log_errors(False)

    try:
        if patterns is None:
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

        if row_nulls != NULL and nbytes != 0:
            out_null = <uint8_t*>draken_malloc(<size_t>nbytes)
            if out_null == NULL:
                draken_free(dst)
                raise MemoryError()
            memcpy(out_null, row_nulls, nbytes)
            if (n & 7) != 0:
                mask = <uint8_t>((1 << (n & 7)) - 1)
                out_null[nbytes - 1] &= mask

        for i in range(n):
            if row_nulls != NULL and ((row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
                continue

            sel_i = dv.selection[i]
            row_start = offsets[sel_i]
            row_end = offsets[sel_i + 1]
            matched = False

            for j in range(row_start, row_end):
                if child_nulls != NULL and ((child_nulls[j >> 3] >> (j & 7)) & 1) == 0:
                    continue
                slen = str_length(&child_arena.slots[child_sel[j]])
                sptr = str_data(&child_arena.slots[child_sel[j]], child_arena.arena)
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


cdef BoolVector _regex_match_any_array_array(Vector arr, Vector patterns, int flags=0, bint invert=False):
    """Row-wise SQL LIKE ANY: arr[i] (array<string>) LIKE ANY(patterns[i] (array<string>))."""
    from opteryx.utils.sql import sql_like_to_regex

    cdef DrakenVector* dv = arr.unified()
    cdef int32_t* offsets = <int32_t*>dv.data
    cdef const DrakenVector* child_dv = draken_array_child_unwrap(<PyObject*>arr)
    cdef DrakenStringArena* child_arena = <DrakenStringArena*>child_dv.data
    cdef uint32_t* child_sel = <uint32_t*>child_dv.selection

    cdef DrakenVector* p_dv = patterns.unified()
    cdef int32_t* p_offsets = <int32_t*>p_dv.data
    cdef const DrakenVector* p_child_dv = draken_array_child_unwrap(<PyObject*>patterns)
    cdef DrakenStringArena* p_child_arena = <DrakenStringArena*>p_child_dv.data
    cdef uint32_t* p_child_sel = <uint32_t*>p_child_dv.selection

    cdef Py_ssize_t n = <Py_ssize_t>dv.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef size_t sz = <size_t>nbytes if nbytes > 0 else 1
    cdef uint8_t* row_nulls = dv.validity
    cdef uint8_t* p_row_nulls = p_dv.validity
    cdef uint8_t* child_nulls = child_dv.validity
    cdef uint8_t* p_child_nulls = p_child_dv.validity
    cdef uint8_t* dst
    cdef uint8_t* out_null = NULL
    cdef uint8_t mask
    cdef Py_ssize_t i, j, pj, k
    cdef int32_t row_start, row_end, p_row_start, p_row_end
    cdef uint32_t sel_i, p_sel_i
    cdef uint32_t slen, p_slen
    cdef const uint8_t* sptr
    cdef const uint8_t* p_sptr
    cdef bint matched
    cdef bint ignore_case = (flags & 2) != 0
    cdef char* pat_buf = <char*>0
    cdef Py_ssize_t pat_len = 0
    cdef bytes pattern_bytes, regex_bytes
    cdef RE2Any* regex
    cdef RE2OptionsAny options
    cdef StringPieceAny text_piece
    cdef vector[RE2Any*] row_patterns

    if dv.length != p_dv.length:
        raise ValueError("array and pattern vectors must have the same length")

    dst = <uint8_t*>draken_malloc(sz)
    if dst == NULL:
        raise MemoryError()
    memset(dst, 0, sz)

    if (row_nulls != NULL or p_row_nulls != NULL) and nbytes != 0:
        out_null = <uint8_t*>draken_malloc(<size_t>nbytes)
        if out_null == NULL:
            draken_free(dst)
            raise MemoryError()
        memset(out_null, 0xFF, nbytes)
        if (n & 7) != 0:
            mask = <uint8_t>((1 << (n & 7)) - 1)
            out_null[nbytes - 1] &= mask

    options = RE2OptionsAny()
    options.set_case_sensitive(not ignore_case)
    options.set_log_errors(False)

    try:
        for i in range(n):
            if not _bit_is_set(row_nulls, i) or not _bit_is_set(p_row_nulls, i):
                if out_null != NULL:
                    out_null[i >> 3] &= ~(<uint8_t>1 << (i & 7))
                continue

            _release_regex_vector(row_patterns)
            matched = False

            p_sel_i = p_dv.selection[i]
            p_row_start = p_offsets[p_sel_i]
            p_row_end = p_offsets[p_sel_i + 1]

            for pj in range(p_row_start, p_row_end):
                if not _bit_is_set(p_child_nulls, pj):
                    continue
                p_slen = str_length(&p_child_arena.slots[p_child_sel[pj]])
                p_sptr = str_data(&p_child_arena.slots[p_child_sel[pj]], p_child_arena.arena)
                pattern_bytes = PyBytes_FromStringAndSize(<char*>p_sptr, <Py_ssize_t>p_slen)
                regex_text = sql_like_to_regex(pattern_bytes.decode("utf-8", errors="replace"))
                regex_bytes = (<str>regex_text).encode("utf-8")
                PyBytes_AsStringAndSize(regex_bytes, &pat_buf, &pat_len)
                regex = new RE2Any(string(pat_buf, <size_t>pat_len), options)
                if not regex.ok():
                    del regex
                    raise ValueError("Invalid LIKE pattern")
                row_patterns.push_back(regex)

            sel_i = dv.selection[i]
            row_start = offsets[sel_i]
            row_end = offsets[sel_i + 1]

            for j in range(row_start, row_end):
                if not _bit_is_set(child_nulls, j):
                    continue
                slen = str_length(&child_arena.slots[child_sel[j]])
                sptr = str_data(&child_arena.slots[child_sel[j]], child_arena.arena)
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

        return from_decoded(<void*>dst, out_null, <size_t>n)
    finally:
        _release_regex_vector(row_patterns)


cpdef BoolVector regex_match_any(Vector arr, Vector patterns, int flags=0, bint invert=False):
    """Row-wise SQL LIKE ANY: arr[i] (string) LIKE ANY(patterns[i] (array<string>))."""
    from opteryx.utils.sql import sql_like_to_regex

    cdef DrakenVector* arr_dv = arr.unified()
    cdef DrakenStringArena* arr_arena = <DrakenStringArena*>arr_dv.data

    cdef DrakenVector* p_dv = patterns.unified()
    cdef int32_t* p_offsets = <int32_t*>p_dv.data
    cdef const DrakenVector* p_child_dv = draken_array_child_unwrap(<PyObject*>patterns)
    cdef DrakenStringArena* p_child_arena = <DrakenStringArena*>p_child_dv.data
    cdef uint32_t* p_child_sel = <uint32_t*>p_child_dv.selection

    cdef Py_ssize_t n = <Py_ssize_t>arr_dv.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef size_t sz = <size_t>nbytes if nbytes > 0 else 1
    cdef uint8_t* row_nulls = arr_dv.validity
    cdef uint8_t* p_row_nulls = p_dv.validity
    cdef uint8_t* p_child_nulls = p_child_dv.validity
    cdef uint8_t* dst
    cdef uint8_t* out_null = NULL
    cdef uint8_t mask
    cdef Py_ssize_t i, pj, k
    cdef int32_t p_row_start, p_row_end
    cdef uint32_t arr_sel_i, p_sel_i
    cdef uint32_t slen, p_slen
    cdef const uint8_t* sptr
    cdef const uint8_t* p_sptr
    cdef bint matched
    cdef bint ignore_case = (flags & 2) != 0
    cdef char* pat_buf = <char*>0
    cdef Py_ssize_t pat_len = 0
    cdef bytes pattern_bytes, regex_bytes
    cdef RE2Any* regex
    cdef RE2OptionsAny options
    cdef StringPieceAny text_piece
    cdef vector[RE2Any*] row_patterns

    if arr_dv.length != p_dv.length:
        raise ValueError("array and pattern vectors must have the same length")

    dst = <uint8_t*>draken_malloc(sz)
    if dst == NULL:
        raise MemoryError()
    memset(dst, 0, sz)

    if (row_nulls != NULL or p_row_nulls != NULL) and nbytes != 0:
        out_null = <uint8_t*>draken_malloc(<size_t>nbytes)
        if out_null == NULL:
            draken_free(dst)
            raise MemoryError()
        memset(out_null, 0xFF, nbytes)
        if (n & 7) != 0:
            mask = <uint8_t>((1 << (n & 7)) - 1)
            out_null[nbytes - 1] &= mask

    options = RE2OptionsAny()
    options.set_case_sensitive(not ignore_case)
    options.set_log_errors(False)

    try:
        for i in range(n):
            if not _bit_is_set(row_nulls, i) or not _bit_is_set(p_row_nulls, i):
                if out_null != NULL:
                    out_null[i >> 3] &= ~(<uint8_t>1 << (i & 7))
                continue

            _release_regex_vector(row_patterns)
            matched = False

            p_sel_i = p_dv.selection[i]
            p_row_start = p_offsets[p_sel_i]
            p_row_end = p_offsets[p_sel_i + 1]

            for pj in range(p_row_start, p_row_end):
                if not _bit_is_set(p_child_nulls, pj):
                    continue
                p_slen = str_length(&p_child_arena.slots[p_child_sel[pj]])
                p_sptr = str_data(&p_child_arena.slots[p_child_sel[pj]], p_child_arena.arena)
                pattern_bytes = PyBytes_FromStringAndSize(<char*>p_sptr, <Py_ssize_t>p_slen)
                regex_text = sql_like_to_regex(pattern_bytes.decode("utf-8", errors="replace"))
                regex_bytes = (<str>regex_text).encode("utf-8")
                PyBytes_AsStringAndSize(regex_bytes, &pat_buf, &pat_len)
                regex = new RE2Any(string(pat_buf, <size_t>pat_len), options)
                if not regex.ok():
                    del regex
                    raise ValueError("Invalid LIKE pattern")
                row_patterns.push_back(regex)

            arr_sel_i = arr_dv.selection[i]
            slen = str_length(&arr_arena.slots[arr_sel_i])
            sptr = str_data(&arr_arena.slots[arr_sel_i], arr_arena.arena)
            text_piece = StringPieceAny(<const char*>sptr, <size_t>slen)

            for k in range(<Py_ssize_t>row_patterns.size()):
                if RE2Any.PartialMatch(text_piece, row_patterns[k][0]):
                    matched = True
                    break

            if invert:
                matched = not matched
            if matched:
                dst[i >> 3] |= (<uint8_t>1 << (i & 7))

        return from_decoded(<void*>dst, out_null, <size_t>n)
    finally:
        _release_regex_vector(row_patterns)


cpdef BoolVector vector_anyop_like(object literal, Vector column, bint negate=False):
    cdef const DrakenVector* lit_dv
    if isinstance(literal, Vector):
        lit_dv = draken_vector_unwrap(<PyObject*>literal)
        if lit_dv != NULL and lit_dv.type == DRAKEN_ARRAY:
            return _regex_match_any_array_array(column, <Vector>literal, flags=0, invert=negate)
    return _regex_match_any_literal(column, literal, flags=0, invert=negate)


cpdef BoolVector vector_anyop_ilike(object literal, Vector column, bint negate=False):
    cdef const DrakenVector* lit_dv
    if isinstance(literal, Vector):
        lit_dv = draken_vector_unwrap(<PyObject*>literal)
        if lit_dv != NULL and lit_dv.type == DRAKEN_ARRAY:
            return _regex_match_any_array_array(column, <Vector>literal, flags=2, invert=negate)
    return _regex_match_any_literal(column, literal, flags=2, invert=negate)
