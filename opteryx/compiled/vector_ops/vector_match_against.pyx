# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.stdint cimport uint8_t, uint16_t, uint32_t, int64_t
from libc.string cimport memset

import numpy
cimport numpy

from opteryx.draken.core.buffers cimport DrakenDictionaryBuffer
from opteryx.draken.vectors.bool_vector cimport BoolVector
from opteryx.draken.vectors.dictionary_vector cimport DictionaryVector
from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors.string_vector cimport _StringVectorCIterator
from opteryx.draken.vectors.string_vector cimport StringElement

numpy.import_array()

from cpython.unicode cimport PyUnicode_DecodeUTF8


cdef inline bint _is_ascii_whitespace(unsigned char ch) noexcept nogil:
    return ch == 9 or ch == 10 or ch == 11 or ch == 12 or ch == 13 or ch == 32


cdef inline uint32_t _read_code(const DrakenDictionaryBuffer* ptr, Py_ssize_t i) noexcept nogil:
    if ptr.code_width == 1:
        return (<uint8_t*>ptr.codes)[i]
    if ptr.code_width == 2:
        return (<uint16_t*>ptr.codes)[i]
    return (<uint32_t*>ptr.codes)[i]


cdef StringVector _wrap_dictionary_values(DictionaryVector values):
    cdef StringVector wrapped = StringVector(wrap=True)
    wrapped.ptr = values.ptr.dictionary_values
    wrapped.owns_data = False
    wrapped._arrow_data_buf = values
    wrapped._arrow_offs_buf = None
    wrapped._arrow_null_buf = None
    return wrapped


cdef BoolVector _vector_match_against_string_vector(
    StringVector values,
    object provider,
    str query_text,
    float min_score=0.6,
):
    cdef object embedded
    cdef numpy.ndarray positions_arr
    cdef numpy.ndarray scores_arr
    cdef int64_t[::1] positions
    cdef numpy.float32_t[::1] scores
    cdef Py_ssize_t i
    cdef Py_ssize_t n = values.ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef Py_ssize_t row_index
    cdef Py_ssize_t text_len
    cdef const char* text_ptr
    cdef unsigned char* unsigned_ptr
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef _StringVectorCIterator it
    cdef StringElement elem
    cdef list active_positions
    cdef list active_texts
    cdef object decoded
    cdef object vector_search
    cdef object query_vector
    cdef object row_vectors
    cdef object valid_mask
    cdef Py_ssize_t current_index
    from opteryx.embeddings import embed_text_matrix

    memset(dst, 0, nbytes)

    active_positions = []
    active_texts = []
    it = values.c_iter()
    current_index = 0

    while it.next(&elem):
        if elem.is_null:
            current_index += 1
            continue

        text_ptr = elem.ptr
        text_len = elem.length
        unsigned_ptr = <unsigned char*>text_ptr

        while text_len > 0 and _is_ascii_whitespace(unsigned_ptr[0]):
            text_ptr += 1
            unsigned_ptr += 1
            text_len -= 1
        while text_len > 0 and _is_ascii_whitespace(unsigned_ptr[text_len - 1]):
            text_len -= 1
        if text_len == 0:
            current_index += 1
            continue

        decoded = PyUnicode_DecodeUTF8(text_ptr, text_len, "ignore")
        if decoded is None:
            current_index += 1
            continue
        active_positions.append(current_index)
        active_texts.append(decoded)
        current_index += 1

    if not active_texts:
        return out

    embedded = embed_text_matrix([query_text, *active_texts])
    query_vector = embedded[0]
    row_vectors = embedded[1:]

    try:
        from opteryx.nanobind import vector_search

        scores_arr = numpy.asarray(
            vector_search.score_cosine(query_vector, row_vectors), dtype=numpy.float32
        )
    except (ImportError, ValueError):
        scores_arr = numpy.zeros(len(active_texts), dtype=numpy.float32)
        if numpy.linalg.norm(query_vector) != 0.0:
            valid_mask = numpy.linalg.norm(row_vectors, axis=1) != 0.0
            if numpy.any(valid_mask):
                scores_arr[valid_mask] = (
                    numpy.dot(row_vectors[valid_mask], query_vector)
                    / (numpy.linalg.norm(row_vectors[valid_mask], axis=1) * numpy.linalg.norm(query_vector))
                )

    positions_arr = numpy.asarray(active_positions, dtype=numpy.int64)

    positions = positions_arr
    scores = scores_arr

    for i in range(positions.shape[0]):
        if scores[i] < min_score:
            continue
        row_index = positions[i]
        if row_index < 0 or row_index >= n:
            continue
        dst[row_index >> 3] |= (1 << (row_index & 7))

    return out


cdef BoolVector _vector_match_against_dictionary_vector(
    DictionaryVector values,
    object provider,
    str query_text,
    float min_score=0.6,
):
    cdef DrakenDictionaryBuffer* ptr = values.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef uint8_t* row_nulls = ptr.null_bitmap
    cdef BoolVector dict_matches
    cdef uint8_t* dict_bits
    cdef Py_ssize_t i
    cdef uint32_t code

    memset(dst, 0, nbytes)

    if ptr.dictionary_values == NULL or ptr.dictionary_values.length == 0:
        return out

    dict_matches = _vector_match_against_string_vector(
        _wrap_dictionary_values(values),
        provider,
        query_text,
        min_score,
    )
    dict_bits = <uint8_t*>dict_matches.ptr.data

    for i in range(n):
        if row_nulls != NULL and ((row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
            continue
        code = _read_code(ptr, i)
        if (dict_bits[code >> 3] >> (code & 7)) & 1:
            dst[i >> 3] |= <uint8_t>(1 << (i & 7))

    return out


cpdef BoolVector vector_match_against(
    object values,
    object provider,
    str query_text,
    float min_score=0.6,
):
    if isinstance(values, StringVector):
        return _vector_match_against_string_vector(values, provider, query_text, min_score)
    if isinstance(values, DictionaryVector):
        return _vector_match_against_dictionary_vector(values, provider, query_text, min_score)
    raise TypeError(f"vector_match_against requires StringVector or DictionaryVector, got {type(values)}")
