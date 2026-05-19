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
from libc.stdint cimport uint8_t, uint32_t, int64_t
from libc.string cimport memset
from cpython.array cimport array, clone

from draken.core.buffers cimport DrakenVarBuffer, DrakenVector, DrakenGermanArena, GermanString, gs_length, gs_data
from draken.vectors.bool_vector cimport BoolVector
from draken.vectors.float32_vector cimport Float32Vector
from draken.vectors.string_vector cimport StringVector
from draken.vectors.string_vector cimport _StringVectorCIterator
from draken.vectors.string_vector cimport StringElement
from draken.vectors.string_vector cimport StringVectorBuilder
from draken.vectors.vector cimport Vector
from draken.vectors.vector_vector cimport VectorVector

from cpython.unicode cimport PyUnicode_DecodeUTF8


cdef inline bint _is_ascii_whitespace(unsigned char ch) noexcept nogil:
    return ch == 9 or ch == 10 or ch == 11 or ch == 12 or ch == 13 or ch == 32


cdef StringVector _materialize_german_dict_entries(StringVector vec):
    """Build a dense StringVector containing only the dict entries (not full expansion)."""
    cdef DrakenGermanArena* gdv = vec._german_dict_values
    cdef Py_ssize_t dict_size = <Py_ssize_t>gdv.length
    cdef GermanString* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen
    cdef Py_ssize_t j
    cdef StringVectorBuilder builder = StringVectorBuilder.with_estimate(dict_size, 16)
    for j in range(dict_size):
        slot = &gdv.slots[j]
        slen = gs_length(slot)
        sdata = gs_data(slot, gdv.arena)
        builder.append_bytes(<const char*>sdata, <Py_ssize_t>slen)
    return builder.finish()


cdef BoolVector _vector_match_against_string_vector(
    StringVector values,
    object provider,
    str query_text,
    float min_score=0.6,
):
    cdef VectorVector embedded
    cdef VectorVector docs
    cdef array positions_arr = array('q')  # 'q' = signed long long (int64)
    cdef array scores_arr = array('f')   # 'f' = float32
    cdef array take_idx = array('i')     # row indices for VectorVector.take
    cdef int64_t[::1] positions
    cdef float[::1] scores
    cdef int32_t[::1] take_view
    cdef float[::1] query_view
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
    cdef object query_vector
    cdef object row_vectors
    cdef Py_ssize_t current_index
    from opteryx.vectors.embeddings import embed_text_matrix

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

    from opteryx.compiled.nanobind import vector_search

    scores_list = vector_search.score_cosine(query_vector, row_vectors)
    for score in scores_list:
        scores_arr.append(float(score))

    # Convert positions to Cython array
    for pos in active_positions:
        positions_arr.append(pos)

    # Create memoryviews from arrays
    positions = positions_arr
    scores = scores_arr

    for i in range(len(positions)):
        if scores[i] < min_score:
            continue
        row_index = positions[i]
        if row_index < 0 or row_index >= n:
            continue
        dst[row_index >> 3] |= (1 << (row_index & 7))

    return out


cdef BoolVector _vector_match_against_dictionary_accessor(
    object owner,
    const DrakenVector* uv,
    object provider,
    str query_text,
    float min_score=0.6,
):
    cdef Py_ssize_t n = uv.length
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    cdef uint8_t* row_nulls = uv.validity
    cdef BoolVector dict_matches
    cdef uint8_t* dict_bits
    cdef Py_ssize_t i
    cdef uint32_t code

    memset(dst, 0, nbytes)

    if uv.data == NULL or uv.data_length == 0:
        return out

    dict_matches = _vector_match_against_string_vector(
        _materialize_german_dict_entries(<StringVector>owner),
        provider,
        query_text,
        min_score,
    )
    dict_bits = <uint8_t*>dict_matches.ptr.data

    for i in range(n):
        if row_nulls != NULL and ((row_nulls[i >> 3] >> (i & 7)) & 1) == 0:
            continue
        code = uv.selection[i]
        if (dict_bits[code >> 3] >> (code & 7)) & 1:
            dst[i >> 3] |= <uint8_t>(1 << (i & 7))

    return out


cdef BoolVector _vector_match_against_vector_vector(
    VectorVector values,
    str query_text,
    float min_score,
):
    """Fast path for pre-embedded VectorVector columns.

    Skips the per-row text embedding step (which dominates cost in the
    StringVector path). Embeds only the query, then uses VectorVector's
    cosine kernel to score every row in one C call, and thresholds in-place.
    """
    from opteryx.vectors.embeddings import embed_text_matrix
    from opteryx.vectors import vector_math

    cdef Py_ssize_t n = len(values)
    cdef Py_ssize_t nbytes = (n + 7) >> 3
    cdef BoolVector out = BoolVector(<size_t>n)
    cdef uint8_t* dst = <uint8_t*>out.ptr.data
    memset(dst, 0, nbytes)

    cdef str text = (query_text or "").strip()
    if not text:
        return out

    cdef VectorVector embedded = embed_text_matrix([text])
    if len(embedded) == 0 or embedded.dimensions != values.dimensions:
        return out

    cdef object query_fp32 = vector_math.row_as_fp32_array(embedded, 0)
    cdef float[::1] query_view = query_fp32
    cdef Float32Vector scores = values.cosine_similarity(query_view)
    cdef float* score_ptr = <float*> scores.ptr.data
    cdef uint8_t* score_nulls = scores.ptr.null_bitmap
    cdef Py_ssize_t i
    cdef bint row_valid

    for i in range(n):
        if score_nulls != NULL:
            row_valid = (score_nulls[i >> 3] >> (i & 7)) & 1
            if not row_valid:
                continue
        if score_ptr[i] >= min_score:
            dst[i >> 3] |= <uint8_t>(1 << (i & 7))

    return out


cpdef BoolVector vector_match_against(
    object values,
    object provider,
    str query_text,
    float min_score=0.6,
):
    cdef DrakenVector* uv = NULL

    if isinstance(values, VectorVector):
        return _vector_match_against_vector_vector(values, query_text, min_score)

    if isinstance(values, Vector):
        uv = (<Vector>values).unified()

    if uv != NULL and isinstance(values, StringVector) and (<StringVector>values)._german_dict_values != NULL:
        return _vector_match_against_dictionary_accessor(values, uv, provider, query_text, min_score)
    if isinstance(values, StringVector):
        return _vector_match_against_string_vector(values, provider, query_text, min_score)
    raise TypeError(f"vector_match_against requires StringVector, dictionary-encoded Vector, or VectorVector, got {type(values)}")
