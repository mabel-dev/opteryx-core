# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: cdivision=True

from array import array

from cpython.bytes cimport PyBytes_FromStringAndSize
from cython.operator cimport dereference, preincrement
from libc.stddef cimport size_t
from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t
from libc.stdlib cimport free, malloc

from opteryx.compiled.aggregations.aggregate_kernels cimport AGG_AVG
from opteryx.compiled.aggregations.aggregate_kernels cimport AGG_COUNT_DISTINCT
from opteryx.compiled.aggregations.aggregate_kernels cimport AGG_COUNT_STAR
from opteryx.draken.core.buffers cimport DrakenDictionaryBuffer
from opteryx.draken.core.buffers cimport DrakenFixedBuffer
from opteryx.draken.core.buffers cimport DrakenVarBuffer
from opteryx.draken.core.buffers cimport DRAKEN_BOOL
from opteryx.draken.core.buffers cimport DRAKEN_FLOAT32
from opteryx.draken.core.buffers cimport DRAKEN_FLOAT64
from opteryx.draken.core.buffers cimport DRAKEN_INT16
from opteryx.draken.core.buffers cimport DRAKEN_INT32
from opteryx.draken.core.buffers cimport DRAKEN_INT64
from opteryx.draken.core.buffers cimport DRAKEN_INT8
from opteryx.draken.core.buffers cimport DRAKEN_STRING
from opteryx.draken.morsels.morsel cimport Morsel
from opteryx.draken.vectors.dictionary_vector cimport DictionaryVector
from opteryx.draken.vectors.float64_vector cimport Float64Vector
from opteryx.draken.vectors.int64_vector cimport Int64Vector
from opteryx.draken.vectors.vector cimport mix_hash
from opteryx.third_party.cyan4973.xxhash cimport cy_xxhash3_64
from opteryx.third_party.abseil.containers cimport IdentityHash
from opteryx.third_party.abseil.containers cimport flat_hash_map
from opteryx.third_party.abseil.containers cimport flat_hash_set


cdef object _KERNEL_MISSING = object()


cdef inline uint32_t _dict_read_code(const DrakenDictionaryBuffer* ptr, Py_ssize_t row_idx) noexcept nogil:
    if ptr.code_width == 1:
        return (<uint8_t*>ptr.codes)[row_idx]
    if ptr.code_width == 2:
        return (<uint16_t*>ptr.codes)[row_idx]
    return (<uint32_t*>ptr.codes)[row_idx]


cdef inline bint _dict_row_null(
    const DrakenDictionaryBuffer* ptr,
    uint32_t code,
    Py_ssize_t row_idx,
) noexcept nogil:
    cdef DrakenVarBuffer* dict_values = ptr.dictionary_values
    if ptr.null_bitmap != NULL:
        if ((ptr.null_bitmap[row_idx >> 3] >> (row_idx & 7)) & 1) == 0:
            return True
    if dict_values != NULL and dict_values.null_bitmap != NULL:
        if ((dict_values.null_bitmap[code >> 3] >> (code & 7)) & 1) == 0:
            return True
    return False


cdef inline object _dict_code_to_object(const DrakenDictionaryBuffer* ptr, uint32_t code):
    cdef DrakenVarBuffer* dict_values = ptr.dictionary_values
    cdef int32_t start
    cdef int32_t end
    cdef int dict_type
    if dict_values == NULL:
        raise ValueError("DictionaryVector is missing dictionary values")
    if code >= dict_values.length:
        raise ValueError("Dictionary code out of bounds")
    dict_type = dict_values.type
    if dict_values.null_bitmap != NULL:
        if ((dict_values.null_bitmap[code >> 3] >> (code & 7)) & 1) == 0:
            return None

    if dict_type == DRAKEN_STRING:
        start = dict_values.offsets[code]
        end = dict_values.offsets[code + 1]
        return PyBytes_FromStringAndSize(<char*>dict_values.data + start, end - start)
    if dict_type == DRAKEN_INT8:
        return (<int8_t*>dict_values.data)[code]
    if dict_type == DRAKEN_INT16:
        return (<int16_t*>dict_values.data)[code]
    if dict_type == DRAKEN_INT32:
        return (<int32_t*>dict_values.data)[code]
    if dict_type == DRAKEN_INT64:
        return (<int64_t*>dict_values.data)[code]
    if dict_type == DRAKEN_FLOAT32:
        return (<float*>dict_values.data)[code]
    if dict_type == DRAKEN_FLOAT64:
        return (<double*>dict_values.data)[code]
    if dict_type == DRAKEN_BOOL:
        return (<uint8_t*>dict_values.data)[code] != 0
    raise TypeError("Unsupported dictionary value type in group-by key materialization")


cdef uint64_t* _build_dict_hashes(const DrakenDictionaryBuffer* ptr) except NULL:
    cdef DrakenVarBuffer* dict_values = ptr.dictionary_values
    cdef Py_ssize_t dict_n
    cdef Py_ssize_t code
    cdef uint64_t* out_hashes = NULL
    cdef int32_t start, end
    cdef int dict_type
    cdef int32_t itemsize

    if dict_values == NULL:
        raise ValueError("DictionaryVector is missing dictionary values")

    dict_n = dict_values.length
    dict_type = dict_values.type
    itemsize = 0
    if dict_type == DRAKEN_INT8 or dict_type == DRAKEN_BOOL:
        itemsize = 1
    elif dict_type == DRAKEN_INT16:
        itemsize = 2
    elif dict_type == DRAKEN_INT32 or dict_type == DRAKEN_FLOAT32:
        itemsize = 4
    elif dict_type == DRAKEN_INT64 or dict_type == DRAKEN_FLOAT64:
        itemsize = 8

    if dict_n <= 0:
        return NULL

    out_hashes = <uint64_t*>malloc(<size_t>dict_n * sizeof(uint64_t))
    if out_hashes == NULL:
        raise MemoryError("Failed to allocate dictionary hash table")

    for code in range(dict_n):
        if dict_values.null_bitmap != NULL:
            if ((dict_values.null_bitmap[code >> 3] >> (code & 7)) & 1) == 0:
                out_hashes[code] = 0
                continue
        if dict_type == DRAKEN_STRING:
            start = dict_values.offsets[code]
            end = dict_values.offsets[code + 1]
            out_hashes[code] = mix_hash(
                0,
                cy_xxhash3_64(<const void*>(dict_values.data + start), <size_t>(end - start)),
            )
        elif dict_type == DRAKEN_INT64:
            out_hashes[code] = <uint64_t>(<int64_t*>dict_values.data)[code]
        elif dict_type == DRAKEN_FLOAT64:
            out_hashes[code] = (<uint64_t*>dict_values.data)[code]
        elif itemsize > 0:
            out_hashes[code] = mix_hash(
                0,
                cy_xxhash3_64(
                    <const void*>(dict_values.data + (code * itemsize)),
                    <size_t>itemsize,
                ),
            )
        else:
            free(out_hashes)
            raise TypeError("Unsupported dictionary value type in group-by hashing")

    return out_hashes
