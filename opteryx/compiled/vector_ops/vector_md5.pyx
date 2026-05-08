# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.stdint cimport int32_t, uint8_t

from draken.vectors.string_vector cimport StringVector
from draken.vectors.string_vector cimport from_packed_dict
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport (
    DrakenVarBuffer, DrakenConstantStringPayload,
    DRAKEN_ENCODING_DICTIONARY,
)

cdef extern from "md5.h":
    ctypedef struct MD5_CTX:
        pass
    int MD5_Init(MD5_CTX *c) nogil
    int MD5_Update(MD5_CTX *c, const void *data, size_t len) nogil
    int MD5_Final(unsigned char *md, MD5_CTX *c) nogil


cpdef StringVector vector_md5(StringVector vec):
    """MD5 hash of each string element."""
    cdef Py_ssize_t n = vec.ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef MD5_CTX ctx
    cdef unsigned char digest[16]
    cdef char hex_buf[33]
    cdef DrakenVarBuffer* ptr
    cdef uint8_t* null_bm
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    hex_buf[32] = 0

    # --- Constant encoding: compute once, replicate ---
    if vec._has_const:
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)
        if vec._const_is_null or vec._const_value == NULL:
            for i in range(n):
                builder.append_null()
        else:
            MD5_Init(&ctx)
            MD5_Update(&ctx, <const void*>vec._const_value.data,
                       <size_t>vec._const_value.length)
            MD5_Final(digest, &ctx)
            _to_hex(digest, 16, hex_buf)
            for i in range(n):
                builder.append_bytes(hex_buf, 32)
        return builder.finish()

    # --- Dictionary encoding: transform dict entries, repack with same codes ---
    if vec._encoding == DRAKEN_ENCODING_DICTIONARY:
        dict_size = vec._dict_values.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 32)
        for i in range(dict_size):
            start = vec._dict_values.offsets[i]
            end = vec._dict_values.offsets[i + 1]
            MD5_Init(&ctx)
            MD5_Update(&ctx, <const uint8_t*>vec._dict_values.data + start,
                       <size_t>(end - start))
            MD5_Final(digest, &ctx)
            _to_hex(digest, 16, hex_buf)
            dict_builder.append_bytes(hex_buf, 32)
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            vec._dict_codes, vec._dict_code_width, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            vec._dict_accessor.row_nulls,
        )

    # --- Dense encoding: row-by-row ---
    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)
    ptr = vec.ptr
    null_bm = ptr.null_bitmap
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        MD5_Init(&ctx)
        MD5_Update(&ctx, (<const uint8_t*>ptr.data) + start, <size_t>(end - start))
        MD5_Final(digest, &ctx)
        _to_hex(digest, 16, hex_buf)
        builder.append_bytes(hex_buf, 32)
    return builder.finish()
