# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.stdint cimport int32_t, uint8_t

from opteryx.compiled.draken.vectors.string_vector cimport StringVector
from opteryx.compiled.draken.vectors.string_vector cimport from_packed_dict
from opteryx.compiled.draken.vectors import string_vector as string_vector_module
from opteryx.compiled.draken.core.buffers cimport (
    DrakenVarBuffer, DrakenConstantStringPayload,
    DRAKEN_ENCODING_DICTIONARY,
)

cdef extern from *:
    """
    #if defined(__APPLE__)
    #include <CommonCrypto/CommonDigest.h>
    typedef CC_MD5_CTX MD5_CTX;
    typedef CC_SHA1_CTX SHA_CTX;
    typedef CC_SHA256_CTX SHA256_CTX;
    typedef CC_SHA512_CTX SHA512_CTX;
    #define MD5_Init CC_MD5_Init
    #define MD5_Update CC_MD5_Update
    #define MD5_Final CC_MD5_Final
    #define SHA1_Init CC_SHA1_Init
    #define SHA1_Update CC_SHA1_Update
    #define SHA1_Final CC_SHA1_Final
    #define SHA256_Init CC_SHA256_Init
    #define SHA256_Update CC_SHA256_Update
    #define SHA256_Final CC_SHA256_Final
    #define SHA512_Init CC_SHA512_Init
    #define SHA512_Update CC_SHA512_Update
    #define SHA512_Final CC_SHA512_Final
    #else
    #include "md5.h"
    #include "sha1.h"
    #include "sha2.h"
    #endif
    """
    ctypedef struct MD5_CTX:
        pass
    ctypedef struct SHA_CTX:
        pass
    ctypedef struct SHA256_CTX:
        pass
    ctypedef struct SHA512_CTX:
        pass

    int MD5_Init(MD5_CTX *c) nogil
    int MD5_Update(MD5_CTX *c, const void *data, size_t len) nogil
    int MD5_Final(unsigned char *md, MD5_CTX *c) nogil
    int SHA1_Init(SHA_CTX *c) nogil
    int SHA1_Update(SHA_CTX *c, const void *data, size_t len) nogil
    int SHA1_Final(unsigned char *md, SHA_CTX *c) nogil
    int SHA256_Init(SHA256_CTX *c) nogil
    int SHA256_Update(SHA256_CTX *c, const void *data, size_t len) nogil
    int SHA256_Final(unsigned char *md, SHA256_CTX *c) nogil
    int SHA512_Init(SHA512_CTX *c) nogil
    int SHA512_Update(SHA512_CTX *c, const void *data, size_t len) nogil
    int SHA512_Final(unsigned char *md, SHA512_CTX *c) nogil


cdef const char* _HEX = "0123456789abcdef"


cdef inline void _to_hex(const unsigned char* digest, size_t dlen, char* out) noexcept nogil:
    cdef size_t i
    cdef unsigned char b
    for i in range(dlen):
        b = digest[i]
        out[2 * i] = _HEX[(b >> 4) & 0x0F]
        out[2 * i + 1] = _HEX[b & 0x0F]


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


cpdef StringVector vector_sha1(StringVector vec):
    """SHA-1 hash of each string element."""
    cdef Py_ssize_t n = vec.ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef SHA_CTX ctx
    cdef unsigned char digest[20]
    cdef char hex_buf[41]
    cdef DrakenVarBuffer* ptr
    cdef uint8_t* null_bm
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    hex_buf[40] = 0

    if vec._has_const:
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 40)
        if vec._const_is_null or vec._const_value == NULL:
            for i in range(n):
                builder.append_null()
        else:
            SHA1_Init(&ctx)
            SHA1_Update(&ctx, <const void*>vec._const_value.data,
                        <size_t>vec._const_value.length)
            SHA1_Final(digest, &ctx)
            _to_hex(digest, 20, hex_buf)
            for i in range(n):
                builder.append_bytes(hex_buf, 40)
        return builder.finish()

    if vec._encoding == DRAKEN_ENCODING_DICTIONARY:
        dict_size = vec._dict_values.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 40)
        for i in range(dict_size):
            start = vec._dict_values.offsets[i]
            end = vec._dict_values.offsets[i + 1]
            SHA1_Init(&ctx)
            SHA1_Update(&ctx, <const uint8_t*>vec._dict_values.data + start,
                        <size_t>(end - start))
            SHA1_Final(digest, &ctx)
            _to_hex(digest, 20, hex_buf)
            dict_builder.append_bytes(hex_buf, 40)
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            vec._dict_codes, vec._dict_code_width, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            vec._dict_accessor.row_nulls,
        )

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 40)
    ptr = vec.ptr
    null_bm = ptr.null_bitmap
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        SHA1_Init(&ctx)
        SHA1_Update(&ctx, (<const uint8_t*>ptr.data) + start, <size_t>(end - start))
        SHA1_Final(digest, &ctx)
        _to_hex(digest, 20, hex_buf)
        builder.append_bytes(hex_buf, 40)
    return builder.finish()


cpdef StringVector vector_sha256(StringVector vec):
    """SHA-256 hash of each string element."""
    cdef Py_ssize_t n = vec.ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef SHA256_CTX ctx
    cdef unsigned char digest[32]
    cdef char hex_buf[65]
    cdef DrakenVarBuffer* ptr
    cdef uint8_t* null_bm
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    hex_buf[64] = 0

    if vec._has_const:
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 64)
        if vec._const_is_null or vec._const_value == NULL:
            for i in range(n):
                builder.append_null()
        else:
            SHA256_Init(&ctx)
            SHA256_Update(&ctx, <const void*>vec._const_value.data,
                          <size_t>vec._const_value.length)
            SHA256_Final(digest, &ctx)
            _to_hex(digest, 32, hex_buf)
            for i in range(n):
                builder.append_bytes(hex_buf, 64)
        return builder.finish()

    if vec._encoding == DRAKEN_ENCODING_DICTIONARY:
        dict_size = vec._dict_values.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 64)
        for i in range(dict_size):
            start = vec._dict_values.offsets[i]
            end = vec._dict_values.offsets[i + 1]
            SHA256_Init(&ctx)
            SHA256_Update(&ctx, <const uint8_t*>vec._dict_values.data + start,
                          <size_t>(end - start))
            SHA256_Final(digest, &ctx)
            _to_hex(digest, 32, hex_buf)
            dict_builder.append_bytes(hex_buf, 64)
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            vec._dict_codes, vec._dict_code_width, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            vec._dict_accessor.row_nulls,
        )

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 64)
    ptr = vec.ptr
    null_bm = ptr.null_bitmap
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        SHA256_Init(&ctx)
        SHA256_Update(&ctx, (<const uint8_t*>ptr.data) + start, <size_t>(end - start))
        SHA256_Final(digest, &ctx)
        _to_hex(digest, 32, hex_buf)
        builder.append_bytes(hex_buf, 64)
    return builder.finish()


cpdef StringVector vector_sha512(StringVector vec):
    """SHA-512 hash of each string element."""
    cdef Py_ssize_t n = vec.ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef SHA512_CTX ctx
    cdef unsigned char digest[64]
    cdef char hex_buf[129]
    cdef DrakenVarBuffer* ptr
    cdef uint8_t* null_bm
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    hex_buf[128] = 0

    if vec._has_const:
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 128)
        if vec._const_is_null or vec._const_value == NULL:
            for i in range(n):
                builder.append_null()
        else:
            SHA512_Init(&ctx)
            SHA512_Update(&ctx, <const void*>vec._const_value.data,
                          <size_t>vec._const_value.length)
            SHA512_Final(digest, &ctx)
            _to_hex(digest, 64, hex_buf)
            for i in range(n):
                builder.append_bytes(hex_buf, 128)
        return builder.finish()

    if vec._encoding == DRAKEN_ENCODING_DICTIONARY:
        dict_size = vec._dict_values.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 128)
        for i in range(dict_size):
            start = vec._dict_values.offsets[i]
            end = vec._dict_values.offsets[i + 1]
            SHA512_Init(&ctx)
            SHA512_Update(&ctx, <const uint8_t*>vec._dict_values.data + start,
                          <size_t>(end - start))
            SHA512_Final(digest, &ctx)
            _to_hex(digest, 64, hex_buf)
            dict_builder.append_bytes(hex_buf, 128)
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            vec._dict_codes, vec._dict_code_width, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            vec._dict_accessor.row_nulls,
        )

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 128)
    ptr = vec.ptr
    null_bm = ptr.null_bitmap
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        start = ptr.offsets[i]
        end = ptr.offsets[i + 1]
        SHA512_Init(&ctx)
        SHA512_Update(&ctx, (<const uint8_t*>ptr.data) + start, <size_t>(end - start))
        SHA512_Final(digest, &ctx)
        _to_hex(digest, 64, hex_buf)
        builder.append_bytes(hex_buf, 128)
    return builder.finish()
