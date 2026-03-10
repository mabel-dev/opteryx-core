# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stddef cimport size_t
from libc.stdint cimport int32_t, uint8_t

from opteryx.draken.vectors.string_vector cimport StringVector
from opteryx.draken.vectors import string_vector as string_vector_module
from opteryx.draken.core.buffers cimport DrakenVarBuffer

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
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef MD5_CTX ctx
    cdef unsigned char digest[16]
    cdef char hex_buf[33]
    hex_buf[32] = 0

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 32)

    for i in range(n):
        if ptr.null_bitmap != NULL and not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
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
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef SHA_CTX ctx
    cdef unsigned char digest[20]
    cdef char hex_buf[41]
    hex_buf[40] = 0

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 40)

    for i in range(n):
        if ptr.null_bitmap != NULL and not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
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
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef SHA256_CTX ctx
    cdef unsigned char digest[32]
    cdef char hex_buf[65]
    hex_buf[64] = 0

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 64)

    for i in range(n):
        if ptr.null_bitmap != NULL and not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
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
    cdef DrakenVarBuffer* ptr = vec.ptr
    cdef Py_ssize_t n = ptr.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef SHA512_CTX ctx
    cdef unsigned char digest[64]
    cdef char hex_buf[129]
    hex_buf[128] = 0

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 128)

    for i in range(n):
        if ptr.null_bitmap != NULL and not ((ptr.null_bitmap[i >> 3] >> (i & 7)) & 1):
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
