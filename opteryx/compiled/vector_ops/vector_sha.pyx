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

from draken.vectors.string_vector cimport StringVector
from draken.vectors.string_vector cimport from_packed_dict
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport (
    DrakenVarBuffer, DrakenConstantStringPayload, DrakenVector,
)

cdef extern from "sha1.h":
    ctypedef struct SHA_CTX:
        pass
    int SHA1_Init(SHA_CTX *c) nogil
    int SHA1_Update(SHA_CTX *c, const void *data, size_t len) nogil
    int SHA1_Final(unsigned char *md, SHA_CTX *c) nogil

cdef extern from "sha2.h":
    ctypedef struct SHA256_CTX:
        pass
    ctypedef struct SHA512_CTX:
        pass
    int SHA256_Init(SHA256_CTX *c) nogil
    int SHA256_Update(SHA256_CTX *c, const void *data, size_t len) nogil
    int SHA256_Final(unsigned char *md, SHA256_CTX *c) nogil
    int SHA512_Init(SHA512_CTX *c) nogil
    int SHA512_Update(SHA512_CTX *c, const void *data, size_t len) nogil
    int SHA512_Final(unsigned char *md, SHA512_CTX *c) nogil


cpdef StringVector vector_sha1(StringVector vec):
    """SHA-1 hash of each string element."""
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef SHA_CTX ctx
    cdef unsigned char digest[20]
    cdef char hex_buf[41]
    cdef DrakenVarBuffer* vbuf
    cdef uint8_t* null_bm
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef DrakenConstantStringPayload* csp
    hex_buf[40] = 0

    if uv.data_length == 1:  # constant
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 40)
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = <DrakenConstantStringPayload*>uv.data
            SHA1_Init(&ctx)
            SHA1_Update(&ctx, <const void*>csp.data, <size_t>csp.length)
            SHA1_Final(digest, &ctx)
            _to_hex(digest, 20, hex_buf)
            for i in range(n):
                builder.append_bytes(hex_buf, 40)
        return builder.finish()

    if uv.selection != NULL:  # dictionary
        vbuf = <DrakenVarBuffer*>uv.data
        dict_size = <Py_ssize_t>vbuf.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 40)
        for i in range(dict_size):
            start = vbuf.offsets[i]
            end = vbuf.offsets[i + 1]
            SHA1_Init(&ctx)
            SHA1_Update(&ctx, <const uint8_t*>vbuf.data + start, <size_t>(end - start))
            SHA1_Final(digest, &ctx)
            _to_hex(digest, 20, hex_buf)
            dict_builder.append_bytes(hex_buf, 40)
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, uv.sel_width, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 40)
    vbuf = <DrakenVarBuffer*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        start = vbuf.offsets[i]
        end = vbuf.offsets[i + 1]
        SHA1_Init(&ctx)
        SHA1_Update(&ctx, (<const uint8_t*>vbuf.data) + start, <size_t>(end - start))
        SHA1_Final(digest, &ctx)
        _to_hex(digest, 20, hex_buf)
        builder.append_bytes(hex_buf, 40)
    return builder.finish()


cpdef StringVector vector_sha256(StringVector vec):
    """SHA-256 hash of each string element."""
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef SHA256_CTX ctx
    cdef unsigned char digest[32]
    cdef char hex_buf[65]
    cdef DrakenVarBuffer* vbuf
    cdef uint8_t* null_bm
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef DrakenConstantStringPayload* csp
    hex_buf[64] = 0

    if uv.data_length == 1:  # constant
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 64)
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = <DrakenConstantStringPayload*>uv.data
            SHA256_Init(&ctx)
            SHA256_Update(&ctx, <const void*>csp.data, <size_t>csp.length)
            SHA256_Final(digest, &ctx)
            _to_hex(digest, 32, hex_buf)
            for i in range(n):
                builder.append_bytes(hex_buf, 64)
        return builder.finish()

    if uv.selection != NULL:  # dictionary
        vbuf = <DrakenVarBuffer*>uv.data
        dict_size = <Py_ssize_t>vbuf.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 64)
        for i in range(dict_size):
            start = vbuf.offsets[i]
            end = vbuf.offsets[i + 1]
            SHA256_Init(&ctx)
            SHA256_Update(&ctx, <const uint8_t*>vbuf.data + start, <size_t>(end - start))
            SHA256_Final(digest, &ctx)
            _to_hex(digest, 32, hex_buf)
            dict_builder.append_bytes(hex_buf, 64)
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, uv.sel_width, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 64)
    vbuf = <DrakenVarBuffer*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        start = vbuf.offsets[i]
        end = vbuf.offsets[i + 1]
        SHA256_Init(&ctx)
        SHA256_Update(&ctx, (<const uint8_t*>vbuf.data) + start, <size_t>(end - start))
        SHA256_Final(digest, &ctx)
        _to_hex(digest, 32, hex_buf)
        builder.append_bytes(hex_buf, 64)
    return builder.finish()


cpdef StringVector vector_sha512(StringVector vec):
    """SHA-512 hash of each string element."""
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t n = <Py_ssize_t>uv.length
    cdef Py_ssize_t i
    cdef int32_t start, end
    cdef SHA512_CTX ctx
    cdef unsigned char digest[64]
    cdef char hex_buf[129]
    cdef DrakenVarBuffer* vbuf
    cdef uint8_t* null_bm
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef DrakenConstantStringPayload* csp
    hex_buf[128] = 0

    if uv.data_length == 1:  # constant
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 128)
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = <DrakenConstantStringPayload*>uv.data
            SHA512_Init(&ctx)
            SHA512_Update(&ctx, <const void*>csp.data, <size_t>csp.length)
            SHA512_Final(digest, &ctx)
            _to_hex(digest, 64, hex_buf)
            for i in range(n):
                builder.append_bytes(hex_buf, 128)
        return builder.finish()

    if uv.selection != NULL:  # dictionary
        vbuf = <DrakenVarBuffer*>uv.data
        dict_size = <Py_ssize_t>vbuf.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 128)
        for i in range(dict_size):
            start = vbuf.offsets[i]
            end = vbuf.offsets[i + 1]
            SHA512_Init(&ctx)
            SHA512_Update(&ctx, <const uint8_t*>vbuf.data + start, <size_t>(end - start))
            SHA512_Final(digest, &ctx)
            _to_hex(digest, 64, hex_buf)
            dict_builder.append_bytes(hex_buf, 128)
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, uv.sel_width, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 128)
    vbuf = <DrakenVarBuffer*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        start = vbuf.offsets[i]
        end = vbuf.offsets[i + 1]
        SHA512_Init(&ctx)
        SHA512_Update(&ctx, (<const uint8_t*>vbuf.data) + start, <size_t>(end - start))
        SHA512_Final(digest, &ctx)
        _to_hex(digest, 64, hex_buf)
        builder.append_bytes(hex_buf, 128)
    return builder.finish()
