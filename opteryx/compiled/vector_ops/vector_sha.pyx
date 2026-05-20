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
    DrakenStringArena, DrakenStringSlot, str_length, str_data,
)
from draken.vectors.string_vector cimport _ConstView
from draken.vectors.string_vector cimport _const_view

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
    cdef uint8_t* null_bm
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef _ConstView csp
    cdef DrakenStringArena* sha1_gdv
    cdef DrakenStringSlot* sha1_slot
    cdef const uint8_t* sha1_sdata
    cdef uint32_t sha1_slen
    cdef DrakenStringArena* sha1_dense_arena
    cdef DrakenStringSlot* sha1_dense_slot
    cdef const uint8_t* sha1_dense_sdata
    cdef uint32_t sha1_dense_slen
    hex_buf[40] = 0

    if vec._unified_view.data_length == 1:  # constant
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 40)
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = _const_view(<DrakenStringArena*>uv.data)
            SHA1_Init(&ctx)
            SHA1_Update(&ctx, <const void*>csp.data, <size_t>csp.length)
            SHA1_Final(digest, &ctx)
            _to_hex(digest, 20, hex_buf)
            for i in range(n):
                builder.append_bytes(hex_buf, 40)
        return builder.finish()

    if vec._unified_view.data_length < vec._unified_view.length:  # dictionary
        sha1_gdv = <DrakenStringArena*>vec._unified_view.data
        dict_size = <Py_ssize_t>sha1_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 40)
        for i in range(dict_size):
            sha1_slot = &sha1_gdv.slots[i]
            sha1_slen = str_length(sha1_slot)
            sha1_sdata = str_data(sha1_slot, sha1_gdv.arena)
            SHA1_Init(&ctx)
            SHA1_Update(&ctx, <const void*>sha1_sdata, <size_t>sha1_slen)
            SHA1_Final(digest, &ctx)
            _to_hex(digest, 20, hex_buf)
            dict_builder.append_bytes(hex_buf, 40)
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 40)
    sha1_dense_arena = <DrakenStringArena*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        sha1_dense_slot = &sha1_dense_arena.slots[i]
        sha1_dense_sdata = str_data(sha1_dense_slot, sha1_dense_arena.arena)
        sha1_dense_slen = str_length(sha1_dense_slot)
        SHA1_Init(&ctx)
        SHA1_Update(&ctx, <const void*>sha1_dense_sdata, <size_t>sha1_dense_slen)
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
    cdef uint8_t* null_bm
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef _ConstView csp
    cdef DrakenStringArena* sha256_gdv
    cdef DrakenStringSlot* sha256_slot
    cdef const uint8_t* sha256_sdata
    cdef uint32_t sha256_slen
    cdef DrakenStringArena* sha256_dense_arena
    cdef DrakenStringSlot* sha256_dense_slot
    cdef const uint8_t* sha256_dense_sdata
    cdef uint32_t sha256_dense_slen
    hex_buf[64] = 0

    if vec._unified_view.data_length == 1:  # constant
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 64)
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = _const_view(<DrakenStringArena*>uv.data)
            SHA256_Init(&ctx)
            SHA256_Update(&ctx, <const void*>csp.data, <size_t>csp.length)
            SHA256_Final(digest, &ctx)
            _to_hex(digest, 32, hex_buf)
            for i in range(n):
                builder.append_bytes(hex_buf, 64)
        return builder.finish()

    if vec._unified_view.data_length < vec._unified_view.length:  # dictionary
        sha256_gdv = <DrakenStringArena*>vec._unified_view.data
        dict_size = <Py_ssize_t>sha256_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 64)
        for i in range(dict_size):
            sha256_slot = &sha256_gdv.slots[i]
            sha256_slen = str_length(sha256_slot)
            sha256_sdata = str_data(sha256_slot, sha256_gdv.arena)
            SHA256_Init(&ctx)
            SHA256_Update(&ctx, <const void*>sha256_sdata, <size_t>sha256_slen)
            SHA256_Final(digest, &ctx)
            _to_hex(digest, 32, hex_buf)
            dict_builder.append_bytes(hex_buf, 64)
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 64)
    sha256_dense_arena = <DrakenStringArena*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        sha256_dense_slot = &sha256_dense_arena.slots[i]
        sha256_dense_sdata = str_data(sha256_dense_slot, sha256_dense_arena.arena)
        sha256_dense_slen = str_length(sha256_dense_slot)
        SHA256_Init(&ctx)
        SHA256_Update(&ctx, <const void*>sha256_dense_sdata, <size_t>sha256_dense_slen)
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
    cdef uint8_t* null_bm
    cdef Py_ssize_t dict_size
    cdef DrakenVarBuffer* ndp
    cdef _ConstView csp
    cdef DrakenStringArena* sha512_gdv
    cdef DrakenStringSlot* sha512_slot
    cdef const uint8_t* sha512_sdata
    cdef uint32_t sha512_slen
    cdef DrakenStringArena* sha512_dense_arena
    cdef DrakenStringSlot* sha512_dense_slot
    cdef const uint8_t* sha512_dense_sdata
    cdef uint32_t sha512_dense_slen
    hex_buf[128] = 0

    if vec._unified_view.data_length == 1:  # constant
        builder = string_vector_module.StringVectorBuilder.with_estimate(n, 128)
        if uv.validity != NULL:  # null constant
            for i in range(n):
                builder.append_null()
        else:
            csp = _const_view(<DrakenStringArena*>uv.data)
            SHA512_Init(&ctx)
            SHA512_Update(&ctx, <const void*>csp.data, <size_t>csp.length)
            SHA512_Final(digest, &ctx)
            _to_hex(digest, 64, hex_buf)
            for i in range(n):
                builder.append_bytes(hex_buf, 128)
        return builder.finish()

    if vec._unified_view.data_length < vec._unified_view.length:  # dictionary
        sha512_gdv = <DrakenStringArena*>vec._unified_view.data
        dict_size = <Py_ssize_t>sha512_gdv.length
        dict_builder = string_vector_module.StringVectorBuilder.with_estimate(dict_size, 128)
        for i in range(dict_size):
            sha512_slot = &sha512_gdv.slots[i]
            sha512_slen = str_length(sha512_slot)
            sha512_sdata = str_data(sha512_slot, sha512_gdv.arena)
            SHA512_Init(&ctx)
            SHA512_Update(&ctx, <const void*>sha512_sdata, <size_t>sha512_slen)
            SHA512_Final(digest, &ctx)
            _to_hex(digest, 64, hex_buf)
            dict_builder.append_bytes(hex_buf, 128)
        new_dict_sv = dict_builder.finish()
        ndp = (<StringVector>new_dict_sv).ptr
        return from_packed_dict(
            <uint8_t*>uv.selection, 4, n,
            ndp.offsets, <const uint8_t*>ndp.data, dict_size,
            uv.validity,
        )

    builder = string_vector_module.StringVectorBuilder.with_estimate(n, 128)
    sha512_dense_arena = <DrakenStringArena*>uv.data
    null_bm = uv.validity
    for i in range(n):
        if null_bm != NULL and not ((null_bm[i >> 3] >> (i & 7)) & 1):
            builder.append_null()
            continue
        sha512_dense_slot = &sha512_dense_arena.slots[i]
        sha512_dense_sdata = str_data(sha512_dense_slot, sha512_dense_arena.arena)
        sha512_dense_slen = str_length(sha512_dense_slot)
        SHA512_Init(&ctx)
        SHA512_Update(&ctx, <const void*>sha512_dense_sdata, <size_t>sha512_dense_slen)
        SHA512_Final(digest, &ctx)
        _to_hex(digest, 64, hex_buf)
        builder.append_bytes(hex_buf, 128)
    return builder.finish()
