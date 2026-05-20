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
from libc.stdint cimport uint8_t

from draken.vectors.string_vector cimport StringVector
from draken.vectors.string_vector cimport from_packed_dict
from draken.vectors import string_vector as string_vector_module
from draken.core.buffers cimport (
    DrakenVarBuffer, DrakenVector,
    DrakenStringArena, DrakenStringSlot, str_length, str_data,
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
    cdef Py_ssize_t slot_count = <Py_ssize_t>uv.data_length
    cdef Py_ssize_t i
    cdef SHA_CTX ctx
    cdef unsigned char digest[20]
    cdef char hex_buf[41]
    cdef DrakenVarBuffer* ndp
    cdef DrakenStringArena* in_arena = <DrakenStringArena*>uv.data
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen
    hex_buf[40] = 0

    slot_builder = string_vector_module.StringVectorBuilder.with_estimate(slot_count, 40)
    for i in range(slot_count):
        slot = &in_arena.slots[i]
        slen = str_length(slot)
        sdata = str_data(slot, in_arena.arena)
        SHA1_Init(&ctx)
        SHA1_Update(&ctx, <const void*>sdata, <size_t>slen)
        SHA1_Final(digest, &ctx)
        _to_hex(digest, 20, hex_buf)
        slot_builder.append_bytes(hex_buf, 40)
    new_dict_sv = slot_builder.finish()
    ndp = (<StringVector>new_dict_sv).ptr
    return from_packed_dict(
        <uint8_t*>uv.selection, 4, <Py_ssize_t>uv.length,
        ndp.offsets, <const uint8_t*>ndp.data, slot_count,
        uv.validity,
    )


cpdef StringVector vector_sha256(StringVector vec):
    """SHA-256 hash of each string element."""
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t slot_count = <Py_ssize_t>uv.data_length
    cdef Py_ssize_t i
    cdef SHA256_CTX ctx
    cdef unsigned char digest[32]
    cdef char hex_buf[65]
    cdef DrakenVarBuffer* ndp
    cdef DrakenStringArena* in_arena = <DrakenStringArena*>uv.data
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen
    hex_buf[64] = 0

    slot_builder = string_vector_module.StringVectorBuilder.with_estimate(slot_count, 64)
    for i in range(slot_count):
        slot = &in_arena.slots[i]
        slen = str_length(slot)
        sdata = str_data(slot, in_arena.arena)
        SHA256_Init(&ctx)
        SHA256_Update(&ctx, <const void*>sdata, <size_t>slen)
        SHA256_Final(digest, &ctx)
        _to_hex(digest, 32, hex_buf)
        slot_builder.append_bytes(hex_buf, 64)
    new_dict_sv = slot_builder.finish()
    ndp = (<StringVector>new_dict_sv).ptr
    return from_packed_dict(
        <uint8_t*>uv.selection, 4, <Py_ssize_t>uv.length,
        ndp.offsets, <const uint8_t*>ndp.data, slot_count,
        uv.validity,
    )


cpdef StringVector vector_sha512(StringVector vec):
    """SHA-512 hash of each string element."""
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t slot_count = <Py_ssize_t>uv.data_length
    cdef Py_ssize_t i
    cdef SHA512_CTX ctx
    cdef unsigned char digest[64]
    cdef char hex_buf[129]
    cdef DrakenVarBuffer* ndp
    cdef DrakenStringArena* in_arena = <DrakenStringArena*>uv.data
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen
    hex_buf[128] = 0

    slot_builder = string_vector_module.StringVectorBuilder.with_estimate(slot_count, 128)
    for i in range(slot_count):
        slot = &in_arena.slots[i]
        slen = str_length(slot)
        sdata = str_data(slot, in_arena.arena)
        SHA512_Init(&ctx)
        SHA512_Update(&ctx, <const void*>sdata, <size_t>slen)
        SHA512_Final(digest, &ctx)
        _to_hex(digest, 64, hex_buf)
        slot_builder.append_bytes(hex_buf, 128)
    new_dict_sv = slot_builder.finish()
    ndp = (<StringVector>new_dict_sv).ptr
    return from_packed_dict(
        <uint8_t*>uv.selection, 4, <Py_ssize_t>uv.length,
        ndp.offsets, <const uint8_t*>ndp.data, slot_count,
        uv.validity,
    )
