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

cdef extern from "md5.h":
    ctypedef struct MD5_CTX:
        pass
    int MD5_Init(MD5_CTX *c) nogil
    int MD5_Update(MD5_CTX *c, const void *data, size_t len) nogil
    int MD5_Final(unsigned char *md, MD5_CTX *c) nogil


cpdef StringVector vector_md5(StringVector vec):
    """MD5 hash of each string element."""
    cdef DrakenVector* uv = vec.unified()
    cdef Py_ssize_t slot_count = <Py_ssize_t>uv.data_length
    cdef Py_ssize_t i
    cdef MD5_CTX ctx
    cdef unsigned char digest[16]
    cdef char hex_buf[33]
    cdef DrakenVarBuffer* ndp
    cdef DrakenStringArena* in_arena = <DrakenStringArena*>uv.data
    cdef DrakenStringSlot* slot
    cdef const uint8_t* sdata
    cdef uint32_t slen
    hex_buf[32] = 0

    slot_builder = string_vector_module.StringVectorBuilder.with_estimate(slot_count, 32)
    for i in range(slot_count):
        slot = &in_arena.slots[i]
        slen = str_length(slot)
        sdata = str_data(slot, in_arena.arena)
        MD5_Init(&ctx)
        MD5_Update(&ctx, <const void*>sdata, <size_t>slen)
        MD5_Final(digest, &ctx)
        _to_hex(digest, 16, hex_buf)
        slot_builder.append_bytes(hex_buf, 32)
    new_dict_sv = slot_builder.finish()
    ndp = (<StringVector>new_dict_sv).ptr
    return from_packed_dict(
        <uint8_t*>uv.selection, 4, <Py_ssize_t>uv.length,
        ndp.offsets, <const uint8_t*>ndp.data, slot_count,
        uv.validity,
    )
