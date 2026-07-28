# Hand-written declaration mirroring draken/core/fp16.h — the fp16<->fp32
# conversion used by consumers that render VECTOR_FP16 storage as plain
# floats (writers; VECTOR_FP16 has no native wire representation, so it is
# always emitted as an array of floats).

from libc.stdint cimport uint16_t


cdef extern from "core/fp16.h":
    float draken_fp16_to_fp32(uint16_t h) noexcept nogil
