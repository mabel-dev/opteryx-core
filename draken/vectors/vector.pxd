# Vector cdef-class wrapper and utilities.
#
# Vector is the main nanobind-wrapped class (draken_native.so) representing
# a single Draken column. The evaluator uses Vector for type checking and
# accessing the unified DrakenVector* layout.
#
# simd_popcount is also exposed here as it's needed by the bytecode VM.

from libc.stdint cimport int32_t, uint8_t, uint32_t, uint64_t
from libc.stddef cimport size_t

from draken.core.buffers cimport DrakenVector, DrakenType

# Hash mixing constants from src/cpp/simd_hash.h.
cdef extern from "simd_hash.h" nogil:
    const uint64_t NULL_HASH
    const uint64_t MIX_HASH_CONSTANT

cdef inline uint64_t mix_hash(uint64_t current, uint64_t value) noexcept nogil:
    return (current ^ value) * MIX_HASH_CONSTANT + 1

cdef class Vector:
    """Vector — Cython shim wrapping draken.draken_native.Vector.

    _nb : the nanobind Vector handle (keeps memory alive).
    _dv : borrowed DrakenVector* valid for the lifetime of _nb.
    """
    cdef public object _nb
    cdef const DrakenVector* _dv
    cdef DrakenVector* unified(self) noexcept
    cdef uint8_t* null_bitmap_ptr(self) noexcept
    cdef bint c_hash_single(self, uint64_t* out, int32_t n) except -1 nogil
    cdef bint c_hash_distinct(self, uint64_t* out) except -1 nogil



# from_decoded — create a dense Vector from hand-allocated (draken_malloc) buffers.
# Analogous to BoolVector.from_decoded in bool_vector.pxd.
cdef Vector from_decoded(void* data, uint8_t* validity, uint32_t length, DrakenType dtype)


# dict_int64_from_decoded — create a dict-encoded int64 Vector from draken_malloc'd buffers.
# dict_vals, codes, and validity MUST be draken_malloc'd; ownership transferred on call.
cdef Vector dict_int64_from_decoded(void* dict_vals, uint32_t data_length,
                                     uint32_t* codes, uint32_t length,
                                     uint8_t* validity)


# Bitmap utilities (simd_popcount is also needed here for bytecode VM).

cdef extern from "core/bitmap_ops.h" nogil:
    # Count set bits in a bitmap (byte array).
    size_t simd_popcount(const uint8_t* data, size_t nbytes) nogil
