# Vector cdef-class wrapper and utilities.
#
# Vector is the main nanobind-wrapped class (draken_native.so) representing
# a single Draken column. The evaluator uses Vector for type checking and
# accessing the unified DrakenVector* layout.
#
# simd_popcount is also exposed here as it's needed by the bytecode VM.

from libc.stdint cimport int32_t, uint8_t, uint32_t, uint64_t
from libc.stddef cimport size_t

from draken.core.buffers cimport DrakenVector

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


# Bitmap utilities (simd_popcount is also needed here for bytecode VM).

cdef extern from "core/bitmap_ops.h" nogil:
    # Count set bits in a bitmap (byte array).
    size_t simd_popcount(const uint8_t* data, size_t nbytes) nogil
