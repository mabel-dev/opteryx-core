# BoolVector cdef-class wrapper and bitmap operation declarations.
#
# BoolVector is a nanobind-wrapped C++ Vector with type DRAKEN_BOOL.
# The bytecode VM (evaluation.pyx) uses BoolVector to:
#   - Type-check columns at runtime (isinstance checks)
#   - Access the unified DrakenVector* layout via .unified()
#   - Wrap raw bitmap results via bool_vector_from_bits()
#
# Bitmap operations manipulate raw uint8_t* bitmaps in the nogil inner loop.

from libc.stdint cimport uint8_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free
from libc.stddef cimport size_t
from draken.core.buffers cimport DrakenVector
from draken.vectors.vector cimport Vector

cdef class BoolVector(Vector):
    """BoolVector — Cython shim for a draken_native Vector with type DRAKEN_BOOL."""
    pass


# Bitmap operation exports (implemented in C/C++, linked into draken_native.so).

cdef extern from "core/bitmap_ops.h" nogil:
    # AND two bitmaps: out = left & right.
    int c_and_bitmap(
        uint8_t* out, uint8_t* out_null,
        const uint8_t* left, const uint8_t* left_null,
        const uint8_t* right, const uint8_t* right_null,
        size_t nbytes, uint32_t num_rows
    ) nogil

    # OR two bitmaps: out = left | right.
    int c_or_bitmap(
        uint8_t* out, uint8_t* out_null,
        const uint8_t* left, const uint8_t* left_null,
        const uint8_t* right, const uint8_t* right_null,
        size_t nbytes, uint32_t num_rows
    ) nogil

    # XOR two bitmaps: out = left ^ right.
    int c_xor_bitmap(
        uint8_t* out, uint8_t* out_null,
        const uint8_t* left, const uint8_t* left_null,
        const uint8_t* right, const uint8_t* right_null,
        size_t nbytes, uint32_t num_rows
    ) nogil

    # NOT a bitmap: out = ~src.
    int c_not_bitmap(
        uint8_t* out, uint8_t* out_null,
        const uint8_t* src, const uint8_t* src_null,
        size_t nbytes, uint32_t num_rows
    ) nogil

    # In-place AND: dst &= src over nbytes bytes (word-wide).
    void c_bitmap_and_inplace(uint8_t* dst, const uint8_t* src, size_t nbytes) nogil

    # Extract bitmap pointers (currently a stub).
    void c_get_bitmap_ptrs(void* draken_vector) nogil

    # Count set bits in a bitmap.
    size_t simd_popcount(const uint8_t* data, size_t nbytes) nogil

# Create a BoolVector from raw bitmap buffers — returns Python object (BoolVector).
# Declared outside nogil block because it returns a Python object.
cdef extern from "core/bitmap_ops.h":
    object bool_vector_from_bits(uint8_t* bitmap, uint8_t* null_bitmap, uint32_t num_rows)


# Cython-level factory function exported via __pyx_capi__.
cdef BoolVector from_decoded(void* data, uint8_t* null_bitmap, size_t length)
