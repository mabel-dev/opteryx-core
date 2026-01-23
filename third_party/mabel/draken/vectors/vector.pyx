# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Base Vector class for Draken columnar data structures.

This module provides the abstract base class for all Vector implementations
in Draken. Vectors are columnar data containers that provide:
- Zero-copy interoperability with Apache Arrow
- Efficient memory layout for analytical workloads
- Type-specific optimized implementations

The Vector class defines the common interface that all concrete vector
types (Int64Vector, StringVector, etc.) implement.
"""

from libc.stdint cimport uint64_t, int64_t
from cpython.mem cimport PyMem_Calloc

from opteryx.draken.interop.arrow cimport vector_from_arrow
from opteryx.compiled.structures.relation_statistics cimport to_int

cdef const uint64_t MIX_HASH_CONSTANT = <uint64_t>0x9e3779b97f4a7c15ULL
cdef const uint64_t NULL_HASH = <uint64_t>0x4c3f95a36ab8eccaULL

cdef class Vector:

    @classmethod
    def from_arrow(cls, arrow_array):
        return vector_from_arrow(arrow_array)

    cpdef object null_bitmap(self):
        """Return the null bitmap for this vector, or ``None`` when the vector has no nulls."""
        return None

    def __str__(self):
        return f"<{self.__class__.__name__} len={len(self)}>"

    cdef void hash_into(
        self,
        uint64_t[::1] out_buf,
        Py_ssize_t offset=0,
    ) except *:
        """Default implementation delegates to Python overrides when available."""
        cdef object py_self = <object>self
        cdef object py_hash = getattr(py_self, "hash_into", None)

        if py_hash is None:
            raise NotImplementedError(
                f"{self.__class__.__name__} does not implement hash_into"
            )

        py_hash(out_buf, offset=offset)

    cpdef uint64_t[::1] hash(self):
        """Create an output buffer, call `hash_into`, and return the buffer.

        This is a Python-callable helper for convenience in tests and callers
        that want a standalone hash for a single vector.
        """
        cdef Py_ssize_t n = len(self)
        if n == 0:
            # Return an empty Python array so `memoryview()` sees format 'Q'.
            from array import array
            return array("Q")

        cdef uint64_t* out_buf = <uint64_t*> PyMem_Calloc(n, sizeof(uint64_t))
        if out_buf == NULL:
            raise MemoryError()

        cdef uint64_t[::1] out_view = <uint64_t[:n]> out_buf
        # Delegate to the low-level implementation
        self.hash_into(out_view, 0)
        return out_view

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Default compress_into implementation.

        If a concrete vector implements its own `compress_into`, that will be
        invoked. Otherwise we fall back to a generic implementation that
        iterates Python values and uses `to_int` from
        `opteryx.compiled.structures.relation_statistics` to map each value
        to an int64, writing into `out_buf` (starting at `offset`).
        """
        cdef object py_self = <object> self
        # Check for Python override (or per-concrete-class override)
        cdef object py_comp = getattr(py_self, "compress_into", None)
        if py_comp is not None:
            # A Python-level implementation exists on the instance/class
            py_comp(out_buf, offset=offset)
            return

        cdef Py_ssize_t n = len(self)
        # Validate buffer size
        if out_buf.shape[0] - offset < n:
            raise ValueError(f"output buffer too small")

        cdef Py_ssize_t i
        try:
            vals = self.to_pylist()
        except Exception:
            # Fallback: try iterating
            vals = [self[i] for i in range(n)]

        for i in range(n):
            out_buf[offset + i] = <int64_t>to_int(vals[i])

    cpdef int64_t[::1] compress(self):
        """Allocate an int64 buffer, call `compress`, and return the buffer.

        Returns a memoryview compatible with `array('q')` (format 'q'). For
        empty vectors returns an empty `array('q')`.
        """
        cdef Py_ssize_t n = len(self)
        if n == 0:
            from array import array
            return array("q")

        cdef int64_t* out_buf = <int64_t*> PyMem_Calloc(n, sizeof(int64_t))
        if out_buf == NULL:
            raise MemoryError()

        cdef int64_t[::1] out_view = <int64_t[:n]> out_buf
        self.compress_into(out_view, 0)
        return out_view
