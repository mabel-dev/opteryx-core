# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True
# cython: freethreading_compatible=True

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

from libc.stdint cimport uint64_t, int64_t, uint8_t
from libc.string cimport memset

from cpython.mem cimport PyMem_Calloc, PyMem_Free

from draken.core.buffers cimport (
    DrakenVector,
)
from opteryx.compiled.structures.relation_statistics cimport to_int

cdef const uint64_t MIX_HASH_CONSTANT = <uint64_t>0x9e3779b97f4a7c15ULL
cdef const uint64_t NULL_HASH = <uint64_t>0x4c3f95a36ab8eccaULL

# Sentinel for zero-length hash returns — valid memory, never accessed.
cdef uint64_t _EMPTY_UINT64_SENTINEL = 0

# Platform-correct buffer-protocol format string for uint64_t.
# 'Q' on macOS/ARM (uint64_t = unsigned long long);
# 'L' on Linux x86-64 (uint64_t = unsigned long).
# Probed once at module init so __getbuffer__ is always right.
cdef bytes _uint64_format_bytes

def _probe_uint64_fmt():
    cdef uint64_t[1] probe
    probe[0] = 0
    cdef uint64_t[::1] view = probe
    return memoryview(view).format.encode("ascii") + b"\x00"

_uint64_format_bytes = _probe_uint64_fmt()


cdef class _Uint64Buffer:
    """Heap-allocated uint64_t array with proper Python lifetime management.

    Exposes the buffer protocol so a ``uint64_t[::1]`` typed memoryview can be
    created from it.  The buffer is freed in ``__dealloc__``, so as long as at
    least one typed memoryview (or Python memoryview) holds a reference to this
    object, the memory is alive.
    """

    def __cinit__(self):
        self.data = NULL
        self.n = 0

    @staticmethod
    cdef _Uint64Buffer create(Py_ssize_t n):
        cdef _Uint64Buffer self = _Uint64Buffer.__new__(_Uint64Buffer)
        self.n = n
        self.data = <uint64_t*>PyMem_Calloc(n, sizeof(uint64_t))
        if self.data == NULL:
            raise MemoryError()
        self._shape[0] = n
        self._strides[0] = <Py_ssize_t>sizeof(uint64_t)
        return self

    def __dealloc__(self):
        if self.data != NULL:
            PyMem_Free(self.data)
            self.data = NULL

    def __getbuffer__(self, Py_buffer* view, int flags):
        view.buf = self.data
        view.obj = self          # INCREF self; PyBuffer_Release will DECREF it
        view.len = self.n * sizeof(uint64_t)
        view.readonly = 0
        view.itemsize = sizeof(uint64_t)
        view.ndim = 1
        view.shape = self._shape
        view.strides = self._strides
        view.suboffsets = NULL
        view.format = <char*>_uint64_format_bytes

    def __releasebuffer__(self, Py_buffer* view):
        pass  # reference released via view.obj by PyBuffer_Release

cdef class Vector:

    def __cinit__(self):
        pass

    cpdef object null_bitmap(self):
        """Return the null bitmap for this vector, or ``None`` when the vector has no nulls."""
        return None

    cdef uint8_t* null_bitmap_ptr(self) noexcept:
        """Return the per-row validity bitmap, or NULL if all rows valid.

        Reads through the unified view so the answer is correct for every
        layout. Equivalent to `self.unified().validity`.
        """
        return self.unified().validity

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

    cdef bint c_hash_into(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        """Nogil hash variant. Returns 0 on success, 1 if GIL is required."""
        return 1  # base class / unknown type; caller must fall back to hash_into

    cdef bint c_hash_single(self, uint64_t* out, Py_ssize_t n) noexcept nogil:
        """Single-column hash: out[i] = hash(row_i), no prior dest state.
        Falls back to c_hash_into on a zeroed buffer for unknown types."""
        memset(out, 0, <size_t>n * sizeof(uint64_t))
        return self.c_hash_into(out, n)

    cpdef uint64_t[::1] hash(self):
        """Create an output buffer, call `hash_into`, and return the buffer.

        This is a Python-callable helper for convenience in tests and callers
        that want a standalone hash for a single vector.
        """
        cdef Py_ssize_t n = len(self)
        if n == 0:
            return <uint64_t[:0:1]>&_EMPTY_UINT64_SENTINEL

        cdef _Uint64Buffer backing = _Uint64Buffer.create(n)
        cdef uint64_t[::1] out_view = backing
        self.hash_into(out_view, 0)
        return out_view

    cdef void compress_into(self, int64_t[::1] out_buf, Py_ssize_t offset=0) except *:
        """Default compress_into implementation.

        If a concrete vector implements its own `compress_into`, that will be
        invoked. Otherwise we fall back to a generic implementation that
        iterates Python values one-at-a-time (not materializing the full list)
        and uses `to_int` from `opteryx.compiled.structures.relation_statistics`
        to map each value to an int64, writing into `out_buf` (starting at `offset`).
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

        # Iterate one item at a time without materializing full list
        cdef Py_ssize_t i
        cdef object item
        for i in range(n):
            item = self[i]
            out_buf[offset + i] = <int64_t>to_int(item)

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

    cpdef int compare_at(self, Py_ssize_t left_idx, Py_ssize_t right_idx) except? 0:
        """Compare values at two indices. Subclasses must override."""
        raise NotImplementedError(f"{type(self).__name__} does not implement compare_at")

    cpdef bint is_null_at(self, Py_ssize_t idx) except? False:
        """Check if value at index is null. Subclasses must override."""
        raise NotImplementedError(f"{type(self).__name__} does not implement is_null_at")

    cpdef Vector materialize(self):
        """Return a dense (non-encoded) version of this vector.

        Default implementation returns self — the vector is already dense.
        Subclasses with dictionary, constant, or RLE encoding must override
        to expand to a dense representation without going through Arrow.
        """
        return self

    cdef object item_at(self, Py_ssize_t i):
        return self.__getitem__(i)

    @property
    def nbytes(self):
        """Approximate memory footprint in bytes.

        Default returns 0. Concrete vector types override with accurate counts
        so Morsel.nbytes can sum all columns without touching Arrow.
        """
        return 0

    cdef DrakenVector* unified(self) noexcept:
        """Return the unified view for this vector.

        Concrete types set _unified_view directly at construction.
        The pointer is &self._unified_view — lifetime == self.
        """
        return &self._unified_view

