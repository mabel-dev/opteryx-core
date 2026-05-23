# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True
# distutils: language = c++

"""
Two surfaces:

  * Cython-native (unprefixed, `cdef`, nogil-safe) — for production callers
    that `cimport MemoryPool` and want zero Python overhead. Returns raw
    structs (ReadResult, ReserveResult, etc.). Callers handle latch
    discipline explicitly.

  * Python-visible (`py_*` prefix, `cpdef`) — for tests and rare Python
    callers. Constructs Python objects (bytes, memoryview, dict, ...) at
    the boundary. Pays the GIL/allocation tax; that's the point of the
    prefix — to make the cost visible at every call site.
"""
from libc.stdint cimport int64_t, uintptr_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from cpython.bytes cimport PyBytes_FromStringAndSize


cdef class MemoryPool:
    def __cinit__(self, int64_t size, str name="Memory Pool",
                  bint auto_resize=False, int64_t alignment=1):
        cdef string cpp_name = name.encode("utf-8")
        # Constructor may throw std::invalid_argument or std::bad_alloc;
        # except+ translates to Python ValueError / MemoryError.
        self._pool = new CppMemoryPool(size, cpp_name, auto_resize, alignment)

    def __dealloc__(self):
        if self._pool is not NULL:
            del self._pool
            self._pool = NULL

    # ─────────────────────────────────────────────────────────────────────
    # Cython-native surface
    # All methods are nogil-callable and return raw C/C++ types.
    # Single-line pass-throughs to the C++ pool.
    # ─────────────────────────────────────────────────────────────────────

    cdef int64_t commit(self, const void* data, int64_t length) except + nogil:
        return self._pool.commit(data, length)

    cdef ReadResult read(self, int64_t ref_id, bint latch) except + nogil:
        return self._pool.read(ref_id, latch)

    cdef ReserveResult reserve_for_write(self, int64_t size) except + nogil:
        return self._pool.reserve_for_write(size)

    cdef void finalize_commit(self, int64_t ref_id, int64_t actual_length) except + nogil:
        self._pool.finalize_commit(ref_id, actual_length)

    cdef void release(self, int64_t ref_id) except + nogil:
        self._pool.release(ref_id)

    cdef void latch(self, int64_t ref_id) except + nogil:
        self._pool.latch(ref_id)

    cdef void unlatch(self, int64_t ref_id) except + nogil:
        self._pool.unlatch(ref_id)

    cdef void clear(self) except + nogil:
        self._pool.clear()

    cdef int64_t available_space(self) except + nogil:
        return self._pool.available_space()

    cdef int64_t get_fragmentation(self) except + nogil:
        return self._pool.get_fragmentation()

    cdef void compaction(self) except + nogil:
        self._pool.compaction()

    cdef vector[MetadataSnapshot] snapshot_metadata(self) except + nogil:
        return self._pool.snapshot_metadata()

    cdef vector[FreeSegmentSnapshot] snapshot_free_segments(self) except + nogil:
        return self._pool.snapshot_free_segments()

    # ─────────────────────────────────────────────────────────────────────
    # Python surface (`py_*` prefix). Builds Python objects at the boundary.
    # Used by tests and a small number of Python-level callers.
    # ─────────────────────────────────────────────────────────────────────

    cpdef int64_t py_commit(self, const uint8_t[::1] data):
        cdef int64_t length = data.shape[0]
        cdef const void* ptr = NULL
        cdef int64_t ref_id

        if length > 0:
            ptr = <const void*>&data[0]

        with nogil:
            ref_id = self._pool.commit(ptr, length)

        return ref_id

    cpdef object py_read(self, int64_t ref_id, bint zero_copy=False, bint latch=False):
        cdef ReadResult result

        with nogil:
            result = self._pool.read(ref_id, latch)

        # Zero-length: return empty memoryview or empty bytes
        if result.length == 0:
            if zero_copy:
                return memoryview(b"")
            return b""

        if zero_copy:
            return memoryview(<char[:result.length]>(<char*>result.ptr))

        return PyBytes_FromStringAndSize(<char*>result.ptr, result.length)

    cpdef tuple py_reserve_for_write_ptr(self, int64_t size):
        cdef ReserveResult result
        with nogil:
            result = self._pool.reserve_for_write(size)
        return (result.ref_id, <uintptr_t>result.ptr, result.capacity)

    cpdef void py_finalize_commit(self, int64_t ref_id, int64_t actual_length):
        with nogil:
            self._pool.finalize_commit(ref_id, actual_length)

    cpdef void py_release(self, int64_t ref_id):
        with nogil:
            self._pool.release(ref_id)

    cpdef void py_latch(self, int64_t ref_id):
        with nogil:
            self._pool.latch(ref_id)

    cpdef void py_unlatch(self, int64_t ref_id):
        with nogil:
            self._pool.unlatch(ref_id)

    cpdef void py_clear(self):
        with nogil:
            self._pool.clear()

    cpdef int64_t py_available_space(self):
        cdef int64_t result
        with nogil:
            result = self._pool.available_space()
        return result

    cpdef int64_t py_get_fragmentation(self):
        cdef int64_t result
        with nogil:
            result = self._pool.get_fragmentation()
        return result

    cpdef void py_compaction(self):
        with nogil:
            self._pool.compaction()

    cpdef dict py_get_stats(self):
        cdef PoolStats s
        with nogil:
            s = self._pool.get_stats()
        return {
            'total_size': s.total_size,
            'used_size': s.used_size,
            'free_size': s.free_size,
            'used_blocks': s.used_blocks,
            'free_blocks': s.free_blocks,
            'largest_free_block': s.largest_free_block,
            'fragmentation': s.fragmentation,
            'commits': s.commits,
            'failed_commits': s.failed_commits,
            'reads': s.reads,
            'releases': s.releases,
            'compactions': s.compactions,
            'resizes': s.resizes,
        }

    cpdef list py_get_free_segments(self):
        cdef vector[FreeSegmentSnapshot] snap
        with nogil:
            snap = self._pool.snapshot_free_segments()

        cdef size_t i
        cdef list out = []
        for i in range(snap.size()):
            out.append({"start": snap[i].start, "length": snap[i].length})

        return out

    # Python-only introspection properties (used by tests).
    @property
    def py_used_segments(self):
        cdef vector[MetadataSnapshot] snap
        with nogil:
            snap = self._pool.snapshot_metadata()

        cdef size_t i
        cdef dict out = {}
        for i in range(snap.size()):
            out[snap[i].ref_id] = {
                "start": snap[i].start,
                "length": snap[i].length,
                "latches": snap[i].latches,
                "orig_length": snap[i].orig_length,
            }

        return out

    @property
    def py_free_segments(self):
        return self.py_get_free_segments()

    @property
    def py_size(self):
        return self._pool.get_stats().total_size
