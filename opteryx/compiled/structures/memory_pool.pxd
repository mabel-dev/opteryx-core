# distutils: language = c++
# cython: language_level=3
from libc.stdint cimport int64_t, uint8_t, uintptr_t
from libcpp.vector cimport vector
from libcpp.string cimport string

cdef extern from "memory_pool.hpp" namespace "opteryx":
    cdef struct PoolStats:
        int64_t total_size
        int64_t used_size
        int64_t free_size
        int64_t used_blocks
        int64_t free_blocks
        int64_t largest_free_block
        int64_t fragmentation
        int64_t commits
        int64_t failed_commits
        int64_t reads
        int64_t releases
        int64_t compactions
        int64_t resizes

    cdef struct ReadResult:
        const void* ptr
        int64_t length

    cdef struct ReserveResult:
        int64_t ref_id
        void* ptr
        int64_t capacity

    cdef struct MetadataSnapshot:
        int64_t ref_id
        int64_t start
        int64_t length
        int64_t latches
        int64_t orig_length

    cdef struct FreeSegmentSnapshot:
        int64_t start
        int64_t length

    cdef cppclass CppMemoryPool "opteryx::MemoryPool":
        CppMemoryPool(int64_t size, string name, bint auto_resize, int64_t alignment) except +
        int64_t commit(const void* data, int64_t length) except + nogil
        ReadResult read(int64_t ref_id, bint latch) except + nogil
        void release(int64_t ref_id) except + nogil
        void latch(int64_t ref_id) except + nogil
        void unlatch(int64_t ref_id) except + nogil
        ReserveResult reserve_for_write(int64_t size) except + nogil
        void finalize_commit(int64_t ref_id, int64_t actual_length) except + nogil
        void clear() except + nogil
        int64_t available_space() except + nogil
        int64_t get_fragmentation() except + nogil
        PoolStats get_stats() except + nogil
        void compaction() except + nogil
        vector[MetadataSnapshot] snapshot_metadata() except + nogil
        vector[FreeSegmentSnapshot] snapshot_free_segments() except + nogil


cdef class MemoryPool:
    cdef CppMemoryPool* _pool

    # ─── Cython-native surface (no Python objects, nogil-safe) ──────────────
    cdef int64_t        commit(self, const void* data, int64_t length)               except + nogil
    cdef ReadResult     read(self, int64_t ref_id, bint latch)                       except + nogil
    cdef ReserveResult  reserve_for_write(self, int64_t size)                        except + nogil
    cdef void           finalize_commit(self, int64_t ref_id, int64_t actual_length) except + nogil
    cdef void           release(self, int64_t ref_id)                                except + nogil
    cdef void           latch(self, int64_t ref_id)                                  except + nogil
    cdef void           unlatch(self, int64_t ref_id)                                except + nogil
    cdef void           clear(self)                                                  except + nogil
    cdef int64_t        available_space(self)                                        except + nogil
    cdef int64_t        get_fragmentation(self)                                      except + nogil
    cdef void           compaction(self)                                             except + nogil
    cdef vector[MetadataSnapshot]    snapshot_metadata(self)                         except + nogil
    cdef vector[FreeSegmentSnapshot] snapshot_free_segments(self)                    except + nogil

    # ─── Python surface (py_* prefix; for tests and rare Python callers) ────
    cpdef int64_t  py_commit(self, const uint8_t[::1] data)
    cpdef object   py_read(self, int64_t ref_id, bint zero_copy=*, bint latch=*)
    cpdef tuple    py_reserve_for_write_ptr(self, int64_t size)
    cpdef void     py_finalize_commit(self, int64_t ref_id, int64_t actual_length)
    cpdef void     py_release(self, int64_t ref_id)
    cpdef void     py_latch(self, int64_t ref_id)
    cpdef void     py_unlatch(self, int64_t ref_id)
    cpdef void     py_clear(self)
    cpdef int64_t  py_available_space(self)
    cpdef int64_t  py_get_fragmentation(self)
    cpdef dict     py_get_stats(self)
    cpdef list     py_get_free_segments(self)
    cpdef void     py_compaction(self)
