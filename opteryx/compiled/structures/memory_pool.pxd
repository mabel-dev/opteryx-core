# Cython declaration file for MemoryPool
# Allows other .pyx modules to use typed references via cimport

from libc.stdint cimport int64_t, uint8_t
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map

cdef struct MemorySegment:
    int64_t start
    int64_t length
    int64_t latches
    int64_t ref_id
    bint is_free

cdef struct SegmentMetadata:
    int64_t start
    int64_t length
    int64_t latches
    int64_t orig_length

cdef class MemoryPool:
    cdef:
        unsigned char* pool
        public int64_t size
        public int64_t used_size
        public vector[MemorySegment] segments
        unordered_map[int64_t, SegmentMetadata] c_metadata
        public str name
        public int64_t commits, failed_commits, reads, read_locks
        public int64_t l1_compaction, l2_compaction, releases, resizes
        object lock
        int64_t next_ref_id
        int64_t alignment
        bint auto_resize

    cpdef int64_t commit(self, const uint8_t[::1] data)
    cpdef bytes read(self, int64_t ref_id, bint zero_copy, bint latch)
    cpdef void release(self, int64_t ref_id)
    cpdef void latch(self, int64_t ref_id)
    cpdef void unlatch(self, int64_t ref_id)
    cpdef void clear(self)
    cpdef dict get_stats(self)
    cpdef list get_free_segments(self)
    cpdef int64_t get_fragmentation(self)
    cpdef tuple reserve_for_write_ptr(self, int64_t size)
    cpdef void finalize_commit(self, int64_t ref_id, int64_t actual_length)
    cpdef void _level1_compaction(self)
    cpdef void _level2_compaction(self)
    cdef void _print_stats(self)
    cdef bint _resize_pool(self, int64_t new_size)
    cdef inline int64_t _find_best_fit_segment(self, int64_t size)
    cdef inline void _merge_adjacent_free_segments(self)
    cdef void _defragment_memory(self)
