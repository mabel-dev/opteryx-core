# Cython declaration file for MemoryPool
# Allows other .pyx modules to use typed references via cimport

from libc.stdint cimport int64_t
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
