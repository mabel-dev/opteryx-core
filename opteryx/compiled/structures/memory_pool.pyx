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
            'peak_used_size': s.peak_used_size,
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


# The process memory account (draken/core/mem_account.h). The account itself
# lives in draken_native; these read it for query telemetry.
cdef extern from "core/mem_account.h" nogil:
    int64_t draken_mem_charged()
    int64_t draken_mem_peak()
    void draken_mem_reset_peak()


def process_memory_charged() -> int:
    """Bytes currently charged to the process memory account."""
    return draken_mem_charged()


def process_memory_peak() -> int:
    """High-water mark of the process memory account since the last reset."""
    return draken_mem_peak()


def reset_process_memory_peak() -> None:
    """Restart the high-water mark at the current charge (query start)."""
    draken_mem_reset_peak()


cdef extern from "core/mem_account.h" nogil:
    void draken_mem_configure(int64_t container_bytes, int64_t reserve_bytes, int64_t cache_budget_bytes)
    int64_t draken_mem_container()
    int64_t draken_mem_reserve()
    int64_t draken_mem_cache_budget()
    int64_t draken_mem_cache_limit()
    int64_t draken_mem_cache_bytes()
    int64_t draken_mem_compress_rate_max()
    int64_t draken_mem_pressure_samples()


cdef extern from "core/slab_pool.h" nogil:
    ctypedef struct DrakenSlabStats:
        int64_t reserved_bytes
        int64_t mapped_bytes
        int64_t faulted_bytes
        int64_t in_use_bytes
        int64_t free_faulted_bytes
        int64_t class_slabs
        int64_t acquires
        int64_t faults
        int64_t give_backs
        int64_t huge_pages
    void draken_slab_configure(int64_t budget_bytes, int64_t prefault_bytes)
    int64_t draken_slab_budget_bytes()
    int64_t draken_slab_prefault_bytes()
    DrakenSlabStats draken_slab_stats()


cdef extern from "core/chunk_cache.h" nogil:
    ctypedef struct DrakenChunkCacheStats:
        int64_t bytes
        int64_t entries
        int64_t hits
        int64_t misses
        int64_t inserts
        int64_t refused
        int64_t evictions
        int64_t give_way_bytes
        int64_t probation_bytes
        int64_t promotions
        int64_t remembered_hits
        int64_t compressed_bytes
        int64_t compressed_hits
        int64_t swaps
    DrakenChunkCacheStats draken_cc_stats()
    void draken_cc_flush()


# The cache is off below this budget: a small install would pay the fill cost
# and thrash (ruling, docs/C7_PAGE_CACHE_DESIGN.md §14).
CHUNK_CACHE_MIN_BUDGET_BYTES = 2 << 30


def configure_chunk_cache(int64_t container_bytes, int64_t memory_percent, int64_t reserve_bytes) -> int:
    """Configure the process memory account and return the cache budget it got.

    budget = memory_percent% x container - reserve; under 2 GiB the cache is
    off (budget 0). Called once at import with the config values."""
    if container_bytes < 0 or reserve_bytes < 0 or not (0 <= memory_percent <= 100):
        raise ValueError(
            f"chunk cache configuration out of range: container={container_bytes}, "
            f"percent={memory_percent}, reserve={reserve_bytes}"
        )
    cdef int64_t budget = container_bytes * memory_percent // 100 - reserve_bytes
    if budget < CHUNK_CACHE_MIN_BUDGET_BYTES:
        budget = 0
    draken_mem_configure(container_bytes, reserve_bytes, budget)
    if budget == 0:
        draken_cc_flush()
    return budget


def chunk_cache_budget_bytes() -> int:
    """The chunk cache's configured budget (0 = off)."""
    return draken_mem_cache_budget()


def configure_slab_pool(int64_t budget_bytes, int64_t prefault_bytes) -> None:
    """Set the slab pool's retained-free budget and boot prefault (slab_pool.h).

    Called once at import with the config values; a prefault faults that many
    bytes in right here."""
    if budget_bytes < 0 or prefault_bytes < 0:
        raise ValueError(
            f"slab pool configuration out of range: budget={budget_bytes}, prefault={prefault_bytes}"
        )
    draken_slab_configure(budget_bytes, prefault_bytes)


def slab_pool_budget_bytes() -> int:
    """Bytes of free slots the slab pool keeps faulted."""
    return draken_slab_budget_bytes()


def slab_pool_prefault_bytes() -> int:
    """Bytes the slab pool faulted in at import."""
    return draken_slab_prefault_bytes()


def slab_pool_stats() -> dict:
    """Process-lifetime slab pool counters (slab_pool.h DrakenSlabStats)."""
    cdef DrakenSlabStats s = draken_slab_stats()
    return {
        "reserved_bytes": s.reserved_bytes,
        "mapped_bytes": s.mapped_bytes,
        "faulted_bytes": s.faulted_bytes,
        "in_use_bytes": s.in_use_bytes,
        "free_faulted_bytes": s.free_faulted_bytes,
        "class_slabs": s.class_slabs,
        "acquires": s.acquires,
        "faults": s.faults,
        "give_backs": s.give_backs,
        "huge_pages": s.huge_pages,
    }


def chunk_cache_stats() -> dict:
    """Process-lifetime chunk cache counters plus the current limit."""
    cdef DrakenChunkCacheStats s = draken_cc_stats()
    return {
        "bytes": s.bytes,
        "entries": s.entries,
        "hits": s.hits,
        "misses": s.misses,
        "inserts": s.inserts,
        "refused": s.refused,
        "evictions": s.evictions,
        "give_way_bytes": s.give_way_bytes,
        "probation_bytes": s.probation_bytes,
        "promotions": s.promotions,
        "remembered_hits": s.remembered_hits,
        "compressed_bytes": s.compressed_bytes,
        "compressed_hits": s.compressed_hits,
        "swaps": s.swaps,
        "budget": draken_mem_cache_budget(),
        "limit": draken_mem_cache_limit(),
        # Compressor pressure since the last peak reset (query start): highest
        # compression rate sampled (pages/s) and samples over the give-way threshold.
        "compress_rate_max": draken_mem_compress_rate_max(),
        "pressure_samples": draken_mem_pressure_samples(),
        "container": draken_mem_container(),
        "reserve": draken_mem_reserve(),
    }


def chunk_cache_headroom() -> int:
    """Bytes the chunk cache can still take before it must evict: its current
    limit, min(budget, container - reserve - charged query memory), less the
    bytes it already holds. 0 when the cache is off or full."""
    cdef int64_t room = draken_mem_cache_limit() - draken_mem_cache_bytes()
    return room if room > 0 else 0


def flush_chunk_cache() -> None:
    """Drop every chunk cache entry (pinned ones go when their readers finish)."""
    draken_cc_flush()
