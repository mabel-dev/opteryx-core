# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

"""
LRU caches for Parquet footer data.

ParquetFooterBytesCache  — raw footer envelope bytes (16 MB MemoryPool).
                           Avoids repeated network/disk fetches.

ParquetParsedFooterCache — parsed FileStats structs (≈64 MB, 512 entries).
                           Avoids repeated Thrift deserialization.

Both caches are keyed by the canonical file path (not signed URLs).
Files are immutable, so cached entries never go stale.
"""

import threading
from libc.stdint cimport int64_t, uint8_t
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
from cython.operator cimport dereference as deref

from opteryx.compiled.structures.lru_k cimport LRU_K
from opteryx.compiled.structures.memory_pool cimport MemoryPool
from rugo.parquet_reader cimport FileStats


cdef class ParquetFooterBytesCache:
    """LRU cache for Parquet footer envelope bytes.

    Stores raw footer bytes (magic + footer_data + magic + length) in a MemoryPool
    with LRU eviction. When the pool fills, oldest LRU entries are evicted.

    Thread-safe. This is a process-global cache (one ``_FOOTER_CACHE`` shared by
    every scan in every concurrently-executing query), so it is genuinely shared
    mutable state — per-instance isolation is not possible by design. Under
    free-threading (no GIL) the compound read-check-write sequences over the
    Python ``_path_to_ref`` dict and the (un-synchronised) ``LRU_K`` would race
    across scans, so every public method runs under ``_lock``. The MemoryPool has
    its own internal ``std::mutex``; the ``with nogil`` pool calls below stay inside
    ``_lock`` (a Python lock does not hold the GIL, so releasing the GIL while
    holding it is legal) — ``_lock`` serialises this cache's bookkeeping, the pool
    mutex serialises the pool.
    """

    # cdef attributes declared in footer_cache.pxd

    def __cinit__(self, int64_t pool_size_bytes=16*1024*1024):
        """Initialize footer cache with fixed memory pool.

        Args:
            pool_size_bytes: Size of memory pool (default 16MB)
        """
        self.pool = MemoryPool(pool_size_bytes, name="parquet-footer", auto_resize=False)
        self.lru = LRU_K(k=1, max_memory=0, max_size=0)
        self._path_to_ref = {}
        self._lock = threading.Lock()

    cpdef object get(self, str path):
        """Retrieve cached footer envelope bytes for a path.

        Returns:
            bytes: The footer envelope, or None if not cached.
        """
        with self._lock:
            if path not in self._path_to_ref:
                return None

            # Update LRU access history
            self.lru.get(path.encode())

            # Read from pool and return as bytes (one copy for safety).
            # Consumer is a Python parquet metadata parser, so we use the py_ surface.
            ref_id = self._path_to_ref[path]
            return self.pool.py_read(ref_id, False, False)

    cpdef bint put(self, str path, const uint8_t[::1] envelope):
        """Store footer envelope in cache with LRU tracking.

        On cache miss, commits to pool. If pool fills, evicts LRU entries until
        there's space. Returns False only if pool exhausted and nothing can evict.

        Args:
            path: File path (cache key)
            envelope: Raw footer bytes (magic + footer + magic + length).
                Accepts any contiguous read-only byte buffer (bytes, bytearray,
                memoryview).

        Returns:
            bool: True if stored successfully, False if pool exhausted
        """
        cdef int64_t env_len = envelope.shape[0]
        cdef const void* env_ptr = NULL
        cdef int64_t ref_id
        cdef int64_t old_ref

        if env_len > 0:
            env_ptr = <const void*>&envelope[0]

        with self._lock:
            while True:
                with nogil:
                    ref_id = self.pool.commit(env_ptr, env_len)
                if ref_id != -1:  # Success
                    self._path_to_ref[path] = ref_id
                    self.lru.set(path.encode(), b'', True)  # Dummy value, evict=True
                    return True

                # Pool exhausted, evict oldest LRU entry and retry
                evict_key, _ = self.lru.evict(False)
                if evict_key is None:
                    # Nothing to evict
                    return False

                evict_path = evict_key.decode()
                old_ref = self._path_to_ref.pop(evict_path)
                with nogil:
                    self.pool.release(old_ref)

    cpdef void clear(self):
        """Clear all cached footers and reset pool."""
        cdef int64_t ref_id
        with self._lock:
            # Release all refs from pool
            for ref_id in self._path_to_ref.values():
                with nogil:
                    self.pool.release(ref_id)

            self._path_to_ref.clear()
            self.lru.clear(False)

    cpdef dict stats(self):
        """Return cache statistics.

        Returns:
            dict with keys:
                - cached_paths: Number of cached paths
                - pool_used: Bytes used in pool
                - pool_free: Bytes free in pool
                - pool_size: Total pool size
                - lru_hits: LRU hits
                - lru_misses: LRU misses
        """
        # Python-surface getter — consumer is a Python dict
        with self._lock:
            pool_stats = self.pool.py_get_stats()
            lru_stats = self.lru.stats
            cached_paths = len(self._path_to_ref)

        return {
            "cached_paths": cached_paths,
            "pool_used": pool_stats["used_size"],
            "pool_free": pool_stats["free_size"],
            "pool_size": pool_stats["total_size"],
            "lru_hits": lru_stats[0],
            "lru_misses": lru_stats[1],
        }


cdef class ParquetParsedFooterCache:
    """LRU cache for parsed Parquet FileStats structs.

    Caches the result of Thrift deserialization so repeated reads of the same
    file skip parsing entirely. Holds up to max_entries (default 512) parsed
    footers — approximately 64 MB assuming ~128 KB per FileStats.

    Files are immutable; cached entries never go stale.

    Thread-safe. Process-global singleton (_PARSED_FOOTER_CACHE in pool_reader).
    All methods that touch the C++ unordered_map run under _lock.

    try_get / put_fs are cdef (Cython-only) because FileStats is a C++ struct.
    """

    def __cinit__(self, int max_entries=512):
        self.lru = LRU_K(k=1, max_memory=0, max_size=0)
        self._max_entries = max_entries
        self._lock = threading.Lock()

    cdef bint try_get(self, str path, FileStats* out):
        """Return True and fill *out if path is cached, else return False."""
        cdef string key = path.encode('utf-8')
        cdef unordered_map[string, FileStats].iterator it
        with self._lock:
            it = self._map.find(key)
            if it == self._map.end():
                return False
            out[0] = deref(it).second
            self.lru.get(key)
            return True

    cdef void put_fs(self, str path, const FileStats& fs):
        """Store a parsed FileStats under path, evicting LRU entries if full."""
        cdef string key = path.encode('utf-8')
        cdef bytes evict_key
        with self._lock:
            # Already cached — refresh LRU position.
            if self._map.count(key):
                self._map[key] = fs
                self.lru.get(key)
                return
            # Evict until under the entry cap.
            while <int>self._map.size() >= self._max_entries:
                evict_key_obj, _ = self.lru.evict(False)
                if evict_key_obj is None:
                    return
                evict_key = <bytes>evict_key_obj
                self._map.erase(<string>evict_key)
            self._map[key] = fs
            self.lru.set(key, b'', True)

    cpdef void clear(self):
        """Evict all cached entries."""
        with self._lock:
            self._map.clear()
            self.lru.clear(False)

    cpdef dict stats(self):
        """Return cache statistics."""
        with self._lock:
            lru_stats = self.lru.stats
            count = self._map.size()
        return {
            "cached_paths": count,
            "max_entries": self._max_entries,
            "lru_hits": lru_stats[0],
            "lru_misses": lru_stats[1],
        }
