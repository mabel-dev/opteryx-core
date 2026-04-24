"""
LRU cache for raw Parquet footer bytes using MemoryPool storage.

Files are immutable, so we cache raw footer envelope bytes to avoid repeated
GCS fetches. The cache uses a fixed 16MB MemoryPool with LRU eviction.

Key: file path (str)
Value: raw footer envelope bytes (stored in MemoryPool, accessed via ref_id)
"""

from typing import Optional

from opteryx.compiled.structures.lru_k import LRU_K
from opteryx.compiled.structures.memory_pool import MemoryPool


cdef class ParquetFooterBytesCache:
    """LRU cache for Parquet footer envelope bytes.

    Stores raw footer bytes (magic + footer_data + magic + length) in a MemoryPool
    with LRU eviction. When the pool fills, oldest LRU entries are evicted.

    Thread-safe: both MemoryPool and LRU_K use RLock internally.
    """

    cdef MemoryPool pool
    cdef LRU_K lru
    cdef dict _path_to_ref  # path -> ref_id mapping

    def __cinit__(self, int64_t pool_size_bytes=16*1024*1024):
        """Initialize footer cache with fixed memory pool.

        Args:
            pool_size_bytes: Size of memory pool (default 16MB)
        """
        self.pool = MemoryPool(pool_size_bytes, name="parquet-footer", auto_resize=False)
        self.lru = LRU_K(k=1, max_memory=0, max_size=0)
        self._path_to_ref = {}

    cpdef Optional[bytes] get(self, str path):
        """Retrieve cached footer envelope bytes for a path.

        Returns:
            bytes: The footer envelope, or None if not cached.
        """
        if path not in self._path_to_ref:
            return None

        # Update LRU access history
        self.lru.get(path.encode())

        # Read from pool and return as bytes (one copy for safety)
        ref_id = self._path_to_ref[path]
        return self.pool.read(ref_id, zero_copy=False)

    cpdef bint put(self, str path, bytes envelope):
        """Store footer envelope in cache with LRU tracking.

        On cache miss, commits to pool. If pool fills, evicts LRU entries until
        there's space. Returns False only if pool exhausted and nothing can evict.

        Args:
            path: File path (cache key)
            envelope: Raw footer bytes (magic + footer + magic + length)

        Returns:
            bool: True if stored successfully, False if pool exhausted
        """
        while True:
            ref_id = self.pool.commit(envelope)
            if ref_id != -1:  # Success
                self._path_to_ref[path] = ref_id
                self.lru.set(path.encode(), b'')  # Dummy value
                return True

            # Pool exhausted, evict oldest LRU entry and retry
            evict_key, _ = self.lru.evict()
            if evict_key is None:
                # Nothing to evict
                return False

            evict_path = evict_key.decode()
            old_ref = self._path_to_ref.pop(evict_path)
            self.pool.release(old_ref)

    cpdef void clear(self):
        """Clear all cached footers and reset pool."""
        # Release all refs from pool
        for ref_id in self._path_to_ref.values():
            self.pool.release(ref_id)

        self._path_to_ref.clear()
        self.lru.clear()

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
        pool_stats = self.pool.get_stats()
        lru_stats = self.lru.stats

        return {
            "cached_paths": len(self._path_to_ref),
            "pool_used": pool_stats["used_size"],
            "pool_free": pool_stats["free_size"],
            "pool_size": pool_stats["total_size"],
            "lru_hits": lru_stats[0],
            "lru_misses": lru_stats[1],
        }
