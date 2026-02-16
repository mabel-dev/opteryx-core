"""
Paged Memory Pool

A wrapper around multiple Cython MemoryPool instances to reduce lock contention
in multi-threaded scenarios. Distributes allocations across pages using round-robin
selection with timeout-based failover.

Each page is an independent MemoryPool with its own lock, allowing concurrent
commits to different pages without serialization.
"""

import os
import threading
from typing import Optional
from typing import Union

from opteryx import config
from opteryx.compiled.structures.memory_pool import MemoryPool


class PagedMemoryPool:
    """
    Paged memory pool that distributes allocations across multiple MemoryPool
    instances to reduce lock contention.

    Uses round-robin page selection with timeout-based failover to avoid deadlocks
    while maintaining balanced load distribution.

    The interface matches MemoryPool exactly, making it a drop-in replacement.
    """

    def __init__(
        self,
        page_size: Optional[int] = None,
        num_pages: Optional[int] = None,
        name: str = "Paged Memory Pool",
        auto_resize: bool = False,
        alignment: int = 1,
        lock_timeout_ms: Optional[int] = None,
    ):
        """
        Initialize paged memory pool.

        Args:
            page_size: Size of each page in bytes (default: from config, 512MB)
            num_pages: Number of pages (default: from config, CPU count min 2)
            name: Pool name for debugging
            auto_resize: Whether pages can auto-resize when full
            alignment: Memory alignment for allocations (power of 2)
            lock_timeout_ms: Timeout for lock acquisition in milliseconds (default: from config, 100ms)
        """
        # Use config defaults if not specified
        if page_size is None:
            page_size = config.READ_BUFFER_PAGE_SIZE
        if num_pages is None:
            num_pages = config.READ_BUFFER_NUM_PAGES
        if lock_timeout_ms is None:
            lock_timeout_ms = config.READ_BUFFER_LOCK_TIMEOUT_MS

        # Determine number of pages if still None
        if num_pages is None:
            num_pages = max(2, os.cpu_count() or 2)

        if num_pages < 1:
            raise ValueError("num_pages must be at least 1")

        if page_size <= 0:
            raise ValueError("page_size must be positive")

        self.page_size = page_size
        self.num_pages = num_pages
        self.name = name
        self.lock_timeout_ms = lock_timeout_ms
        self.lock_timeout_sec = lock_timeout_ms / 1000.0

        # Create individual pages (Cython MemoryPool instances) with Python locks
        # Each page has its own lock for independent locking
        self.pages = []
        self.page_locks = []
        for i in range(num_pages):
            page = MemoryPool(
                size=page_size, name=f"{name}-Page{i}", auto_resize=auto_resize, alignment=alignment
            )
            page_lock = threading.RLock()
            self.pages.append(page)
            self.page_locks.append(page_lock)

        # Round-robin counter (lightweight lock only for counter)
        self._next_page_index = 0
        self._selection_lock = threading.Lock()

        # Statistics
        self.lock_timeouts = 0
        self.page_full_retries = 0

    @property
    def size(self) -> int:
        """Total capacity across all pages."""
        return self.page_size * self.num_pages

    @property
    def used_size(self) -> int:
        """Total used size across all pages."""
        return sum(page.used_size for page in self.pages)

    @property
    def commits(self) -> int:
        """Total commits across all pages."""
        return sum(page.commits for page in self.pages)

    @property
    def failed_commits(self) -> int:
        """Total failed commits across all pages."""
        return sum(page.failed_commits for page in self.pages)

    @property
    def reads(self) -> int:
        """Total reads across all pages."""
        return sum(page.reads for page in self.pages)

    @property
    def read_locks(self) -> int:
        """Total read locks (latches) across all pages."""
        return sum(page.read_locks for page in self.pages)

    @property
    def releases(self) -> int:
        """Total releases across all pages."""
        return sum(page.releases for page in self.pages)

    @property
    def l1_compaction(self) -> int:
        """Total L1 compactions across all pages."""
        return sum(page.l1_compaction for page in self.pages)

    @property
    def l2_compaction(self) -> int:
        """Total L2 compactions across all pages."""
        return sum(page.l2_compaction for page in self.pages)

    @property
    def resizes(self) -> int:
        """Total resizes across all pages."""
        return sum(page.resizes for page in self.pages)

    def commit(self, data: Union[bytes, memoryview]) -> int:
        """
        Commit data to pool using round-robin page selection with timeout.

        Tries each page sequentially with lock timeout, moving to next page
        if lock cannot be acquired or page is full.

        Args:
            data: Data to commit (bytes or buffer-compatible object)

        Returns:
            ref_id: Encoded reference (page_id << 48) | local_ref
            -1: All pages exhausted or timed out
        """
        # Get starting page via round-robin
        with self._selection_lock:
            start_page = self._next_page_index
            self._next_page_index = (self._next_page_index + 1) % self.num_pages

        # Try up to num_pages pages
        for attempt in range(self.num_pages):
            page_idx = (start_page + attempt) % self.num_pages
            page = self.pages[page_idx]
            page_lock = self.page_locks[page_idx]

            # Try to acquire lock with timeout
            acquired = page_lock.acquire(timeout=self.lock_timeout_sec)

            if acquired:
                try:
                    # Call existing Cython MemoryPool.commit()
                    local_ref = page.commit(data)

                    if local_ref != -1:
                        # Success - encode ref_id with page index
                        # Upper 16 bits = page_idx, lower 48 bits = local_ref
                        ref_id = (page_idx << 48) | local_ref
                        return ref_id
                    else:
                        # Page full, try next page
                        self.page_full_retries += 1
                finally:
                    page_lock.release()
            else:
                # Lock timeout, try next page
                self.lock_timeouts += 1

        # All pages exhausted or timed out
        return -1

    def read(self, ref_id: int, zero_copy: bool = False, latch: bool = False):
        """
        Read data from pool by decoding ref_id to find page.

        Args:
            ref_id: Reference ID returned by commit()
            zero_copy: If True, return memoryview; if False, return bytes copy
            latch: If True, increment latch count to prevent compaction

        Returns:
            Data as bytes or memoryview

        Raises:
            ValueError: If ref_id is invalid
        """
        # Decode ref_id: upper 16 bits = page_idx, lower 48 bits = local_ref
        page_idx = ref_id >> 48
        local_ref = ref_id & 0xFFFFFFFFFFFF

        if page_idx >= self.num_pages:
            raise ValueError(f"Invalid page index {page_idx} (num_pages={self.num_pages})")

        # Delegate to Cython MemoryPool
        return self.pages[page_idx].read(local_ref, zero_copy=zero_copy, latch=latch)

    def release(self, ref_id: int):
        """
        Release data by decoding ref_id to find page.

        Args:
            ref_id: Reference ID returned by commit()

        Raises:
            ValueError: If ref_id is invalid
        """
        page_idx = ref_id >> 48
        local_ref = ref_id & 0xFFFFFFFFFFFF

        if page_idx >= self.num_pages:
            raise ValueError(f"Invalid page index {page_idx} (num_pages={self.num_pages})")

        # Delegate to Cython MemoryPool
        self.pages[page_idx].release(local_ref)

    def unlatch(self, ref_id: int):
        """
        Unlatch data by decoding ref_id to find page.

        Args:
            ref_id: Reference ID returned by commit()

        Raises:
            ValueError: If ref_id is invalid
        """
        page_idx = ref_id >> 48
        local_ref = ref_id & 0xFFFFFFFFFFFF

        if page_idx >= self.num_pages:
            raise ValueError(f"Invalid page index {page_idx} (num_pages={self.num_pages})")

        # Delegate to Cython MemoryPool
        self.pages[page_idx].unlatch(local_ref)

    def get_stats(self) -> dict:
        """
        Get aggregated statistics across all pages.

        Returns:
            Dictionary with total and per-page statistics
        """
        return {
            "name": self.name,
            "num_pages": self.num_pages,
            "page_size": self.page_size,
            "total_size": self.size,
            "total_used_size": self.used_size,
            "total_free_size": self.size - self.used_size,
            "total_commits": self.commits,
            "total_failed_commits": self.failed_commits,
            "total_reads": self.reads,
            "total_read_locks": self.read_locks,
            "total_releases": self.releases,
            "total_l1_compaction": self.l1_compaction,
            "total_l2_compaction": self.l2_compaction,
            "total_resizes": self.resizes,
            "lock_timeouts": self.lock_timeouts,
            "page_full_retries": self.page_full_retries,
            "per_page": [
                {
                    "name": page.name,
                    "size": page.size,
                    "used_size": page.used_size,
                    "free_size": page.size - page.used_size,
                    "commits": page.commits,
                    "failed_commits": page.failed_commits,
                    "reads": page.reads,
                    "releases": page.releases,
                }
                for page in self.pages
            ],
        }

    def __repr__(self) -> str:
        return (
            f"PagedMemoryPool(name='{self.name}', "
            f"num_pages={self.num_pages}, "
            f"page_size={self.page_size:,}, "
            f"total_size={self.size:,}, "
            f"used_size={self.used_size:,})"
        )
