"""Centralized thread pool management for Parquet I/O operations.

Provides C++ thread pool (BS::thread_pool via Cython) with automatic
fallback to Python ThreadPoolExecutor if C++ extension unavailable.

This module manages all global thread pools used across Opteryx:
- Range read pools (local, GCS, HTTP)
- Column decode pools
- Footer prefetch pools
"""

import os
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import logging

logger = logging.getLogger(__name__)

# Try to import C++ thread pool, fall back to Python ThreadPoolExecutor
_USE_CPP_POOL = os.getenv("OPTERYX_USE_CPP_POOL", "auto")

_cpp_thread_pool_available = False
if _USE_CPP_POOL.lower() in ("auto", "1", "true"):
    try:
        from opteryx.compiled.thread_pool import CppThreadPool
        _cpp_thread_pool_available = True
        logger.info("Using C++ BS::thread_pool for parquet I/O (lock-free, work-stealing)")
    except (ImportError, AttributeError) as e:
        logger.debug(f"C++ thread pool unavailable, falling back to ThreadPoolExecutor: {e}")
        _cpp_thread_pool_available = False
else:
    logger.info(f"C++ thread pool disabled via OPTERYX_USE_CPP_POOL={_USE_CPP_POOL}")


def create_thread_pool(
    name: str,
    max_workers: int,
    use_cpp: bool = True,
) -> "ThreadPool":
    """Create a thread pool with C++ backend or Python fallback.

    Args:
        name: Name for the pool (used for logging and thread names)
        max_workers: Maximum number of concurrent workers
        use_cpp: Allow C++ backend (will fallback to Python if unavailable)

    Returns:
        Thread pool instance (either CppThreadPool or ThreadPoolExecutor wrapper)
    """
    if use_cpp and _cpp_thread_pool_available:
        return CppThreadPool(max_workers=max_workers, name=name)

    # Fallback: Python ThreadPoolExecutor
    return PythonThreadPoolWrapper(
        ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=name)
    )


class ThreadPool:
    """Abstract base for thread pool implementations.

    Provides a unified interface for both C++ and Python thread pools.
    """

    def submit(self, fn, *args, **kwargs):
        """Submit a callable to be executed asynchronously.

        Args:
            fn: Callable to execute
            *args: Arguments to fn
            **kwargs: Keyword arguments to fn

        Returns:
            Future-like object (concurrent.futures.Future or CppFuture)
        """
        raise NotImplementedError

    def shutdown(self, wait: bool = True):
        """Shutdown the thread pool and wait for tasks to complete.

        Args:
            wait: If True, wait for all pending tasks to complete
        """
        raise NotImplementedError

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.shutdown(wait=True)
        return False


class PythonThreadPoolWrapper(ThreadPool):
    """Wrapper around Python ThreadPoolExecutor to match ThreadPool interface."""

    def __init__(self, executor: ThreadPoolExecutor):
        """Initialize wrapper.

        Args:
            executor: ThreadPoolExecutor instance
        """
        self.executor = executor

    def submit(self, fn, *args, **kwargs):
        """Submit task to executor."""
        return self.executor.submit(fn, *args, **kwargs)

    def shutdown(self, wait: bool = True):
        """Shutdown the executor."""
        self.executor.shutdown(wait=wait)


class LazyPoolProxy:
    """Proxy that defers to a getter function for thread pool access.

    This allows module-level references to always get the fresh pool from the cache,
    even if the original pool was shut down and recreated (e.g., during testing).
    """

    def __init__(self, getter_fn):
        """Initialize proxy with a getter function.

        Args:
            getter_fn: Callable that returns the current thread pool
        """
        self._getter_fn = getter_fn

    def submit(self, fn, *args, **kwargs):
        """Submit task to the current pool."""
        return self._getter_fn().submit(fn, *args, **kwargs)

    def shutdown(self, wait: bool = True):
        """Shutdown the current pool."""
        return self._getter_fn().shutdown(wait=wait)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.shutdown(wait=True)
        return False

    @property
    def name(self) -> str:
        """Get pool name from current pool."""
        pool = self._getter_fn()
        return getattr(pool, 'name', 'lazy-pool')

    @property
    def max_workers(self) -> int:
        """Get max workers from current pool."""
        pool = self._getter_fn()
        return getattr(pool, 'max_workers', 0)


# Global pool instances (created on demand)
_pools: dict[str, Optional[ThreadPool]] = {}
_pool_lock = None

def _get_or_create_pool(name: str, max_workers: int) -> ThreadPool:
    """Get or create a thread pool by name.

    This provides lazy initialization of global pools and ensures only one
    instance of each named pool exists.

    Args:
        name: Unique pool name (e.g., "parquet-range", "gcs-range")
        max_workers: Maximum workers if creating new pool

    Returns:
        ThreadPool instance
    """
    global _pool_lock
    if _pool_lock is None:
        import threading
        _pool_lock = threading.Lock()

    with _pool_lock:
        if name not in _pools or _pools[name] is None:
            _pools[name] = create_thread_pool(name, max_workers)
            logger.debug(f"Created thread pool '{name}' with {max_workers} workers "
                        f"(backend: {'C++' if _cpp_thread_pool_available else 'Python'})")
        return _pools[name]


def get_decode_pool(max_workers: Optional[int] = None) -> ThreadPool:
    """Get the global column decode pool.

    Args:
        max_workers: Max workers (default: cpu_count from config)

    Returns:
        Shared decode thread pool
    """
    if max_workers is None:
        import os
        max_workers = os.cpu_count() or 4

    return _get_or_create_pool("parquet-decode", max_workers)


def get_range_pool(name: str = "parquet-range", max_workers: int = 32) -> ThreadPool:
    """Get a range read thread pool.

    Args:
        name: Pool name (allows multiple independent pools)
        max_workers: Maximum workers

    Returns:
        Shared range read thread pool
    """
    return _get_or_create_pool(name, max_workers)


def get_footer_pool(max_workers: int = 64) -> ThreadPool:
    """Get the global footer prefetch pool.

    Args:
        max_workers: Maximum workers

    Returns:
        Shared footer prefetch thread pool
    """
    return _get_or_create_pool("parquet-footer", max_workers)


def get_filesystem_pool(protocol: str, max_workers: int) -> ThreadPool:
    """Get a filesystem-specific thread pool.

    Args:
        protocol: Protocol name (e.g., "local", "gcs", "http")
        max_workers: Maximum workers

    Returns:
        Shared filesystem thread pool
    """
    return _get_or_create_pool(f"{protocol}-range", max_workers)


def shutdown_all_pools(wait: bool = True):
    """Shutdown all global thread pools.

    Should be called at application shutdown to clean up resources.

    Args:
        wait: If True, wait for all pending tasks to complete
    """
    global _pool_lock
    if _pool_lock is None:
        return

    with _pool_lock:
        for pool in _pools.values():
            if pool is not None:
                pool.shutdown(wait=wait)
        _pools.clear()

    logger.debug("Shutdown all thread pools")


# Module initialization: log thread pool backend at import time
if __name__ != "__main__":
    backend = "C++ BS::thread_pool" if _cpp_thread_pool_available else "Python ThreadPoolExecutor"
    logger.debug(f"Thread pool backend initialized: {backend}")
