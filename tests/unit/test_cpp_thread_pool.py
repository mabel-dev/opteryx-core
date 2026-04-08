"""Unit tests for C++ thread pool implementation.

Tests both CppThreadPool and thread pool manager to ensure correct behavior,
exception handling, and compatibility with ThreadPoolExecutor interface.
"""

import concurrent.futures
import pytest
import time

from opteryx.compiled.thread_pool import CppThreadPool
from opteryx.connectors.parquet_io.thread_pool_manager import (
    create_thread_pool,
    get_decode_pool,
    get_range_pool,
    get_footer_pool,
    get_filesystem_pool,
    shutdown_all_pools,
    PythonThreadPoolWrapper,
    _pools,
)


class TestCppThreadPool:
    """Test CppThreadPool Cython wrapper."""

    def test_pool_creation(self):
        """Test basic pool creation."""
        pool = CppThreadPool(max_workers=4, name="test-pool")
        assert pool.name == "test-pool"
        assert pool.max_workers == 4
        pool.shutdown()

    def test_pool_default_name(self):
        """Test pool creation with default name."""
        pool = CppThreadPool(max_workers=2)
        assert pool.name == "cpp-pool"
        pool.shutdown()

    def test_pool_submit_and_execute(self):
        """Test submitting tasks to pool."""
        pool = CppThreadPool(max_workers=4)

        def add(a, b):
            return a + b

        # Submit task
        future = pool.submit(add, 2, 3)

        # Get result
        result = future.result()
        assert result == 5

        pool.shutdown()

    def test_pool_submit_multiple_tasks(self):
        """Test multiple concurrent tasks."""
        pool = CppThreadPool(max_workers=4)

        def multiply(x, y):
            return x * y

        # Submit multiple tasks
        futures = [pool.submit(multiply, i, i) for i in range(1, 6)]

        # Verify results
        results = [f.result() for f in futures]
        expected = [1, 4, 9, 16, 25]

        assert results == expected
        pool.shutdown()

    def test_pool_submit_with_kwargs(self):
        """Test submitting tasks with keyword arguments."""
        pool = CppThreadPool(max_workers=2)

        def power(base, exponent=2):
            return base ** exponent

        future = pool.submit(power, 2, exponent=3)
        result = future.result()

        assert result == 8  # 2^3
        pool.shutdown()

    def test_pool_context_manager(self):
        """Test pool as context manager."""
        with CppThreadPool(max_workers=2) as pool:
            def square(x):
                return x * x

            future = pool.submit(square, 5)
            assert future.result() == 25

        # Pool should be shutdown after context exit
        # (can't really verify, but should not raise)

    def test_pool_exception_handling(self):
        """Test exception propagation from tasks."""
        pool = CppThreadPool(max_workers=2)

        def failing_task():
            raise ValueError("Test error")

        future = pool.submit(failing_task)

        with pytest.raises(ValueError, match="Test error"):
            future.result()

        pool.shutdown()

    def test_pool_concurrent_execution(self):
        """Test that tasks execute concurrently."""
        pool = CppThreadPool(max_workers=2)

        call_times = []

        def slow_task(task_id):
            call_times.append((task_id, time.time()))
            time.sleep(0.1)
            return task_id

        start = time.time()
        futures = [pool.submit(slow_task, i) for i in range(4)]
        results = [f.result() for f in futures]
        elapsed = time.time() - start

        # With 2 workers and 4 tasks (0.1s each), should take ~0.2s concurrently
        # If sequential, would take ~0.4s
        assert elapsed < 0.35  # Allow some overhead
        assert results == [0, 1, 2, 3]

        pool.shutdown()

    def test_pool_shutdown(self):
        """Test pool shutdown behavior."""
        pool = CppThreadPool(max_workers=2)

        def task():
            return "done"

        future = pool.submit(task)
        result = future.result()
        assert result == "done"

        # Shutdown should complete cleanly
        pool.shutdown(wait=True)


class TestThreadPoolManager:
    """Test thread pool manager."""

    def setup_method(self):
        """Clear pools before each test."""
        shutdown_all_pools(wait=True)
        _pools.clear()

    def teardown_method(self):
        """Cleanup after each test."""
        shutdown_all_pools(wait=True)
        _pools.clear()

    def test_create_thread_pool_cpp(self):
        """Test creating C++ thread pool."""
        pool = create_thread_pool(name="test", max_workers=4, use_cpp=True)
        assert pool is not None
        assert pool.max_workers == 4
        pool.shutdown()

    def test_create_thread_pool_python_fallback(self):
        """Test creating Python thread pool fallback."""
        # Use use_cpp=False to force Python fallback
        pool = create_thread_pool(name="test", max_workers=4, use_cpp=False)
        assert isinstance(pool, PythonThreadPoolWrapper)
        pool.shutdown()

    def test_get_decode_pool(self):
        """Test get_decode_pool creates pool on demand."""
        pool = get_decode_pool(max_workers=8)
        assert pool is not None

        # Second call should return same pool
        pool2 = get_decode_pool(max_workers=8)
        assert pool is pool2

        pool.shutdown()

    def test_get_decode_pool_default_uses_cpu_count_minus_two(self, monkeypatch):
        """Test default decode pool sizing derives from cpu_count()-2."""
        import os

        monkeypatch.setattr(os, "cpu_count", lambda: 8)
        pool = get_decode_pool()
        assert pool.max_workers == 6
        pool.shutdown()

    def test_get_range_pool(self):
        """Test get_range_pool creates named pool."""
        pool1 = get_range_pool(name="range-1", max_workers=32)
        pool2 = get_range_pool(name="range-2", max_workers=64)

        # Different names should get different pools
        assert pool1 is not pool2

        pool1.shutdown()
        pool2.shutdown()

    def test_get_footer_pool(self):
        """Test get_footer_pool."""
        pool = get_footer_pool(max_workers=64)
        assert pool is not None
        pool.shutdown()

    def test_get_filesystem_pool(self):
        """Test get_filesystem_pool for different protocols."""
        gcs_pool = get_filesystem_pool(protocol="gcs", max_workers=128)
        http_pool = get_filesystem_pool(protocol="http", max_workers=96)
        local_pool = get_filesystem_pool(protocol="local", max_workers=64)

        # Different protocols should get different pools
        assert gcs_pool is not http_pool
        assert http_pool is not local_pool

        gcs_pool.shutdown()
        http_pool.shutdown()
        local_pool.shutdown()

    def test_pool_task_execution(self):
        """Test executing tasks via pool manager."""
        pool = get_range_pool(name="test-exec", max_workers=4)

        def task(x):
            return x * 2

        future = pool.submit(task, 21)
        result = future.result()

        assert result == 42
        pool.shutdown()

    def test_shutdown_all_pools(self):
        """Test shutting down all pools."""
        pool1 = get_decode_pool(max_workers=4)
        pool2 = get_range_pool(name="range-test", max_workers=4)
        pool3 = get_footer_pool(max_workers=4)

        # Pools should exist
        assert len(_pools) == 3

        # Shutdown all
        shutdown_all_pools(wait=True)

        # All should be cleared
        assert len(_pools) == 0

    def test_pool_reuse_after_creation(self):
        """Test that pools are reused when requested again."""
        pool1 = get_decode_pool(max_workers=4)

        # Submit task
        future1 = pool1.submit(lambda: 42)
        result1 = future1.result()

        # Get "same" pool again (should be cached)
        pool2 = get_decode_pool(max_workers=4)
        assert pool1 is pool2

        # Should still work
        future2 = pool2.submit(lambda: 99)
        result2 = future2.result()

        assert result1 == 42
        assert result2 == 99

        pool1.shutdown()


class TestPythonThreadPoolWrapper:
    """Test Python fallback wrapper."""

    def test_wrapper_creation(self):
        """Test creating wrapper."""
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        wrapper = PythonThreadPoolWrapper(executor)
        assert wrapper is not None
        wrapper.shutdown()

    def test_wrapper_submit(self):
        """Test submitting tasks via wrapper."""
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        wrapper = PythonThreadPoolWrapper(executor)

        future = wrapper.submit(lambda x: x * 2, 5)
        result = future.result()

        assert result == 10
        wrapper.shutdown()

    def test_wrapper_context_manager(self):
        """Test wrapper as context manager."""
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

        with PythonThreadPoolWrapper(executor) as wrapper:
            future = wrapper.submit(lambda: "done")
            assert future.result() == "done"


class TestThreadPoolConcurrency:
    """Test concurrent execution characteristics."""

    def test_multiple_concurrent_tasks(self):
        """Test that multiple tasks run concurrently."""
        pool = CppThreadPool(max_workers=4)

        results = []
        lock = __import__("threading").Lock()

        def record_time(task_id):
            with lock:
                results.append((task_id, time.time()))
            time.sleep(0.05)
            return task_id

        start = time.time()
        futures = [pool.submit(record_time, i) for i in range(4)]
        task_results = [f.result() for f in futures]
        elapsed = time.time() - start

        # 4 tasks at 50ms each with 4 workers should take ~50ms, not 200ms
        assert elapsed < 0.15
        assert sorted(task_results) == [0, 1, 2, 3]

        pool.shutdown()

    def test_pool_scalability(self):
        """Test pool with varying worker counts."""
        for workers in [1, 2, 4, 8]:
            pool = CppThreadPool(max_workers=workers)

            def task():
                return workers

            futures = [pool.submit(task) for _ in range(workers * 2)]
            results = [f.result() for f in futures]

            assert all(r == workers for r in results)
            pool.shutdown()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
