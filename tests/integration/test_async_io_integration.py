"""Integration tests for async I/O module with parquet reading."""

import asyncio
import pytest
from unittest.mock import MagicMock

from opteryx.connectors.parquet_io.async_io import (
    async_read_column_task,
    async_read_multiple_ranges,
    AsyncIOPool,
)


class TestAsyncReadColumnTask:
    """Test async_read_column_task function."""

    def test_async_read_column_sync_fallback(self):
        """Test sync fallback when filesystem doesn't support async."""

        async def async_test():
            # Create mock filesystem with only sync read_ranges
            mock_fs = MagicMock()
            mock_fs.read_ranges = MagicMock(return_value=(b"test_data",))

            result = await async_read_column_task(
                filesystem=mock_fs,
                path="file.parquet",
                rg_idx=0,
                column_name="col1",
                offset=100,
                length=50,
            )

            # Verify result structure
            assert "raw_bytes" in result
            assert "bytes_fetched" in result
            assert "bytes_requested" in result
            assert "read_ns" in result
            assert "task_total_ns" in result
            assert result["raw_bytes"] == b"test_data"
            assert result["bytes_requested"] == 50

        asyncio.run(async_test())

    def test_async_read_column_without_session(self):
        """Test column read without http_session (uses sync path)."""

        async def async_test():
            mock_fs = MagicMock()
            mock_fs.read_ranges = MagicMock(return_value=(b"data",))

            result = await async_read_column_task(
                filesystem=mock_fs,
                path="gs://bucket/file.parquet",
                rg_idx=1,
                column_name="values",
                offset=0,
                length=100,
                http_session=None,
            )

            assert result["bytes_fetched"] == 4
            mock_fs.read_ranges.assert_called_once()

        asyncio.run(async_test())

    def test_async_read_column_with_queue_timing(self):
        """Test queue wait time calculation."""

        async def async_test():
            import time

            mock_fs = MagicMock()
            mock_fs.read_ranges = MagicMock(return_value=(b"x" * 1000,))

            submitted_ns = time.monotonic_ns() - int(1e7)  # 10ms ago

            result = await async_read_column_task(
                filesystem=mock_fs,
                path="file.parquet",
                rg_idx=0,
                column_name="col",
                offset=0,
                length=1000,
                submitted_ns=submitted_ns,
            )

            assert result["queue_wait_ns"] > 0
            assert result["task_total_ns"] >= result["read_ns"]

        asyncio.run(async_test())


class TestAsyncReadMultipleRanges:
    """Test async_read_multiple_ranges function."""

    def test_empty_ranges(self):
        """Test empty ranges list."""

        async def async_test():
            mock_fs = MagicMock()
            result = await async_read_multiple_ranges(
                filesystem=mock_fs,
                path="file.parquet",
                ranges=[],
            )
            assert result == []

        asyncio.run(async_test())

    def test_single_range(self):
        """Test single range (no async benefit)."""

        async def async_test():
            mock_fs = MagicMock()
            mock_fs.read_ranges = MagicMock(return_value=(b"range1",))

            result = await async_read_multiple_ranges(
                filesystem=mock_fs,
                path="file.parquet",
                ranges=[(0, 6)],
            )

            assert result == [b"range1"]

        asyncio.run(async_test())

    def test_multiple_ranges_sync_fallback(self):
        """Test multiple ranges with sync fallback."""

        async def async_test():
            mock_fs = MagicMock()
            mock_fs.read_ranges = MagicMock(
                return_value=(b"range1", b"range2", b"range3")
            )

            result = await async_read_multiple_ranges(
                filesystem=mock_fs,
                path="file.parquet",
                ranges=[(0, 6), (100, 6), (200, 6)],
            )

            assert len(result) == 3
            mock_fs.read_ranges.assert_called_once()

        asyncio.run(async_test())


class TestAsyncIOPool:
    """Test AsyncIOPool class."""

    def test_pool_initialization(self):
        """Test pool can be created."""
        pool = AsyncIOPool(max_concurrent=32)
        assert pool.max_concurrent == 32
        assert pool.session is None

    def test_pool_requires_aiohttp(self):
        """Test pool requires aiohttp for initialization."""

        async def async_test():
            pool = AsyncIOPool()
            # Should raise if aiohttp not available (or initialize if available)
            try:
                await pool.initialize()
                assert pool.session is not None
                await pool.close()
            except RuntimeError as e:
                assert "aiohttp" in str(e)

        asyncio.run(async_test())

    def test_pool_stats_tracking(self):
        """Test stats collection."""
        pool = AsyncIOPool()
        stats = pool.get_stats()

        assert "total_tasks" in stats
        assert "total_bytes" in stats
        assert "total_time_ns" in stats
        assert "errors" in stats
        assert stats["total_tasks"] == 0

    def test_pool_concurrent_semaphore(self):
        """Test concurrent request limiting via semaphore."""

        async def async_test():
            try:
                pool = AsyncIOPool(max_concurrent=2)
                await pool.initialize()

                # Create mock filesystem
                mock_fs = MagicMock()
                mock_fs.read_ranges = MagicMock(return_value=(b"data",))

                # Submit multiple tasks
                tasks = []
                for i in range(4):
                    task = pool.read_column(
                        filesystem=mock_fs,
                        path=f"file{i}.parquet",
                        rg_idx=i,
                        column_name=f"col{i}",
                        offset=0,
                        length=100,
                    )
                    tasks.append(task)

                # Run all tasks (semaphore should limit to max_concurrent)
                results = await asyncio.gather(*tasks)

                assert len(results) == 4
                assert all(r["bytes_fetched"] == 4 for r in results)

                await pool.close()
            except RuntimeError:
                # aiohttp not available
                pytest.skip("aiohttp not installed")

        asyncio.run(async_test())

    def test_pool_stats_aggregation(self):
        """Test stats are aggregated correctly."""

        async def async_test():
            try:
                pool = AsyncIOPool()
                await pool.initialize()

                mock_fs = MagicMock()
                mock_fs.read_ranges = MagicMock(return_value=(b"x" * 100,))

                # Read a column
                result = await pool.read_column(
                    filesystem=mock_fs,
                    path="file.parquet",
                    rg_idx=0,
                    column_name="col",
                    offset=0,
                    length=100,
                )

                stats = pool.get_stats()
                assert stats["total_tasks"] == 1
                assert stats["total_bytes"] == 100

                await pool.close()
            except RuntimeError:
                pytest.skip("aiohttp not installed")

        asyncio.run(async_test())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
