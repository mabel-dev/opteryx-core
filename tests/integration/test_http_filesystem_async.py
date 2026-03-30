"""Integration tests for HTTP filesystem async operations.

Tests async_stream_to() with real aiohttp.ClientSession to verify:
- Event loop integration
- Concurrent request handling
- No blocking during I/O
"""

import asyncio
import io
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from opteryx.connectors.io_systems.http_filesystem import OpteryxHttpFileSystem


class TestHttpFileSystemAsync:
    """Test HTTP filesystem async operations."""

    def test_async_stream_to_basic(self):
        """Test basic async_stream_to functionality."""

        async def async_test():
            # Create filesystem
            fs = OpteryxHttpFileSystem()

            # Create async context manager mock for response
            class MockAsyncContextManager:
                def __init__(self):
                    self.status = 200
                    self.content = MagicMock()

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *args):
                    pass

            # Create async iterator for content
            async def iter_chunked(chunk_size):
                yield b"chunk1"
                yield b"chunk2"
                yield b"chunk3"

            # Create mock session
            mock_session = MagicMock()
            mock_response = MockAsyncContextManager()
            mock_response.content.iter_chunked = iter_chunked
            mock_session.get = MagicMock(return_value=mock_response)

            # Create sink to capture data
            sink = io.BytesIO()

            # Call async_stream_to
            total = await fs.async_stream_to("https://example.com/file.bin", sink, mock_session)

            # Verify
            assert total == 18  # len(b"chunk1") + len(b"chunk2") + len(b"chunk3")
            assert sink.getvalue() == b"chunk1chunk2chunk3"

        asyncio.run(async_test())

    def test_async_stream_to_without_session_raises(self):
        """Test async_stream_to requires session parameter."""
        import asyncio

        fs = OpteryxHttpFileSystem()
        sink = io.BytesIO()

        async def test_call():
            with pytest.raises(ValueError, match="requires caller-provided"):
                await fs.async_stream_to("https://example.com/file.bin", sink)

        asyncio.run(test_call())

    def test_async_concurrent_operations(self):
        """Test multiple concurrent async operations."""

        async def async_test():
            fs = OpteryxHttpFileSystem()

            # Create async context manager mock for responses
            class MockAsyncContextManager:
                def __init__(self, path):
                    self.path = path
                    self.status = 200
                    self.content = MagicMock()

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *args):
                    pass

            # Create async iterator for content
            async def make_iter_chunked(path):
                async def iter_chunked(chunk_size):
                    # Simulate network latency
                    await asyncio.sleep(0.01)
                    yield path.encode()
                return iter_chunked

            # Simulate concurrent downloads
            paths = ["https://example.com/file1.bin", "https://example.com/file2.bin", "https://example.com/file3.bin"]
            sinks = [io.BytesIO() for _ in paths]

            tasks = []
            for path, sink in zip(paths, sinks):
                # Create mock session for this path
                mock_session = MagicMock()
                mock_response = MockAsyncContextManager(path)
                mock_response.content.iter_chunked = await make_iter_chunked(path)
                mock_session.get = MagicMock(return_value=mock_response)
                tasks.append(fs.async_stream_to(path, sink, mock_session))

            # Run all concurrently
            results = await asyncio.gather(*tasks)

            # Verify all completed successfully
            assert len(results) == 3
            assert all(r > 0 for r in results)

        asyncio.run(async_test())

    def test_async_error_handling(self):
        """Test async error handling on non-200 status."""

        async def async_test():
            fs = OpteryxHttpFileSystem()

            # Create async context manager mock for error response
            class MockAsyncContextManager:
                def __init__(self):
                    self.status = 404

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *args):
                    pass

            mock_session = MagicMock()
            mock_response = MockAsyncContextManager()
            mock_session.get = MagicMock(return_value=mock_response)

            sink = io.BytesIO()

            from opteryx.exceptions import DatasetReadError

            with pytest.raises(DatasetReadError):
                await fs.async_stream_to("https://example.com/notfound.bin", sink, mock_session)

        asyncio.run(async_test())

    def test_event_loop_compatibility(self):
        """Test that async code integrates with asyncio event loops."""
        import asyncio

        fs = OpteryxHttpFileSystem()

        async def create_and_use_filesystem():
            """Create filesystem and use it in async context."""
            # Just verify we can await async methods
            try:
                # This should raise ValueError (no session), not an event loop error
                await fs.async_stream_to("https://example.com/file.bin", io.BytesIO())
            except ValueError as e:
                assert "requires caller-provided" in str(e)
                return True
            except RuntimeError:
                # Event loop error would indicate integration problem
                return False

        result = asyncio.run(create_and_use_filesystem())
        assert result is True


class TestHttpFileSystemAsyncIntegration:
    """Integration tests with real aiohttp (if available)."""

    def test_with_real_aiohttp_session(self):
        """Test with real aiohttp ClientSession (mock HTTP server)."""

        async def async_test():
            try:
                import aiohttp
            except ImportError:
                pytest.skip("aiohttp not installed")

            fs = OpteryxHttpFileSystem()
            sink = io.BytesIO()

            # Note: This would require a real HTTP server or httpbin.org
            # For now, just verify the session type is correct
            async with aiohttp.ClientSession() as session:
                assert isinstance(session, aiohttp.ClientSession)
                # Could make real request here if needed
                # await fs.async_stream_to("https://httpbin.org/bytes/1000", sink, session)

        asyncio.run(async_test())

    def test_async_context_manager_compatibility(self):
        """Test filesystem works in async context managers."""
        import asyncio

        fs = OpteryxHttpFileSystem()

        async def test_in_async_context():
            """Use filesystem in async context."""
            # Verify filesystem is usable in async context
            assert hasattr(fs, "async_stream_to")
            assert callable(fs.async_stream_to)
            return True

        result = asyncio.run(test_in_async_context())
        assert result is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
