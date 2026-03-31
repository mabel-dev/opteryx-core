"""Unit tests for OpteryxHttpFileSystem."""

import io
import pytest
from unittest.mock import Mock, patch, MagicMock

from opteryx.connectors.io_systems.http_filesystem import (
    OpteryxHttpFileSystem,
    FileInfo,
    FileType,
)
from opteryx.exceptions import DatasetReadError


class TestOpteryxHttpFileSystem:
    """Tests for OpteryxHttpFileSystem."""

    def test_init_creates_http_client(self):
        """Test filesystem initialization creates HTTP client."""
        fs = OpteryxHttpFileSystem()
        assert fs.http_client is not None
        assert fs.base_url == ""

    def test_init_with_base_url(self):
        """Test filesystem initialization with base URL."""
        fs = OpteryxHttpFileSystem(base_url="https://example.com/data/")
        assert fs.base_url == "https://example.com/data"  # trailing slash stripped

    def test_normalize_url_absolute(self):
        """Test URL normalization with absolute URLs."""
        fs = OpteryxHttpFileSystem(base_url="https://example.com/data/")
        url = fs._normalize_url("https://other.com/file.bin")
        assert url == "https://other.com/file.bin"

    def test_normalize_url_relative_with_base(self):
        """Test URL normalization with relative paths and base URL."""
        fs = OpteryxHttpFileSystem(base_url="https://example.com/data")
        url = fs._normalize_url("file.bin")
        assert url == "https://example.com/data/file.bin"

    def test_normalize_url_relative_without_base(self):
        """Test URL normalization fails without base URL."""
        fs = OpteryxHttpFileSystem()
        with pytest.raises(ValueError, match="Invalid HTTP path"):
            fs._normalize_url("file.bin")

    def test_normalize_url_http(self):
        """Test URL normalization with http:// protocol."""
        fs = OpteryxHttpFileSystem()
        url = fs._normalize_url("http://example.com/file.bin")
        assert url == "http://example.com/file.bin"

    def test_get_file_info_single_path(self):
        """Test get_file_info with single path."""
        fs = OpteryxHttpFileSystem()

        # Replace with mock http_client
        fs.http_client = Mock()
        fs.http_client.head = Mock(return_value={"content-length": "1024"})

        info = fs.get_file_info("https://example.com/file.bin")

        assert isinstance(info, FileInfo)
        assert info.type == FileType.File
        assert info.size == 1024

    def test_get_file_info_not_found(self):
        """Test get_file_info when file not found."""
        fs = OpteryxHttpFileSystem()

        # Replace with mock http_client
        fs.http_client = Mock()
        fs.http_client.head = Mock(side_effect=RuntimeError("404 Not Found"))

        info = fs.get_file_info("https://example.com/notfound.bin")

        assert isinstance(info, FileInfo)
        assert info.type == FileType.NotFound

    def test_read_ranges_single_range(self):
        """Test read_ranges with single byte range."""
        fs = OpteryxHttpFileSystem()

        # Replace with mock http_client
        fs.http_client = Mock()
        fs.http_client.get = Mock(return_value=b"test_data")

        result = fs.read_ranges("https://example.com/file.bin", [(0, 9)])

        assert result == [b"test_data"]
        fs.http_client.get.assert_called_once()
        call_args = fs.http_client.get.call_args
        assert "Range" in call_args[1]["headers"]
        assert call_args[1]["headers"]["Range"] == "bytes=0-8"

    def test_read_ranges_multiple_ranges(self):
        """Test read_ranges with multiple byte ranges."""
        fs = OpteryxHttpFileSystem()

        # Replace with mock http_client
        fs.http_client = Mock()
        fs.http_client.get = Mock(return_value=b"chunk")

        ranges = [(0, 5), (100, 5), (200, 5)]
        result = fs.read_ranges("https://example.com/file.bin", ranges)

        assert len(result) == 3
        # All chunks should be b"chunk"
        assert all(chunk == b"chunk" for chunk in result)

    def test_read_ranges_error(self):
        """Test read_ranges raises on HTTP error."""
        fs = OpteryxHttpFileSystem()

        # Replace with mock http_client
        fs.http_client = Mock()
        fs.http_client.get = Mock(side_effect=RuntimeError("HTTP error"))

        with pytest.raises(DatasetReadError):
            fs.read_ranges("https://example.com/file.bin", [(0, 100)])

    def test_stream_to(self):
        """Test stream_to with chunked reading."""
        fs = OpteryxHttpFileSystem()

        # Replace with mock http_client
        full_data = b"chunk1chunk2chunk3"
        fs.http_client = Mock()
        fs.http_client.get = Mock(return_value=full_data)

        sink = Mock()
        sink.write = Mock(return_value=None)

        total = fs.stream_to("https://example.com/file.bin", sink, chunk_size=6)

        assert total == 18  # len(full_data)
        assert sink.write.call_count == 3

    def test_stream_to_error(self):
        """Test stream_to raises on HTTP error."""
        fs = OpteryxHttpFileSystem()

        # Replace with mock http_client
        fs.http_client = Mock()
        fs.http_client.get = Mock(side_effect=RuntimeError("HTTP error"))

        sink = Mock()
        with pytest.raises(DatasetReadError):
            fs.stream_to("https://example.com/file.bin", sink)

    def test_async_stream_to_requires_session(self):
        """Test async_stream_to requires http_session parameter."""
        import asyncio

        fs = OpteryxHttpFileSystem()
        sink = Mock()

        # Test that calling async_stream_to without session raises ValueError
        async def test_async():
            with pytest.raises(ValueError, match="requires caller-provided"):
                await fs.async_stream_to("https://example.com/file.bin", sink)

        asyncio.run(test_async())

    def test_open_input_stream(self):
        """Test open_input_stream returns BytesIO."""
        fs = OpteryxHttpFileSystem()

        # Replace with mock http_client
        fs.http_client = Mock()
        fs.http_client.get = Mock(return_value=b"file_content")

        stream = fs.open_input_stream("https://example.com/file.bin")

        assert hasattr(stream, "memoryview")
        assert bytes(stream.memoryview) == b"file_content"

    def test_open_input_stream_no_projection(self):
        """Test open_input_stream rejects column projection."""
        fs = OpteryxHttpFileSystem()

        with pytest.raises(NotImplementedError, match="Column projection"):
            fs.open_input_stream("https://example.com/file.bin", columns=["col1"])

    def test_open_input_stream_no_filters(self):
        """Test open_input_stream rejects filters."""
        fs = OpteryxHttpFileSystem()

        with pytest.raises(NotImplementedError, match="filtering"):
            fs.open_input_stream("https://example.com/file.bin", filters="col1 > 5")

    def test_open_input_file(self):
        """Test open_input_file returns BytesIO."""
        fs = OpteryxHttpFileSystem()

        # Replace with mock http_client
        fs.http_client = Mock()
        fs.http_client.get = Mock(return_value=b"file_content")

        file_obj = fs.open_input_file("https://example.com/file.bin")

        assert hasattr(file_obj, "memoryview")
        assert bytes(file_obj.memoryview) == b"file_content"

    def test_open_input_stream_error(self):
        """Test open_input_stream raises on HTTP error."""
        fs = OpteryxHttpFileSystem()

        # Replace with mock http_client
        fs.http_client = Mock()
        fs.http_client.get = Mock(side_effect=RuntimeError("HTTP error"))

        with pytest.raises(DatasetReadError):
            fs.open_input_stream("https://example.com/file.bin")
