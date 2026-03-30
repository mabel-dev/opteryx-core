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

    def test_init_creates_session(self):
        """Test filesystem initialization creates HTTP session."""
        fs = OpteryxHttpFileSystem()
        assert fs.session is not None
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

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"content-length": "1024"}
        fs.session.head = Mock(return_value=mock_response)

        info = fs.get_file_info("https://example.com/file.bin")

        assert isinstance(info, FileInfo)
        assert info.type == FileType.File
        assert info.size == 1024

    def test_get_file_info_not_found(self):
        """Test get_file_info when file not found."""
        fs = OpteryxHttpFileSystem()

        mock_response = Mock()
        mock_response.status_code = 404
        fs.session.head = Mock(return_value=mock_response)

        info = fs.get_file_info("https://example.com/notfound.bin")

        assert isinstance(info, FileInfo)
        assert info.type == FileType.NotFound

    def test_read_ranges_single_range(self):
        """Test read_ranges with single byte range."""
        fs = OpteryxHttpFileSystem()

        mock_response = Mock()
        mock_response.status_code = 206
        mock_response.content = b"test_data"
        fs.session.get = Mock(return_value=mock_response)

        result = fs.read_ranges("https://example.com/file.bin", [(0, 9)])

        assert result == [b"test_data"]
        fs.session.get.assert_called_once()
        call_args = fs.session.get.call_args
        assert "Range" in call_args[1]["headers"]
        assert call_args[1]["headers"]["Range"] == "bytes=0-8"

    def test_read_ranges_multiple_ranges(self):
        """Test read_ranges with multiple byte ranges."""
        fs = OpteryxHttpFileSystem()

        # Mock multiple responses
        responses = [
            Mock(status_code=206, content=b"chunk1"),
            Mock(status_code=206, content=b"chunk2"),
            Mock(status_code=206, content=b"chunk3"),
        ]
        fs.session.get = Mock(side_effect=responses)

        ranges = [(0, 6), (100, 6), (200, 6)]
        result = fs.read_ranges("https://example.com/file.bin", ranges)

        assert len(result) == 3
        # Results may not be in order due to thread pool, but should contain all chunks
        assert set(result) == {b"chunk1", b"chunk2", b"chunk3"}

    def test_read_ranges_error(self):
        """Test read_ranges raises on HTTP error."""
        fs = OpteryxHttpFileSystem()

        mock_response = Mock()
        mock_response.status_code = 404
        fs.session.get = Mock(return_value=mock_response)

        with pytest.raises(DatasetReadError):
            fs.read_ranges("https://example.com/file.bin", [(0, 100)])

    def test_stream_to(self):
        """Test stream_to with chunked reading."""
        fs = OpteryxHttpFileSystem()

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.iter_content = Mock(return_value=[b"chunk1", b"chunk2", b"chunk3"])
        fs.session.get = Mock(return_value=mock_response)

        sink = Mock()
        sink.write = Mock(return_value=None)

        total = fs.stream_to("https://example.com/file.bin", sink)

        assert total == 18  # len(b"chunk1") + len(b"chunk2") + len(b"chunk3")
        assert sink.write.call_count == 3

    def test_stream_to_error(self):
        """Test stream_to raises on HTTP error."""
        fs = OpteryxHttpFileSystem()

        mock_response = Mock()
        mock_response.status_code = 404
        fs.session.get = Mock(return_value=mock_response)

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

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"file_content"
        fs.session.get = Mock(return_value=mock_response)

        stream = fs.open_input_stream("https://example.com/file.bin")

        assert isinstance(stream, io.BytesIO)
        assert stream.read() == b"file_content"

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

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"file_content"
        fs.session.get = Mock(return_value=mock_response)

        file_obj = fs.open_input_file("https://example.com/file.bin")

        assert isinstance(file_obj, io.BytesIO)
        assert file_obj.read() == b"file_content"

    def test_open_input_stream_error(self):
        """Test open_input_stream raises on HTTP error."""
        fs = OpteryxHttpFileSystem()

        mock_response = Mock()
        mock_response.status_code = 404
        fs.session.get = Mock(return_value=mock_response)

        with pytest.raises(DatasetReadError):
            fs.open_input_stream("https://example.com/file.bin")
