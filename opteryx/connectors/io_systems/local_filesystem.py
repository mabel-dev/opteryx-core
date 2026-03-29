"""
Local filesystem implementation using Opteryx's optimized I/O.

This implements pyarrow.fs.FileSystem interface but uses memory-mapped files
and stream wrappers for high-performance local file access.
"""

import datetime
import os
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from typing import List
from typing import Tuple

_MAX_PARALLEL_RANGE_READS = 64

# Module-level thread pool for intra-read_ranges parallelism.
# Reused across calls to avoid per-call thread creation/destruction overhead.
_LOCAL_RANGE_POOL: ThreadPoolExecutor = ThreadPoolExecutor(
    max_workers=_MAX_PARALLEL_RANGE_READS,
    thread_name_prefix="local-range",
)


class MemoryMappedFile:
    """
    Wrapper providing file-like interface over memory-mapped files.

    This allows Arrow to treat our optimized memory-mapped files as
    standard file objects while maintaining zero-copy semantics.
    """

    def __init__(self, path: str):
        """Initialize memory-mapped file."""
        from opteryx.compiled.io.disk_reader import read_file_mmap

        self.path = path
        self.mmap_obj = read_file_mmap(path)
        self.memoryview = memoryview(self.mmap_obj)
        self.pos = 0
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        """Read bytes from the file."""
        if self.closed:
            raise ValueError("I/O operation on closed file")

        if size == -1:
            # Read all remaining bytes
            data = bytes(self.memoryview[self.pos :])
            self.pos = len(self.memoryview)
        else:
            end_pos = min(self.pos + size, len(self.memoryview))
            data = bytes(self.memoryview[self.pos : end_pos])
            self.pos = end_pos

        return data

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek to a position in the file."""
        if self.closed:
            raise ValueError("I/O operation on closed file")

        if whence == 0:  # SEEK_SET
            self.pos = offset
        elif whence == 1:  # SEEK_CUR
            self.pos += offset
        elif whence == 2:  # SEEK_END
            self.pos = len(self.memoryview) + offset

        self.pos = max(0, min(self.pos, len(self.memoryview)))
        return self.pos

    def tell(self) -> int:
        """Return current position."""
        if self.closed:
            raise ValueError("I/O operation on closed file")
        return self.pos

    def close(self):
        """Close and cleanup the memory mapping."""
        if not self.closed:
            try:
                # Import and call unmap_memory inside try/except because during
                # interpreter shutdown the import machinery may be torn down
                # (sys.meta_path can be None) which would raise ImportError.
                from opteryx.compiled.io.disk_reader import unmap_memory

                if self.mmap_obj is not None:
                    unmap_memory(self.mmap_obj)
            except Exception:
                # Swallow any exception during cleanup; we're either shutting
                # down or the compiled helper is unavailable. Destructor should
                # never raise.
                pass
            finally:
                self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            # Ensure destructor never propagates exceptions during interpreter
            # shutdown when global state may be partially torn down.
            pass


class OpteryxLocalFileSystem:
    """
    Custom local filesystem using Opteryx's optimized I/O.

    This provides an Arrow-compatible filesystem interface (duck-typed) while using
    Opteryx's memory-view-based readers for optimal performance.
    """

    def __init__(self):
        pass  # No initialization needed

    def get_file_info(self, paths):
        """
        Get info about files/directories.

        Args:
            paths: Single path, list of paths, or FileSelector

        Returns:
            FileInfo or list of FileInfo objects
        """
        from pyarrow.fs import FileInfo
        from pyarrow.fs import FileSelector
        from pyarrow.fs import FileType

        # Handle FileSelector for recursive directory listing
        if isinstance(paths, FileSelector):
            base_dir = paths.base_dir
            recursive = paths.recursive

            infos = []
            if os.path.isdir(base_dir):
                # Try using compiled fast directory listing for better performance,
                # but fall back to Python os.walk/os.listdir if compiled module is not present.
                try:
                    from opteryx.compiled.io.disk_reader import list_directory
                    from opteryx.compiled.io.disk_reader import list_files_info

                    compiled_available = True
                except ImportError:
                    compiled_available = False

                if recursive and compiled_available:
                    entries = list_files_info(base_dir, ())
                    for entry in entries:
                        path, is_dir, is_file, size, mtime = entry
                        if is_file:
                            info = FileInfo(
                                path=path,
                                type=FileType.File,
                                size=size,
                                mtime=datetime.datetime.fromtimestamp(mtime),
                            )
                            infos.append(info)
                elif recursive:
                    # fallback to os.walk
                    for root, dirs, files in os.walk(base_dir):
                        for filename in files:
                            filepath = os.path.join(root, filename)
                            stat = os.stat(filepath)
                            info = FileInfo(
                                path=filepath,
                                type=FileType.File,
                                size=stat.st_size,
                                mtime=datetime.datetime.fromtimestamp(stat.st_mtime),
                            )
                            infos.append(info)
                elif compiled_available:
                    entries = list_directory(base_dir)
                    for name, is_dir, is_file, size, mtime in entries:
                        if is_file:
                            filepath = os.path.join(base_dir, name)
                            info = FileInfo(
                                path=filepath,
                                type=FileType.File,
                                size=size,
                                mtime=datetime.datetime.fromtimestamp(mtime),
                            )
                            infos.append(info)
                else:
                    for item in os.listdir(base_dir):
                        filepath = os.path.join(base_dir, item)
                        if os.path.isfile(filepath):
                            stat = os.stat(filepath)
                            info = FileInfo(
                                path=filepath,
                                type=FileType.File,
                                size=stat.st_size,
                                mtime=datetime.datetime.fromtimestamp(stat.st_mtime),
                            )
                            infos.append(info)
            return infos

        # Handle single path or list of paths
        single_path = isinstance(paths, str)
        if single_path:
            paths = [paths]

        infos = []
        for path in paths:
            if os.path.isfile(path):
                stat = os.stat(path)
                info = FileInfo(
                    path=path,
                    type=FileType.File,
                    size=stat.st_size,
                    mtime=datetime.datetime.fromtimestamp(stat.st_mtime),
                )
            elif os.path.isdir(path):
                info = FileInfo(path=path, type=FileType.Directory)
            else:
                info = FileInfo(path=path, type=FileType.NotFound)
            infos.append(info)

        return infos[0] if single_path else infos

    def read_ranges(self, path: str, ranges: List[Tuple[int, int]]) -> List[bytes]:
        """Read multiple byte ranges from a local file.

        Args:
            path: Absolute or relative path to the local file.
            ranges: List of (offset, length) tuples specifying byte ranges to read.

        Returns:
            List of byte buffers in the same order as ranges.
        """
        if not ranges:
            return []

        # Avoid threadpool overhead for trivial calls or environments without pread().
        if len(ranges) == 1 or not hasattr(os, "pread"):
            result = []
            with open(path, "rb") as f:
                for offset, length in ranges:
                    f.seek(offset)
                    result.append(f.read(length))
            return result

        # Use pread() to read independent ranges from one fd in parallel while
        # preserving output order.
        fd = os.open(path, os.O_RDONLY)
        try:
            worker_count = min(_MAX_PARALLEL_RANGE_READS, len(ranges))
            result: List[bytes] = [b""] * len(ranges)

            def _read_one(idx: int, offset: int, length: int) -> Tuple[int, bytes]:
                return idx, os.pread(fd, length, offset)

            futures = [
                _LOCAL_RANGE_POOL.submit(_read_one, idx, offset, length)
                for idx, (offset, length) in enumerate(ranges)
            ]
            for fut in as_completed(futures):
                idx, chunk = fut.result()
                result[idx] = chunk

            return result
        finally:
            os.close(fd)

    def stream_to(self, path: str, sink, chunk_size: int = 1 << 20) -> int:
        """Stream a local file directly into *sink* without an intermediate buffer.

        Calls ``sink.write(chunk)`` for each chunk read from the file, giving
        callers a zero-copy path when *sink* writes directly into a shared-memory
        slot.

        Args:
            path:       Absolute or relative path to the local file.
            sink:       Any object with a ``write(bytes) -> int`` method.
            chunk_size: Read chunk size in bytes (default 1 MiB).

        Returns:
            Total bytes written to *sink*.
        """
        total = 0
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                sink.write(chunk)
                total += len(chunk)
        return total

    async def async_stream_to(
        self,
        path: str,
        sink,
        http_session=None,
        chunk_size: int = 1 << 20,
    ) -> int:
        """Async variant of ``stream_to`` via ``asyncio.to_thread``.

        Local disk I/O is blocking; this offloads it to a thread so the event
        loop remains responsive.  ``http_session`` is accepted but ignored.
        """
        import asyncio

        return await asyncio.to_thread(self.stream_to, path, sink, chunk_size)

    def open_input_stream(self, path: str, columns=None, filters=None):
        """
        Open a file for reading as a stream.

        Args:
            path: Path to the file
            columns: Not supported on local filesystem
            filters: Not supported on local filesystem

        Returns:
            Stream wrapper backed by memory views
        """
        if columns or filters:
            raise NotImplementedError(
                "Column projection and filtering are not supported for local filesystem reads."
            )
        return MemoryMappedFile(path)

    def open_input_file(self, path: str, columns=None, filters=None):
        """
        Open a file for random access reading.

        Args:
            path: Path to the file
            columns: Not supported on local filesystem
            filters: Not supported on local filesystem

        Returns:
            Random access file object (same as stream for our implementation)
        """
        return MemoryMappedFile(path)
