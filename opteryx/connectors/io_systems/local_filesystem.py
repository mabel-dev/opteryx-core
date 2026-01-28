"""
Local filesystem implementation using Opteryx's optimized I/O.

This implements pyarrow.fs.FileSystem interface but uses memory-mapped files
and stream wrappers for high-performance local file access.
"""

import datetime
import os


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

    def open_input_stream(self, path: str):
        """
        Open a file for reading as a stream.

        Args:
            path: Path to the file

        Returns:
            Stream wrapper backed by memory views
        """
        return MemoryMappedFile(path)

    def open_input_file(self, path: str):
        """
        Open a file for random access reading.

        Args:
            path: Path to the file

        Returns:
            Random access file object (same as stream for our implementation)
        """
        return MemoryMappedFile(path)
