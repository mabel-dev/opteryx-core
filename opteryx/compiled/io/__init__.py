"""
Compiled I/O operations for high-performance file access.
"""

from .disk_reader import list_directory
from .disk_reader import list_files
from .disk_reader import list_files_info
from .disk_reader import read_file
from .disk_reader import read_file_to_bytes
from .disk_reader import read_file_slice
from .disk_reader import read_file_slice_to_bytes
from .disk_reader import read_file_mmap_slice
from .disk_reader import unmap_memory

__all__ = [
    "read_file",
    "read_file_to_bytes",
    "read_file_slice",
    "read_file_slice_to_bytes",
    "read_file_mmap_slice",
    "list_directory",
    "list_files",
    "list_files_info",
    "unmap_memory",
]
