"""
Compiled I/O operations for high-performance file access.
"""

"""
Compiled I/O operations for high-performance file access.
"""

__all__ = []
try:
    from . import disk_reader as _dr
except Exception:
    _dr = None

if _dr is not None:
    if hasattr(_dr, "read_file"):
        read_file = _dr.read_file
        __all__.append("read_file")
    if hasattr(_dr, "read_file_to_bytes"):
        read_file_to_bytes = _dr.read_file_to_bytes
        __all__.append("read_file_to_bytes")
    if hasattr(_dr, "list_directory"):
        list_directory = _dr.list_directory
        __all__.append("list_directory")
    if hasattr(_dr, "list_files"):
        list_files = _dr.list_files
        __all__.append("list_files")
    if hasattr(_dr, "list_files_info"):
        list_files_info = _dr.list_files_info
        __all__.append("list_files_info")
