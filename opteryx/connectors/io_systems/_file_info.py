"""File metadata container for filesystem operations."""

from __future__ import annotations

__slots__ = ("path", "size", "mtime")


class FileInfoLike:
    __slots__ = ("path", "size", "mtime")

    def __init__(self, path: str, size: int = 0, mtime: float = 0.0):
        self.path = path
        self.size = size
        self.mtime = mtime

    def __repr__(self) -> str:
        return f"FileInfoLike(path={self.path!r}, size={self.size}, mtime={self.mtime})"
