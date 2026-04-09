"""Lightweight replacement for pyarrow.fs.FileInfo."""

from __future__ import annotations

__slots__ = ("path", "size")


class FileInfoLike:
    __slots__ = ("path", "size")

    def __init__(self, path: str, size: int = 0):
        self.path = path
        self.size = size

    def __repr__(self) -> str:
        return f"FileInfoLike(path={self.path!r}, size={self.size})"
