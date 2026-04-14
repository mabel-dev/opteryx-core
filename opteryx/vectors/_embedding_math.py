"""Lightweight math utilities for embeddings without numpy dependency."""

import math
from typing import Sequence


def zeros(shape: int | tuple, dtype="float32") -> list | list[list]:
    """Create a zero-filled array (as lists)."""
    if isinstance(shape, int):
        return [0.0] * shape
    elif isinstance(shape, tuple):
        if len(shape) == 2:
            rows, cols = shape
            return [[0.0] * cols for _ in range(rows)]
    raise ValueError(f"Unsupported shape: {shape}")


def empty(shape: int | tuple, dtype="float32") -> list | list[list]:
    """Create an empty array (as lists). For our purposes, same as zeros."""
    return zeros(shape, dtype)


def asarray(data, dtype="float32") -> list | list[list]:
    """Convert sequence to list (preserving nested structure)."""
    if isinstance(data, list):
        # Check if it's a list of lists (matrix)
        if data and isinstance(data[0], (list, tuple)):
            return [list(row) for row in data]
        return list(data)
    if hasattr(data, '__iter__') and not isinstance(data, (str, bytes)):
        return list(data)
    return [data]


def vstack(arrays: Sequence[Sequence[float]]) -> list[list[float]]:
    """Stack arrays vertically (list of rows)."""
    return [list(row) for row in arrays]


def norm(vector: Sequence[float]) -> float:
    """Calculate L2 norm of a vector."""
    return math.sqrt(sum(x * x for x in vector))


def log1p(x: float) -> float:
    """Compute log(1 + x) accurately for small x."""
    return math.log1p(x)


def dot(a: Sequence[float], b: Sequence[float]) -> float:
    """Compute dot product of two vectors."""
    return sum(x * y for x, y in zip(a, b))


def matmul(matrix: Sequence[Sequence[float]], vector: Sequence[float]) -> list[float]:
    """Matrix-vector multiplication."""
    return [dot(row, vector) for row in matrix]


def arange(n: int, dtype="int64") -> list[int]:
    """Create array of integers from 0 to n-1."""
    return list(range(n))


def argsort(data: Sequence[float], reverse: bool = False) -> list[int]:
    """Return indices that would sort an array."""
    return sorted(range(len(data)), key=lambda i: data[i], reverse=reverse)


def argpartition(data: Sequence[float], k: int) -> list[int]:
    """Partial sort to find k largest elements (returns their indices)."""
    # For our use case, we can use argsort and take top-k
    return argsort(data, reverse=True)[:k]


def any(data: Sequence[bool]) -> bool:
    """Check if any element is True."""
    return any(data)


# Type aliases for compatibility
float32 = float
int64 = int
