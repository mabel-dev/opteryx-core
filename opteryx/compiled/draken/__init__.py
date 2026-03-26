"""Draken: Cython/Arrow Interoperability Library.

This package provides efficient columnar data structures and algorithms
with zero-copy interoperability with Apache Arrow. It includes:
- Vector classes for different data types (int64, float64, string, bool)
- Morsel data structures for batch processing
- Arrow integration for seamless data exchange

Main exports:
- Vector: Base vector class for columnar data
- Morsel: Batch data processing container
"""

from opteryx.compiled.draken.morsels.align import align_tables
from opteryx.compiled.draken.morsels.align import align_tables_pyarray
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.compiled.draken.vectors.vector import Vector

__all__ = ("Vector", "Morsel", "align_tables", "align_tables_pyarray")
