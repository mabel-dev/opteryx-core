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

from draken.morsels.align import align_tables
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector

# Set to True during testing to assert that no draken RLE vectors are
# constructed.  Default is off; toggled in Phase 2 verification.
_RLE_FORBIDDEN = False

__all__ = ("Vector", "Morsel", "align_tables", "_RLE_FORBIDDEN")
