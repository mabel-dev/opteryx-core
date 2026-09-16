"""Draken interoperability utilities.

`vector_from_sequence` is the supported Python-list ingestion entry point.
It dispatches by dtype to the typed `draken.draken_native.vector_*_from_sequence`
constructors, and — unlike those bytes-only native builders — encodes `str`
elements to UTF-8 for the VARCHAR/NVARCHAR path at a single ingestion point.
"""

from draken.interop.vector_sequence import vector_from_sequence

__all__ = ["vector_from_sequence"]
