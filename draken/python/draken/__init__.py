"""
Draken: Modern columnar vector library for Python

Draken provides a high-performance, SIMD-accelerated columnar vector library
designed as an alternative to Apache Arrow. It features:

- Zero-copy, memory-efficient vector types (bool, int, float, string, temporal, array)
- Morsel-based batch processing for columnar operations
- Arrow C Data Interface interoperability
- Native Python integration without external dependencies
- SIMD acceleration (AVX2 on x86_64, NEON on ARM)

Basic usage:
    import draken

    # Create vectors
    v1 = draken.Int64Vector([1, 2, 3, 4, 5])
    v2 = draken.Float64Vector([1.1, 2.2, 3.3, 4.4, 5.5])

    # Create a morsel (batch of vectors)
    morsel = draken.Morsel({"id": v1, "value": v2})

    # Align tables
    aligned = draken.align_tables([morsel])
"""

__version__ = "0.1.0"

# TODO: Import compiled extensions
# from draken.vectors import (
#     Vector, BoolVector, Int8Vector, Int16Vector, Int32Vector, Int64Vector,
#     Float32Vector, Float64Vector, DecimalVector, StringVector,
#     Date32Vector, TimestampVector, TimeVector, IntervalVector,
#     ArrayVector, VectorVector, DictionaryVector
# )
# from draken.morsels import Morsel, align_tables, align_tables_pyarray
# from draken.interop import vector_from_arrow, vector_from_sequence
# from draken.storage import read_morsel, write_morsel

__all__ = [
    # Vector types (to be imported from compiled extensions)
    # "Vector", "BoolVector", "Int8Vector", "Int16Vector", "Int32Vector", "Int64Vector",
    # "Float32Vector", "Float64Vector", "DecimalVector", "StringVector",
    # "Date32Vector", "TimestampVector", "TimeVector", "IntervalVector",
    # "ArrayVector", "VectorVector", "DictionaryVector",
    # "Morsel", "align_tables", "align_tables_pyarray",
    # "vector_from_arrow", "vector_from_sequence",
    # "read_morsel", "write_morsel",
]
