# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Numeric cosine similarity / distance kernels.

Input must be a VectorVector (FP16 embedding column) with a single broadcast
query vector materialized as float[::1]. The query is widened fp16→fp32 once
and the row loop runs entirely in C via the VectorVector cosine kernel.

Return type is Float32Vector with row-level null propagation.
"""

from libc.math cimport sqrt as c_sqrt
from libc.stddef cimport size_t

from array import array as _pyarray

from draken.vectors.float32_vector cimport Float32Vector
from draken.vectors.vector_vector cimport VectorVector


cdef object _coerce_query_to_fp32_view(object val):
    """Extract a single broadcast query vector as a float[::1] memoryview.

    Returns None when ``val`` doesn't look like a single-row broadcast scalar.
    Accepts: list/tuple of numbers, or a length-1 sequence whose [0] is one.
    """
    if val is None:
        return None
    if hasattr(val, "__len__") and len(val) == 1:
        single = val[0]
        if single is None:
            return None
        try:
            return memoryview(_pyarray("f", single))
        except (TypeError, ValueError):
            return None
    return None


cpdef Float32Vector vector_cosine_similarity(object arr, object val):
    """Cosine similarity over a VectorVector column against a single query.

    ``arr`` must be a VectorVector. ``val`` must be a length-1 sequence
    containing the query vector. Raises TypeError for any other input.

    Returns a Float32Vector of length ``len(arr)`` with row-level null
    propagation. Empty input yields an empty Float32Vector.
    """
    cdef Py_ssize_t n
    if hasattr(arr, "__len__"):
        n = len(arr)
    else:
        n = 0
    if n == 0:
        return Float32Vector(<size_t> 0)

    if not isinstance(arr, VectorVector):
        raise TypeError(
            f"vector_cosine_similarity requires a VectorVector, got {type(arr).__name__}. "
            "Embed your data into a VectorVector column before calling distance kernels."
        )

    query_view = _coerce_query_to_fp32_view(val)
    if query_view is None:
        raise ValueError(
            "val must be a length-1 sequence containing the query vector as a list of floats."
        )

    return (<VectorVector> arr).cosine_similarity(query_view)


cpdef Float32Vector vector_cosine_distance(object arr, object val):
    """Cosine distance = 1 - clip(similarity, -1, 1).

    Wraps vector_cosine_similarity and applies the clip + complement in a
    typed Cython loop. Null rows propagate.
    """
    cdef Float32Vector sims = vector_cosine_similarity(arr, val)
    cdef Py_ssize_t n = sims.ptr.length
    cdef float* data = <float*> sims.ptr.data
    cdef Py_ssize_t i
    cdef float v
    for i in range(n):
        v = data[i]
        if v < -1.0:
            v = -1.0
        elif v > 1.0:
            v = 1.0
        data[i] = 1.0 - v
    return sims
