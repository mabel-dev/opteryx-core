# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from opteryx.draken.vectors.string_vector cimport StringVector


cpdef StringVector list_encode_utf8(StringVector vec):
    """
    'Encode to UTF-8' — since StringVector stores UTF-8 bytes natively,
    this is an identity operation.
    """
    return vec
