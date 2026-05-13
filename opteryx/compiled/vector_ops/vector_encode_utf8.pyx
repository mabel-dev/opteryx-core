# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

from draken.vectors.string_vector cimport StringVector


cpdef StringVector vector_encode_utf8(StringVector vec):
    """
    'Encode to UTF-8' — since StringVector stores UTF-8 bytes natively,
    this is an identity operation.
    """
    return vec
