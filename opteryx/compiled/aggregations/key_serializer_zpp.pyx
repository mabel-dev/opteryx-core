# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

"""
Key serialization using zpp_bits header-only C++ library.
Fast binary encoding of Python objects to bytes.
"""

from libcpp.vector cimport vector
from libc.stdint cimport uint8_t


cdef extern from "key_serializer.h" namespace "opteryx::aggregations":
    vector[uint8_t] serialize_key_components_zpp(object py_list) except +


cdef vector[uint8_t] serialize_key_components_vector(list components):
    """
    Serialize a list of key components to vector[uint8_t].

    For C++ use only - returns vector directly without Python conversion.
    Used for storage in Abseil flat_hash_map.
    """
    return serialize_key_components_zpp(components)


cpdef bytes serialize_key_components_fast(list components):
    """
    Serialize a list of key components to bytes using zpp_bits.

    Optimized for aggregation hot loops.
    Handles: None, bool, int, float, str, bytes
    """
    cdef vector[uint8_t] result = serialize_key_components_zpp(components)
    return bytes(result)
