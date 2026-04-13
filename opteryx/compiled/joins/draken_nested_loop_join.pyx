# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
Draken-Native Nested Loop Join

Pure Draken implementation that works entirely with Morsel objects.
No Arrow conversion except at final output alignment (unavoidable integration point).
"""

from libc.stdint cimport int64_t, uint64_t

from opteryx.compiled.structures.buffers cimport IntBuffer, Int32Buffer
from opteryx.compiled.draken.morsels.morsel cimport Morsel


def draken_nested_loop_join(object left_morsel, object right_morsel, list left_columns, list right_columns):
    """
    Perform a nested loop join on Draken Morsels.
    
    Uses native Morsel.hash() to compute row hashes, enabling pure Draken flow.
    No Arrow table conversion, no buffer access patterns - direct hash-based join.
    
    Inputs:
        left_morsel: Left Morsel (build side)
        right_morsel: Right Morsel (probe side)
        left_columns: Column identities for left join keys
        right_columns: Column identities for right join keys
        
    Returns:
        (left_indexes, right_indexes) tuples (as Int32Buffer for alignment)
    """
    cdef Morsel lm = left_morsel
    cdef Morsel rm = right_morsel
    
    if lm is None or rm is None:
        return IntBuffer().to_int32_buffer(), IntBuffer().to_int32_buffer()
    
    cdef Py_ssize_t nl = lm.num_rows
    cdef Py_ssize_t nr = rm.num_rows
    
    if nl == 0 or nr == 0:
        return IntBuffer().to_int32_buffer(), IntBuffer().to_int32_buffer()
    
    # Get hash values for both sides (Draken-native)
    cdef uint64_t[::1] left_hashes = lm.hash(left_columns)
    cdef uint64_t[::1] right_hashes = rm.hash(right_columns)
    
    cdef IntBuffer left_indexes = IntBuffer()
    cdef IntBuffer right_indexes = IntBuffer()
    
    cdef Py_ssize_t i, j
    cdef uint64_t left_hash, right_hash
    
    # Nested loop join: smaller side outer for better cache locality
    if nl <= nr:
        for i in range(nl):
            left_hash = left_hashes[i]
            for j in range(nr):
                if left_hash == right_hashes[j]:
                    left_indexes.append(i)
                    right_indexes.append(j)
    else:
        for j in range(nr):
            right_hash = right_hashes[j]
            for i in range(nl):
                if right_hash == left_hashes[i]:
                    left_indexes.append(i)
                    right_indexes.append(j)
    
    return left_indexes.to_int32_buffer(), right_indexes.to_int32_buffer()
