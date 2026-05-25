# cython: language_level=3
# Cython shim for draken.vectors.array_vector — E.24 vtable bridge.

from libc.stdint cimport int32_t, uint8_t

from draken.core.buffers cimport DrakenArrayBuffer
from draken.vectors.string_vector cimport StringVector
from draken.vectors.vector cimport Vector


cdef class ArrayVector(Vector):
    pass


cdef ArrayVector from_sequence(object data):
    raise NotImplementedError("ArrayVector.from_sequence not implemented in E.24 shim")


cdef ArrayVector array_vector_from_parts(
    StringVector flat_child,
    int32_t* offsets,
    uint8_t* list_null_bitmap,
    Py_ssize_t num_rows,
):
    from draken.draken_native import vector_array_from_sequence
    cdef list child_strings = flat_child._nb.to_pylist()
    cdef list result = []
    cdef Py_ssize_t i, start, end
    for i in range(num_rows):
        if list_null_bitmap != NULL:
            if not ((list_null_bitmap[i >> 3] >> (i & 7)) & 1):
                result.append(None)
                continue
        start = offsets[i]
        end = offsets[i + 1]
        result.append(child_strings[start:end])
    return ArrayVector(vector_array_from_sequence(result))
