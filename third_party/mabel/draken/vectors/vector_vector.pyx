# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
VectorVector: fixed-width numeric vector column for Draken.

This is a specialization of ArrayVector for Arrow FixedSizeList columns with
numeric children. It preserves the vector width as first-class metadata and
round-trips back to Arrow as a FixedSizeListArray instead of degrading to a
generic variable-width list.
"""

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING

from libc.stddef cimport size_t
from libc.stdint cimport int32_t, intptr_t, uint8_t
from libc.stdlib cimport free, malloc

from opteryx.compiled.draken.core.buffers cimport DrakenArrayBuffer, DRAKEN_NON_NATIVE
from opteryx.compiled.draken.interop.arrow cimport arrow_type_to_draken, vector_from_arrow
from opteryx.compiled.draken.vectors.array_vector cimport ArrayVector
from opteryx.compiled.draken.vectors.vector cimport Vector


cdef inline DrakenArrayBuffer* _alloc_array_buffer() except *:
    cdef DrakenArrayBuffer* buf = <DrakenArrayBuffer*> malloc(sizeof(DrakenArrayBuffer))
    if buf == NULL:
        raise MemoryError()
    buf.offsets = NULL
    buf.values = NULL
    buf.null_bitmap = NULL
    buf.length = 0
    buf.value_type = DRAKEN_NON_NATIVE
    return buf


cdef inline void _detach_array_vector(ArrayVector src):
    src.ptr = NULL
    src._child = None
    src.owns_offsets = False
    src.owns_null_bitmap = False
    src._arrow_parent = None
    src._arrow_offsets_buf = None
    src._arrow_null_buf = None
    src._arrow_child_array = None
    src._child_arrow_type = None
    src._child_decode_utf8 = False


cdef VectorVector _wrap_array_vector(ArrayVector src, Py_ssize_t dimensions):
    cdef VectorVector vec = VectorVector.__new__(VectorVector)
    vec.ptr = src.ptr
    vec._child = src._child
    vec.owns_offsets = src.owns_offsets
    vec.owns_null_bitmap = src.owns_null_bitmap
    vec._arrow_parent = src._arrow_parent
    vec._arrow_offsets_buf = src._arrow_offsets_buf
    vec._arrow_null_buf = src._arrow_null_buf
    vec._arrow_child_array = src._arrow_child_array
    vec._child_arrow_type = src._child_arrow_type
    vec._child_decode_utf8 = src._child_decode_utf8
    vec._dimensions = dimensions
    _detach_array_vector(src)
    return vec


cdef class VectorVector(ArrayVector):

    def __cinit__(self):
        self._dimensions = 0

    @property
    def dimensions(self):
        return self._dimensions

    def to_arrow(self):
        if self.ptr == NULL:
            import pyarrow as pa

            return pa.array([], type=pa.list_(pa.null(), self._dimensions))

        if self._child is None:
            raise ValueError("VectorVector child vector is not initialized")

        import pyarrow as pa

        child_arrow = (<Vector> self._child).to_arrow()

        if self._child_decode_utf8 and child_arrow.type == pa.binary():
            try:
                child_arrow = child_arrow.cast(pa.utf8())
            except Exception:
                pass

        if self._child_arrow_type is not None and child_arrow.type != self._child_arrow_type:
            try:
                child_arrow = child_arrow.cast(self._child_arrow_type)
            except Exception:
                pass

        buffers = []
        if self.ptr.null_bitmap != NULL:
            buffers.append(
            pa.foreign_buffer(
                    <intptr_t> self.ptr.null_bitmap,
                    (self.ptr.length + 7) // 8,
                    base=self,
                )
            )
        else:
            buffers.append(None)

        return pa.Array.from_buffers(
            pa.list_(child_arrow.type, self._dimensions),
            self.length,
            buffers,
            children=[child_arrow],
        )

    def take(self, indices):
        if not isinstance(indices, list) and not isinstance(indices, tuple):
            try:
                indices = [int(index) for index in indices]
            except TypeError:
                pass
        cdef ArrayVector taken = <ArrayVector> ArrayVector.take(self, indices)
        return _wrap_array_vector(taken, self._dimensions)

    def __str__(self):
        if self.ptr == NULL:
            return "<VectorVector uninitialized>"
        preview = self.to_pylist()[:10]
        return f"<VectorVector len={self.length} dimensions={self._dimensions} values={preview}>"


cdef VectorVector from_arrow(object array):
    import pyarrow as pa

    cdef object pa_type = array.type
    cdef Py_ssize_t length = len(array)
    cdef Py_ssize_t dimensions
    cdef Py_ssize_t i
    cdef Py_ssize_t offset
    cdef intptr_t null_addr = 0
    cdef Py_ssize_t n_bytes
    cdef bytes new_bitmap
    cdef uint8_t* dst_bitmap
    cdef uint8_t* src_bitmap
    cdef int bit_offset
    cdef Py_ssize_t byte_offset
    cdef int shift_down
    cdef int shift_up
    cdef uint8_t val
    cdef int32_t* offsets_buf
    cdef object child_array
    cdef object bufs
    cdef VectorVector vec = VectorVector()

    if not pa.types.is_fixed_size_list(pa_type):
        raise TypeError("VectorVector requires a FixedSizeList Arrow type")
    if not (pa.types.is_integer(pa_type.value_type) or pa.types.is_floating(pa_type.value_type)):
        raise TypeError("VectorVector requires numeric child values")

    vec.ptr = _alloc_array_buffer()
    vec.ptr.length = <size_t> length
    vec.ptr.value_type = arrow_type_to_draken(pa_type.value_type)
    vec._dimensions = pa_type.list_size

    dimensions = vec._dimensions
    offsets_buf = <int32_t*> malloc((length + 1) * sizeof(int32_t))
    if offsets_buf == NULL:
        raise MemoryError()
    for i in range(length + 1):
        offsets_buf[i] = <int32_t> (i * dimensions)
    vec.ptr.offsets = offsets_buf
    vec.owns_offsets = True

    bufs = array.buffers()
    vec._arrow_parent = array
    vec._arrow_null_buf = bufs[0]
    offset = array.offset

    if bufs[0] is not None:
        null_addr = <intptr_t> bufs[0].address
        if offset % 8 == 0:
            vec.ptr.null_bitmap = <uint8_t*> (null_addr + (offset >> 3))
        else:
            n_bytes = (length + 7) // 8
            new_bitmap = PyBytes_FromStringAndSize(NULL, n_bytes)
            dst_bitmap = <uint8_t*> PyBytes_AS_STRING(new_bitmap)

            byte_offset = offset >> 3
            bit_offset = offset & 7
            src_bitmap = <uint8_t*> null_addr + byte_offset

            shift_down = bit_offset
            shift_up = 8 - bit_offset

            for i in range(n_bytes):
                val = src_bitmap[i] >> shift_down
                val |= (src_bitmap[i + 1] << shift_up)
                dst_bitmap[i] = val

            vec.ptr.null_bitmap = dst_bitmap
            vec._arrow_null_buf = new_bitmap
    else:
        vec.ptr.null_bitmap = NULL

    child_array = array.values
    if offset != 0 or len(child_array) != length * dimensions:
        child_array = child_array.slice(offset * dimensions, length * dimensions)

    vec._arrow_child_array = child_array
    vec._child_arrow_type = pa_type.value_type
    vec._child = vector_from_arrow(child_array)

    return vec
