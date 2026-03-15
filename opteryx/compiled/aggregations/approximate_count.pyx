# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from libc.stdint cimport uint64_t
from libc.stddef cimport size_t
from libc.stdlib cimport malloc, free
from libc.string cimport memset

cimport cython

from opteryx.draken.interop.arrow cimport vector_from_arrow
from opteryx.draken.vectors.vector cimport Vector
from opteryx.third_party.cyan4973.xxhash cimport hash_bytes
import pyarrow
import struct


cdef extern from "hllpp.h":
    cdef cppclass HllppSketch:
        HllppSketch(int precision, size_t explicit_threshold, size_t sparse_threshold) except +
        void add_hash(uint64_t hash)
        void add_hashes(const uint64_t* hashes, size_t count)
        bint merge(const HllppSketch& other) except +
        uint64_t estimate() const


cdef uint64_t _hash_scalar(object value):
    cdef bytes payload
    if value is None:
        return 0
    if isinstance(value, bool):
        return <uint64_t>(1 if value else 0)
    if isinstance(value, int):
        return <uint64_t>(int(value) & 0xFFFFFFFFFFFFFFFF)
    if isinstance(value, float):
        payload = struct.pack("<d", float(value))
        return <uint64_t>int.from_bytes(payload, "little", signed=False)
    if isinstance(value, str):
        return hash_bytes(value.encode("utf-8"))
    if isinstance(value, (bytes, bytearray, memoryview)):
        return hash_bytes(bytes(value))
    return hash_bytes(repr(value).encode("utf-8"))


cdef class ApproximateCountState:
    cdef HllppSketch* _sketch

    def __cinit__(self, int precision=14):
        self._sketch = new HllppSketch(precision, 0, 0)

    def __dealloc__(self):
        if self._sketch != NULL:
            del self._sketch
            self._sketch = NULL

    cpdef void add_hashes(self, object hashes):
        cdef uint64_t[::1] view
        cdef Py_ssize_t n
        if hashes is None:
            return
        try:
            view = hashes
            n = view.shape[0]
            if n > 0:
                self._sketch.add_hashes(&view[0], <size_t>n)
            return
        except Exception:
            pass

        for value in hashes:
            self._sketch.add_hash(<uint64_t>value)

    cpdef void add_value(self, object value):
        if value is None:
            return
        self._sketch.add_hash(_hash_scalar(value))

    cpdef void add_repeated_value(self, object value, Py_ssize_t count):
        cdef Py_ssize_t i
        cdef uint64_t hashed
        if value is None or count <= 0:
            return
        hashed = _hash_scalar(value)
        for i in range(count):
            self._sketch.add_hash(hashed)

    cpdef void update_arrow(self, object column):
        cdef list chunks
        cdef Vector draken_vector
        cdef Py_ssize_t row_count = 0
        cdef Py_ssize_t num_chunks = 0
        cdef Py_ssize_t i
        cdef uint64_t* data_ptr = NULL
        cdef uint64_t[::1] hash_buffer
        cdef Py_ssize_t max_rows = 0

        if column is None:
            return

        if isinstance(column, pyarrow.ChunkedArray):
            chunks = column.chunks
            num_chunks = len(chunks)
        else:
            chunks = [column]
            num_chunks = 1

        for i in range(num_chunks):
            max_rows = max(max_rows, len(chunks[i]))

        if max_rows > 0:
            data_ptr = <uint64_t*>malloc(max_rows * cython.sizeof(uint64_t))
            if data_ptr == NULL:
                raise MemoryError("Failed to allocate hash buffer")

        try:
            for i in range(num_chunks):
                chunk = chunks[i]
                row_count = len(chunk)
                if row_count == 0:
                    continue

                memset(data_ptr, 0, row_count * cython.sizeof(uint64_t))
                hash_buffer = <uint64_t[:row_count]>data_ptr
                draken_vector = <Vector>vector_from_arrow(chunk)
                draken_vector.hash_into(hash_buffer)
                self._sketch.add_hashes(&hash_buffer[0], <size_t>row_count)
        finally:
            if data_ptr != NULL:
                free(data_ptr)

    cpdef void update_draken(self, object vector_or_hashes):
        if vector_or_hashes is None:
            return
        if hasattr(vector_or_hashes, "hash"):
            self.add_hashes(vector_or_hashes.hash())
            return
        self.add_hashes(vector_or_hashes)

    cpdef object estimate(self):
        return self._sketch.estimate()


cpdef object approximate_count(object column, object sketch):
    if sketch is None:
        sketch = ApproximateCountState()
    sketch.update_arrow(column)
    return sketch


cpdef object approximate_count_draken(object column, object sketch):
    if sketch is None:
        sketch = ApproximateCountState()
    sketch.update_draken(column)
    return sketch
