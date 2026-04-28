# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

from draken.vectors.int64_vector cimport Int64Vector
from draken.vectors.int64_vector cimport from_sequence as int64_from_sequence
from draken.vectors.vector cimport Vector
from draken.interop.vector_sequence cimport vector_from_sequence
from opteryx.compiled.structures.buffers cimport IntBuffer
from opteryx.compiled.structures.buffers cimport ObjectBuffer
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper
from opteryx.third_party.fastfloat.fast_float cimport c_parse_fast_float as parse_fast_float
from libc.stdint cimport int64_t

from libc.stdint cimport int32_t, int64_t, uint64_t, uint8_t, uintptr_t
from cpython.unicode cimport PyUnicode_DecodeUTF8
from cpython.object cimport PyObject_Hash
from cpython.bytes cimport PyBytes_FromStringAndSize

cpdef tuple build_rows_indices_and_column(object column):
    cdef:
        object child_elements = column.values
        list buffers = column.buffers()
        Py_ssize_t row_count = len(column)
        Py_ssize_t total_size
        Py_ssize_t i
        Py_ssize_t j
        Py_ssize_t index_pos
        IntBuffer indices_buf
        ObjectBuffer flat_data_buf
        const int64_t[::1] indices
        Py_ssize_t arr_offset = column.offset
        const int32_t* offsets32 = <const int32_t*><uintptr_t>(buffers[1].address)

        # Child array variables
        Py_ssize_t child_offset = child_elements.offset
        list child_buffers = child_elements.buffers()
        const int32_t* child_offsets32 = <const int32_t*><uintptr_t>(child_buffers[1].address)
        const char* child_data = <const char*><uintptr_t>(child_buffers[2].address)

        const uint8_t* parent_valid = <const uint8_t*><uintptr_t>(buffers[0].address) if buffers[0] else NULL
        const uint8_t* child_valid = <const uint8_t*><uintptr_t>(child_buffers[0].address) if child_buffers[0] else NULL

        Py_ssize_t start
        Py_ssize_t end
        Py_ssize_t str_start
        Py_ssize_t str_end

    if row_count == 0:
        return int64_from_sequence(None), []

    total_size = offsets32[arr_offset + row_count] - offsets32[arr_offset]
    if total_size == 0:
        return int64_from_sequence(None), []

    indices_buf = IntBuffer(total_size)
    flat_data_buf = ObjectBuffer(total_size)

    index_pos = 0
    for i in range(row_count):
        if parent_valid and not (parent_valid[i >> 3] & (1 << (i & 7))):
            continue

        start = offsets32[arr_offset + i]
        end = offsets32[arr_offset + i + 1]

        if start >= end:
            continue

        for j in range(start, end):
            indices_buf.append(i)

            if child_valid and not (child_valid[(child_offset + j) >> 3] & (1 << ((child_offset + j) & 7))):
                flat_data_buf.append(None)
            else:
                str_start = child_offsets32[child_offset + j]
                str_end = child_offsets32[child_offset + j + 1]

                if str_end > str_start:
                    flat_data_buf.append(PyUnicode_DecodeUTF8(
                        child_data + str_start, str_end - str_start, "replace"
                    )
                )
                else:
                    flat_data_buf.append("")
            index_pos += 1

    indices = indices_buf.get_buffer()
    cdef Int64Vector vec = int64_from_sequence(indices)
    vec._arrow_data_buf = indices_buf
    return vec, flat_data_buf.to_list()


cpdef tuple build_filtered_rows_indices_and_column(object column, set valid_values):
    """
    Arrow-native version of build_filtered_rows_indices_and_column.
    Filters values from a ListArray column based on membership in `valid_values`.
    Returns matching row indices and values (as bytes, not str).
    """
    cdef:
        object child_elements = column.values
        list buffers = column.buffers()
        Py_ssize_t row_count = len(column)
        Py_ssize_t arr_offset = column.offset
        const int32_t* offsets32 = <const int32_t*><uintptr_t>(buffers[1].address)
        Py_ssize_t i = 0
        Py_ssize_t j = 0
        Py_ssize_t k = 0
        Py_ssize_t start
        Py_ssize_t end
        Py_ssize_t str_len
        Py_ssize_t str_end
        Py_ssize_t allocated_size = row_count * 4 if row_count > 0 else 4

        list child_buffers = child_elements.buffers()
        const int32_t* child_offsets32 = <const int32_t*><uintptr_t>(child_buffers[1].address)
        const char* child_data = <const char*><uintptr_t>(child_buffers[2].address)
        Py_ssize_t child_offset = child_elements.offset
        const uint8_t* parent_bitmap = NULL
        const uint8_t* child_bitmap = NULL

        Py_ssize_t str_start

    if buffers[0]:
        parent_bitmap = <const uint8_t*><uintptr_t>(buffers[0].address)
    if child_buffers[0]:
        child_bitmap = <const uint8_t*><uintptr_t>(child_buffers[0].address)

    # Normalize valid_values to bytes
    cdef set valid_bytes = set()
    for v in valid_values:
        valid_bytes.add(v.encode("utf8") if isinstance(v, str) else v)

    cdef IntBuffer indices_buf = IntBuffer(allocated_size)
    cdef list flat_list = []

    for i in range(row_count):
        if parent_bitmap is not NULL and not (parent_bitmap[i >> 3] & (1 << (i & 7))):
            continue

        start = offsets32[arr_offset + i]
        end = offsets32[arr_offset + i + 1]

        for j in range(start, end):
            if child_bitmap is not NULL and not (child_bitmap[(child_offset + j) >> 3] & (1 << ((child_offset + j) & 7))):
                continue

            str_start = child_offsets32[child_offset + j]
            str_end = child_offsets32[child_offset + j + 1]
            str_len = str_end - str_start

            # Materialize only matched values as bytes
            value_bytes = PyBytes_FromStringAndSize(child_data + str_start, str_len)

            if value_bytes in valid_bytes:
                indices_buf.append(i)
                flat_list.append(value_bytes)

    k = indices_buf.size()
    if k == 0:
        return int64_from_sequence(None), vector_from_sequence([])

    # Convert IntBuffer to Int64Vector (native path)
    # We must ensure the IntBuffer stays alive to back the memoryview
    cdef const int64_t[::1] mv = indices_buf.get_buffer()
    cdef Int64Vector vec = int64_from_sequence(mv)
    vec._arrow_data_buf = indices_buf  # Anchor the buffer object
    return vec, vector_from_sequence(flat_list)


cpdef tuple list_distinct(Vector values, Int64Vector indices, CarcharSetWrapper seen_hashes=None):
    """
    Filter duplicates from values using hash-based deduplication (Draken-native).

    Args:
        values: Draken Vector of values
        indices: Int64Vector of row indices
        seen_hashes: CarcharSetWrapper to track seen hash values

    Returns:
        tuple of (deduplicated_values, deduplicated_indices_vector, seen_hashes)
    """
    cdef:
        Py_ssize_t i = 0
        Py_ssize_t n = len(values)
        uint64_t hash_value
        object v
        list new_values_list = []
        list new_indices_list = []

    if seen_hashes is None:
        seen_hashes = CarcharSetWrapper()

    for i in range(n):
        v = values[i]
        hash_value = <uint64_t>(PyObject_Hash(v) & 0xFFFFFFFFFFFFFFFF)
        if seen_hashes.insert(hash_value):
            new_values_list.append(v)
            new_indices_list.append(indices[i])

    # Create Draken vectors from collected data
    return vector_from_sequence(new_values_list), vector_from_sequence(new_indices_list), seen_hashes
