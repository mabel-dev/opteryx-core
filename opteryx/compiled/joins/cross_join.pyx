# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy

from opteryx.compiled.draken.vectors.int64_vector cimport Int64Vector
from opteryx.compiled.draken.vectors.int64_vector cimport from_sequence as int64_from_sequence
from opteryx.compiled.draken.interop.arrow cimport vector_from_sequence
from opteryx.compiled.structures.buffers cimport IntBuffer, ObjectBuffer
from opteryx.compiled.structures.carchar_set cimport CarcharSetWrapper
from opteryx.third_party.fastfloat.fast_float cimport c_parse_fast_float as parse_fast_float

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
        return int64_from_sequence(None), numpy.empty(0, dtype=object)

    total_size = offsets32[arr_offset + row_count] - offsets32[arr_offset]
    if total_size == 0:
        return int64_from_sequence(None), numpy.empty(0, dtype=object)

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
    return vec, flat_data_buf.to_numpy()


cpdef tuple numpy_build_rows_indices_and_column(numpy.ndarray column_data, object element_type=None):
    """Build row indices and typed column data from ARRAY column.

    Parameters:
        column_data: numpy.ndarray
            Array of array-like elements to flatten
        element_type: object
            Target type for the flattened data (Arrow/Draken type). If None, inferred from input.

    Returns:
        tuple of (Int64Vector, Draken vector)
            Row indices and flattened typed data
    """
    cdef int64_t row_count = column_data.shape[0]
    cdef int64_t i
    cdef int64_t total_size = 0
    cdef numpy.dtype numpy_element_dtype
    cdef list flat_data_list = []
    cdef IntBuffer indices_buf

    if not isinstance(column_data[0], numpy.ndarray):
        raise TypeError("UNNEST requires an ARRAY column.")

    # Infer element type if not provided
    if element_type is None:
        numpy_element_dtype = column_data[0].dtype
    else:
        numpy_element_dtype = None  # Will use element_type directly

    # Calculate total_size and collect flattened data
    for i in range(row_count):
        array_i = column_data[i]
        if array_i is not None and array_i.size > 0:
            total_size += array_i.shape[0]
            # Extend flat_data list with array elements
            flat_data_list.extend(array_i)

    # Early exit if total_size is zero
    if total_size == 0:
        return (int64_from_sequence(None), vector_from_sequence([], element_type))

    # Build indices buffer (which row each flattened element came from)
    indices_buf = IntBuffer(total_size)
    cdef int64_t idx = 0
    for i in range(row_count):
        array_i = column_data[i]
        if array_i is not None:
            for _ in array_i:
                indices_buf.append(i)
                idx += 1

    # Create typed vector from flattened data
    cdef const int64_t[::1] mv = indices_buf.get_buffer()
    cdef Int64Vector vec = int64_from_sequence(mv)
    vec._arrow_data_buf = indices_buf

    # Create typed vector from collected data
    data_vector = vector_from_sequence(flat_data_list, element_type)
    return (vec, data_vector)


cpdef tuple numpy_build_filtered_rows_indices_and_column(numpy.ndarray column_data, set valid_values, object element_type=None):
    """Build row indices and typed column data for filtered ARRAY column.

    Parameters:
        column_data: numpy.ndarray
            Array of arrays from which to create row indices and flattened data.
        valid_values: set
            A set of values to filter by during the cross join.
        element_type: object
            Target type for the flattened data (Arrow/Draken type). If None, inferred from input.

    Returns:
        tuple of (Int64Vector, Draken vector)
            Row indices and flattened typed data for values in valid_values
    """
    cdef int64_t row_count = column_data.shape[0]
    cdef int64_t index = 0
    cdef int64_t i, j, len_i
    cdef object array_i
    cdef IntBuffer indices_buf
    cdef numpy.dtype numpy_element_dtype = numpy.dtype(object)
    cdef object value
    cdef list flat_data_list = []
    cdef set valid_values_typed = None

    # Determine the dtype of the elements
    for i in range(row_count):
        array_i = column_data[i]
        if array_i is not None and array_i.size > 0:
            numpy_element_dtype = array_i.dtype
            break

    # Initialize indices buffer
    indices_buf = IntBuffer(row_count * 4)  # Estimate initial size

    # Handle set initialization based on element dtype for faster lookups
    if numpy.issubdtype(numpy_element_dtype, numpy.integer):
        valid_values_typed = {int(v) for v in valid_values}
    elif numpy.issubdtype(numpy_element_dtype, numpy.floating):
        valid_values_typed = {parse_fast_float(v) for v in valid_values}
    elif numpy.issubdtype(numpy_element_dtype, numpy.str_):
        valid_values_typed = {unicode(v) for v in valid_values}
    else:
        valid_values_typed = valid_values  # Fallback to generic Python set

    # Main loop: collect filtered data
    for i in range(row_count):
        array_i = column_data[i]
        if array_i is None:
            continue
        len_i = array_i.shape[0]
        if len_i == 0:
            continue

        for j in range(len_i):
            value = array_i[j]
            if value in valid_values_typed:
                flat_data_list.append(value)
                indices_buf.append(i)
                index += 1

    if index == 0:
        return (int64_from_sequence(None), vector_from_sequence([], element_type))

    cdef const int64_t[::1] mv = indices_buf.get_buffer()
    cdef Int64Vector vec = int64_from_sequence(mv)
    vec._arrow_data_buf = indices_buf

    # Create typed vector from collected data
    data_vector = vector_from_sequence(flat_data_list, element_type)
    return (vec, data_vector)


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
        return int64_from_sequence(numpy.empty(0, dtype=numpy.int64)), numpy.empty(0, dtype=object)

    # Convert IntBuffer to Int64Vector (native path)
    # We must ensure the IntBuffer stays alive to back the memoryview
    cdef const int64_t[::1] mv = indices_buf.get_buffer()
    cdef Int64Vector vec = int64_from_sequence(mv)
    vec._arrow_data_buf = indices_buf  # Anchor the buffer object
    return vec, numpy.array(flat_list, dtype=object)


cpdef tuple list_distinct(numpy.ndarray values, int64_t[::1] indices, CarcharSetWrapper seen_hashes=None):
    cdef:
        Py_ssize_t i = 0
        Py_ssize_t j = 0
        Py_ssize_t n = values.shape[0]
        uint64_t hash_value
        object v
        numpy.dtype dtype = values.dtype
        numpy.ndarray new_values = numpy.empty(n, dtype=dtype)
        int64_t[::1] new_indices = numpy.empty(n, dtype=numpy.int64)

    if seen_hashes is None:
        seen_hashes = CarcharSetWrapper()

    for i in range(n):
        v = values[i]
        hash_value = <uint64_t>(PyObject_Hash(v) & 0xFFFFFFFFFFFFFFFF)
        if seen_hashes.insert(hash_value):
            new_values[j] = v
            new_indices[j] = indices[i]
            j += 1

    return new_values[:j], int64_from_sequence(new_indices[:j]), seen_hashes
